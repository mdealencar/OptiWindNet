"""Re-run PathFinder over routesets and store length mismatches.

The command rebuilds each stored route topology from the source DB, runs a
fresh `PathFinder.create_detours()`, compares the resulting length against the
stored length, writes one JSON record per route, and stores mismatched
routesets in a separate sqlite database for later inspection or promotion.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import logging
import time
import traceback
from pathlib import Path

from joblib import Parallel, delayed

from .lengths import length_matches
from .mesh_cache import DEFAULT_CACHE_DIR, build_PA, load_PA

DEFAULT_SOURCE_DB = Path("graphdb.v4.sqlite")
DEFAULT_OUTPUT_DB = Path("sweep_diffs.sqlite")
DEFAULT_LOG = Path("sweep_diffs.jsonl")


def _branched_from_row(creator: str, options: dict | None) -> bool:
    if creator.startswith("baselines."):
        return False
    if creator.startswith("MILP."):
        topo = (options or {}).get("topology")
        if topo == "branched":
            return True
        if topo == "radial":
            return False
        raise ValueError(f"MILP row with unexpected topology: {topo!r}")
    raise ValueError(f"unrecognized creator: {creator!r}")


def _enumerate_jobs(source_db: Path) -> list[tuple[int, str, bool, float, str]]:
    """Return `(rid, digest_hex, branched, old_length, creator)` jobs."""
    from optiwindnet.db import RouteSet, database_connection

    jobs = []
    with database_connection(str(source_db)):
        q = RouteSet.select(
            RouteSet.id,
            RouteSet.creator,
            RouteSet.length,
            RouteSet.nodes,
            RouteSet.method,
        ).order_by(RouteSet.id)
        for r in q.iterator():
            digest_hex = r.nodes.digest.hex()
            branched = _branched_from_row(r.creator, r.method.options)
            jobs.append((r.id, digest_hex, branched, r.length, r.creator))
    return jobs


def _produce(
    rid: int, branched: bool, old_length: float, source_db: Path, cache_dir: Path
):
    """Run PathFinder on one routeset; carry `G_new` only for mismatches."""
    logging.getLogger().setLevel(logging.CRITICAL)
    from optiwindnet.crossings import find_geometric_crossings
    from optiwindnet.db import G_from_routeset, RouteSet, database_connection
    from optiwindnet.interarraylib import G_from_S, S_from_G
    from optiwindnet.pathfinding import PathFinder

    t0 = time.perf_counter()
    try:
        with database_connection(str(source_db)):
            r = RouteSet.get_by_id(rid)
            digest_hex = r.nodes.digest.hex()
            G_db = G_from_routeset(r)
        S = S_from_G(G_db)
        P, A = load_PA(digest_hex, source_db=source_db, cache_dir=cache_dir)
        G_tent = G_from_S(S, A)

        pf = PathFinder(G_tent, P, A, branched=branched)
        G_new = pf.create_detours()
        new_length = G_new.size(weight="length")
        failed_detours = len(G_new.graph.get("tentative") or [])
        new_crossings = len(find_geometric_crossings(G_new))
        matches = length_matches(new_length, old_length)

        result = dict(
            rid=rid,
            ok=True,
            branched=branched,
            old_length=old_length,
            new_length=new_length,
            new_crossings=new_crossings,
            failed_detours=failed_detours,
            iterations=pf.iterations,
            iterations_limit=pf.iterations_limit,
            matches=matches,
            t=time.perf_counter() - t0,
        )
        if not matches:
            result["G_new"] = G_new
        return result
    except Exception as exc:
        return dict(
            rid=rid,
            ok=False,
            error=f"{type(exc).__name__}: {exc}",
            traceback=traceback.format_exc(),
            t=time.perf_counter() - t0,
        )


def _build_method_options() -> dict:
    from optiwindnet.pathfinding import PathFinder

    path = Path(inspect.getsourcefile(PathFinder.create_detours) or "")
    funhash = hashlib.sha256(path.read_bytes()).digest()
    return dict(
        fun_fingerprint=dict(
            funhash=funhash,
            funname="PathFinder.create_detours",
            funfile="optiwindnet/pathfinding.py",
        ),
        solver_name="pathfinder/database_reprocess",
    )


def _wipe_cache(cache_dir: Path) -> int:
    if not cache_dir.exists():
        return 0
    n = 0
    for path in cache_dir.glob("PA_*.pkl"):
        path.unlink()
        n += 1
    return n


def run(
    *,
    source_db: Path,
    output_db: Path,
    log_path: Path,
    cache_dir: Path,
    jobs_count: int,
    wipe_cache: bool,
) -> None:
    if wipe_cache:
        print(f"wiping PA cache in {cache_dir}...")
        print(f"  removed {_wipe_cache(cache_dir)} entries")
    else:
        n = sum(1 for _ in cache_dir.glob("PA_*.pkl")) if cache_dir.exists() else 0
        print(f"reusing PA cache ({n} entries; pass --wipe-cache to clear)")
    cache_dir.mkdir(parents=True, exist_ok=True)

    print(f"enumerating routesets in {source_db}...")
    jobs = _enumerate_jobs(source_db)
    digests = sorted({j[1] for j in jobs})
    print(f"  {len(jobs)} routesets across {len(digests)} unique digests")

    print(f"pre-warming PA cache for {len(digests)} digests...")
    t0 = time.perf_counter()
    Parallel(n_jobs=jobs_count, backend="loky", verbose=10)(
        delayed(build_PA)(digest, source_db=source_db, cache_dir=cache_dir)
        for digest in digests
    )
    print(f"  done in {time.perf_counter() - t0:.1f}s")

    print(f"running PathFinder over {len(jobs)} routesets...")
    t0 = time.perf_counter()
    results = Parallel(n_jobs=jobs_count, backend="loky", verbose=5)(
        delayed(_produce)(rid, branched, old_length, source_db, cache_dir)
        for rid, _, branched, old_length, _ in jobs
    )
    print(f"  done in {time.perf_counter() - t0:.1f}s")

    n_ok = n_match = n_diff = n_err = n_xings = n_failed = 0
    crossings_rids: list[int] = []
    failed_rids: list[int] = []
    err_rids: list[int] = []

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w") as fh:
        for r in results:
            slim = {k: v for k, v in r.items() if k not in ("G_new", "traceback")}
            fh.write(json.dumps(slim) + "\n")
            if not r.get("ok"):
                n_err += 1
                err_rids.append(r["rid"])
                continue
            n_ok += 1
            if r["matches"]:
                n_match += 1
            else:
                n_diff += 1
            if r.get("new_crossings", 0) > 0:
                n_xings += 1
                crossings_rids.append(r["rid"])
            if r.get("failed_detours", 0) > 0:
                n_failed += 1
                failed_rids.append(r["rid"])

    print(f"persisting {n_diff} mismatched routesets to {output_db}...")
    if output_db.exists():
        output_db.unlink()
    method_options = _build_method_options()
    from optiwindnet.db import database_connection
    from optiwindnet.db.storage import store_G

    written = 0
    t1 = time.perf_counter()
    with database_connection(str(output_db), create_db=True):
        for r in results:
            if not r.get("ok") or r.get("matches"):
                continue
            G = r["G_new"]
            G.graph["method_options"] = method_options
            G.graph["creator"] = "PathFinder/database_reprocess"
            G.graph["runtime"] = 0.0
            G.graph["runtime_unit"] = "s"
            G.graph["orig_rid"] = r["rid"]
            G.graph["orig_length"] = r["old_length"]
            G.graph["new_crossings"] = r["new_crossings"]
            G.graph["failed_detours"] = r["failed_detours"]
            store_G(G)
            written += 1
    print(f"  persisted {written} (dt={time.perf_counter() - t1:.1f}s)")

    print()
    print("=== summary ===")
    print(f"  total      : {len(results)}")
    print(f"  ok         : {n_ok}")
    print(f"  errors     : {n_err}")
    print(f"  matches    : {n_match}")
    print(f"  mismatches : {n_diff}  -> stored in {output_db}")
    if n_xings:
        print(f"  WARN: {n_xings} routesets returned non-zero crossings")
        print(f"        sample rids: {crossings_rids[:20]}")
    if n_failed:
        print(f"  WARN: {n_failed} routesets had failed_detours > 0")
        print(f"        sample rids: {failed_rids[:20]}")
    if n_err:
        print(f"  WARN: {n_err} routesets raised an exception")
        print(f"        sample rids: {err_rids[:20]}")
    print(f"log -> {log_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-run PathFinder over a routeset database and keep length mismatches.",
    )
    parser.add_argument(
        "--source-db",
        type=Path,
        default=DEFAULT_SOURCE_DB,
        help="sqlite database containing the original routesets",
    )
    parser.add_argument(
        "--output-db",
        type=Path,
        default=DEFAULT_OUTPUT_DB,
        help="sqlite database to recreate with mismatched reprocessed routesets",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=DEFAULT_LOG,
        help="JSONL report path; one record is written per source routeset",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help="directory for cached navigation mesh / available-links pickles",
    )
    parser.add_argument(
        "--jobs", type=int, default=12, help="number of parallel joblib workers"
    )
    parser.add_argument(
        "--wipe-cache",
        action="store_true",
        help="delete cached PA_*.pkl files before processing",
    )
    args = parser.parse_args()

    run(
        source_db=args.source_db,
        output_db=args.output_db,
        log_path=args.log,
        cache_dir=args.cache_dir,
        jobs_count=args.jobs,
        wipe_cache=args.wipe_cache,
    )


if __name__ == "__main__":
    main()
