"""Benchmark constructor variants against the per-instance MILP-optimal reference.

Mirrors the structure of ``reprocess_routesets``: enumerate jobs, pre-warm the
PA mesh cache, run in parallel, write one JSONL record per problem instance.

A problem instance is a ``(nodeset, capacity)`` pair. The reference length is
the minimum over all routesets whose ``creator`` starts with ``MILP.`` (the
optimal / near-optimal solutions). For each instance every constructor variant
is run, routed with ``PathFinder.create_detours()`` and its length recorded;
``analyze_excess`` turns the JSONL into a relative-excess-length distribution.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import traceback
from pathlib import Path

from joblib import Parallel, delayed

from .mesh_cache import DEFAULT_CACHE_DIR, build_PA, load_PA

# the constructor under test carries experiment toggles not present in the
# shipped optiwindnet.heuristics.constructor
CONSTRUCTOR_EXP_DIR = '/root/docs/aisandbox/ew_constructor_compare'

DEFAULT_SOURCE_DB = Path('docs/notebooks/graphdb.v4.sqlite')
DEFAULT_LOG = Path('bench_constructor.jsonl')

# experiment-toggle defaults (all off) and the variants layered on top
TOGGLE_DEFAULTS = dict(
    DISABLE_INSERTIONS=False, TIEBREAK_NODE_RANK=False, BLOCK_ONLY_FULL=False,
    RETRY_PARKED=False, DEFER_BLOCK=False, EAGER_CROSS=False, SELECT_NONCROSSING=False,
    NBEW_MODE=False, NO_SUBROOT_CHANGE=False, INSERTION_MARGIN=1.0,
    INSERTION_FALLBACK=False, INSERTION_BIAS=1.0, NBEW_SELECT=False,
    INSERTION_NO_CLOSE=False, INSERTION_PROTECT_BRIDGE=False, SUBROOT_FULLGATE=False,
)
_NSR = dict(NBEW_SELECT=True, TIEBREAK_NODE_RANK=True, NO_SUBROOT_CHANGE=True, RETRY_PARKED=True)
# note: binning (NBEW_SELECT) is only paired with NO_SUBROOT_CHANGE. NBEW's
# weight binning normalizes by min weight and assumes strictly positive weights;
# radial_EW's subroot-change can yield extents <= 0, so binning+subroot-change is
# an incoherent pairing and is intentionally not benchmarked.
RADIAL_VARIANTS = {
    'base':  dict(),
    'nosc':  dict(NO_SUBROOT_CHANGE=True),
    'retry': dict(RETRY_PARKED=True),
    'nsr':   dict(**_NSR),
}
# the experiment toggles are radial_EW-specific; biased_EW just runs as shipped
BIASED_VARIANTS = {'base': dict()}


def variants_for(method: str) -> dict:
    return BIASED_VARIANTS if method == 'biased_EW' else RADIAL_VARIANTS


def _enumerate_jobs(source_db: Path) -> list[tuple[str, int, float]]:
    """Return `(digest_hex, capacity, milp_ref_length)` jobs (MILP-referenced)."""
    import sqlite3
    con = sqlite3.connect(f'file:{source_db}?mode=ro', uri=True)
    rows = con.execute(
        "SELECT nodes_id, capacity, MIN(length) FROM routeset "
        "WHERE creator LIKE 'MILP.%' GROUP BY nodes_id, capacity").fetchall()
    con.close()
    return [(nid.hex(), int(cap), float(ref)) for nid, cap, ref in rows]


def _produce(digest_hex: str, capacity: int, ref: float, *,
             method: str, mode: dict, source_db: Path, cache_dir: Path):
    logging.getLogger().setLevel(logging.CRITICAL)
    if CONSTRUCTOR_EXP_DIR not in sys.path:
        sys.path.insert(0, CONSTRUCTOR_EXP_DIR)
    import constructor_exp
    from constructor_exp import constructor
    from optiwindnet.interarraylib import G_from_S
    from optiwindnet.pathfinding import PathFinder

    t0 = time.perf_counter()
    try:
        P, A = load_PA(digest_hex, source_db=source_db, cache_dir=cache_dir)
    except Exception as exc:
        return dict(digest=digest_hex, capacity=capacity, ok=False,
                    error=f'{type(exc).__name__}: {exc}',
                    traceback=traceback.format_exc())
    rec = dict(digest=digest_hex, capacity=capacity, ref=ref, ok=True, lengths={})
    for name, flags in variants_for(method).items():
        for k, dv in TOGGLE_DEFAULTS.items():
            setattr(constructor_exp, k, flags.get(k, dv))
        try:
            S = constructor(A, capacity=capacity, method=method, **mode)
            G = PathFinder(G_from_S(S, A), P, A, branched=False).create_detours()
            rec['lengths'][name] = float(G.size(weight='length'))
        except Exception:
            rec['lengths'][name] = None
    rec['t'] = time.perf_counter() - t0
    return rec


def run(*, source_db: Path, log_path: Path, cache_dir: Path, jobs_count: int,
        method: str, feeder: str, limit: int) -> None:
    mode = (dict(straight_feeder_route=False, weigh_detours=True) if feeder == 'segmented'
            else dict(straight_feeder_route=True, weigh_detours=False))

    print(f'enumerating MILP-referenced instances in {source_db}...')
    jobs = _enumerate_jobs(source_db)
    if limit:
        jobs = jobs[:limit]
    digests = sorted({d for d, _, _ in jobs})
    print(f'  {len(jobs)} instances across {len(digests)} unique nodesets')

    n = sum(1 for _ in cache_dir.glob('PA_*.pkl')) if cache_dir.exists() else 0
    print(f'pre-warming PA cache ({n} already present)...')
    t0 = time.perf_counter()
    Parallel(n_jobs=jobs_count, backend='loky', verbose=5)(
        delayed(build_PA)(d, source_db=source_db, cache_dir=cache_dir) for d in digests)
    print(f'  done in {time.perf_counter()-t0:.0f}s')

    print(f'running {len(variants_for(method))} variants ({method}, {feeder}) over {len(jobs)} instances...')
    t0 = time.perf_counter()
    results = Parallel(n_jobs=jobs_count, backend='loky', verbose=5)(
        delayed(_produce)(d, c, r, method=method, mode=mode,
                          source_db=source_db, cache_dir=cache_dir)
        for d, c, r in jobs)
    print(f'  done in {time.perf_counter()-t0:.0f}s')

    log_path.parent.mkdir(parents=True, exist_ok=True)
    n_ok = n_err = 0
    with log_path.open('w') as fh:
        for r in results:
            slim = {k: v for k, v in r.items() if k != 'traceback'}
            slim['method'] = method
            slim['feeder'] = feeder
            fh.write(json.dumps(slim) + '\n')
            n_ok += int(r.get('ok', False))
            n_err += int(not r.get('ok', False))
    print(f'\n=== summary ===\n  instances: {len(results)}  ok: {n_ok}  errors: {n_err}')
    print(f'log -> {log_path}\n  analyze with: python -m dbreprocess.analyze_excess {log_path}')


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-db', type=Path, default=DEFAULT_SOURCE_DB)
    p.add_argument('--log', type=Path, default=DEFAULT_LOG)
    p.add_argument('--cache-dir', type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument('--jobs', type=int, default=12)
    p.add_argument('--method', default='radial_EW')
    p.add_argument('--feeder', choices=('segmented', 'straight'), default='segmented')
    p.add_argument('--limit', type=int, default=0, help='cap instances (0=all)')
    a = p.parse_args()
    run(source_db=a.source_db, log_path=a.log, cache_dir=a.cache_dir,
        jobs_count=a.jobs, method=a.method, feeder=a.feeder, limit=a.limit)


if __name__ == '__main__':
    main()
