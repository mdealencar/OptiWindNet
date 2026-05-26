"""Compare constructor weigh_detours=False vs True for the branched methods
('biased_EW', 'rootlust') against the MILP-branched optimum.

weigh_detours only takes effect with straight_feeder_route=False (the
constructor forces it off otherwise), so all configs use segmented feeders.
Branched topologies are routed with PathFinder(branched=True).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from pathlib import Path

from joblib import Parallel, delayed

from .analyze_excess import milp_reference
from .mesh_cache import DEFAULT_CACHE_DIR, load_PA

CONSTRUCTOR_EXP_DIR = '/root/docs/aisandbox/ew_constructor_compare'
DEFAULT_SOURCE_DB = Path('docs/notebooks/graphdb.v4.sqlite')
DEFAULT_LOG = Path('/root/docs/aisandbox/ew_constructor_compare/bench_detour.jsonl')

# (label, method, weigh_detours)
CONFIGS = [
    ('biased_EW.wdF', 'biased_EW', False),
    ('biased_EW.wdT', 'biased_EW', True),
    ('rootlust.wdF', 'rootlust', False),
    ('rootlust.wdT', 'rootlust', True),
]


def _produce(digest_hex, cap, ref, source_db, cache_dir):
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    if CONSTRUCTOR_EXP_DIR not in sys.path:
        sys.path.insert(0, CONSTRUCTOR_EXP_DIR)
    from constructor_exp import constructor
    from optiwindnet.interarraylib import G_from_S
    from optiwindnet.pathfinding import PathFinder

    t0 = time.perf_counter()
    try:
        P, A = load_PA(digest_hex, source_db=source_db, cache_dir=cache_dir)
    except Exception as exc:
        return dict(digest=digest_hex, capacity=cap, ok=False,
                    error=f'{type(exc).__name__}: {exc}')
    rec = dict(digest=digest_hex, capacity=cap, ref=ref, ok=True, lengths={})
    for label, method, wd in CONFIGS:
        try:
            S = constructor(A, capacity=cap, method=method,
                            straight_feeder_route=False, weigh_detours=wd)
            G = PathFinder(G_from_S(S, A), P, A, branched=True).create_detours()
            rec['lengths'][label] = float(G.size(weight='length'))
        except Exception:
            rec['lengths'][label] = None
            rec.setdefault('errs', {})[label] = traceback.format_exc().splitlines()[-1]
    rec['t'] = time.perf_counter() - t0
    return rec


def run(*, source_db, log_path, cache_dir, jobs_count, limit):
    ref = milp_reference(source_db, 'branched')
    jobs = sorted(ref.keys())
    if limit:
        jobs = jobs[:limit]
    print(f'branched-MILP instances: {len(jobs)}', flush=True)
    t0 = time.perf_counter()
    results = Parallel(n_jobs=jobs_count, backend='loky', verbose=5)(
        delayed(_produce)(d, c, ref[(d, c)], source_db, cache_dir) for d, c in jobs)
    print(f'  done in {time.perf_counter()-t0:.0f}s', flush=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    n_ok = n_err = 0
    with log_path.open('w') as fh:
        for r in results:
            fh.write(json.dumps(r) + '\n')
            n_ok += int(r.get('ok', False))
            n_err += int(not r.get('ok', False))
    nfail = {lab: sum(1 for r in results if r.get('ok') and r['lengths'].get(lab) is None)
             for lab, _, _ in CONFIGS}
    print(f'\n=== summary ===\n  instances: {len(results)}  ok: {n_ok}  errors: {n_err}')
    print(f'  per-config constructor failures: {nfail}')
    print(f'log -> {log_path}')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-db', type=Path, default=DEFAULT_SOURCE_DB)
    p.add_argument('--log', type=Path, default=DEFAULT_LOG)
    p.add_argument('--cache-dir', type=Path, default=DEFAULT_CACHE_DIR)
    p.add_argument('--jobs', type=int, default=12)
    p.add_argument('--limit', type=int, default=0)
    a = p.parse_args()
    run(source_db=a.source_db, log_path=a.log, cache_dir=a.cache_dir,
        jobs_count=a.jobs, limit=a.limit)


if __name__ == '__main__':
    main()
