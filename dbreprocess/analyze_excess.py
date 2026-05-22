"""Turn a bench_constructor JSONL into a relative-excess-length distribution,
referenced against the topology-matched MILP optimum.

Reference per instance (nodeset, capacity) = MIN length over routesets whose
creator starts with 'MILP.' AND whose method options topology matches --topology
('radial' for radial_EW, 'branched' for biased_EW). The reference is applied
here (not baked into the JSONL), so a single saved run can be re-referenced.

  python -m dbreprocess.analyze_excess RUN.jsonl --topology radial
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics as st
from pathlib import Path

PCTL = (50, 75, 90, 95, 99)


def milp_reference(source_db: Path, topology: str) -> dict[tuple[str, int], float]:
    con = sqlite3.connect(f'file:{source_db}?mode=ro', uri=True)
    ref: dict[tuple[str, int], float] = {}
    q = con.execute(
        "SELECT r.nodes_id, r.capacity, r.length FROM routeset r "
        "JOIN method m ON r.method_id = m.digest "
        "WHERE r.creator LIKE 'MILP.%' "
        "AND json_extract(m.options, '$.topology') = ?", (topology,))
    for nid, cap, length in q:
        k = (nid.hex(), int(cap))
        if k not in ref or length < ref[k]:
            ref[k] = float(length)
    con.close()
    return ref


def _quantile(sv, q):
    if not sv:
        return float('nan')
    return sv[min(len(sv) - 1, int(round(q / 100 * (len(sv) - 1))))]


def _summarize(label, excess_by_variant, variants):
    n = len(next(iter(excess_by_variant.values()), []))
    print(f'\n=== {label}  (n={n}) ===')
    print(f'{"variant":8s} {"mean":>7s} ' + ' '.join(f'p{p:<4d}' for p in PCTL)
          + f' {"max":>7s} {"<=1%":>6s} {"<=5%":>6s}')
    for name in variants:
        e = sorted(excess_by_variant.get(name, []))
        if not e:
            print(f'{name:8s}  (no data)'); continue
        row = [st.fmean(e)] + [_quantile(e, p) for p in PCTL] + [e[-1]]
        w1 = 100 * sum(x <= 1.0 for x in e) / len(e)
        w5 = 100 * sum(x <= 5.0 for x in e) / len(e)
        print(f'{name:8s} ' + ' '.join(f'{v:7.2f}' for v in row) + f' {w1:5.1f}% {w5:5.1f}%')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('jsonl', type=Path)
    ap.add_argument('--topology', choices=('radial', 'branched'), required=True)
    ap.add_argument('--source-db', type=Path, default=Path('docs/notebooks/graphdb.v4.sqlite'))
    ap.add_argument('--per-cap', action='store_true', help='also break down by capacity')
    a = ap.parse_args()

    ref = milp_reference(a.source_db, a.topology)
    records = [json.loads(l) for l in a.jsonl.open()]
    ok = [r for r in records if r.get('ok')]
    variants = list(next((r['lengths'] for r in ok), {}).keys())
    meth = ok[0].get('method', '?') if ok else '?'
    feed = ok[0].get('feeder', '?') if ok else '?'

    # keep only instances that have a topology-matched MILP reference
    used = [r for r in ok if (r['digest'], int(r['capacity'])) in ref]
    print(f'records={len(records)} ok={len(ok)} with {a.topology}-MILP ref={len(used)}  '
          f'method={meth} feeder={feed}')

    def collect(rows):
        out = {v: [] for v in variants}
        for r in rows:
            rf = ref[(r['digest'], int(r['capacity']))]
            for v in variants:
                ln = r['lengths'].get(v)
                if ln is not None and rf:
                    out[v].append(100.0 * (ln - rf) / rf)
        return out

    _summarize(f'excess over MILP-{a.topology} — ALL capacities', collect(used), variants)
    if a.per_cap:
        for cap in sorted({int(r['capacity']) for r in used}):
            _summarize(f'capacity {cap}', collect([r for r in used if int(r['capacity']) == cap]), variants)


if __name__ == '__main__':
    main()
