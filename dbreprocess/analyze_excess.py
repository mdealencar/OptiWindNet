"""Turn a bench_constructor JSONL into a relative-excess-length distribution.

Excess = 100 * (variant_length - milp_ref) / milp_ref, per problem instance.
Reports the distribution per variant overall and broken down by capacity.
"""

from __future__ import annotations

import json
import statistics as st
import sys
from pathlib import Path

JSONL = Path(sys.argv[1] if len(sys.argv) > 1 else 'bench_constructor.jsonl')
PCTL = (50, 75, 90, 95, 99)


def _quantile(sorted_vals, q):
    if not sorted_vals:
        return float('nan')
    idx = min(len(sorted_vals) - 1, int(round(q / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def _summarize(label, excess_by_variant, variants):
    print(f'\n=== {label}  (n={len(next(iter(excess_by_variant.values()), []))}) ===')
    hdr = f'{"variant":8s} {"mean":>7s} ' + ' '.join(f'p{p:<4d}' for p in PCTL) + f' {"max":>7s} {"<=1%":>6s} {"<=5%":>6s}'
    print(hdr)
    for name in variants:
        e = sorted(excess_by_variant.get(name, []))
        if not e:
            print(f'{name:8s}  (no data)')
            continue
        row = [st.fmean(e)] + [_quantile(e, p) for p in PCTL] + [e[-1]]
        w1 = 100 * sum(1 for x in e if x <= 1.0) / len(e)
        w5 = 100 * sum(1 for x in e if x <= 5.0) / len(e)
        print(f'{name:8s} ' + ' '.join(f'{v:7.2f}' for v in row) + f' {w1:5.1f}% {w5:5.1f}%')


def main() -> None:
    records = [json.loads(line) for line in JSONL.open()]
    ok = [r for r in records if r.get('ok')]
    errs = [r for r in records if not r.get('ok')]
    variants = list(next((r['lengths'] for r in ok), {}).keys())

    def collect(rows):
        out = {v: [] for v in variants}
        for r in rows:
            ref = r['ref']
            for v in variants:
                ln = r['lengths'].get(v)
                if ln is not None and ref:
                    out[v].append(100.0 * (ln - ref) / ref)
        return out

    meth = ok[0].get('method', '?') if ok else '?'
    feed = ok[0].get('feeder', '?') if ok else '?'
    print(f'records={len(records)}  ok={len(ok)}  errors={len(errs)}  '
          f'method={meth}  feeder={feed}')
    if errs:
        print(f'  (first errors: {[r.get("error") for r in errs[:3]]})')

    _summarize('relative excess over MILP — ALL capacities', collect(ok), variants)

    caps = sorted({r['capacity'] for r in ok})
    for cap in caps:
        rows = [r for r in ok if r['capacity'] == cap]
        _summarize(f'capacity {cap}', collect(rows), variants)


if __name__ == '__main__':
    main()
