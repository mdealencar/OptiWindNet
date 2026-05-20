"""Triage the reprocess results in sweep_diffs.jsonl.

Splits length mismatches into those explained by non-default PathFinder limit
parameters (recorded in routeset.misc) and genuine divergences worth a closer
look.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

JSONL = Path(sys.argv[1] if len(sys.argv) > 1 else "sweep_diffs.jsonl")
SRC_DB = Path(sys.argv[2] if len(sys.argv) > 2 else "graphdb.v4.sqlite")
LIMIT_KEYS = (
    "iterations_limit_pfinder",
    "bad_streak_limit_pfinder",
    "traversals_limit_pfinder",
)


def nondefault_limits(src_db: Path) -> dict[int, dict]:
    con = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
    out: dict[int, dict] = {}
    for rid, misc in con.execute("select id, misc from routeset where misc is not null"):
        try:
            d = json.loads(misc)
        except Exception:
            continue
        present = {k: d[k] for k in LIMIT_KEYS if k in d}
        if present:
            out[rid] = present
    con.close()
    return out


def main() -> None:
    records = [json.loads(line) for line in JSONL.open()]
    total = len(records)
    ok = [r for r in records if r.get("ok")]
    errs = [r for r in records if not r.get("ok")]
    matches = [r for r in ok if r["matches"]]
    mism = [r for r in ok if not r["matches"]]
    xings = [r for r in ok if r.get("new_crossings", 0) > 0]
    faild = [r for r in ok if r.get("failed_detours", 0) > 0]

    nd = nondefault_limits(SRC_DB)

    print("=== reprocess triage ===")
    print(f"  total       : {total}")
    print(f"  ok          : {len(ok)}")
    print(f"  errors      : {len(errs)}")
    print(f"  matches     : {len(matches)}  ({100*len(matches)/max(len(ok),1):.4f}% of ok)")
    print(f"  mismatches  : {len(mism)}")
    print(f"  new crossings>0 : {len(xings)}")
    print(f"  failed_detours>0: {len(faild)}")

    mism_nd = [r for r in mism if r["rid"] in nd]
    mism_genuine = [r for r in mism if r["rid"] not in nd]
    print()
    print(f"  mismatches w/ non-default pfinder limits (expected): {len(mism_nd)}")
    for r in sorted(mism_nd, key=lambda r: r["rid"]):
        rel = abs(r["new_length"] - r["old_length"]) / max(abs(r["old_length"]), 1e-9)
        print(
            f"    rid={r['rid']:6d} old={r['old_length']:.6f} new={r['new_length']:.6f}"
            f" rel={rel:.3e} limits={nd[r['rid']]}"
        )
    print()
    print(f"  GENUINE mismatches (default params -> investigate): {len(mism_genuine)}")
    for r in sorted(mism_genuine, key=lambda r: r["rid"]):
        rel = abs(r["new_length"] - r["old_length"]) / max(abs(r["old_length"]), 1e-9)
        print(
            f"    rid={r['rid']:6d} old={r['old_length']:.6f} new={r['new_length']:.6f}"
            f" rel={rel:.3e} branched={r.get('branched')}"
            f" iters={r.get('iterations')}/{r.get('iterations_limit')}"
        )

    if errs:
        print()
        print("  errors:")
        for r in sorted(errs, key=lambda r: r["rid"])[:30]:
            print(f"    rid={r['rid']:6d} {r.get('error')}")


if __name__ == "__main__":
    main()
