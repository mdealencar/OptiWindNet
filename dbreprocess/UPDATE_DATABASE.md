# Routeset database maintenance after PathFinder revalidation

This runbook describes two updates to the published routeset database
(`optiwindnet-routesets-r26.05-v4.sqlite`) identified while revalidating
`PathFinder.create_detours()`:

1. storing three strictly-shorter solutions found by parameter tuning, and
2. recording the PathFinder limit parameters needed to reproduce a set of
   stored solutions (so each row becomes self-describing / re-derivable).

It complements the `dbreprocess` tooling in this package.

## Background

Re-running `PathFinder.create_detours()` over all 49,204 routesets with
**default** PathFinder parameters reproduces the stored length for 49,169 of
them (bit-for-bit). The remaining **35** differ — and in every case the
recomputed route also contains geometric crossings, so the two symptoms
coincide exactly (no length-only or crossing-only cases).

None of the 35 are caused by the chain refactor *"drop dead one-end fence
demotion in PathFinder chains"*: the rebase branch and `main` produce
byte-identical output (length, crossings, fence count, `chain_access`) for all
35, and identically across 2,857 chain-stressing routesets (Cazzaro-2022,
Cazzaro-2022G-140/210, Taylor-2023, Yi-2019). The 35 are purely
parameter-tuning artifacts; 20 of the 21 below are capacity-2 instances.

The 35 split into:

- **14** whose `misc` already records non-default limits — re-running with
  those limits reproduces the stored crossing-free solution exactly.
  *No action needed* (listed at the end).
- **21** stored with default-parameter records. All 21 are resolved by
  tuning; they drive the two actions below.

## Action 1 — store improved (shorter) solutions for 3 routesets

Three routesets of location **Rudong H6** (capacity 2, T=100) admit a strictly
shorter, crossing-free route than the stored one, found with
`iterations_limit=80000, bad_streak_limit=8, traversals_limit=15`:

| rid   | creator          | stored length | improved length | Δ        |
|-------|------------------|---------------|-----------------|----------|
| 21285 | baselines.hgs    | 328974.082368 | 328959.581352   | −0.0044% |
| 22178 | MILP.pyomo.cplex | 328974.082368 | 328959.581352   | −0.0044% |
| 34293 | MILP.pyomo.cplex | 328974.082368 | 328959.581352   | −0.0044% |

Re-run PathFinder with those limits, assert the result is crossing-free and
shorter, tag the graph with the parameters, and append it as a new RouteSet
record. `store_G` inserts a new row; the shorter length makes it the new best
for that nodeset (prune the superseded rows per your retention policy).

```python
from optiwindnet.db import (
    RouteSet, NodeSet, database_connection, G_from_routeset, L_from_nodeset,
)
from optiwindnet.interarraylib import G_from_S, S_from_G
from optiwindnet.mesh import make_planar_embedding
from optiwindnet.pathfinding import PathFinder
from optiwindnet.crossings import find_geometric_crossings
from optiwindnet.db.storage import store_G
from dbreprocess.reprocess_routesets import _build_method_options

PARAMS = dict(iterations_limit=80000, bad_streak_limit=8, traversals_limit=15)
IMPROVE = (21285, 22178, 34293)

with database_connection('graphdb.v4.sqlite'):  # writable connection
    method_options = _build_method_options()
    for rid in IMPROVE:
        r = RouteSet.get_by_id(rid)
        branched = (
            r.creator.startswith('MILP.')
            and (r.method.options or {}).get('topology') == 'branched'
        )
        old_len = r.length
        S = S_from_G(G_from_routeset(r))
        L = L_from_nodeset(NodeSet.get(NodeSet.digest == r.nodes.digest))
        P, A = make_planar_embedding(L)
        G = PathFinder(G_from_S(S, A), P, A, branched=branched, **PARAMS).create_detours()
        assert len(find_geometric_crossings(G)) == 0, f'{rid}: crossings remain'
        assert G.size(weight='length') < old_len, f'{rid}: not shorter'
        # extra G.graph keys are persisted into RouteSet.misc by pack_G
        G.graph['method_options'] = method_options
        G.graph['creator'] = 'PathFinder/retuned'
        G.graph['runtime'] = 0.0
        G.graph['runtime_unit'] = 's'
        for k, v in PARAMS.items():
            G.graph[f'{k}_pfinder'] = v
        store_G(G)
```

## Action 2 — record PathFinder parameter metadata

For the other 18 routesets, the current PathFinder reproduces the **stored**
crossing-free solution only with non-default limits. Record those limits in
`misc` (keys `iterations_limit_pfinder`, `bad_streak_limit_pfinder`,
`traversals_limit_pfinder`) so each row is self-describing and re-derivable.

| rids | resolving parameters |
|------|----------------------|
| 408, 5394, 5787, 13409, 16070, 19084, 20556, 32755, 39623, 45964, 46437, 46552, 46634, 46644, 46995, 47239, 48433 (17) | `iterations_limit=80000, bad_streak_limit=8, traversals_limit=15` |
| 47742 (1) | `iterations_limit=180000, bad_streak_limit=15, traversals_limit=30` |

These are *sufficient* values (verified to reproduce the stored route exactly,
crossing-free) — not necessarily minimal. Note that overshooting hurts:
`traversals_limit` ≳ 50 makes the search wander past `iterations_limit` and
reintroduces crossings, so prefer the values above rather than larger ones.

```python
from optiwindnet.db import RouteSet, database_connection

RESOLVING = {
    rid: dict(iterations_limit_pfinder=80000, bad_streak_limit_pfinder=8,
              traversals_limit_pfinder=15)
    for rid in (408, 5394, 5787, 13409, 16070, 19084, 20556, 32755, 39623,
                45964, 46437, 46552, 46634, 46644, 46995, 47239, 48433)
}
RESOLVING[47742] = dict(iterations_limit_pfinder=180000,
                        bad_streak_limit_pfinder=15, traversals_limit_pfinder=30)

with database_connection('graphdb.v4.sqlite'):  # writable connection
    for rid, params in RESOLVING.items():
        r = RouteSet.get_by_id(rid)
        misc = dict(r.misc or {})
        misc.update(params)
        r.misc = misc
        r.save()
```

## Already-correct routesets (no action)

14 routesets already record their non-default limits in `misc` and reproduce
their stored crossing-free solution exactly:
41116, 41862, 42025, 42451, 42569, 42803, 43125, 43566, 44001, 44124, 44413,
44656, 45150, 45507.

## Verification

After applying both actions, reprocess every row passing its recorded
`*_pfinder` limits to `PathFinder`; all 35 should then reproduce their
(possibly updated) stored length, crossing-free. The remaining 49,169 rows are
unaffected and continue to match under default parameters.
