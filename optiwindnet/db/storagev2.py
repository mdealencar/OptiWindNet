# SPDX-License-Identifier: MIT
# https://gitlab.windenergy.dtu.dk/TOPFARM/OptiWindNet/

import base64
import io
import json
import os
from collections.abc import Sequence
from functools import partial
from hashlib import sha256
from itertools import chain, pairwise
from socket import getfqdn, gethostname
from typing import Any, Mapping

import awkward as ak
import networkx as nx
import numpy as np

from .modelv2 import ParquetDatabase, create_empty_data
from ..interarraylib import calcload
from ..utils import make_handle

__all__ = ()

PackType = Mapping[str, Any]

_misc_not = {
    'VertexC', 'anglesYhp', 'anglesXhp', 'anglesRank', 'angles', 'd2rootsRank',
    'd2roots', 'name', 'boundary', 'capacity', 'B', 'runtime', 'runtime_unit',
    'edges_fun', 'D', 'DetourC', 'fnT', 'landscape_angle', 'Root',
    'creation_options', 'G_nodeset', 'T', 'non_A_gates', 'funfile', 'funhash',
    'funname', 'diagonals', 'planar', 'has_loads', 'R', 'Subtree', 'handle',
    'non_A_edges', 'max_load', 'fun_fingerprint', 'hull', 'solver_log',
    'length_mismatch_on_db_read', 'gnT', 'C', 'border', 'obstacles',
    'num_diagonals', 'crossings_map', 'tentative', 'method_options',
    'is_normalized', 'norm_scale', 'norm_offset', 'detextra', 'rogue',
    'clone2prime', 'valid', 'path_in_P', 'shortened_contours', 'nonAedges',
    'method', 'num_stunts', 'crossings', 'creator',
    'inter_terminal_clearance_min', 'inter_terminal_clearance_safe',
    'stunts_primes',
}


def _normalize_int_array(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        return value.astype(int).tolist()
    return [int(v) for v in value]


def open_database(filepath: str, create_db: bool = False) -> ParquetDatabase:
    """Open or create the v2 awkward/parquet database file.

    Args:
        filepath: Path to parquet-backed database file.
        create_db: If True, create a new database file when missing.

    Returns:
        ParquetDatabase handle exposing NodeSet/Method/Machine/RouteSet tables.
    """
    filepath = os.path.abspath(os.path.expanduser(filepath))
    if not os.path.exists(filepath) or (create_db and os.path.getsize(filepath) == 0):
        if not create_db:
            raise OSError(f'Database file does not exist: {filepath}')
        data = create_empty_data()
        db = ParquetDatabase(filepath, data)
        db.save()
        return db
    data = ak.from_parquet(filepath).to_list()[0]
    return ParquetDatabase(filepath, data)


def L_from_nodeset(nodeset: object, handle: str | None = None) -> nx.Graph:
    """Translate a NodeSet record into a location graph.

    Args:
        nodeset: NodeSet-like record from the database.
        handle: Optional location handle override.

    Returns:
        NetworkX graph with location geometry and node kinds.
    """
    T = nodeset.T
    R = nodeset.R
    B = nodeset.B
    border = np.array(nodeset.constraint_vertices[: nodeset.constraint_groups[0]])
    name = nodeset.name
    if handle is None:
        handle = make_handle(name if name[0] != '!' else name[1 : name.index('!', 1)])
    L = nx.Graph(
        R=R,
        T=T,
        B=B,
        name=name,
        handle=handle,
        VertexC=np.lib.format.read_array(io.BytesIO(nodeset.VertexC)),
        landscape_angle=nodeset.landscape_angle,
    )
    if len(border) > 0:
        L.graph['border'] = border
    if len(nodeset.constraint_groups) > 1:
        obstacle_idx = np.cumsum(np.array(nodeset.constraint_groups))
        L.graph.update(
            obstacles=[
                np.array(nodeset.constraint_vertices[a:b])
                for a, b in pairwise(obstacle_idx)
            ]
        )
    L.add_nodes_from(((n, {'kind': 'wtg'}) for n in range(T)))
    L.add_nodes_from(((r, {'kind': 'oss'}) for r in range(-R, 0)))
    return L


def G_from_routeset(routeset: object) -> nx.Graph:
    """Translate a RouteSet record into a routeset graph.

    Args:
        routeset: RouteSet-like record from the database.

    Returns:
        NetworkX graph reconstructed from terse edge representation.
    """
    nodeset = routeset.nodes
    R = nodeset.R
    G = L_from_nodeset(nodeset)
    G.graph.update(
        C=routeset.C,
        D=routeset.D,
        handle=routeset.handle,
        capacity=routeset.capacity,
        creator=routeset.creator,
        method=dict(
            solver_name=routeset.method.solver_name,
            timestamp=getattr(routeset.method, 'timestamp', None),
            funname=routeset.method.funname,
            funfile=routeset.method.funfile,
            funhash=routeset.method.funhash,
        ),
        runtime=routeset.runtime,
        method_options=routeset.method.options,
        **(routeset.misc or {}),
    )
    if routeset.detextra is not None:
        G.graph['detextra'] = routeset.detextra
    if routeset.stuntC:
        stuntC = np.lib.format.read_array(io.BytesIO(routeset.stuntC))
        num_stunts = len(stuntC)
        G.graph['num_stunts'] = num_stunts
        G.graph['B'] += num_stunts
        VertexC = G.graph['VertexC']
        G.graph['VertexC'] = np.vstack((VertexC[:-R], stuntC, VertexC[-R:]))
    untersify_to_G(G, terse=np.array(routeset.edges), clone2prime=routeset.clone2prime)
    calc_length = G.size(weight='length')
    if abs(calc_length / routeset.length - 1) > 1e-5:
        G.graph['length_mismatch_on_db_read'] = calc_length - routeset.length
    if routeset.rogue:
        for u, v in zip(routeset.rogue[::2], routeset.rogue[1::2]):
            G[u][v]['kind'] = 'rogue'
    if routeset.tentative:
        for r, n in zip(routeset.tentative[::2], routeset.tentative[1::2]):
            G[r][n]['kind'] = 'tentative'
    return G


def packnodes(G: nx.Graph) -> PackType:
    """Pack a location graph into a NodeSet-compatible payload."""
    R, T, B = (G.graph[k] for k in 'RTB')
    VertexC = G.graph['VertexC']
    num_stunts = G.graph.get('num_stunts')
    if num_stunts:
        B -= num_stunts
        VertexC = np.vstack((VertexC[: T + B], VertexC[-R:]))
    VertexC_npy_io = io.BytesIO()
    np.lib.format.write_array(VertexC_npy_io, VertexC, version=(3, 0))
    VertexC_npy = VertexC_npy_io.getvalue()
    digest = sha256(VertexC_npy).digest()
    if G.name[0] == '!':
        name = G.name + base64.b64encode(digest).decode('ascii')
    else:
        name = G.name
    constraint_vertices = list(chain((G.graph.get('border', ()),), G.graph.get('obstacles', ())))
    return dict(
        T=T,
        R=R,
        B=B,
        name=name,
        VertexC=VertexC_npy,
        constraint_groups=[p.shape[0] for p in constraint_vertices],
        constraint_vertices=np.concatenate(constraint_vertices, dtype=int, casting='unsafe'),
        landscape_angle=G.graph.get('landscape_angle', 0.0),
        digest=digest,
    )


def packmethod(method_options: dict) -> PackType:
    """Pack solver/method metadata into a Method-compatible payload."""
    options = {k: method_options[k] for k in sorted(method_options) if k not in ('fun_fingerprint', 'solver_name')}
    ffprint = method_options['fun_fingerprint']
    digest = sha256(ffprint['funhash'] + json.dumps(options).encode()).digest()
    return dict(digest=digest, solver_name=method_options['solver_name'], options=options, **ffprint)


def add_if_absent(entity: object, pack: PackType) -> bytes:
    """Insert a row if its digest is not present and return digest PK."""
    digest = pack['digest']
    if not entity.exists(digest=digest):
        row = dict(pack)
        if 'constraint_vertices' in row:
            row['constraint_vertices'] = _normalize_int_array(row['constraint_vertices'])
        if 'constraint_groups' in row:
            row['constraint_groups'] = _normalize_int_array(row['constraint_groups'])
        if 'timestamp' in row and hasattr(row['timestamp'], 'isoformat'):
            row['timestamp'] = row['timestamp'].isoformat()
        entity.add(row)
    return digest


def method_from_G(G: nx.Graph, db: ParquetDatabase) -> bytes:
    """Ensure Method exists for graph G and return its digest PK."""
    return add_if_absent(db.Method, packmethod(G.graph['method_options']))


def nodeset_from_G(G: nx.Graph, db: ParquetDatabase) -> bytes:
    """Ensure NodeSet exists for graph G and return its digest PK."""
    return add_if_absent(db.NodeSet, packnodes(G))


def terse_pack_from_G(G: nx.Graph) -> PackType:
    """Pack graph edges to terse directed representation for storage."""
    R, T, B = (G.graph[k] for k in 'RTB')
    C, D = (G.graph.get(k, 0) for k in 'CD')
    terse = np.empty((T + C + D,), dtype=int)
    if not G.graph.get('has_loads'):
        calcload(G)
    for u, v, reverse in G.edges(data='reverse'):
        if reverse is None:
            raise ValueError('reverse must not be None')
        u, v = (u, v) if u < v else (v, u)
        i, target = (u, v) if reverse else (v, u)
        terse[i if i < T else i - B] = target
    terse_pack = dict(edges=terse)
    if C > 0 or D > 0:
        terse_pack['clone2prime'] = G.graph['fnT'][T + B : -R]
    return terse_pack


def untersify_to_G(G: nx.Graph, terse: np.ndarray, clone2prime: list) -> None:
    """Expand terse edge representation and attach weighted edges to G."""
    R, T, B = (G.graph[k] for k in 'RTB')
    C, D = (G.graph.get(k, 0) for k in 'CD')
    VertexC = G.graph['VertexC']
    source = np.arange(len(terse))
    if clone2prime:
        source[T:] += B
        contournodes = range(T + B, T + B + C)
        detournodes = range(T + B + C, T + B + C + D)
        G.add_nodes_from(contournodes, kind='contour')
        G.add_nodes_from(detournodes, kind='detour')
        fnT = np.arange(R + T + B + C + D)
        fnT[T + B : T + B + C + D] = clone2prime
        fnT[-R:] = range(-R, 0)
        G.graph['fnT'] = fnT
        Length = np.hypot(*(VertexC[fnT[terse]] - VertexC[fnT[source]]).T)
    else:
        Length = np.hypot(*(VertexC[terse] - VertexC[source]).T)
    G.add_weighted_edges_from(zip(source.tolist(), terse, Length.tolist()), weight='length')
    if clone2prime:
        for _, _, edgeD in G.edges(contournodes, data=True):
            edgeD['kind'] = 'contour'
        for _, _, edgeD in G.edges(detournodes, data=True):
            edgeD['kind'] = 'detour'
    calcload(G)


def oddtypes_to_serializable(obj):
    if isinstance(obj, (list, tuple)):
        return type(obj)(oddtypes_to_serializable(item) for item in obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.int64, np.int32)):
        return int(obj)
    return obj


def pack_G(G: nx.Graph) -> dict[str, Any]:
    """Pack a routeset graph into a RouteSet-compatible payload."""
    R, T, B = (G.graph[k] for k in 'RTB')
    C, D = (G.graph.get(k, 0) for k in 'CD')
    terse_pack = terse_pack_from_G(G)
    misc = {key: G.graph[key] for key in G.graph.keys() - _misc_not}
    for k, v in misc.items():
        misc[k] = oddtypes_to_serializable(v)
    length = G.size(weight='length')
    handle = G.graph.get('handle') or make_handle(G.graph['name'])
    packed_G = dict(
        R=R, T=T, C=C, D=D, handle=handle, capacity=G.graph['capacity'],
        length=length, creator=G.graph['creator'],
        is_normalized=G.graph.get('is_normalized', False), runtime=G.graph['runtime'],
        num_gates=[len(G[root]) for root in range(-R, 0)], misc=misc, **terse_pack,
    )
    num_stunts = G.graph.get('num_stunts')
    if num_stunts:
        VertexC = G.graph['VertexC']
        stuntC = VertexC[T + B - num_stunts : T + B].copy()
        stuntC_npy_io = io.BytesIO()
        np.lib.format.write_array(stuntC_npy_io, stuntC, version=(3, 0))
        packed_G['stuntC'] = stuntC_npy_io.getvalue()
    if C + D > 0:
        packed_G['clone2prime'] = G.graph['fnT'][-C - D - R : -R].tolist()
    concatenate_tuples = partial(sum, start=())
    for k, fun in (('detextra', None), ('num_diagonals', None), ('valid', None), ('tentative', concatenate_tuples), ('rogue', concatenate_tuples)):
        if k in G.graph:
            packed_G[k] = fun(G.graph[k]) if fun else G.graph[k]
    return packed_G


def store_G(G: nx.Graph, db: ParquetDatabase) -> int:
    """Store graph G as a new RouteSet and return its integer id."""
    packed_G = pack_G(G)
    nodesetID = nodeset_from_G(G, db)
    methodID = method_from_G(G, db)
    machineID = get_machine_pk(db)
    row = dict(packed_G)
    row['id'] = db._next_id('routesets')
    row['nodes'] = nodesetID
    row['method'] = methodID
    row['machine'] = machineID
    row['edges'] = _normalize_int_array(row['edges'])
    row['clone2prime'] = _normalize_int_array(row.get('clone2prime'))
    row['num_gates'] = _normalize_int_array(row.get('num_gates'))
    row['tentative'] = _normalize_int_array(row.get('tentative'))
    row['rogue'] = _normalize_int_array(row.get('rogue'))
    row['misc'] = row.get('misc') or None
    db.RouteSet.add(row)
    db.save()
    return row['id']


def get_machine_pk(db: ParquetDatabase) -> int:
    """Get or create current machine record and return its id."""
    fqdn = getfqdn()
    hostname = gethostname()
    if fqdn == 'localhost':
        machine = hostname
    elif hostname.startswith('n-'):
        machine = fqdn[len(hostname) :]
    else:
        machine = fqdn
    entry = db.Machine.get(name=machine)
    if entry:
        return entry.id
    mid = db._next_id('machines')
    db.Machine.add({'id': mid, 'name': machine, 'attrs': None})
    return mid


def G_by_method(G: nx.Graph, method: object, db: ParquetDatabase) -> nx.Graph:
    """Fetch routeset for location/capacity matching the provided method."""
    farmname = G.name
    c = G.graph['capacity']
    rs = db.RouteSet.get(lambda rs: rs.nodes.name == farmname and rs.method.digest == method.digest and rs.capacity == c)
    Gdb = G_from_routeset(rs)
    calcload(Gdb)
    return Gdb


def Gs_from_attrs(farm: object, methods: object | Sequence[object], capacities: int | Sequence[int], db: ParquetDatabase) -> list[tuple[nx.Graph]]:
    """Fetch tuples of routeset graphs for methods across capacities."""
    Gs = []
    if not isinstance(methods, Sequence):
        methods = (methods,)
    if not isinstance(capacities, Sequence):
        capacities = (capacities,)
    for c in capacities:
        Gtuple = tuple(
            G_from_routeset(db.RouteSet.get(lambda rs: rs.nodes.name == farm.name and rs.method.digest == m.digest and rs.capacity == c))
            for m in methods
        )
        for G in Gtuple:
            calcload(G)
        Gs.append(Gtuple)
    return Gs
