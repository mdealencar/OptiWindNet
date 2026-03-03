# SPDX-License-Identifier: MIT

import argparse
import io
import json
import pickle
import sqlite3
from types import SimpleNamespace

import networkx as nx
import numpy as np

from ..interarraylib import calcload
from .storagev2 import G_from_routeset, open_database, store_G


def _loads_int_array(value):
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, bytes):
        txt = value.decode('utf-8')
        return json.loads(txt)
    return list(value)


def _detect_version(conn: sqlite3.Connection) -> int:
    cols = {row[1] for row in conn.execute('pragma table_info(NodeSet)').fetchall()}
    if 'B' in cols:
        return 2
    if 'handle' in cols:
        return 0
    return 1


def _migrate_v2(conn: sqlite3.Connection, out_db):
    conn.row_factory = sqlite3.Row
    nodes = {r['digest']: r for r in conn.execute('select * from NodeSet')}
    methods = {r['digest']: r for r in conn.execute('select * from Method')}
    for row in conn.execute('select * from RouteSet order by id'):
        nrow = nodes[row['nodes']]
        mrow = methods[row['method']]
        nodeset = SimpleNamespace(
            T=nrow['T'], R=nrow['R'], B=nrow['B'], name=nrow['name'],
            constraint_groups=_loads_int_array(nrow['constraint_groups']),
            constraint_vertices=_loads_int_array(nrow['constraint_vertices']),
            landscape_angle=nrow['landscape_angle'], VertexC=nrow['VertexC'],
        )
        method = SimpleNamespace(
            solver_name=mrow['solver_name'], timestamp=mrow['timestamp'],
            funname=mrow['funname'], funfile=mrow['funfile'],
            funhash=mrow['funhash'], options=json.loads(mrow['options']) if isinstance(mrow['options'], str) else mrow['options'],
        )
        routeset = SimpleNamespace(
            nodes=nodeset, method=method, C=row['C'], D=row['D'], handle=row['handle'],
            capacity=row['capacity'], creator=row['creator'] or 'legacy', runtime=row['runtime'] or 0.0,
            misc=json.loads(row['misc']) if isinstance(row['misc'], str) else (row['misc'] or {}),
            detextra=row['detextra'], stuntC=row['stuntC'],
            edges=_loads_int_array(row['edges']), clone2prime=_loads_int_array(row['clone2prime']),
            length=row['length'], rogue=_loads_int_array(row['rogue']), tentative=_loads_int_array(row['tentative']),
        )
        G = G_from_routeset(routeset)
        G.graph['method_options'] = dict(method.options)
        G.graph['method_options']['solver_name'] = method.solver_name
        G.graph['method_options']['fun_fingerprint'] = {
            'funname': method.funname,
            'funfile': method.funfile,
            'funhash': method.funhash or b'',
        }
        store_G(G, out_db)


def _migrate_v1(conn: sqlite3.Connection, out_db):
    conn.row_factory = sqlite3.Row
    nodes = {r['digest']: r for r in conn.execute('select * from NodeSet')}
    methods = {r['digest']: r for r in conn.execute('select * from Method')}
    for row in conn.execute('select * from EdgeSet order by id'):
        nrow = nodes[row['nodes']]
        mrow = methods[row['method']]
        VertexC = pickle.loads(nrow['VertexC'])
        T, R = nrow['T'], nrow['R']
        G = nx.Graph(name=nrow['name'], T=T, R=R, B=0, VertexC=VertexC)
        G.add_nodes_from(((n, {'kind': 'wtg'}) for n in range(T)))
        G.add_nodes_from(((r, {'kind': 'oss'}) for r in range(-R, 0)))
        edges = _loads_int_array(row['edges'])
        clone2prime = _loads_int_array(row['clone2prime'])
        if clone2prime:
            D = len(clone2prime)
            G.graph['D'] = D
            G.add_nodes_from(((s, {'kind': 'detour'}) for s in range(T, T + D)))
            fnT = np.arange(T + D + R)
            fnT[T:T + D] = clone2prime
            fnT[-R:] = range(-R, 0)
            G.graph['fnT'] = fnT
            allc = np.vstack((VertexC[:T], VertexC[clone2prime], VertexC[-R:]))
            src = range(T + D)
        else:
            allc = VertexC
            src = range(T)
        lengths = np.hypot(*(allc[list(src)] - allc[edges]).T)
        G.add_weighted_edges_from(zip(src, edges, lengths), weight='length')
        G.graph.update(
            handle=row['handle'], capacity=row['capacity'], creator='legacy-v1', runtime=row['runtime'] or 0.0,
            method_options={
                'solver_name': 'legacy',
                **(json.loads(mrow['options']) if isinstance(mrow['options'], str) else (mrow['options'] or {})),
                'fun_fingerprint': {
                    'funname': mrow['funname'], 'funfile': mrow['funfile'], 'funhash': mrow['funhash'] or b'',
                },
            },
        )
        misc = json.loads(row['misc']) if isinstance(row['misc'], str) else (row['misc'] or {})
        G.graph.update(misc)
        calcload(G)
        store_G(G, out_db)


def _migrate_v0(conn: sqlite3.Connection, out_db):
    conn.row_factory = sqlite3.Row
    nodes = {r['digest']: r for r in conn.execute('select * from NodeSet')}
    methods = {r['digest']: r for r in conn.execute('select * from Method')}
    for row in conn.execute('select rowid, * from EdgeSet order by rowid'):
        nrow = nodes[row['nodes']]
        mrow = methods[row['method']]
        VertexC = pickle.loads(nrow['VertexC'])
        T, R = nrow['T'], nrow['R']
        G = nx.Graph(name=nrow['name'], T=T, R=R, B=0, VertexC=VertexC)
        G.add_nodes_from(((n, {'kind': 'wtg'}) for n in range(T)))
        G.add_nodes_from(((r, {'kind': 'oss'}) for r in range(-R, 0)))
        edge_pairs = pickle.loads(row['edges'])
        U, V = edge_pairs.T
        lengths = np.hypot(*(VertexC[U] - VertexC[V]).T)
        G.add_weighted_edges_from(zip(U, V, lengths), weight='length')
        opts = json.loads(mrow['options']) if isinstance(mrow['options'], str) else {}
        misc = pickle.loads(row['misc']) if row['misc'] else {}
        G.graph.update(
            capacity=row['capacity'], creator='legacy-v0', runtime=row['runtime'] or 0.0,
            method_options={'solver_name': 'legacy', **opts,
                            'fun_fingerprint': {'funname': mrow['funname'], 'funfile': 'legacy_v0', 'funhash': mrow['funhash'] or b''}},
            **misc,
        )
        calcload(G)
        store_G(G, out_db)


def migrate_database(src_path: str, dest_path: str):
    conn = sqlite3.connect(src_path)
    version = _detect_version(conn)
    db = open_database(dest_path, create_db=True)
    db.autosave = False
    if version == 2:
        _migrate_v2(conn, db)
    elif version == 1:
        _migrate_v1(conn, db)
    else:
        _migrate_v0(conn, db)
    db.save()


def main():
    parser = argparse.ArgumentParser(description='Migrate legacy Pony sqlite DB (v0/v1/v2) to awkward/parquet v2.')
    parser.add_argument('source', help='Path to legacy sqlite database')
    parser.add_argument('destination', help='Path to destination parquet database file')
    args = parser.parse_args()
    migrate_database(args.source, args.destination)


if __name__ == '__main__':
    main()
