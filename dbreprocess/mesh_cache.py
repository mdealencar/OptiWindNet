"""Cached construction of navigation mesh and available-links graphs."""

from __future__ import annotations

import pickle
from pathlib import Path


DEFAULT_CACHE_DIR = Path("cache")


def build_PA(
    digest_hex: str, *, source_db: Path, cache_dir: Path = DEFAULT_CACHE_DIR
) -> Path:
    """Build and cache `(P, A)` for a nodeset digest, returning the cache path."""
    from optiwindnet.db import L_from_nodeset, NodeSet, database_connection
    from optiwindnet.mesh import make_planar_embedding

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"PA_{digest_hex}.pkl"
    if cache_path.exists():
        return cache_path

    with database_connection(str(source_db)):
        ns = NodeSet.get(NodeSet.digest == bytes.fromhex(digest_hex))
        L = L_from_nodeset(ns)
    P, A = make_planar_embedding(L)
    with cache_path.open("wb") as fh:
        pickle.dump((P, A), fh, protocol=pickle.HIGHEST_PROTOCOL)
    return cache_path


def load_PA(digest_hex: str, *, source_db: Path, cache_dir: Path = DEFAULT_CACHE_DIR):
    """Load cached `(P, A)`, building it first when missing."""
    cache_path = cache_dir / f"PA_{digest_hex}.pkl"
    if not cache_path.exists():
        build_PA(digest_hex, source_db=source_db, cache_dir=cache_dir)
    with cache_path.open("rb") as fh:
        return pickle.load(fh)
