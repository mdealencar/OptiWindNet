"""Length comparison helpers for deterministic routeset reruns."""

from __future__ import annotations

import math


def length_matches(
    observed: float, reference: float, *, rel_tol: float = 1e-12
) -> bool:
    """Return True when two deterministic route lengths are effectively equal."""
    return math.isclose(observed, reference, rel_tol=rel_tol, abs_tol=0.0)
