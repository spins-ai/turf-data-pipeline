"""
utils.math
~~~~~~~~~~
Shared math helper functions used across feature builders.

These were previously duplicated in 6-8 feature_builders modules.
"""

from __future__ import annotations

import statistics
from typing import Optional


def safe_mean(values: list, *, ndigits: int | None = None) -> Optional[float]:
    """Mean of non-None numeric values, or None if empty.

    Parameters
    ----------
    values : list
        May contain None entries which are filtered out.
    ndigits : int | None
        If given, round the result to this many decimal places.
    """
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    result = sum(clean) / len(clean)
    return round(result, ndigits) if ndigits is not None else result


def safe_rate(count: int, total: int, *, ndigits: int | None = None) -> Optional[float]:
    """Safe division returning None when *total* is zero.

    Parameters
    ----------
    count : int
        Numerator.
    total : int
        Denominator.
    ndigits : int | None
        If given, round the result to this many decimal places.
    """
    if total == 0:
        return None
    result = count / total
    return round(result, ndigits) if ndigits is not None else result


def safe_stdev(values: list, *, ndigits: int | None = None) -> Optional[float]:
    """Standard deviation of non-None numeric values, or None if < 2 values.

    Uses ``statistics.stdev`` (sample standard deviation, Bessel-corrected).

    Parameters
    ----------
    values : list
        May contain None entries which are filtered out.
    ndigits : int | None
        If given, round the result to this many decimal places.
    """
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    result = statistics.stdev(clean)
    return round(result, ndigits) if ndigits is not None else result
