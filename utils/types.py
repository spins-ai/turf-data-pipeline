#!/usr/bin/env python3
"""
utils/types.py
==============
Fonctions partagees de conversion de types securisees.

Centralise safe_int() et safe_float() utilises par 10+ scripts du pipeline.

Usage:
    from utils.types import safe_int, safe_float
"""

from __future__ import annotations

__all__ = ["safe_int", "safe_float", "utc_now_iso", "centimes_to_euros"]

from datetime import datetime, timezone
from typing import Any, Optional


def utc_now_iso() -> str:
    """Return current UTC time as ISO 8601 string.

    Returns
    -------
    str
        UTC timestamp formatted as ``YYYY-MM-DDTHH:MM:SSZ``.

    Examples
    --------
    >>> isinstance(utc_now_iso(), str)
    True
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_int(val: Any, default: Optional[int] = None) -> Optional[int]:
    """Convertit une valeur en int de maniere sure.

    Parameters
    ----------
    val : Any
        Valeur a convertir.
    default : int or None
        Valeur par defaut si la conversion echoue.

    Returns
    -------
    int or None
        Valeur convertie ou default.

    Examples
    --------
    >>> safe_int("42")
    42
    >>> safe_int("abc", default=0)
    0
    >>> safe_int(None)
    """
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    """Convertit une valeur en float de maniere sure.

    Parameters
    ----------
    val : Any
        Valeur a convertir.
    default : float or None
        Valeur par defaut si la conversion echoue.

    Returns
    -------
    float or None
        Valeur convertie ou default.

    Examples
    --------
    >>> safe_float("3.14")
    3.14
    >>> safe_float("abc", default=0.0)
    0.0
    >>> safe_float(None)
    """
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def centimes_to_euros(centimes: Optional[int]) -> Optional[float]:
    """Convert a price in centimes to euros.

    Parameters
    ----------
    centimes : int or None
        Amount in centimes.

    Returns
    -------
    float or None
        Amount in euros, or *None* if *centimes* is *None*.

    Examples
    --------
    >>> centimes_to_euros(150)
    1.5
    >>> centimes_to_euros(None) is None
    True
    """
    if centimes is None:
        return None
    return centimes / 100.0
