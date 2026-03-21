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

from typing import Any, Optional


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
