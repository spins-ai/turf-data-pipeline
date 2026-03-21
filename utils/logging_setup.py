#!/usr/bin/env python3
"""
utils/logging_setup.py
======================
Configuration de logging partagee pour tous les scripts du pipeline.

Centralise setup_logging() utilise par 72+ scripts du pipeline.
Chaque script obtient un logger nomme avec sortie console + fichier.

Usage:
    from utils.logging_setup import setup_logging
    logger = setup_logging("mon_script")
    # Cree automatiquement logs/mon_script.log
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path


# Repertoire de logs par defaut (racine du projet)
_DEFAULT_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"


def setup_logging(
    name: str,
    log_dir: Path | str | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Configure et retourne un logger avec sortie console + fichier.

    Parameters
    ----------
    name : str
        Nom du logger (utilise aussi pour le fichier log).
    log_dir : Path or str or None
        Repertoire pour le fichier log. Par defaut: {project_root}/logs/
    level : int
        Niveau de logging (defaut: INFO).

    Returns
    -------
    logging.Logger
        Logger configure.
    """
    logger = logging.getLogger(name)

    # Eviter d'ajouter des handlers en double si deja configure
    if logger.handlers:
        return logger

    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler console (stdout) — force UTF-8 on Windows
    import io
    if hasattr(sys.stdout, "buffer"):
        stream = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    else:
        stream = sys.stdout
    ch = logging.StreamHandler(stream)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Handler fichier
    if log_dir is None:
        log_dir = _DEFAULT_LOG_DIR
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    fh = logging.FileHandler(log_dir / f"{name}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
