#!/usr/bin/env python3
"""
utils/scraping.py
=================
Fonctions utilitaires partagees pour les scrapers du pipeline.

Centralise smart_pause, fetch_with_retry, append_jsonl,
load_checkpoint, save_checkpoint utilises par 50+ scrapers.

Usage:
    from utils.scraping import smart_pause, fetch_with_retry
    from utils.scraping import load_checkpoint, save_checkpoint
    from utils.scraping import append_jsonl
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Any, Optional

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

log = logging.getLogger(__name__)


# ===================================================================
# Rate limiting
# ===================================================================

def smart_pause(base: float = 2.5, jitter: float = 1.5, long_pause_chance: float = 0.08) -> None:
    """Pause intelligente avec jitter aleatoire pour le rate-limiting.

    Parameters
    ----------
    base : float
        Duree de base en secondes (defaut: 2.5).
    jitter : float
        Amplitude du jitter aleatoire ± (defaut: 1.5).
    long_pause_chance : float
        Probabilite d'une pause longue supplementaire (defaut: 0.08 = 8%).
        Mettre a 0.0 pour desactiver les pauses longues.
    """
    pause = base + random.uniform(-jitter, jitter)
    if long_pause_chance > 0 and random.random() < long_pause_chance:
        pause += random.uniform(5, 15)
    time.sleep(max(0.1, pause))


# ===================================================================
# HTTP fetch with retry
# ===================================================================

def fetch_with_retry(
    session,
    url: str,
    max_retries: int = 3,
    timeout: int = 30,
    params: dict | None = None,
    logger: logging.Logger | None = None,
) -> Optional[Any]:
    """GET avec retry automatique.

    Parameters
    ----------
    session : requests.Session
        Session HTTP (requests ou cloudscraper).
    url : str
        URL a fetcher.
    max_retries : int
        Nombre maximum de tentatives (defaut: 3).
    timeout : int
        Timeout en secondes (defaut: 30).
    params : dict or None
        Parametres de requete optionnels.
    logger : logging.Logger or None
        Logger optionnel (utilise le logger du module si None).

    Returns
    -------
    requests.Response or None
        Response HTTP ou None en cas d'echec.
    """
    _log = logger or log
    kwargs: dict[str, Any] = {"timeout": timeout}
    if params:
        kwargs["params"] = params

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, **kwargs)
            if resp.status_code == 200:
                return resp
            _log.warning("HTTP %d sur %s (essai %d/%d)", resp.status_code, url, attempt, max_retries)
            time.sleep(5 * attempt)
        except Exception as e:
            _log.warning("Erreur reseau: %s (essai %d/%d)", e, attempt, max_retries)
            time.sleep(5 * attempt)

    _log.error("Echec apres %d essais: %s", max_retries, url)
    return None


# ===================================================================
# JSONL append
# ===================================================================

def append_jsonl(filepath: str | Path, record: dict, ensure_ascii: bool = False) -> None:
    """Ajoute un record JSON en mode append dans un fichier JSONL.

    Parameters
    ----------
    filepath : str or Path
        Chemin du fichier JSONL.
    record : dict
        Enregistrement a ecrire.
    ensure_ascii : bool
        Si True, echappe les caracteres non-ASCII (defaut: False).
    """
    with open(filepath, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=ensure_ascii, default=str) + "\n")


# ===================================================================
# Checkpoint management
# ===================================================================

def load_checkpoint(checkpoint_file: str | Path) -> dict:
    """Charge un checkpoint depuis un fichier JSON.

    Parameters
    ----------
    checkpoint_file : str or Path
        Chemin du fichier checkpoint.

    Returns
    -------
    dict
        Donnees du checkpoint, ou dict vide si fichier inexistant.
    """
    checkpoint_file = Path(checkpoint_file)
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_checkpoint(checkpoint_file: str | Path, data: dict) -> None:
    """Sauvegarde un checkpoint dans un fichier JSON.

    Parameters
    ----------
    checkpoint_file : str or Path
        Chemin du fichier checkpoint.
    data : dict
        Donnees a sauvegarder.
    """
    checkpoint_file = Path(checkpoint_file)
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
