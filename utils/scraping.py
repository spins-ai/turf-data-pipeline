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
import random
import time
from pathlib import Path
from typing import Any, Optional

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

__all__ = [
    "create_session",
    "rotate_session",
    "smart_pause",
    "fetch_with_retry",
    "append_jsonl",
    "aggregate_cache_to_jsonl",
    "load_checkpoint",
    "save_checkpoint",
]

log = logging.getLogger(__name__)


# ===================================================================
# Session creation
# ===================================================================

def create_session(user_agents=None):
    """Create an HTTP session with optional cloudscraper and random user-agent.

    Parameters
    ----------
    user_agents : list[str] or None
        List of User-Agent strings to pick from randomly.
        If None, a sensible default Chrome UA is used.

    Returns
    -------
    requests.Session or cloudscraper session
        Ready-to-use HTTP session with User-Agent header set.
    """
    try:
        import cloudscraper
        session = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
    except ImportError:
        import requests as _requests
        session = _requests.Session()

    if user_agents:
        session.headers.update({"User-Agent": random.choice(user_agents)})
    else:
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"})

    return session


def rotate_session(
    user_agents: list[str] | None = None,
    headers: dict[str, str] | None = None,
):
    """Create a fresh HTTP session with rotated User-Agent.

    This replaces the per-file ``rotate_session()`` pattern that was
    duplicated across 9+ scrapers.  The caller is responsible for
    assigning the returned session to its module-level variable.

    Parameters
    ----------
    user_agents : list[str] or None
        User-Agent strings to pick from randomly (forwarded to
        ``create_session``).
    headers : dict[str, str] or None
        Extra headers to set on the new session (e.g. Accept,
        Accept-Language, Referer).

    Returns
    -------
    requests.Session
        A fresh session ready to use.

    Example
    -------
    ::

        from utils.scraping import rotate_session
        session = rotate_session(user_agents=USER_AGENTS, headers={
            "Accept": "application/json",
            "Accept-Language": "fr-FR,fr;q=0.9",
            "DNT": "1",
        })
        req_count = 0
    """
    session = create_session(user_agents=user_agents)
    if headers:
        session.headers.update(headers)
    return session


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


def aggregate_cache_to_jsonl(
    cache_dir: str | Path,
    output_file: str | Path,
    logger: logging.Logger | None = None,
) -> int:
    """Read all JSON cache files from *cache_dir* and write them as JSONL.

    Each cache file may contain a JSON list of records or a single dict.
    All records are written as one-JSON-object-per-line into *output_file*.

    Parameters
    ----------
    cache_dir : str or Path
        Directory containing ``*.json`` cache files.
    output_file : str or Path
        Destination JSONL file (overwritten).
    logger : logging.Logger or None
        Logger instance; falls back to module logger.

    Returns
    -------
    int
        Total number of records written.
    """
    _log = logger or log
    cache_dir = Path(cache_dir)
    output_file = Path(output_file)

    if not cache_dir.exists():
        _log.info("No cache directory: %s", cache_dir)
        return 0

    cache_files = sorted(f.name for f in cache_dir.iterdir() if f.suffix == ".json")
    if not cache_files:
        _log.info("No cache files found for aggregation.")
        return 0

    output_file.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with open(output_file, "w", encoding="utf-8") as out:
        for fname in cache_files:
            fpath = cache_dir / fname
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    records = json.load(f)
                if isinstance(records, list):
                    for rec in records:
                        out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        total += 1
                elif isinstance(records, dict):
                    out.write(json.dumps(records, ensure_ascii=False) + "\n")
                    total += 1
            except (json.JSONDecodeError, OSError) as e:
                _log.error("Error reading cache file %s: %s", fname, e)

    _log.info("Aggregated %d records from %d cache files -> %s", total, len(cache_files), output_file)
    return total


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
