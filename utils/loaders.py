#!/usr/bin/env python3
"""
utils/loaders.py
================
Fonctions partagees de chargement de donnees (JSON, JSONL, CSV).

Centralise load_jsonl(), load_json_or_jsonl() et load_json_safe()
utilises par 20+ scripts du pipeline.

Usage:
    from utils.loaders import load_json_or_jsonl, load_jsonl, load_json_safe
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional


def load_jsonl(path: str, logger: logging.Logger) -> list[dict]:
    """Charge un fichier JSONL (une ligne JSON par enregistrement).

    Parameters
    ----------
    path : str
        Chemin vers le fichier .jsonl
    logger : logging.Logger
        Logger pour les messages.

    Returns
    -------
    list[dict]
        Liste des enregistrements.
    """
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    logger.info("Charge %d enregistrements depuis %s", len(records), path)
    return records


def load_json_or_jsonl(path: str, logger: logging.Logger) -> list[dict]:
    """Charge un fichier JSON ou JSONL, preferant JSONL si disponible.

    Si le chemin pointe vers un .json, verifie d'abord si un .jsonl
    equivalent existe (plus rapide a charger et moins gourmand en RAM).

    Parameters
    ----------
    path : str
        Chemin vers le fichier .json ou .jsonl
    logger : logging.Logger
        Logger pour les messages.

    Returns
    -------
    list[dict]
        Liste des enregistrements.

    Raises
    ------
    SystemExit
        Si le fichier est introuvable.
    """
    if path.endswith(".jsonl"):
        return load_jsonl(path, logger)

    # Verifier si un JSONL equivalent existe
    jsonl_path = path.replace(".json", ".jsonl")
    if os.path.exists(jsonl_path):
        return load_jsonl(jsonl_path, logger)

    # Charger le JSON classique
    if os.path.exists(path):
        logger.info("Chargement JSON: %s", path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("  %d entrees chargees", len(data))
        return data

    logger.error("Fichier introuvable: %s", path)
    sys.exit(1)


def load_json_safe(path: str, label: str, logger: logging.Logger) -> list[dict]:
    """Charge un fichier JSON de maniere securisee (retourne [] si absent).

    Pour les gros fichiers (> 4 GB), tente d'utiliser ijson en streaming.

    Parameters
    ----------
    path : str
        Chemin vers le fichier JSON.
    label : str
        Label pour les logs.
    logger : logging.Logger
        Logger pour les messages.

    Returns
    -------
    list[dict]
        Liste des enregistrements, ou [] si absent.
    """
    if not os.path.exists(path):
        return []

    size = os.path.getsize(path) / 1024 / 1024

    if size > 4000:
        logger.info("  %s: %.0f MB — streaming avec ijson", label, size)
        try:
            import ijson
            items = []
            with open(path, "rb") as f:
                for item in ijson.items(f, "item"):
                    items.append(item)
            logger.info("  %s: %d records charges (streaming)", label, len(items))
            return items
        except ImportError:
            logger.warning("  ijson non disponible, chargement standard")
        except Exception as e:
            logger.error("  Erreur streaming %s: %s", label, e)
            return []

    logger.info("  %s: %.0f MB — chargement standard", label, size)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info("  %s: %d records charges", label, len(data) if isinstance(data, list) else 1)
        return data if isinstance(data, list) else [data]
    except (json.JSONDecodeError, ValueError) as e:
        logger.error("  Erreur JSON %s: %s", label, e)
        return []
