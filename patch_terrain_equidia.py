#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_terrain_equidia.py — Comble les trous de terrain/condition via Equidia.

Equidia fournit l'état du terrain pour toutes les courses depuis 2014+.
On requête la C1 de chaque réunion manquante (le terrain est identique
pour toutes les courses d'une réunion).

Usage :
    python3 patch_terrain_equidia.py [--pause 0.5] [--batch 100]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===========================================================================
# CONFIG
# ===========================================================================

NORMALISEES_PATH = Path(os.path.join(BASE_DIR, "output", "01_calendrier_reunions", "reunions_normalisees.json"))
CACHE_PATH = Path(os.path.join(BASE_DIR, "output", "01_calendrier_reunions", "equidia_terrain_cache.json"))
EQUIDIA_URL = "https://www.equidia.fr/courses/{date}/R{num}/C1"
# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# HTTP
# ===========================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=1, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=1, pool_connections=1)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "fr-FR,fr;q=0.9",
    })
    return session


# ===========================================================================
# CACHE
# ===========================================================================

class EquidiaCache:
    def __init__(self, fichier: Path):
        self.fichier = fichier
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self):
        if self.fichier.exists():
            try:
                with open(self.fichier, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._data = {}

    def get(self, key: str) -> Optional[dict]:
        return self._data.get(key)

    def put(self, key: str, data: dict):
        self._data[key] = data

    def save(self):
        self.fichier.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.fichier.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False)
        tmp.replace(self.fichier)

    def __len__(self):
        return len(self._data)


# ===========================================================================
# PARSER EQUIDIA
# ===========================================================================

def extraire_terrain_equidia(html: str) -> dict:
    """Extrait terrain, pénétromètre, partants, corde depuis le HTML Equidia."""
    result = {}

    # 1. Chercher le JSON embarqué avec etat_terrain (ancien format)
    m = re.search(r'"etat_terrain"\s*:\s*"([^"]*)"', html)
    if m:
        result["terrain"] = m.group(1).strip()

    # 2. Chercher "Terrain XXX" dans le HTML brut (nouveau format)
    # Pattern: Terrain\s+VALEUR suivi de chiffre (pénétromètre) ou autre section
    if not result.get("terrain"):
        # Chercher dans le HTML brut (plus rapide que nettoyer tout le HTML)
        m_t = re.search(
            r'>\s*Terrain\s*</[^>]+>\s*<[^>]+>\s*([^<]+)',
            html[:200000],
        )
        if m_t:
            result["terrain"] = m_t.group(1).strip()

    # 3. Fallback: chercher "Terrain VALEUR" dans le texte (limité aux premiers 50K chars)
    if not result.get("terrain"):
        # Nettoyer seulement une partie du HTML
        chunk = html[:100000]
        text = re.sub(r'<[^>]+>', ' ', chunk)
        text = re.sub(r'\s+', ' ', text)
        m_t2 = re.search(r'Terrain\s+([\w\s\u00e0-\u00ff]+?)(?:\s+\d|\s+Partants|\s+Corde)', text)
        if m_t2:
            terrain = m_t2.group(1).strip()
            # Filtrer les faux positifs
            if len(terrain) < 50:
                result["terrain"] = terrain

    # 4. Corde (rapide, regex sur HTML brut)
    m_c = re.search(r'Corde\s+(?:à|a)\s+(gauche|droite)', html[:200000], re.IGNORECASE)
    if m_c:
        result["corde"] = m_c.group(1).lower()

    return result


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Patch terrain via Equidia")
    parser.add_argument("--pause", type=float, default=0.5, help="Pause entre requêtes (s)")
    parser.add_argument("--batch", type=int, default=100, help="Sauvegarder tous les N")
    args = parser.parse_args()

    logger = setup_logging("patch_equidia")

    logger.info("=" * 60)
    logger.info("PATCH TERRAIN VIA EQUIDIA")
    logger.info("=" * 60)

    # Charger normalisées
    with open(NORMALISEES_PATH, "r", encoding="utf-8") as f:
        reunions = json.load(f)
    logger.info("Chargées: %d réunions normalisées", len(reunions))

    # Identifier celles sans terrain OU sans condition
    a_enrichir = []
    for i, r in enumerate(reunions):
        if not r.get("terrain") or not r.get("condition"):
            date_iso = r.get("date_reunion_iso", "")
            num = r.get("numero_reunion") or r.get("code_reunion") or 1
            if date_iso:
                a_enrichir.append((i, date_iso, num))

    logger.info("Réunions à enrichir: %d", len(a_enrichir))

    cache = EquidiaCache(CACHE_PATH)
    logger.info("Cache existant: %d entrées", len(cache))

    session = create_session()
    enrichies = 0
    erreurs = 0
    skipped = 0
    requetes = 0

    for idx, (reunion_idx, date_iso, num) in enumerate(a_enrichir, 1):
        cache_key = f"{date_iso}_R{num}"

        # Check cache
        cached = cache.get(cache_key)
        if cached is not None:
            if cached:  # non-vide
                _appliquer_terrain(reunions[reunion_idx], cached)
                enrichies += 1
            skipped += 1
            continue

        # Requêter Equidia
        url = EQUIDIA_URL.format(date=date_iso, num=num)
        try:
            resp = session.get(url, timeout=10)
            requetes += 1
            if resp.status_code == 404:
                cache.put(cache_key, {})
                continue
            resp.raise_for_status()

            data = extraire_terrain_equidia(resp.text)
            cache.put(cache_key, data)

            if data:
                _appliquer_terrain(reunions[reunion_idx], data)
                enrichies += 1

        except requests.exceptions.RequestException as e:
            logger.warning("  Erreur %s: %s", cache_key, str(e)[:80])
            erreurs += 1
            cache.put(cache_key, {})

        # Renouveler la session tous les 500 requêtes
        if requetes > 0 and requetes % 500 == 0:
            session.close()
            session = create_session()
            logger.info("  Session renouvelée")

        if idx % 100 == 0:
            logger.info("  [%d/%d] enrichies=%d, erreurs=%d, cache=%d, req=%d",
                        idx, len(a_enrichir), enrichies, erreurs, len(cache), requetes)

        if idx % args.batch == 0:
            cache.save()

        time.sleep(args.pause)

    cache.save()

    # Sauvegarder les normalisées enrichies
    tmp = NORMALISEES_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(reunions, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(NORMALISEES_PATH)

    logger.info("=" * 60)
    logger.info("TERMINÉ: %d enrichies, %d erreurs, %d en cache, %d requêtes",
                enrichies, erreurs, skipped, requetes)
    logger.info("=" * 60)


def _appliquer_terrain(reunion: dict, data: dict) -> None:
    """Applique les données Equidia à une réunion normalisée."""
    if data.get("terrain") and not reunion.get("terrain"):
        reunion["terrain"] = data["terrain"]
    if data.get("terrain") and not reunion.get("condition"):
        reunion["condition"] = data["terrain"]
    if data.get("corde") and not reunion.get("corde_piste"):
        reunion["corde_piste"] = data["corde"]


if __name__ == "__main__":
    main()
