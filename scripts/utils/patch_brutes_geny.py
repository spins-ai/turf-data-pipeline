#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_brutes_geny.py — Patch les brutes Geny pour enrichir avec pénétromètre et terrain.

Le parser Geny original ne récupérait pas le pénétromètre ni le terrain de manière fiable.
Ce script re-requête le site Geny jour par jour et extrait les données manquantes.

Usage :
    python3 patch_brutes_geny.py [--pause 0.5] [--batch 200]
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# ===========================================================================
# CONFIG
# ===========================================================================

BRUTES_PATH = Path(os.path.join(BASE_DIR, "../../output", "01_calendrier_reunions", "reunions_brut.json"))
CHECKPOINT_PATH = Path(".checkpoint_patch_geny.json")
GENY_URL = "https://www.geny.com/reunions-courses-pmu/_d{date}"
# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# HTTP
# ===========================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
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
# PARSER GENY
# ===========================================================================

def extraire_extras_geny(html_content: str) -> dict[str, dict]:
    """
    Extrait terrain, pénétromètre, non-partants depuis le HTML Geny.
    Retourne {hippodrome_lower: extras_dict}.
    """
    if not HAS_BS4:
        return {}

    soup = BeautifulSoup(html_content, "html.parser")
    result: dict[str, dict] = {}

    cartouches = soup.select(".cartoucheReunion")
    for cartouche in cartouches:
        try:
            nom_el = cartouche.select_one(".nomReunion")
            if not nom_el:
                continue

            raw_text = nom_el.get_text(separator=" ", strip=True)
            # Parse "jeudi : Chantilly (R1)" ou "jeudi : Mons (Belgique) (R5)"
            text = re.sub(
                r"^[a-zéèêëàâäùûüôöîï]+\s*:\s*", "", raw_text, flags=re.IGNORECASE
            ).strip()
            # Enlever (RN)
            text = re.sub(r"\(R\d+\)\s*$", "", text).strip()
            # Enlever (Pays)
            text = re.sub(r"[\(\[]\s*[^)\]]+\s*[\)\]]\s*$", "", text).strip()
            hippo_key = text.strip().lower()

            if not hippo_key:
                continue

            # Extraire infos depuis .infoReunion
            terrain = ""
            penetrometre = ""
            non_partants = ""

            info_el = cartouche.select_one(".infoReunion")
            if info_el:
                info_text = info_el.get_text(separator=" ", strip=True)

                # Terrain
                tm = re.search(r"Terrain\s*:\s*([^\n]+?)(?:\s*(?:Non-partant|Pénétromètre|$))", info_text)
                if tm:
                    terrain = tm.group(1).strip()
                    # Nettoyer terrain des parasites
                    terrain = re.sub(r"\s*Non-partants?\b.*$", "", terrain, flags=re.IGNORECASE).strip()
                    terrain = re.sub(r"\s*Pénétromètre\b.*$", "", terrain, flags=re.IGNORECASE).strip()

                # Pénétromètre
                pm = re.search(r"Pénétromètre\s*:\s*([\d.,]+)", info_text)
                if pm:
                    penetrometre = pm.group(1).strip()

                # Non-partants
                np_el = info_el.select_one(".nonPartant")
                if np_el:
                    np_text = np_el.get_text(strip=True)
                    np_match = re.search(r":\s*(.+)$", np_text)
                    if np_match:
                        non_partants = np_match.group(1).strip()

            # Compter les courses après ce cartouche
            nb_courses = 0
            sib = cartouche.find_next_sibling()
            while sib:
                if sib.name == "a" and sib.get("name", "").startswith("reunion"):
                    break
                classes = sib.get("class", [])
                if "cartoucheReunion" in classes:
                    break
                if "courseParis" in classes:
                    nb_courses += 1
                sib = sib.find_next_sibling()

            extras = {}
            if terrain:
                extras["terrain"] = terrain
            if penetrometre:
                extras["penetrometre"] = penetrometre
            if non_partants:
                extras["non_partants"] = non_partants
            if nb_courses > 0:
                extras["nb_courses"] = nb_courses

            result[hippo_key] = extras

        except Exception as e:
            log.debug(f"  Erreur parsing geny: {e}")
            continue

    return result


# ===========================================================================
# PATCH
# ===========================================================================

def patch_jour(
    session: requests.Session,
    jour: date,
    brutes: list[dict],
    logger: logging.Logger,
) -> int:
    """Patch les brutes Geny pour un jour donné. Retourne le nombre patchées."""
    url = GENY_URL.format(date=jour.isoformat())

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return 0
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning("  Erreur HTTP %s: %s", jour.isoformat(), e)
        return 0

    extras_par_hippo = extraire_extras_geny(resp.text)
    if not extras_par_hippo:
        return 0

    # Matcher avec les brutes Geny de ce jour
    patched = 0
    for b in brutes:
        if b.get("source") != "geny":
            continue
        if b.get("date_reunion_brut") != jour.isoformat():
            continue

        hippo_key = (b.get("hippodrome_brut") or "").strip().lower()
        extras_new = extras_par_hippo.get(hippo_key)

        if extras_new:
            old_extras = b.get("extras", {}) or {}
            old_extras.update(extras_new)
            b["extras"] = old_extras
            # Mettre à jour terrain_brut si amélioré (nettoyé)
            if extras_new.get("terrain"):
                b["terrain_brut"] = extras_new["terrain"]
            patched += 1

    return patched


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Patch brutes Geny")
    parser.add_argument("--pause", type=float, default=0.5, help="Pause entre requêtes (s)")
    parser.add_argument("--batch", type=int, default=200, help="Sauvegarder tous les N jours")
    args = parser.parse_args()

    logger = setup_logging("patch_geny")

    if not HAS_BS4:
        logger.error("BeautifulSoup4 requis: pip install beautifulsoup4")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("PATCH BRUTES GENY")
    logger.info("=" * 60)

    # Charger brutes
    if not BRUTES_PATH.exists():
        logger.error("Fichier brutes introuvable: %s", BRUTES_PATH)
        sys.exit(1)

    with open(BRUTES_PATH, "r", encoding="utf-8") as f:
        brutes = json.load(f)
    logger.info("Chargées: %d brutes", len(brutes))

    # Trouver les jours Geny à patcher
    geny_brutes = [b for b in brutes if b.get("source") == "geny"]
    jours_geny = sorted(set(b["date_reunion_brut"] for b in geny_brutes))
    logger.info("Jours Geny: %d", len(jours_geny))

    # Checkpoint
    start_idx = 0
    if CHECKPOINT_PATH.exists():
        try:
            cp = json.loads(CHECKPOINT_PATH.read_text())
            start_idx = cp.get("last_idx", 0)
            logger.info("Reprise depuis checkpoint: jour %d/%d", start_idx, len(jours_geny))
        except (json.JSONDecodeError, OSError):
            pass

    session = create_session()
    total_patched = 0

    for i, jour_str in enumerate(jours_geny):
        if i < start_idx:
            continue

        jour = date.fromisoformat(jour_str)
        patched = patch_jour(session, jour, brutes, logger)
        total_patched += patched

        if (i + 1) % 50 == 0:
            logger.info("  [%d/%d] %d brutes patchées au total", i + 1, len(jours_geny), total_patched)

        # Sauvegarde intermédiaire
        if (i + 1) % args.batch == 0:
            tmp = BRUTES_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(brutes, f, ensure_ascii=False, indent=None, default=str)
            tmp.replace(BRUTES_PATH)
            logger.info("Brutes sauvegardées: %d entrées", len(brutes))

            CHECKPOINT_PATH.write_text(json.dumps({"last_idx": i + 1}))
            logger.info("  >>> Sauvegarde intermédiaire: %d/%d jours, %d patchées <<<",
                        i + 1, len(jours_geny), total_patched)

        time.sleep(args.pause)

    # Sauvegarde finale
    tmp = BRUTES_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(brutes, f, ensure_ascii=False, indent=None, default=str)
    tmp.replace(BRUTES_PATH)
    logger.info("Brutes sauvegardées: %d entrées", len(brutes))

    # Cleanup checkpoint
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    logger.info("Patch terminé: %d brutes patchées sur %d jours", total_patched, len(jours_geny))

    logger.info("NOTE: Re-normalisation manuelle requise (python3 renormaliser.py)")

    logger.info("=" * 60)
    logger.info("FIN PATCH BRUTES GENY")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
