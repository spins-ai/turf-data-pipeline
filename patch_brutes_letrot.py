#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_brutes_letrot.py — Patch les brutes Le Trot pour enrichir avec les données courses.

Le parser Le Trot original ne récupérait que les infos meeting-level.
Ce script re-requête le site Le Trot jour par jour et extrait les données
race-level (typePiste, corde, countPartant, distance, allocation) pour
chaque réunion.

Usage :
    python3 patch_brutes_letrot.py [--pause 0.5] [--batch 200]
"""

from __future__ import annotations

import argparse
import html as html_module
import json
import logging
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

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

BRUTES_PATH = Path(os.path.join(BASE_DIR, "output", "01_calendrier_reunions", "reunions_brut.json"))
CHECKPOINT_PATH = Path(".checkpoint_patch_letrot.json")
LETROT_URL = "https://www.letrot.com/courses/{date}"
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
# PARSER LE TROT
# ===========================================================================

def extract_program_json(html_content: str) -> Optional[dict]:
    """Extrait le JSON du composant Vue meeting-day."""
    if not HAS_BS4:
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    meeting_day = soup.find("meeting-day")

    if meeting_day:
        for attr_name in (":program", "program", ":initial-program"):
            raw = meeting_day.get(attr_name)
            if raw:
                try:
                    return json.loads(html_module.unescape(raw))
                except json.JSONDecodeError:
                    continue

    # Fallback regex
    pattern = r'<meeting-day\s[^>]*:program="([^"]*)"'
    match = re.search(pattern, html_content)
    if match:
        try:
            return json.loads(html_module.unescape(match.group(1)))
        except json.JSONDecodeError:
            pass

    return None


def extraire_extras_letrot(program_data: dict) -> dict[str, dict]:
    """
    Extrait les extras enrichis pour chaque réunion depuis le JSON Le Trot.
    Retourne {hippodrome_lower: extras_dict}.
    Agrège les données race-level en données meeting-level.
    """
    result: dict[str, dict] = {}
    meetings = program_data.get("meetings", [])

    for meeting in meetings:
        nom_hippo = (meeting.get("nomHippodrome") or "").strip()
        if not nom_hippo:
            continue

        key = nom_hippo.lower()

        # Meeting-level data
        corde_piste = meeting.get("cordePiste", "")
        nb_engages = meeting.get("nbEngages")
        nb_qualifies = meeting.get("nbQualifies")
        condition_txt = meeting.get("condition", "")
        federation = meeting.get("nomFede", "")

        # Extraire terrain depuis cordePiste
        terrain = ""
        if corde_piste:
            m_terrain = re.search(r"\(([^)]+)\)\s*$", corde_piste)
            if m_terrain:
                terrain = m_terrain.group(1).strip()
            # Fallback: chercher le type de piste à la fin
            # Ex: "Corde à gauche - 1.975 m. (GP ); 1.325 m. (PP) - Machefer"
            m_surface = re.search(r"-\s*(\w+)\s*$", corde_piste)
            if m_surface and not terrain:
                terrain = m_surface.group(1).strip()

        # Extraire corde direction
        corde_dir = ""
        if corde_piste:
            if "gauche" in corde_piste.lower():
                corde_dir = "gauche"
            elif "droite" in corde_piste.lower():
                corde_dir = "droite"

        # Race-level data (agrégé)
        races = meeting.get("races", [])
        distances = []
        nb_partants_total = 0
        type_pistes = set()
        allocations = []
        disciplines = set()

        for race in races:
            dist = race.get("distance")
            if dist:
                distances.append(int(dist))

            cp = race.get("countPartant")
            if cp:
                nb_partants_total += int(cp)

            tp = race.get("typePiste", "")
            if tp:
                type_pistes.add(tp.lower())

            alloc = race.get("allocation")
            if alloc:
                allocations.append(int(alloc))

            disc = race.get("discipline", "")
            if disc:
                disciplines.add(disc)

            # corde race-level (plus fiable)
            corde_race = race.get("corde", "")
            if corde_race and not corde_dir:
                corde_dir = "gauche" if corde_race == "G" else "droite" if corde_race == "D" else ""

        # Si terrain vide au meeting-level, prendre du race-level
        if not terrain and type_pistes:
            terrain = ", ".join(sorted(type_pistes))

        # Si nb_engages vide, calculer depuis les courses
        if not nb_engages and nb_partants_total > 0:
            nb_engages = nb_partants_total

        extras = {
            "quinte": meeting.get("quinteEventuel", False),
            "pick5": meeting.get("pick5", False),
            "has_replay": meeting.get("hasReplay", False),
            "heat": meeting.get("heat", False),
            "corde_piste": corde_piste,
            "corde_direction": corde_dir,
            "federation": federation,
            "condition": condition_txt,
            "nb_engages": nb_engages,
            "nb_qualifies": nb_qualifies,
            "terrain": terrain,
            "type_pistes": list(type_pistes),
            "distances": distances,
            "distance_min": min(distances) if distances else None,
            "distance_max": max(distances) if distances else None,
            "allocation_totale": sum(allocations) if allocations else None,
            "nb_courses": len(races),
            "disciplines": list(disciplines),
        }

        result[key] = extras

    return result


# ===========================================================================
# PATCH
# ===========================================================================

def patch_jour(
    session: requests.Session,
    jour: date,
    brutes: list[dict],
    logger: logging.Logger,
    pause: float = 0.5,
) -> int:
    """Patch les brutes Le Trot pour un jour donné. Retourne le nombre patchées."""
    url = LETROT_URL.format(date=jour.isoformat())

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return 0
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning("  Erreur HTTP %s: %s", jour.isoformat(), e)
        return 0

    program_data = extract_program_json(resp.text)
    if not program_data:
        return 0

    extras_par_hippo = extraire_extras_letrot(program_data)

    # Matcher avec les brutes Le Trot de ce jour
    patched = 0
    for b in brutes:
        if b.get("source") != "letrot":
            continue
        if b.get("date_reunion_brut") != jour.isoformat():
            continue

        hippo_key = (b.get("hippodrome_brut") or "").strip().lower()
        extras_new = extras_par_hippo.get(hippo_key)

        if extras_new:
            old_extras = b.get("extras", {}) or {}
            old_extras.update(extras_new)
            b["extras"] = old_extras
            # Mettre à jour terrain_brut si trouvé
            if extras_new.get("terrain") and not b.get("terrain_brut"):
                b["terrain_brut"] = extras_new["terrain"]
            patched += 1

    return patched


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Patch brutes Le Trot")
    parser.add_argument("--pause", type=float, default=0.5, help="Pause entre requêtes (s)")
    parser.add_argument("--batch", type=int, default=200, help="Sauvegarder tous les N jours")
    args = parser.parse_args()

    logger = setup_logging("patch_letrot")

    if not HAS_BS4:
        logger.error("BeautifulSoup4 requis: pip install beautifulsoup4")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("PATCH BRUTES LE TROT")
    logger.info("=" * 60)

    # Charger brutes
    if not BRUTES_PATH.exists():
        logger.error("Fichier brutes introuvable: %s", BRUTES_PATH)
        sys.exit(1)

    with open(BRUTES_PATH, "r", encoding="utf-8") as f:
        brutes = json.load(f)
    logger.info("Chargées: %d brutes", len(brutes))

    # Trouver les jours Le Trot à patcher
    letrot_brutes = [b for b in brutes if b.get("source") == "letrot"]
    jours_letrot = sorted(set(b["date_reunion_brut"] for b in letrot_brutes))
    logger.info("Jours Le Trot: %d", len(jours_letrot))

    # Checkpoint
    start_idx = 0
    if CHECKPOINT_PATH.exists():
        try:
            cp = json.loads(CHECKPOINT_PATH.read_text())
            start_idx = cp.get("last_idx", 0)
            logger.info("Reprise depuis checkpoint: jour %d/%d", start_idx, len(jours_letrot))
        except (json.JSONDecodeError, OSError):
            pass

    session = create_session()
    total_patched = 0

    for i, jour_str in enumerate(jours_letrot):
        if i < start_idx:
            continue

        jour = date.fromisoformat(jour_str)
        patched = patch_jour(session, jour, brutes, logger, args.pause)
        total_patched += patched

        if (i + 1) % 50 == 0:
            logger.info("  [%d/%d] %d brutes patchées au total", i + 1, len(jours_letrot), total_patched)

        # Sauvegarde intermédiaire
        if (i + 1) % args.batch == 0:
            tmp = BRUTES_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(brutes, f, ensure_ascii=False, indent=None, default=str)
            tmp.replace(BRUTES_PATH)
            logger.info("Brutes sauvegardées: %d entrées", len(brutes))

            CHECKPOINT_PATH.write_text(json.dumps({"last_idx": i + 1}))
            logger.info("  >>> Sauvegarde intermédiaire: %d/%d jours, %d patchées <<<",
                        i + 1, len(jours_letrot), total_patched)

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

    logger.info("Patch terminé: %d brutes patchées sur %d jours", total_patched, len(jours_letrot))

    # Re-normaliser
    logger.info("Re-normalisation en cours...")
    import subprocess
    result = subprocess.run(
        ["python3", "01_calendrier_reunions.py", "--normaliser-seulement"],
        capture_output=True, text=True,
    )
    for line in result.stdout.strip().split("\n")[-5:]:
        logger.info("  %s", line)
    if result.returncode != 0:
        logger.error("Erreur normalisation: %s", result.stderr[-500:] if result.stderr else "")

    logger.info("=" * 60)
    logger.info("FIN PATCH BRUTES LE TROT")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
