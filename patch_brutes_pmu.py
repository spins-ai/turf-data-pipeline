#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_brutes_pmu.py — Enrichit les brutes PMU existantes avec les nouveaux champs
sans tout re-collecter.

Lit reunions_brut.json, re-tape l'API PMU uniquement pour les jours
où il manque les extras (terrain, condition, corde, nb_partants, paris, meteo),
patche les brutes en place, sauvegarde, puis re-normalise.

Usage:
    python3 patch_brutes_pmu.py [--pause 0.5] [--batch 200]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===========================================================================
# CONFIG
# ===========================================================================

BASE_DIR = Path(__file__).resolve().parent
BRUTES_PATH = BASE_DIR / "output" / "01_calendrier_reunions" / "reunions_brut.json"
PMU_URL_TEMPLATE = "https://online.turfinfo.api.pmu.fr/rest/client/7/programme/{}"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

CHECKPOINT_PATH = BASE_DIR / "output" / "01_calendrier_reunions" / ".checkpoint_patch_pmu.json"


from utils.logging_setup import setup_logging


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    retry = Retry(total=3, backoff_factor=1.0,
                  status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# NOTE: Not migrated to utils.scraping.load_checkpoint/save_checkpoint because
# these return/accept set[str] (not dict), serialize as sorted list, and use
# atomic write (tmp+rename) -- incompatible signature with the generic util.
def load_checkpoint() -> set[str]:
    """Charge les jours déjà patchés."""
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception as e:
            log.warning(f"  Checkpoint corrompu: {e}")
    return set()


def save_checkpoint(done: set[str]) -> None:
    """Sauvegarde atomique du checkpoint."""
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sorted(done), f)
    tmp.rename(CHECKPOINT_PATH)


def extraire_extras_pmu(item: dict, courses: list) -> dict:
    """Extrait TOUS les extras depuis un item réunion PMU."""
    extras: dict[str, Any] = {}

    # Hippodrome
    hippo_data = item.get("hippodrome", {})
    if isinstance(hippo_data, dict):
        extras["hippodrome_brut_long"] = hippo_data.get("libelleLong", "")

    # Pays
    pays_data = item.get("pays", {})
    if isinstance(pays_data, dict):
        extras["pays_code_brut"] = pays_data.get("code", "")

    # Première course → terrain, condition, corde, nb_partants, distance, parcours
    if courses:
        c0 = courses[0]
        # Corde
        raw_corde = c0.get("corde", "")
        extras["corde"] = (
            str(raw_corde).replace("CORDE_", "").replace("_", " ").title()
            if raw_corde else ""
        )
        # Type de piste → terrain
        type_piste = c0.get("typePiste", "")
        extras["type_piste"] = str(type_piste).upper() if type_piste else ""
        # Pénétromètre → condition
        penetro = c0.get("penetrometre", {})
        if isinstance(penetro, dict):
            extras["condition"] = penetro.get("intitule", "")
            extras["penetrometre_valeur"] = penetro.get("valeurMesure", "")
        else:
            extras["condition"] = ""
            extras["penetrometre_valeur"] = ""
        # Nb partants
        np = c0.get("nombreDeclaresPartants")
        extras["nb_partants"] = int(np) if np is not None else None
        # Distance et parcours
        dist = c0.get("distance")
        extras["distance_m"] = int(dist) if dist is not None else None
        extras["parcours"] = str(c0.get("parcours", ""))

    # Météo
    meteo_raw = item.get("meteo", {})
    if isinstance(meteo_raw, dict):
        extras["meteo_temperature"] = meteo_raw.get("temperature")
        extras["meteo_nebulosite"] = meteo_raw.get("nebulositeLibelleCourt", "")
        extras["meteo_nebulosite_long"] = meteo_raw.get("nebulositeLibelleLong", "")
        extras["meteo_force_vent"] = meteo_raw.get("forceVent")
        extras["meteo_direction_vent"] = meteo_raw.get("directionVent", "")

    # Paris événements
    paris_evt = item.get("parisEvenement", [])
    paris_codes = list({p.get("codePari", "") for p in paris_evt if isinstance(p, dict)})
    extras["has_quinte"] = any("QUINTE" in c for c in paris_codes)
    extras["paris_evenements"] = paris_codes

    # Spécialités
    specialites = item.get("specialites", [])
    disc_mere = item.get("disciplinesMere", [])
    extras["specialites_liste"] = [str(s) for s in specialites]
    extras["disciplines_mere"] = [str(d) for d in disc_mere]

    return extras


def patch_jour(
    session: requests.Session,
    jour_str: str,
    brutes_par_numero: dict[int, dict],
    logger: logging.Logger,
) -> int:
    """
    Re-tape PMU pour un jour, patche les extras des brutes correspondantes.
    Retourne le nombre de brutes patchées.
    """
    d = datetime.strptime(jour_str, "%Y-%m-%d").date()
    url = PMU_URL_TEMPLATE.format(d.strftime("%d%m%Y"))

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 429:
            logger.warning("429 rate-limit pour %s, attente 10s", jour_str)
            time.sleep(10)
            resp = session.get(url, timeout=15)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error("Erreur HTTP %s: %s", jour_str, e)
        return 0

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        logger.error("Non-JSON pour %s", jour_str)
        return 0

    programme = data.get("programme", data) if isinstance(data, dict) else {}
    reunions_api = programme.get("reunions", [])

    patched = 0
    for item in reunions_api:
        numero = item.get("numOfficiel") or item.get("numExterne")
        if numero is None:
            continue
        numero = int(numero)
        courses = item.get("courses", [])

        # Trouver la brute correspondante
        brute = brutes_par_numero.get(numero)
        if brute is None:
            continue

        # Extraire les nouveaux extras
        new_extras = extraire_extras_pmu(item, courses)

        # Patcher la brute
        if not isinstance(brute.get("extras"), dict):
            brute["extras"] = {}
        brute["extras"].update(new_extras)

        # Patcher aussi terrain_brut et meteo_brut si vides
        if not brute.get("terrain_brut") and new_extras.get("type_piste"):
            brute["terrain_brut"] = new_extras["type_piste"]
        if not brute.get("meteo_brut"):
            parts = []
            neb = new_extras.get("meteo_nebulosite", "")
            if neb:
                parts.append(neb)
            temp = new_extras.get("meteo_temperature")
            if temp is not None:
                parts.append(f"{temp}°C")
            fv = new_extras.get("meteo_force_vent")
            dv = new_extras.get("meteo_direction_vent", "")
            if fv is not None:
                parts.append(f"vent {fv}km/h {dv}".strip())
            brute["meteo_brut"] = " | ".join(parts)
        if not brute.get("heure_reunion_brut") and courses:
            ts_depart = courses[0].get("heureDepart")
            if ts_depart and isinstance(ts_depart, (int, float)):
                try:
                    brute["heure_reunion_brut"] = datetime.fromtimestamp(
                        ts_depart / 1000
                    ).strftime("%H:%M")
                except (OSError, ValueError):
                    pass

        patched += 1

    return patched


def sauver_brutes(brutes: list[dict], logger: logging.Logger) -> None:
    """Sauvegarde atomique des brutes patchées."""
    tmp = BRUTES_PATH.with_suffix(".tmp.json")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(brutes, f, ensure_ascii=False, indent=None)
    tmp.rename(BRUTES_PATH)
    logger.info("Brutes sauvegardées: %d entrées", len(brutes))


def renormaliser(logger: logging.Logger) -> None:
    """Re-lance la normalisation/fusion sans re-collecter."""
    import subprocess
    logger.info("Re-normalisation en cours...")
    result = subprocess.run(
        [sys.executable, "01_calendrier_reunions.py",
         "--date-debut", "2016-03-12", "--date-fin", "2026-03-12",
         "--output", str(BASE_DIR / "output" / "01_calendrier_reunions")],
        capture_output=True, text=True, timeout=120,
    )
    for line in result.stdout.splitlines():
        if any(k in line for k in ("Doublons", "Sauvées", "Conflits", "FIN")):
            logger.info("  %s", line.strip())
    if result.returncode != 0:
        logger.error("Erreur re-normalisation: %s", result.stderr[-500:])


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch brutes PMU avec extras manquants")
    parser.add_argument("--pause", type=float, default=0.5, help="Pause inter-jour (s)")
    parser.add_argument("--batch", type=int, default=200, help="Sauvegarde intermédiaire tous les N jours")
    parser.add_argument("--no-renorm", action="store_true", help="Skip re-normalisation finale")
    args = parser.parse_args()

    logger = setup_logging("patch_pmu")
    session = build_session()

    logger.info("=" * 60)
    logger.info("PATCH BRUTES PMU — Enrichissement extras")
    logger.info("=" * 60)

    # Charger brutes
    with open(BRUTES_PATH, "r", encoding="utf-8") as f:
        brutes = json.load(f)
    logger.info("Chargé: %d brutes", len(brutes))

    # Identifier les brutes PMU à patcher (groupées par jour)
    jours_brutes: dict[str, dict[int, dict]] = {}  # jour -> {numero: brute_dict}
    for b in brutes:
        if b.get("source") != "pmu":
            continue
        jour = b.get("date_reunion_brut", "")
        if not jour:
            continue
        # Déjà patché ?
        if isinstance(b.get("extras"), dict) and "condition" in b["extras"]:
            continue
        if jour not in jours_brutes:
            jours_brutes[jour] = {}
        num = b.get("numero_reunion_brut")
        if num is not None:
            jours_brutes[jour][int(num)] = b

    # Checkpoint
    done = load_checkpoint()
    jours_a_patcher = sorted(j for j in jours_brutes if j not in done)
    logger.info("Jours à patcher: %d (déjà faits: %d)", len(jours_a_patcher), len(done))

    if not jours_a_patcher:
        logger.info("Rien à patcher !")
        return

    # Patch
    total_patched = 0
    for i, jour in enumerate(jours_a_patcher, 1):
        n = patch_jour(session, jour, jours_brutes[jour], logger)
        total_patched += n
        done.add(jour)

        if i % 50 == 0:
            logger.info("  [%d/%d] %d brutes patchées au total", i, len(jours_a_patcher), total_patched)

        # Sauvegarde intermédiaire
        if i % args.batch == 0:
            sauver_brutes(brutes, logger)
            save_checkpoint(done)
            logger.info("  >>> Sauvegarde intermédiaire: %d/%d jours, %d patchées <<<",
                        i, len(jours_a_patcher), total_patched)

        if i < len(jours_a_patcher):
            time.sleep(args.pause)

    # Sauvegarde finale
    sauver_brutes(brutes, logger)
    save_checkpoint(done)
    logger.info("Patch terminé: %d brutes patchées sur %d jours", total_patched, len(jours_a_patcher))

    # Re-normaliser
    if not args.no_renorm:
        renormaliser(logger)

    logger.info("=" * 60)
    logger.info("FIN PATCH BRUTES PMU")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
