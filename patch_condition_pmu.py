#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_condition_pmu.py — Enrichit condition, type_piste, corde via l'API PMU.

L'API PMU programme contient :
  - penetrometre.intitule → condition (ex: "Très souple", "Bon", "Léger")
  - typePiste → type de piste (ex: "HERBE", "SABLE", "PSF")
  - corde → direction corde (ex: "CORDE_GAUCHE", "CORDE_DROITE")

Une seule requête par jour couvre toutes les réunions.

Usage :
    python3 patch_condition_pmu.py [--pause 0.3] [--batch 200]
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===========================================================================
# CONFIG
# ===========================================================================

NORMALISEES_PATH = Path(os.path.join(BASE_DIR, "output", "01_calendrier_reunions", "reunions_normalisees.json"))
CACHE_PATH = Path(os.path.join(BASE_DIR, "output", "01_calendrier_reunions", "pmu_condition_cache.json"))
PMU_URL = "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date_ddmmyyyy}"
# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# HTTP
# ===========================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ===========================================================================
# CACHE
# ===========================================================================

class PmuConditionCache:
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
# EXTRACTION
# ===========================================================================

def extraire_condition_pmu(programme_data: dict) -> dict[str, dict]:
    """
    Extrait condition/type_piste/corde depuis le JSON PMU programme.
    Retourne {hippo_lower_Rnum: {condition, type_piste, corde, penetrometre, ...}}.
    """
    result = {}
    reunions = programme_data.get("programme", {}).get("reunions", [])

    for reunion in reunions:
        hippo_info = reunion.get("hippodrome", {})
        hippo_nom = hippo_info.get("libelleCourt", "") or hippo_info.get("libelleLong", "")
        num_officiel = reunion.get("numOfficiel", 0)

        if not hippo_nom:
            continue

        courses = reunion.get("courses", [])
        if not courses:
            continue

        # Prendre la C1 pour les infos terrain (identique pour toutes les courses)
        c1 = courses[0]
        extras: dict = {}

        # Condition depuis pénétromètre
        penetro = c1.get("penetrometre", {})
        if penetro:
            intitule = penetro.get("intitule", "").strip()
            if intitule:
                extras["condition"] = intitule
            valeur = penetro.get("valeurMesure", "")
            if valeur:
                extras["penetrometre_valeur"] = valeur

        # Type de piste
        type_piste = c1.get("typePiste", "")
        if type_piste:
            extras["type_piste"] = type_piste.lower()

        # Corde
        corde = c1.get("corde", "")
        if corde:
            if "GAUCHE" in corde:
                extras["corde"] = "gauche"
            elif "DROITE" in corde:
                extras["corde"] = "droite"

        # Aussi récupérer pour les autres courses si C1 n'a pas ces infos
        if not extras.get("condition"):
            for c in courses[1:]:
                penetro = c.get("penetrometre", {})
                if penetro and penetro.get("intitule"):
                    extras["condition"] = penetro["intitule"].strip()
                    if penetro.get("valeurMesure"):
                        extras["penetrometre_valeur"] = penetro["valeurMesure"]
                    break

        if not extras.get("type_piste"):
            for c in courses[1:]:
                tp = c.get("typePiste", "")
                if tp:
                    extras["type_piste"] = tp.lower()
                    break

        if not extras.get("corde"):
            for c in courses[1:]:
                crd = c.get("corde", "")
                if crd:
                    extras["corde"] = "gauche" if "GAUCHE" in crd else "droite" if "DROITE" in crd else ""
                    break

        key = f"{hippo_nom.strip().lower()}_R{num_officiel}"
        result[key] = extras

    return result


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Patch condition via API PMU")
    parser.add_argument("--pause", type=float, default=0.3, help="Pause entre requêtes (s)")
    parser.add_argument("--batch", type=int, default=200, help="Sauvegarder tous les N jours")
    args = parser.parse_args()

    logger = setup_logging("patch_condition_pmu")

    logger.info("=" * 60)
    logger.info("PATCH CONDITION/CORDE VIA API PMU")
    logger.info("=" * 60)

    # Charger normalisées
    with open(NORMALISEES_PATH, "r", encoding="utf-8") as f:
        reunions = json.load(f)
    logger.info("Chargées: %d réunions normalisées", len(reunions))

    # Index des réunions par (date, hippo_normalise) pour matching rapide
    reunions_index: dict[str, list[int]] = {}
    dates_a_requeter: set[str] = set()

    for i, r in enumerate(reunions):
        needs_enrichment = (
            not r.get("condition") or
            not r.get("corde_piste") or
            not r.get("type_piste")
        )
        if not needs_enrichment:
            continue

        date_iso = r.get("date_reunion_iso", "")
        if not date_iso:
            continue

        hippo = (r.get("hippodrome_normalise", "") or r.get("hippodrome", "")).strip().lower()
        num = r.get("numero_reunion") or 0
        key = f"{date_iso}_{hippo}_R{num}"
        reunions_index.setdefault(key, []).append(i)
        dates_a_requeter.add(date_iso)

    dates_sorted = sorted(dates_a_requeter)
    logger.info("Réunions à enrichir: %d sur %d jours", len(reunions_index), len(dates_sorted))

    cache = PmuConditionCache(CACHE_PATH)
    logger.info("Cache existant: %d entrées", len(cache))

    session = create_session()
    enrichies = 0
    erreurs = 0
    requetes = 0

    for idx, date_iso in enumerate(dates_sorted, 1):
        cache_key = f"day_{date_iso}"

        # Check cache
        cached = cache.get(cache_key)
        if cached is not None:
            # Appliquer depuis le cache
            for hippo_key, extras in cached.items():
                _appliquer_extras(reunions, reunions_index, date_iso, hippo_key, extras)
            continue

        # Requêter API PMU
        date_obj = date.fromisoformat(date_iso)
        url = PMU_URL.format(date_ddmmyyyy=date_obj.strftime("%d%m%Y"))
        try:
            resp = session.get(url, timeout=10)
            requetes += 1
            if resp.status_code == 404:
                cache.put(cache_key, {})
                continue
            resp.raise_for_status()

            data = resp.json()
            pmu_extras = extraire_condition_pmu(data)
            cache.put(cache_key, pmu_extras)

            for hippo_key, extras in pmu_extras.items():
                matched = _appliquer_extras(reunions, reunions_index, date_iso, hippo_key, extras)
                enrichies += matched

        except requests.exceptions.RequestException as e:
            logger.warning("  Erreur %s: %s", date_iso, str(e)[:80])
            erreurs += 1
            cache.put(cache_key, {})
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("  Erreur parsing %s: %s", date_iso, str(e)[:80])
            cache.put(cache_key, {})

        if idx % 100 == 0:
            logger.info("  [%d/%d] enrichies=%d, erreurs=%d, req=%d",
                        idx, len(dates_sorted), enrichies, erreurs, requetes)

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
    logger.info("TERMINÉ: %d enrichies, %d erreurs, %d requêtes", enrichies, erreurs, requetes)
    logger.info("=" * 60)


def _normaliser_hippo(nom: str) -> str:
    """Normalise basique pour matching."""
    import unicodedata
    nom = nom.strip().lower()
    # Retirer accents
    nfkd = unicodedata.normalize('NFKD', nom)
    nom = ''.join(c for c in nfkd if not unicodedata.combining(c))
    # Retirer ponctuation
    nom = nom.replace("-", " ").replace("'", " ").replace("/", " ")
    nom = " ".join(nom.split())
    return nom


def _appliquer_extras(
    reunions: list[dict],
    reunions_index: dict[str, list[int]],
    date_iso: str,
    hippo_key: str,  # "hippo_lower_Rnum" depuis PMU
    extras: dict,
) -> int:
    """Applique les extras PMU aux réunions normalisées. Retourne le nombre enrichi."""
    matched = 0

    # Extraire hippo et num depuis la clé PMU
    parts = hippo_key.rsplit("_R", 1)
    hippo_pmu = _normaliser_hippo(parts[0]) if parts else ""
    num_pmu = int(parts[1]) if len(parts) > 1 else 0

    # Essayer de matcher par date + hippo + num
    for key, indices in reunions_index.items():
        if not key.startswith(date_iso + "_"):
            continue

        # Extraire hippo et num de la clé normalisée
        key_parts = key.split("_", 1)[1].rsplit("_R", 1)
        hippo_norm = key_parts[0] if key_parts else ""
        num_norm = int(key_parts[1]) if len(key_parts) > 1 else 0

        # Match par numéro ET hippo similaire
        if num_pmu > 0 and num_norm > 0 and num_pmu == num_norm:
            for i in indices:
                _appliquer_un(reunions[i], extras)
                matched += 1
        elif hippo_pmu and hippo_norm and hippo_pmu in hippo_norm or hippo_norm in hippo_pmu:
            for i in indices:
                _appliquer_un(reunions[i], extras)
                matched += 1

    return matched


def _appliquer_un(reunion: dict, extras: dict) -> None:
    """Applique les extras à une réunion."""
    if extras.get("condition") and not reunion.get("condition"):
        reunion["condition"] = extras["condition"]
    if extras.get("type_piste") and not reunion.get("type_piste"):
        reunion["type_piste"] = extras["type_piste"]
    if extras.get("corde") and not reunion.get("corde_piste"):
        reunion["corde_piste"] = extras["corde"]
    if extras.get("penetrometre_valeur") and not reunion.get("penetrometre"):
        reunion["penetrometre"] = extras["penetrometre_valeur"]


if __name__ == "__main__":
    main()
