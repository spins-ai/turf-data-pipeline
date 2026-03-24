#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
00_enrichissement_meteo.py — Enrichissement météo des réunions normalisées.

Lit les réunions normalisées produites par 01_calendrier_reunions.py,
récupère les données météo historiques via l'API gratuite Open-Meteo,
et enrichit chaque réunion avec les conditions météo du jour et du lieu.

API utilisée : https://open-meteo.com/en/docs/historical-weather-api
  - Gratuite, sans clé API
  - Historique disponible depuis 1940
  - Données journalières : température, précipitations, vent, etc.

Usage :
    python3 00_enrichissement_meteo.py \\
        --input output/01_calendrier_reunions/reunions_normalisees.json \\
        --output output/01_calendrier_reunions/reunions_normalisees_meteo.json \\
        [--cache meteo_cache.json] \\
        [--pause 0.3] \\
        [--retry 3]

Le script utilise un cache local pour éviter de re-requêter les mêmes
coordonnées + dates. Le cache est sauvegardé atomiquement.
"""

from __future__ import annotations

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hippodromes_db import get_hippodrome_info
from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv, sauver_parquet
from utils.scraping import create_session
# NOTE: local create_session had signature (retry_max: int = 3, backoff: float = 1.0), now uses utils.scraping version

# ===========================================================================
# CONFIGURATION
# ===========================================================================

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Variables météo demandées (journalières)
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "windspeed_10m_max",
    "windgusts_10m_max",
    "winddirection_10m_dominant",
    "weathercode",
    "et0_fao_evapotranspiration",
]

# Codes météo WMO → description lisible
WMO_CODES: dict[int, str] = {
    0: "ciel_degagé",
    1: "principalement_degagé",
    2: "partiellement_nuageux",
    3: "couvert",
    45: "brouillard",
    48: "brouillard_givrant",
    51: "bruine_legere",
    53: "bruine_moderee",
    55: "bruine_forte",
    56: "bruine_verglacante_legere",
    57: "bruine_verglacante_forte",
    61: "pluie_legere",
    63: "pluie_moderee",
    65: "pluie_forte",
    66: "pluie_verglacante_legere",
    67: "pluie_verglacante_forte",
    71: "neige_legere",
    73: "neige_moderee",
    75: "neige_forte",
    77: "grains_de_neige",
    80: "averses_legeres",
    81: "averses_moderees",
    82: "averses_violentes",
    85: "averses_neige_legeres",
    86: "averses_neige_fortes",
    95: "orage",
    96: "orage_grele_legere",
    99: "orage_grele_forte",
}


# ===========================================================================
# DATA MODELS
# ===========================================================================

@dataclass
class MeteoJour:
    """Données météo d'une journée pour un lieu donné."""
    latitude: float
    longitude: float
    date: str
    temperature_max: Optional[float] = None
    temperature_min: Optional[float] = None
    temperature_moyenne: Optional[float] = None
    precipitation_mm: Optional[float] = None
    pluie_mm: Optional[float] = None
    vent_max_kmh: Optional[float] = None
    rafales_max_kmh: Optional[float] = None
    direction_vent_dominante: Optional[int] = None
    code_meteo_wmo: Optional[int] = None
    description_meteo: str = ""
    evapotranspiration_mm: Optional[float] = None


# ===========================================================================
# CLIENT HTTP
# ===========================================================================


class MeteoCache:
    """Cache local persistant pour éviter les requêtes redondantes."""

    def __init__(self, fichier: Path):
        self.fichier = fichier
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if self.fichier.exists():
            try:
                with open(self.fichier, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._data = {}

    def _make_key(self, lat: float, lon: float, date_str: str) -> str:
        return f"{lat:.2f}_{lon:.2f}_{date_str}"

    def get(self, lat: float, lon: float, date_str: str) -> Optional[dict]:
        return self._data.get(self._make_key(lat, lon, date_str))

    def put(self, lat: float, lon: float, date_str: str, meteo: dict) -> None:
        self._data[self._make_key(lat, lon, date_str)] = meteo

    def save(self) -> None:
        self.fichier.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.fichier.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False)
        tmp.replace(self.fichier)

    def __len__(self) -> int:
        return len(self._data)


# ===========================================================================
# FETCHER OPEN-METEO
# ===========================================================================

def fetch_meteo_batch(
    session: requests.Session,
    lat: float,
    lon: float,
    date_debut: str,
    date_fin: str,
    timeout: int = 30,
    max_retries: int = 8,
) -> dict[str, dict]:
    """
    Récupère la météo pour un lieu sur une plage de dates.
    Open-Meteo accepte des plages, donc on batch par coordonnées.
    Retourne {date_iso: {variables météo}}.
    Gère le rate-limiting 429 avec backoff exponentiel.
    """
    params = {
        "latitude": round(lat, 2),
        "longitude": round(lon, 2),
        "start_date": date_debut,
        "end_date": date_fin,
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "auto",
    }

    for attempt in range(max_retries):
        resp = session.get(OPEN_METEO_URL, params=params, timeout=timeout)
        if resp.status_code == 429:
            wait = 200  # Toujours 200s — le rate-limit Open-Meteo est strict
            logging.getLogger("enrichissement_meteo").warning(
                "    429 rate-limit, attente %ds (tentative %d/%d)",
                wait, attempt + 1, max_retries,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        raise requests.exceptions.RequestException(
            f"429 persistant après {max_retries} tentatives pour ({lat}, {lon})"
        )

    data = resp.json()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    result: dict[str, dict] = {}

    for i, d in enumerate(dates):
        meteo_jour = {}
        for var in DAILY_VARIABLES:
            values = daily.get(var, [])
            meteo_jour[var] = values[i] if i < len(values) else None
        result[d] = meteo_jour

    return result


def build_meteo_jour(
    lat: float, lon: float, date_str: str, raw: dict,
) -> MeteoJour:
    """Construit un MeteoJour à partir des données brutes Open-Meteo."""
    code_wmo = raw.get("weathercode")
    return MeteoJour(
        latitude=lat,
        longitude=lon,
        date=date_str,
        temperature_max=raw.get("temperature_2m_max"),
        temperature_min=raw.get("temperature_2m_min"),
        temperature_moyenne=raw.get("temperature_2m_mean"),
        precipitation_mm=raw.get("precipitation_sum"),
        pluie_mm=raw.get("rain_sum"),
        vent_max_kmh=raw.get("windspeed_10m_max"),
        rafales_max_kmh=raw.get("windgusts_10m_max"),
        direction_vent_dominante=raw.get("winddirection_10m_dominant"),
        code_meteo_wmo=code_wmo,
        description_meteo=WMO_CODES.get(code_wmo, "") if code_wmo is not None else "",
        evapotranspiration_mm=raw.get("et0_fao_evapotranspiration"),
    )


# ===========================================================================
# ENRICHISSEMENT PRINCIPAL
# ===========================================================================

def enrichir_reunions(
    reunions: list[dict],
    session: requests.Session,
    cache: MeteoCache,
    logger: logging.Logger,
    pause: float = 0.3,
) -> list[dict]:
    """
    Enrichit chaque réunion avec les données météo.
    Regroupe les requêtes par coordonnées pour minimiser les appels API.
    """
    # 1. Regrouper les (lat, lon, date) uniques à requêter
    a_requeter: dict[tuple[float, float], set[str]] = {}
    reunions_avec_coords: list[tuple[int, float, float]] = []
    sans_coords = 0

    for i, r in enumerate(reunions):
        hippo = r.get("hippodrome_normalise", "") or r.get("hippodrome", "")
        info = get_hippodrome_info(hippo)
        if not info:
            sans_coords += 1
            reunions_avec_coords.append((i, 0.0, 0.0))
            continue

        lat = info["lat"]
        lon = info["lon"]
        reunions_avec_coords.append((i, lat, lon))
        date_str = r.get("date_reunion_iso", "")[:10]

        if not date_str:
            continue

        # Déjà en cache ?
        if cache.get(lat, lon, date_str) is not None:
            continue

        key = (round(lat, 2), round(lon, 2))
        if key not in a_requeter:
            a_requeter[key] = set()
        a_requeter[key].add(date_str)

    logger.info(
        "Enrichissement météo: %d réunions, %d coordonnées uniques à requêter, "
        "%d sans coordonnées (ignorées)",
        len(reunions), len(a_requeter), sans_coords,
    )

    # 2. Requêter par batch de coordonnées
    total_requetes = len(a_requeter)
    erreurs = 0
    erreurs_429_consecutives = 0
    MAX_429_CONSECUTIVES = 2  # Arrêt si le quota journalier est épuisé
    poids_api_consomme = 0
    QUOTA_JOURNALIER = 7000  # Limite conservatrice (vrai quota = 10 000)

    for idx, ((lat, lon), dates) in enumerate(a_requeter.items(), 1):
        if poids_api_consomme >= QUOTA_JOURNALIER:
            logger.warning(
                "QUOTA JOURNALIER ATTEINT (~%d appels pondérés). "
                "Sauvegarde du cache et arrêt. Relancez demain.",
                poids_api_consomme,
            )
            cache.save()
            break
        dates_sorted = sorted(dates)
        date_min = dates_sorted[0]
        date_max = dates_sorted[-1]

        logger.info(
            "  [%d/%d] Météo (%.2f, %.2f) : %s → %s (%d jours)",
            idx, total_requetes, lat, lon, date_min, date_max, len(dates),
        )

        # Découper en chunks annuels pour éviter les 429 (requêtes trop lourdes)
        try:
            from datetime import date as dt_date
            d_min = dt_date.fromisoformat(date_min)
            d_max = dt_date.fromisoformat(date_max)
            chunk_start = d_min
            chunks_fetched = 0
            while chunk_start <= d_max:
                chunk_end = min(
                    dt_date(chunk_start.year, 12, 31),
                    d_max,
                )
                # Skip chunk si TOUTES les dates de ce chunk sont déjà en cache
                chunk_dates_needed = [
                    d for d in dates
                    if chunk_start.isoformat() <= d <= chunk_end.isoformat()
                ]
                all_cached = all(
                    cache.get(lat, lon, d) is not None for d in chunk_dates_needed
                )
                if all_cached and chunk_dates_needed:
                    logger.info("    Chunk %d skip (déjà en cache)", chunk_start.year)
                    chunk_start = dt_date(chunk_start.year + 1, 1, 1)
                    continue

                raw_batch = fetch_meteo_batch(
                    session, lat, lon,
                    chunk_start.isoformat(), chunk_end.isoformat(),
                )
                for d, raw in raw_batch.items():
                    cache.put(lat, lon, d, raw)
                chunks_fetched += 1
                erreurs_429_consecutives = 0  # Reset si succès
                # Estimer le poids API de ce chunk
                days_in_chunk = (chunk_end - chunk_start).days + 1
                poids_api_consomme += (days_in_chunk / 14) * (len(DAILY_VARIABLES) / 10)
                chunk_start = dt_date(chunk_start.year + 1, 1, 1)
                if chunk_start <= d_max:
                    time.sleep(pause)  # pause entre chunks
        except requests.exceptions.RequestException as e:
            logger.error("  Erreur API météo (%.2f, %.2f): %s", lat, lon, e)
            erreurs += 1
            if "429" in str(e):
                erreurs_429_consecutives += 1
                if erreurs_429_consecutives >= MAX_429_CONSECUTIVES:
                    logger.warning(
                        "QUOTA JOURNALIER ÉPUISÉ — %d erreurs 429 consécutives. "
                        "Sauvegarde du cache et arrêt. Relancez demain.",
                        erreurs_429_consecutives,
                    )
                    cache.save()
                    break
        except Exception as e:
            logger.error("  Erreur inattendue (%.2f, %.2f): %s", lat, lon, e)
            erreurs += 1

        # Sauvegarde cache intermédiaire tous les 5 coordonnées
        if idx % 5 == 0:
            cache.save()
            logger.info("  Cache sauvegardé (%d entrées)", len(cache))

        # Pause inter-coordonnée (plus longue que inter-chunk)
        if idx < total_requetes:
            time.sleep(pause * 3)

    cache.save()
    logger.info("Cache final: %d entrées, %d erreurs API", len(cache), erreurs)

    # 3. Enrichir chaque réunion
    enrichies = 0
    for i, lat, lon in reunions_avec_coords:
        r = reunions[i]
        date_str = r.get("date_reunion_iso", "")[:10]

        if lat == 0.0 and lon == 0.0:
            # Pas de coordonnées → champs météo vides
            r["meteo_temperature_max"] = None
            r["meteo_temperature_min"] = None
            r["meteo_temperature_moyenne"] = None
            r["meteo_precipitation_mm"] = None
            r["meteo_pluie_mm"] = None
            r["meteo_vent_max_kmh"] = None
            r["meteo_rafales_max_kmh"] = None
            r["meteo_direction_vent"] = None
            r["meteo_code_wmo"] = None
            r["meteo_description"] = ""
            r["meteo_source"] = ""
            continue

        raw = cache.get(lat, lon, date_str)
        if raw:
            meteo = build_meteo_jour(lat, lon, date_str, raw)
            r["meteo_temperature_max"] = meteo.temperature_max
            r["meteo_temperature_min"] = meteo.temperature_min
            r["meteo_temperature_moyenne"] = meteo.temperature_moyenne
            r["meteo_precipitation_mm"] = meteo.precipitation_mm
            r["meteo_pluie_mm"] = meteo.pluie_mm
            r["meteo_vent_max_kmh"] = meteo.vent_max_kmh
            r["meteo_rafales_max_kmh"] = meteo.rafales_max_kmh
            r["meteo_direction_vent"] = meteo.direction_vent_dominante
            r["meteo_code_wmo"] = meteo.code_meteo_wmo
            r["meteo_description"] = meteo.description_meteo
            r["meteo_source"] = "open-meteo"
            enrichies += 1
        else:
            r["meteo_temperature_max"] = None
            r["meteo_temperature_min"] = None
            r["meteo_temperature_moyenne"] = None
            r["meteo_precipitation_mm"] = None
            r["meteo_pluie_mm"] = None
            r["meteo_vent_max_kmh"] = None
            r["meteo_rafales_max_kmh"] = None
            r["meteo_direction_vent"] = None
            r["meteo_code_wmo"] = None
            r["meteo_description"] = ""
            r["meteo_source"] = ""

    logger.info("Enrichissement terminé: %d/%d réunions avec météo", enrichies, len(reunions))
    return reunions


# ===========================================================================
# SAUVEGARDE
# ===========================================================================


# ===========================================================================
# CLI & MAIN
# ===========================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrichissement météo des réunions normalisées",
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=Path(__file__).resolve().parent / "../../output" / "01_calendrier_reunions" / "reunions_normalisees.json",
        help="Fichier JSON des réunions normalisées (entrée)",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path(__file__).resolve().parent / "../../output" / "01_calendrier_reunions" / "reunions_normalisees_meteo.json",
        help="Fichier JSON enrichi (sortie)",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path(__file__).resolve().parent / "../../output" / "01_calendrier_reunions" / "meteo_cache.json",
        help="Fichier de cache météo",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=0.3,
        help="Pause entre requêtes API (secondes)",
    )
    parser.add_argument(
        "--retry",
        type=int,
        default=3,
        help="Nombre de retries par requête",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("../../logs"),
        help="Dossier des logs",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Exporter aussi en CSV",
    )
    parser.add_argument(
        "--export-parquet",
        action="store_true",
        help="Exporter aussi en Parquet",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = setup_logging("enrichissement_meteo", log_dir=args.log_dir)

    logger.info("=" * 70)
    logger.info("ENRICHISSEMENT MÉTÉO DES RÉUNIONS")
    logger.info("  Input:  %s", args.input)
    logger.info("  Output: %s", args.output)
    logger.info("  Cache:  %s", args.cache)
    logger.info("=" * 70)

    # Charger les réunions
    if not args.input.exists():
        logger.error("Fichier d'entrée introuvable: %s", args.input)
        sys.exit(1)

    with open(args.input, "r", encoding="utf-8") as f:
        reunions = json.load(f)
    logger.info("Chargé: %d réunions depuis %s", len(reunions), args.input.name)

    # Session HTTP + Cache
    session = create_session(retry_max=args.retry)
    cache = MeteoCache(args.cache)
    logger.info("Cache existant: %d entrées", len(cache))

    t_start = time.monotonic()

    # Enrichissement
    reunions = enrichir_reunions(reunions, session, cache, logger, pause=args.pause)

    # Sauvegarde
    sauver_json(reunions, args.output, logger)

    if args.export_csv:
        sauver_csv(reunions, args.output.with_suffix(".csv"), logger)
    if args.export_parquet:
        sauver_parquet(reunions, args.output.with_suffix(".parquet"), logger)

    elapsed = time.monotonic() - t_start
    logger.info("=" * 70)
    logger.info("TERMINÉ en %.1f secondes", elapsed)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
