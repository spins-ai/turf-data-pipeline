#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrichissement_meteo_nasa.py — Enrichissement météo via NASA POWER (gratuit, sans limite).

Alternative à Open-Meteo pour éviter les rate limits.
NASA POWER fournit des données satellite/modèle depuis 1981.
10 ans de données par coordonnée en ~2 secondes.

Usage :
    python3 enrichissement_meteo_nasa.py \\
        --input output/01_calendrier_reunions/reunions_normalisees.json \\
        --output output/01_calendrier_reunions/reunions_normalisees_meteo.json \\
        [--pause 1.0] [--export-csv] [--export-parquet]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date as dt_date
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hippodromes_db import get_hippodrome_info

# ===========================================================================
# CONFIGURATION
# ===========================================================================

NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Variables NASA POWER demandées
NASA_PARAMS = [
    "T2M_MAX",          # Température max 2m (°C)
    "T2M_MIN",          # Température min 2m (°C)
    "T2M",              # Température moyenne 2m (°C)
    "PRECTOTCORR",      # Précipitations corrigées (mm/jour)
    "WS10M_MAX",        # Vent max 10m (m/s)
    "WS10M_MIN",        # Vent min 10m (m/s)
    "WD10M",            # Direction vent dominante 10m (degrés)
    "RH2M",             # Humidité relative 2m (%)
    "ALLSKY_SFC_SW_DWN", # Rayonnement solaire (kW-hr/m²/jour)
]

# Mapping direction vent (degrés → texte)
def _direction_vent_texte(deg: Optional[float]) -> str:
    if deg is None:
        return ""
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSO", "SO", "OSO", "O", "ONO", "NO", "NNO"]
    idx = round(deg / 22.5) % 16
    return directions[idx]


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv, sauver_parquet


# ===========================================================================
# HTTP
# ===========================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504, 429])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ===========================================================================
# CACHE
# ===========================================================================

class NasaCache:
    """Cache local pour éviter les requêtes redondantes."""

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
# FETCHER NASA POWER
# ===========================================================================

def fetch_nasa_coord(
    session: requests.Session,
    lat: float,
    lon: float,
    date_start: str,
    date_end: str,
    logger: logging.Logger,
    timeout: int = 60,
) -> dict[str, dict]:
    """
    Récupère les données météo NASA POWER pour une coordonnée sur toute la plage.
    Retourne {date_YYYYMMDD: {variables}}.
    """
    params = {
        "parameters": ",".join(NASA_PARAMS),
        "community": "RE",
        "longitude": round(lon, 2),
        "latitude": round(lat, 2),
        "start": date_start.replace("-", ""),
        "end": date_end.replace("-", ""),
        "format": "JSON",
    }

    resp = session.get(NASA_POWER_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    props = data.get("properties", {}).get("parameter", {})

    # Convertir au format date ISO
    result: dict[str, dict] = {}
    # Récupérer les clés de dates depuis la première variable
    first_var = next(iter(props.values()), {})
    for date_key in first_var:
        date_iso = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
        meteo_jour = {}
        for var_name in NASA_PARAMS:
            val = props.get(var_name, {}).get(date_key)
            # NASA POWER utilise -999 pour les valeurs manquantes
            if val is not None and val != -999:
                meteo_jour[var_name] = val
            else:
                meteo_jour[var_name] = None
        result[date_iso] = meteo_jour

    return result


# ===========================================================================
# ENRICHISSEMENT
# ===========================================================================

def enrichir_reunions(
    reunions: list[dict],
    session: requests.Session,
    cache: NasaCache,
    logger: logging.Logger,
    pause: float = 1.0,
) -> list[dict]:
    """Enrichit chaque réunion avec les données météo NASA POWER."""

    # 1. Regrouper les (lat, lon, dates) uniques
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

        if cache.get(lat, lon, date_str) is not None:
            continue

        key = (round(lat, 2), round(lon, 2))
        if key not in a_requeter:
            a_requeter[key] = set()
        a_requeter[key].add(date_str)

    logger.info(
        "Enrichissement NASA POWER: %d réunions, %d coordonnées à requêter, %d sans coordonnées",
        len(reunions), len(a_requeter), sans_coords,
    )

    # 2. Requêter par coordonnée (10 ans d'un coup !)
    total = len(a_requeter)
    erreurs = 0

    for idx, ((lat, lon), dates) in enumerate(a_requeter.items(), 1):
        dates_sorted = sorted(dates)
        date_min = dates_sorted[0]
        date_max = dates_sorted[-1]

        logger.info(
            "  [%d/%d] NASA POWER (%.2f, %.2f) : %s → %s (%d jours)",
            idx, total, lat, lon, date_min, date_max, len(dates),
        )

        try:
            raw_batch = fetch_nasa_coord(
                session, lat, lon, date_min, date_max, logger,
            )
            for d, raw in raw_batch.items():
                cache.put(lat, lon, d, raw)

        except requests.exceptions.RequestException as e:
            logger.error("  Erreur API NASA (%.2f, %.2f): %s", lat, lon, e)
            erreurs += 1
        except Exception as e:
            logger.error("  Erreur inattendue (%.2f, %.2f): %s", lat, lon, e)
            erreurs += 1

        # Sauvegarde cache tous les 20 coordonnées
        if idx % 20 == 0:
            cache.save()
            logger.info("  Cache sauvegardé (%d entrées)", len(cache))

        if idx < total:
            time.sleep(pause)

    cache.save()
    logger.info("Cache final: %d entrées, %d erreurs", len(cache), erreurs)

    # 3. Enrichir chaque réunion
    enrichies = 0
    for i, lat, lon in reunions_avec_coords:
        r = reunions[i]
        date_str = r.get("date_reunion_iso", "")[:10]

        if lat == 0.0 and lon == 0.0:
            _set_meteo_vide(r)
            continue

        raw = cache.get(lat, lon, date_str)
        if raw:
            r["meteo_temperature_max"] = raw.get("T2M_MAX")
            r["meteo_temperature_min"] = raw.get("T2M_MIN")
            r["meteo_temperature_moyenne"] = raw.get("T2M")
            r["meteo_precipitation_mm"] = raw.get("PRECTOTCORR")
            r["meteo_pluie_mm"] = raw.get("PRECTOTCORR")  # NASA = précip totale
            r["meteo_vent_max_kmh"] = round(raw["WS10M_MAX"] * 3.6, 1) if raw.get("WS10M_MAX") else None
            r["meteo_rafales_max_kmh"] = None  # NASA n'a pas les rafales séparément
            r["meteo_direction_vent"] = raw.get("WD10M")
            r["meteo_direction_vent_texte"] = _direction_vent_texte(raw.get("WD10M"))
            r["meteo_humidite_pct"] = raw.get("RH2M")
            r["meteo_code_wmo"] = None  # Pas de code WMO dans NASA POWER
            r["meteo_description"] = ""
            r["meteo_source"] = "nasa-power"
            enrichies += 1
        else:
            _set_meteo_vide(r)

    logger.info("Enrichissement terminé: %d/%d réunions avec météo", enrichies, len(reunions))
    return reunions


def _set_meteo_vide(r: dict) -> None:
    r["meteo_temperature_max"] = None
    r["meteo_temperature_min"] = None
    r["meteo_temperature_moyenne"] = None
    r["meteo_precipitation_mm"] = None
    r["meteo_pluie_mm"] = None
    r["meteo_vent_max_kmh"] = None
    r["meteo_rafales_max_kmh"] = None
    r["meteo_direction_vent"] = None
    r["meteo_direction_vent_texte"] = ""
    r["meteo_humidite_pct"] = None
    r["meteo_code_wmo"] = None
    r["meteo_description"] = ""
    r["meteo_source"] = ""


# ===========================================================================
# SAUVEGARDE
# ===========================================================================


# ===========================================================================
# CLI & MAIN
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Enrichissement météo via NASA POWER")
    parser.add_argument("--input", "-i", type=Path,
                        default=Path(__file__).resolve().parent / "output" / "01_calendrier_reunions" / "reunions_normalisees.json")
    parser.add_argument("--output", "-o", type=Path,
                        default=Path(__file__).resolve().parent / "output" / "01_calendrier_reunions" / "reunions_normalisees_meteo.json")
    parser.add_argument("--cache", type=Path,
                        default=Path(__file__).resolve().parent / "output" / "01_calendrier_reunions" / "nasa_meteo_cache.json")
    parser.add_argument("--pause", type=float, default=1.0, help="Pause entre requêtes (s)")
    parser.add_argument("--log-dir", type=Path, default=Path("logs"))
    parser.add_argument("--export-csv", action="store_true")
    parser.add_argument("--export-parquet", action="store_true")
    args = parser.parse_args()

    logger = setup_logging("enrichissement_meteo_nasa")

    logger.info("=" * 70)
    logger.info("ENRICHISSEMENT MÉTÉO VIA NASA POWER")
    logger.info("  Input:  %s", args.input)
    logger.info("  Output: %s", args.output)
    logger.info("  Cache:  %s", args.cache)
    logger.info("=" * 70)

    if not args.input.exists():
        logger.error("Fichier d'entrée introuvable: %s", args.input)
        sys.exit(1)

    with open(args.input, "r", encoding="utf-8") as f:
        reunions = json.load(f)
    logger.info("Chargé: %d réunions", len(reunions))

    session = create_session()
    cache = NasaCache(args.cache)
    logger.info("Cache existant: %d entrées", len(cache))

    t_start = time.monotonic()
    reunions = enrichir_reunions(reunions, session, cache, logger, pause=args.pause)

    sauver_json(reunions, args.output, logger)
    if args.export_csv:
        sauver_csv(reunions, args.output.with_suffix(".csv"), logger)
    if args.export_parquet:
        sauver_parquet(reunions, args.output.with_suffix(".parquet"), logger)

    elapsed = time.monotonic() - t_start
    logger.info("=" * 70)
    logger.info("TERMINÉ en %.1f secondes (%.1f minutes)", elapsed, elapsed / 60)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
