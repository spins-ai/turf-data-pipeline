#!/usr/bin/env python3
"""
04_resultats.py
===============
Collecte des rapports définitifs (résultats officiels / cotes finales) par course.

Source :
  - PMU (API JSON) : /programme/{DDMMYYYY}/R{num}/C{num}/rapports-definitifs

Produit :
  - rapports_brut.json — réponses API brutes par course
  - rapports_normalises.json / .parquet / .csv — rapports normalisés
  - .checkpoint_04.json

Usage :
    python3 04_resultats.py
    python3 04_resultats.py --pause 0.3 --batch 500
    python3 04_resultats.py --date-debut 2024-01-01 --date-fin 2024-12-31
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ===========================================================================
# CONFIG
# ===========================================================================

REFERENCES_PATH = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "courses_references_04.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "04_resultats"

from utils.logging_setup import setup_logging
from utils.output import sauver_json, sauver_csv, sauver_parquet
from utils.types import utc_now_iso

PMU_API_BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/1/programme"
# Endpoint: {base}/{DDMMYYYY}/R{num}/C{num}/rapports-definitifs


# ===========================================================================
# DATACLASSES
# ===========================================================================

@dataclass
class RapportBrut:
    """Rapport tel que collecté depuis l'API."""
    source: str = ""
    course_uid: str = ""
    reunion_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    timestamp_collecte: str = ""
    type_pari: str = ""
    rapports_raw: list = field(default_factory=list)


@dataclass
class RapportNormalise:
    """Rapport normalisé pour le pipeline aval."""
    rapport_uid: str = ""
    course_uid: str = ""
    reunion_uid: str = ""
    date_reunion_iso: str = ""
    hippodrome_normalise: str = ""
    numero_reunion: int = 0
    numero_course: int = 0
    type_pari: str = ""
    combinaison: str = ""
    dividende_euros: Optional[float] = None
    nb_gagnants: Optional[int] = None
    timestamp_collecte: str = ""


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
# UTILITAIRES
# ===========================================================================

def make_uid(*parts: str) -> str:
    h = hashlib.blake2b("|".join(str(p) for p in parts).encode(), digest_size=8)
    return h.hexdigest()


# ===========================================================================
# CHECKPOINT
# ===========================================================================

class CheckpointManager:
    def __init__(self, path: Path):
        self.path = path
        self._data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        return {"completed_courses": []}

    def is_done(self, course_uid: str) -> bool:
        return course_uid in self._data.get("completed_courses", [])

    def mark_done(self, course_uid: str):
        self._data.setdefault("completed_courses", []).append(course_uid)

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False))
        tmp.replace(self.path)

    @property
    def count_done(self) -> int:
        return len(self._data.get("completed_courses", []))


# ===========================================================================
# NORMALISATION TYPE PARI
# ===========================================================================

TYPE_PARI_MAP = {
    "E_SIMPLE_GAGNANT": "simple_gagnant",
    "E_SIMPLE_PLACE": "simple_place",
    "E_COUPLE_GAGNANT": "couple_gagnant",
    "E_COUPLE_PLACE": "couple_place",
    "E_COUPLE_ORDRE": "couple_ordre",
    "E_TRIO": "trio",
    "E_TRIO_ORDRE": "trio_ordre",
    "E_DEUX_SUR_QUATRE": "deux_sur_quatre",
    "E_MULTI": "multi",
    "E_TIERCE": "tierce",
    "E_QUARTE": "quarte",
    "E_QUARTE_PLUS": "quarte",
    "E_QUINTE": "quinte",
    "E_QUINTE_PLUS": "quinte",
    "E_PICK5": "pick5",
    "E_SUPER4": "super4",
    # Variantes désordre
    "E_TIERCE_DESORDRE": "tierce_desordre",
    "E_QUARTE_DESORDRE": "quarte_desordre",
    "E_QUARTE_PLUS_DESORDRE": "quarte_desordre",
    "E_QUINTE_DESORDRE": "quinte_desordre",
    "E_QUINTE_PLUS_DESORDRE": "quinte_desordre",
    "E_TRIO_DESORDRE": "trio_desordre",
    "E_COUPLE_GAGNANT_INTERNATIONAL": "couple_gagnant",
    "E_COUPLE_PLACE_INTERNATIONAL": "couple_place",
    "E_SIMPLE_GAGNANT_INTERNATIONAL": "simple_gagnant",
    "E_SIMPLE_PLACE_INTERNATIONAL": "simple_place",
}


def normaliser_type_pari(raw: str) -> str:
    """Normalise un type de pari PMU en label court."""
    if not raw:
        return ""
    mapped = TYPE_PARI_MAP.get(raw)
    if mapped:
        return mapped
    # Fallback : retirer le préfixe E_ et mettre en minuscules
    clean = raw.strip()
    if clean.startswith("E_"):
        clean = clean[2:]
    return clean.lower()


# ===========================================================================
# FETCH
# ===========================================================================

def fetch_rapports_definitifs(
    session: requests.Session,
    date_ddmmyyyy: str,
    num_reunion: int,
    num_course: int,
    logger: logging.Logger,
) -> Optional[dict]:
    """Récupère les rapports définitifs d'une course."""
    url = f"{PMU_API_BASE}/{date_ddmmyyyy}/R{num_reunion}/C{num_course}/rapports-definitifs"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.warning("  HTTP error R%d/C%d rapports: %s", num_reunion, num_course, str(e)[:80])
        return None
    except json.JSONDecodeError as e:
        logger.warning("  JSON error R%d/C%d rapports: %s", num_reunion, num_course, str(e)[:80])
        return None


# ===========================================================================
# PARSING
# ===========================================================================

def parse_rapports(
    api_data: dict,
    ref: dict,
    timestamp: str,
) -> tuple[list[RapportBrut], list[RapportNormalise]]:
    """Parse la réponse API en RapportBrut + RapportNormalise."""
    course_uid = ref.get("course_uid", "")
    reunion_uid = ref.get("reunion_uid", "")
    date_iso = ref.get("date_reunion_iso", "")
    hippo = ref.get("hippodrome_normalise", "")
    num_reunion = ref.get("numero_reunion", 0)
    num_course = ref.get("numero_course", 0)

    rapports_definitifs = api_data.get("rapportsDefinitifs", [])

    bruts = []
    normalises = []

    for bloc in rapports_definitifs:
        type_pari_raw = bloc.get("typePari", "")
        rapports_list = bloc.get("rapports", [])

        # Brut : un enregistrement par type de pari
        brut = RapportBrut(
            source="pmu",
            course_uid=course_uid,
            reunion_uid=reunion_uid,
            date_reunion_iso=date_iso,
            hippodrome_normalise=hippo,
            numero_reunion=num_reunion,
            numero_course=num_course,
            timestamp_collecte=timestamp,
            type_pari=type_pari_raw,
            rapports_raw=rapports_list,
        )
        bruts.append(brut)

        # Normalisé : un enregistrement par combinaison
        type_pari_norm = normaliser_type_pari(type_pari_raw)

        for rap in rapports_list:
            combinaison = rap.get("combinaison", "")
            dividende_raw = rap.get("dividende")
            dividende_euros = dividende_raw / 100.0 if dividende_raw is not None else None
            nb_gagnants = rap.get("nombreGagnants") or rap.get("nbGagnants")

            rapport_uid = make_uid(
                date_iso, hippo, f"R{num_reunion}", f"C{num_course}",
                type_pari_norm, combinaison,
            )

            norm = RapportNormalise(
                rapport_uid=rapport_uid,
                course_uid=course_uid,
                reunion_uid=reunion_uid,
                date_reunion_iso=date_iso,
                hippodrome_normalise=hippo,
                numero_reunion=num_reunion,
                numero_course=num_course,
                type_pari=type_pari_norm,
                combinaison=combinaison,
                dividende_euros=dividende_euros,
                nb_gagnants=nb_gagnants,
                timestamp_collecte=timestamp,
            )
            normalises.append(norm)

    return bruts, normalises


# ===========================================================================
# SAUVEGARDE
# ===========================================================================





# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Collecte rapports définitifs PMU")
    parser.add_argument("--pause", type=float, default=0.3, help="Pause entre requêtes (s)")
    parser.add_argument("--batch", type=int, default=500, help="Sauvegarde intermédiaire tous les N courses")
    parser.add_argument("--date-debut", type=str, default=None, help="Date début (YYYY-MM-DD)")
    parser.add_argument("--date-fin", type=str, default=None, help="Date fin (YYYY-MM-DD)")
    parser.add_argument("--max-courses", type=int, default=0, help="Max courses à traiter (0=toutes)")
    args = parser.parse_args()

    logger = setup_logging("04_resultats")
    logger.info("=" * 70)
    logger.info("04 — COLLECTE RAPPORTS DEFINITIFS")
    logger.info("=" * 70)

    # Charger références
    if not REFERENCES_PATH.exists():
        logger.error("Fichier références introuvable: %s", REFERENCES_PATH)
        sys.exit(1)

    with open(REFERENCES_PATH, "r", encoding="utf-8") as f:
        all_refs = json.load(f)
    logger.info("Références chargées: %d courses", len(all_refs))

    # Filtrer par date si demandé
    refs = all_refs
    if args.date_debut:
        refs = [r for r in refs if r.get("date_reunion_iso", "") >= args.date_debut]
    if args.date_fin:
        refs = [r for r in refs if r.get("date_reunion_iso", "") <= args.date_fin]

    # Trier par date + réunion + course
    refs.sort(key=lambda r: (
        r.get("date_reunion_iso", ""),
        r.get("numero_reunion", 0),
        r.get("numero_course", 0),
    ))

    logger.info("Courses à traiter: %d", len(refs))

    if args.max_courses > 0:
        refs = refs[:args.max_courses]
        logger.info("Limité à %d courses", args.max_courses)

    # Checkpoint
    checkpoint = CheckpointManager(OUTPUT_DIR / ".checkpoint_04.json")
    logger.info("Checkpoint: %d courses déjà traitées", checkpoint.count_done)

    # Session HTTP
    session = create_session()

    # Accumulateurs
    all_rapports_brut = []
    all_rapports_norm = []

    total_rapports = 0
    total_erreurs = 0
    total_requetes = 0
    courses_traitees = 0

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for idx, ref in enumerate(refs, 1):
        course_uid = ref.get("course_uid", "")

        # Skip si déjà fait
        if checkpoint.is_done(course_uid):
            continue

        date_iso = ref.get("date_reunion_iso", "")
        num_reunion = ref.get("numero_reunion", 0)
        num_course = ref.get("numero_course", 0)

        if not date_iso or num_reunion <= 0 or num_course <= 0:
            continue

        date_obj = date.fromisoformat(date_iso)
        date_ddmmyyyy = date_obj.strftime("%d%m%Y")
        timestamp = utc_now_iso()

        # Requête rapports définitifs
        api_data = fetch_rapports_definitifs(
            session, date_ddmmyyyy, num_reunion, num_course, logger
        )
        total_requetes += 1

        if not api_data:
            total_erreurs += 1
            checkpoint.mark_done(course_uid)
            time.sleep(args.pause)
            continue

        # L'API peut renvoyer une liste ou un dict
        if isinstance(api_data, list):
            api_data = {"rapportsDefinitifs": api_data}

        # Parser
        bruts, normalises = parse_rapports(api_data, ref, timestamp)

        for b in bruts:
            all_rapports_brut.append(asdict(b))
        for n in normalises:
            all_rapports_norm.append(asdict(n))

        total_rapports += len(normalises)

        checkpoint.mark_done(course_uid)
        courses_traitees += 1

        # Progress log every 100 courses
        if courses_traitees % 100 == 0:
            logger.info(
                "  [%d/%d] rapports=%d erreurs=%d req=%d",
                courses_traitees, len(refs), total_rapports,
                total_erreurs, total_requetes,
            )

        # Sauvegarde intermédiaire every 500 courses
        if courses_traitees % args.batch == 0:
            sauver_json(all_rapports_norm, OUTPUT_DIR / "rapports_normalises.json", logger)
            sauver_json(all_rapports_brut, OUTPUT_DIR / "rapports_brut.json", logger)
            checkpoint.save()
            logger.info(
                ">>> Sauvegarde intermédiaire: %d courses, %d rapports <<<",
                courses_traitees, total_rapports,
            )

        # Renouveler session tous les 2000 requêtes
        if total_requetes > 0 and total_requetes % 2000 == 0:
            session.close()
            session = create_session()
            logger.info("  Session HTTP renouvelée")

        time.sleep(args.pause)

    # === Sauvegarde finale ===
    logger.info("Sauvegarde finale...")

    # Rapports brut
    sauver_json(all_rapports_brut, OUTPUT_DIR / "rapports_brut.json", logger)

    # Rapports normalisés
    sauver_json(all_rapports_norm, OUTPUT_DIR / "rapports_normalises.json", logger)
    sauver_parquet(all_rapports_norm, OUTPUT_DIR / "rapports_normalises.parquet", logger)
    sauver_csv(all_rapports_norm, OUTPUT_DIR / "rapports_normalises.csv", logger)

    checkpoint.save()

    logger.info("=" * 70)
    logger.info(
        "TERMINÉ: %d courses, %d rapports, %d erreurs, %d requêtes",
        courses_traitees, total_rapports, total_erreurs, total_requetes,
    )
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
