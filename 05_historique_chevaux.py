#!/usr/bin/env python3
"""
05_historique_chevaux.py
========================
Reconstruit l'historique complet de chaque cheval a partir des partants normalises.

Input :
  - output/02_liste_courses/partants_normalises.json
  - output/02_liste_courses/courses_normalisees.json

Output : output/05_historique_chevaux/
  - historique_chevaux.json / .parquet / .csv

Usage :
    python3 05_historique_chevaux.py
    python3 05_historique_chevaux.py --help
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Imports optionnels
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

# ===========================================================================
# CONFIG
# ===========================================================================

PARTANTS_PATH = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "partants_normalises.json"
COURSES_PATH = Path(__file__).resolve().parent / "output" / "02_liste_courses" / "courses_normalisees.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "05_historique_chevaux"
LOG_DIR = Path(__file__).resolve().parent / "logs"


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("05_historique_chevaux")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "05_historique_chevaux.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ===========================================================================
# SAUVEGARDE
# ===========================================================================

def sauver_json(data: list[dict], path: Path, logger: logging.Logger):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)
    logger.info("Sauve: %s (%d entrees)", path.name, len(data))


def sauver_parquet(data: list[dict], path: Path, logger: logging.Logger):
    if not HAS_PARQUET or not data:
        return
    try:
        # Flatten sets/lists for parquet compatibility
        flat = []
        for row in data:
            r = {}
            for k, v in row.items():
                if isinstance(v, (set, frozenset)):
                    r[k] = sorted(v)
                elif isinstance(v, list) and v and isinstance(v[0], dict):
                    r[k] = json.dumps(v, ensure_ascii=False, default=str)
                else:
                    r[k] = v
            flat.append(r)
        table = pa.Table.from_pylist(flat)
        pq.write_table(table, path)
        logger.info("Sauve: %s", path.name)
    except Exception as e:
        logger.warning("Parquet ignore: %s", e)


def sauver_csv(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(data[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            flat_row = {}
            for k, v in row.items():
                if isinstance(v, (list, set, frozenset)):
                    flat_row[k] = json.dumps(sorted(v) if isinstance(v, (set, frozenset)) else v,
                                             ensure_ascii=False, default=str)
                else:
                    flat_row[k] = v
            writer.writerow(flat_row)
    logger.info("Sauve: %s", path.name)


# ===========================================================================
# LOGIQUE
# ===========================================================================

def safe_float(val: Any) -> float:
    """Convertit en float, retourne 0.0 si impossible."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def compute_forme(courses_detail: list[dict], n: int) -> Optional[float]:
    """Calcule le taux de victoire sur les n dernieres courses (triees par date desc)."""
    recent = courses_detail[:n]
    if not recent:
        return None
    wins = sum(1 for c in recent if c.get("position") == 1)
    return round(wins / len(recent), 4)


def build_historique_chevaux(
    partants: list[dict],
    courses_map: dict[str, dict],
    logger: logging.Logger,
) -> list[dict]:
    """Construit un historique par cheval."""

    # Grouper les partants par nom de cheval
    chevaux: dict[str, list[dict]] = defaultdict(list)
    skipped = 0

    for p in partants:
        nom = p.get("nom_cheval", "").strip()
        if not nom:
            skipped += 1
            continue
        # Ne garder que les partants effectifs
        if p.get("statut") == "non_partant":
            continue
        chevaux[nom].append(p)

    if skipped:
        logger.info("Partants sans nom ignores: %d", skipped)

    logger.info("Chevaux uniques trouves: %d", len(chevaux))

    results = []

    for nom_cheval, courses_list in chevaux.items():
        # Trier par date chronologique
        courses_list.sort(key=lambda x: x.get("date_reunion_iso", ""))

        nb_courses_total = len(courses_list)
        nb_victoires_total = sum(1 for c in courses_list if c.get("is_gagnant"))
        nb_places_total = sum(1 for c in courses_list if c.get("is_place"))
        gains_total = sum(safe_float(c.get("gains_carriere_euros")) for c in courses_list[-1:])

        dates = [c.get("date_reunion_iso", "") for c in courses_list if c.get("date_reunion_iso")]
        premiere_date = dates[0] if dates else ""
        derniere_date = dates[-1] if dates else ""

        disciplines = set()
        hippodromes = set()
        distances_courues = []

        for c in courses_list:
            d = c.get("discipline", "")
            if d:
                disciplines.add(d)
            h = c.get("hippodrome_normalise", "")
            if h:
                hippodromes.add(h)
            dist = c.get("distance")
            if dist is not None:
                distances_courues.append(dist)

        taux_victoire = round(nb_victoires_total / nb_courses_total, 4) if nb_courses_total else None
        taux_place = round(nb_places_total / nb_courses_total, 4) if nb_courses_total else None

        # Construire le detail de chaque course (pour la forme)
        courses_detail = []
        for c in courses_list:
            course_uid = c.get("course_uid", "")
            course_info = courses_map.get(course_uid, {})
            detail = {
                "date": c.get("date_reunion_iso", ""),
                "hippodrome": c.get("hippodrome_normalise", ""),
                "distance": c.get("distance"),
                "discipline": c.get("discipline", ""),
                "position": c.get("position_arrivee"),
                "cote": c.get("cote_finale"),
                "temps_ms": c.get("temps_ms"),
                "reduction_km": c.get("reduction_km_ms"),
            }
            courses_detail.append(detail)

        # Forme sur les dernieres courses (plus recentes en premier)
        courses_detail_rev = list(reversed(courses_detail))
        forme_5 = compute_forme(courses_detail_rev, 5)
        forme_10 = compute_forme(courses_detail_rev, 10)
        forme_20 = compute_forme(courses_detail_rev, 20)

        # Jours moyen entre courses
        jours_moyen = None
        if len(dates) >= 2:
            try:
                dt_objs = [datetime.strptime(d, "%Y-%m-%d") for d in dates if d]
                if len(dt_objs) >= 2:
                    ecarts = [(dt_objs[i+1] - dt_objs[i]).days for i in range(len(dt_objs)-1)]
                    ecarts = [e for e in ecarts if e >= 0]
                    if ecarts:
                        jours_moyen = round(statistics.mean(ecarts), 1)
            except (ValueError, TypeError):
                pass

        record = {
            "nom_cheval": nom_cheval,
            "nb_courses_total": nb_courses_total,
            "nb_victoires_total": nb_victoires_total,
            "nb_places_total": nb_places_total,
            "gains_total_euros": gains_total,
            "premiere_course_date": premiere_date,
            "derniere_course_date": derniere_date,
            "disciplines": sorted(disciplines),
            "hippodromes": sorted(hippodromes),
            "distances_courues": distances_courues,
            "taux_victoire": taux_victoire,
            "taux_place": taux_place,
            "forme_5": forme_5,
            "forme_10": forme_10,
            "forme_20": forme_20,
            "jours_moyen_entre_courses": jours_moyen,
            "courses_detail": courses_detail,
        }
        results.append(record)

    # Trier par nombre de courses decroissant
    results.sort(key=lambda x: (-x["nb_courses_total"], x["nom_cheval"]))
    return results


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Reconstruit l'historique complet de chaque cheval a partir des partants normalises."
    )
    parser.add_argument(
        "--partants", type=str, default=str(PARTANTS_PATH),
        help=f"Chemin vers partants_normalises.json (defaut: {PARTANTS_PATH})"
    )
    parser.add_argument(
        "--courses", type=str, default=str(COURSES_PATH),
        help=f"Chemin vers courses_normalisees.json (defaut: {COURSES_PATH})"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help=f"Repertoire de sortie (defaut: {OUTPUT_DIR})"
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("05 — HISTORIQUE CHEVAUX")
    logger.info("=" * 70)

    partants_path = Path(args.partants)
    courses_path = Path(args.courses)
    output_dir = Path(args.output_dir)

    # Charger partants
    if not partants_path.exists():
        logger.error("Fichier introuvable: %s", partants_path)
        sys.exit(1)
    with open(partants_path, "r", encoding="utf-8") as f:
        partants = json.load(f)
    logger.info("Partants charges: %d", len(partants))

    # Charger courses
    courses_map: dict[str, dict] = {}
    if courses_path.exists():
        with open(courses_path, "r", encoding="utf-8") as f:
            courses_list = json.load(f)
        courses_map = {c["course_uid"]: c for c in courses_list if "course_uid" in c}
        logger.info("Courses chargees: %d", len(courses_map))
    else:
        logger.warning("Fichier courses non trouve: %s (continuer sans)", courses_path)

    # Construire historique
    historique = build_historique_chevaux(partants, courses_map, logger)
    logger.info("Historiques construits: %d chevaux", len(historique))

    # Sauvegarder
    output_dir.mkdir(parents=True, exist_ok=True)
    sauver_json(historique, output_dir / "historique_chevaux.json", logger)
    sauver_parquet(historique, output_dir / "historique_chevaux.parquet", logger)
    sauver_csv(historique, output_dir / "historique_chevaux.csv", logger)

    # Stats
    if historique:
        total_courses = sum(h["nb_courses_total"] for h in historique)
        total_victoires = sum(h["nb_victoires_total"] for h in historique)
        avg_courses = total_courses / len(historique) if historique else 0
        logger.info("-" * 50)
        logger.info("RESUME:")
        logger.info("  Chevaux uniques       : %d", len(historique))
        logger.info("  Total courses         : %d", total_courses)
        logger.info("  Total victoires       : %d", total_victoires)
        logger.info("  Moyenne courses/cheval: %.1f", avg_courses)
        logger.info("  Disciplines presentes : %s",
                     sorted(set(d for h in historique for d in h["disciplines"])))
        logger.info("  Hippodromes presents  : %d",
                     len(set(h2 for h in historique for h2 in h["hippodromes"])))

    logger.info("=" * 70)
    logger.info("TERMINE")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
