#!/usr/bin/env python3
"""
06_historique_jockeys.py
========================
Reconstruit l'historique de chaque jockey/driver et de chaque entraineur
a partir des partants normalises.

Input :
  - output/02_liste_courses/partants_normalises.json
  - output/02_liste_courses/courses_normalisees.json

Output : output/06_historique_jockeys/
  - historique_jockeys.json / .parquet / .csv
  - historique_entraineurs.json / .parquet / .csv

Usage :
    python3 06_historique_jockeys.py
    python3 06_historique_jockeys.py --help
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import Counter, defaultdict
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

PARTANTS_PATH = Path("output/02_liste_courses/partants_normalises.json")
COURSES_PATH = Path("output/02_liste_courses/courses_normalisees.json")
OUTPUT_DIR = Path("output/06_historique_jockeys")
LOG_DIR = Path("logs")


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("06_historique_jockeys")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "06_historique_jockeys.log", encoding="utf-8")
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
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def build_historique_acteurs(
    partants: list[dict],
    courses_map: dict[str, dict],
    champ_nom: str,
    logger: logging.Logger,
) -> list[dict]:
    """
    Construit l'historique pour un type d'acteur (jockey ou entraineur).
    champ_nom : 'jockey_driver' ou 'entraineur'
    """
    acteurs: dict[str, list[dict]] = defaultdict(list)
    skipped = 0

    for p in partants:
        nom = p.get(champ_nom, "").strip()
        if not nom:
            skipped += 1
            continue
        if p.get("statut") == "non_partant":
            continue
        acteurs[nom].append(p)

    if skipped:
        logger.info("Partants sans %s ignores: %d", champ_nom, skipped)

    logger.info("Acteurs uniques (%s): %d", champ_nom, len(acteurs))

    results = []

    for nom, montes in acteurs.items():
        montes.sort(key=lambda x: x.get("date_reunion_iso", ""))

        nb_montes = len(montes)
        nb_victoires = sum(1 for m in montes if m.get("is_gagnant"))
        nb_places = sum(1 for m in montes if m.get("is_place"))

        taux_victoire = round(nb_victoires / nb_montes, 4) if nb_montes else None
        taux_place = round(nb_places / nb_montes, 4) if nb_montes else None

        # Gains total : somme des allocations des courses gagnees
        gains_total = 0.0
        for m in montes:
            if m.get("is_gagnant"):
                course_uid = m.get("course_uid", "")
                course_info = courses_map.get(course_uid, {})
                alloc = course_info.get("allocation_1er")
                if alloc is not None:
                    gains_total += safe_float(alloc)

        disciplines = set()
        hippodromes_counter: Counter = Counter()
        chevaux_set: set[str] = set()

        dates = []
        for m in montes:
            d = m.get("discipline", "")
            if d:
                disciplines.add(d)
            h = m.get("hippodrome_normalise", "")
            if h:
                hippodromes_counter[h] += 1
            ch = m.get("nom_cheval", "")
            if ch:
                chevaux_set.add(ch)
            dt = m.get("date_reunion_iso", "")
            if dt:
                dates.append(dt)

        premiere_date = dates[0] if dates else ""
        derniere_date = dates[-1] if dates else ""

        # Top 10 hippodromes
        hippodromes_frequents = [h for h, _ in hippodromes_counter.most_common(10)]

        record = {
            "nom": nom,
            "nb_montes": nb_montes,
            "nb_victoires": nb_victoires,
            "nb_places": nb_places,
            "taux_victoire": taux_victoire,
            "taux_place": taux_place,
            "gains_total_euros": round(gains_total, 2),
            "disciplines": sorted(disciplines),
            "hippodromes_frequents": hippodromes_frequents,
            "chevaux_montes": len(chevaux_set),
            "premiere_course_date": premiere_date,
            "derniere_course_date": derniere_date,
        }
        results.append(record)

    # Trier par nombre de montes decroissant
    results.sort(key=lambda x: (-x["nb_montes"], x["nom"]))
    return results


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Reconstruit l'historique de chaque jockey/driver et entraineur."
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
    logger.info("06 — HISTORIQUE JOCKEYS / ENTRAINEURS")
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

    output_dir.mkdir(parents=True, exist_ok=True)

    # === Jockeys ===
    logger.info("-" * 50)
    logger.info("Construction historique JOCKEYS...")
    historique_jockeys = build_historique_acteurs(partants, courses_map, "jockey_driver", logger)
    logger.info("Jockeys construits: %d", len(historique_jockeys))

    sauver_json(historique_jockeys, output_dir / "historique_jockeys.json", logger)
    sauver_parquet(historique_jockeys, output_dir / "historique_jockeys.parquet", logger)
    sauver_csv(historique_jockeys, output_dir / "historique_jockeys.csv", logger)

    # === Entraineurs ===
    logger.info("-" * 50)
    logger.info("Construction historique ENTRAINEURS...")
    historique_entraineurs = build_historique_acteurs(partants, courses_map, "entraineur", logger)
    logger.info("Entraineurs construits: %d", len(historique_entraineurs))

    sauver_json(historique_entraineurs, output_dir / "historique_entraineurs.json", logger)
    sauver_parquet(historique_entraineurs, output_dir / "historique_entraineurs.parquet", logger)
    sauver_csv(historique_entraineurs, output_dir / "historique_entraineurs.csv", logger)

    # Stats
    logger.info("-" * 50)
    logger.info("RESUME JOCKEYS:")
    if historique_jockeys:
        total_montes_j = sum(h["nb_montes"] for h in historique_jockeys)
        total_vic_j = sum(h["nb_victoires"] for h in historique_jockeys)
        logger.info("  Jockeys uniques       : %d", len(historique_jockeys))
        logger.info("  Total montes          : %d", total_montes_j)
        logger.info("  Total victoires       : %d", total_vic_j)
        logger.info("  Moyenne montes/jockey : %.1f",
                     total_montes_j / len(historique_jockeys))

    logger.info("RESUME ENTRAINEURS:")
    if historique_entraineurs:
        total_montes_e = sum(h["nb_montes"] for h in historique_entraineurs)
        total_vic_e = sum(h["nb_victoires"] for h in historique_entraineurs)
        logger.info("  Entraineurs uniques    : %d", len(historique_entraineurs))
        logger.info("  Total partants         : %d", total_montes_e)
        logger.info("  Total victoires        : %d", total_vic_e)
        logger.info("  Moyenne partants/entr. : %.1f",
                     total_montes_e / len(historique_entraineurs))

    logger.info("=" * 70)
    logger.info("TERMINE")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
