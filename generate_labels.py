#!/usr/bin/env python3
"""
generate_labels.py
==================
Genere les labels d'entrainement a partir de partants_master.jsonl.

Pour chaque partant, produit :
  - is_winner      : bool — a gagne la course
  - is_place       : bool — dans le top 3
  - position       : int  — position d'arrivee (None si DNF/DQ)
  - is_dnf         : bool — did not finish (non-classe, disqualifie, tombe, etc.)
  - roi_final_odds : float — ROI si mise gagnante a la cote finale
  - value_label    : bool — esperance positive (cote finale > 1/proba implicite)

Cle de jointure : partant_uid ou (date + reunion + course + numPmu)

Entree  : data_master/partants_master.jsonl (streaming, 17 GB)
Sortie  : output/labels/training_labels.jsonl

Usage :
    python generate_labels.py
    python generate_labels.py --input data_master/partants_master.jsonl
    python generate_labels.py --format parquet
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.types import safe_int
from utils.types import safe_float as _safe_float

# Imports optionnels
try:
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

# ===========================================================================
# CONFIG
# ===========================================================================

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output" / "labels"
# Fichiers d'entree possibles (tries par preference)
INPUT_CANDIDATES = [
    BASE_DIR / "data_master" / "partants_master.jsonl",
    BASE_DIR / "data_master" / "partants_master_enrichi.jsonl",
]

# Statuts consideres comme DNF
DNF_STATUTS = {
    "non_place", "tombe", "arrete", "disqualifie", "distanced",
    "non_partant", "reste_au_poteau", "dnf", "nr", "pulled_up",
    "fell", "unseated", "refused",
}


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# DATA LOADING
# ===========================================================================

def load_json(path: Path, logger: logging.Logger) -> List[Dict]:
    """Charge un fichier JSON (liste de dicts) ou JSONL."""
    logger.info("Chargement JSON : %s", path)
    with open(path, "r", encoding="utf-8") as f:
        first_char = f.read(1)
        f.seek(0)
        if first_char == "[":
            data = json.load(f)
        else:
            # JSONL
            data = [json.loads(line) for line in f if line.strip()]
    logger.info("  %d enregistrements charges", len(data))
    return data


def load_parquet(path: Path, logger: logging.Logger) -> List[Dict]:
    """Charge un fichier parquet et retourne une liste de dicts."""
    if not HAS_PARQUET:
        logger.error("pyarrow non installe — impossible de lire %s", path)
        sys.exit(1)
    logger.info("Chargement Parquet : %s", path)
    table = pq.read_table(path)
    data = table.to_pylist()
    logger.info("  %d enregistrements charges", len(data))
    return data


def find_input(explicit_path: Optional[str], logger: logging.Logger) -> Path:
    """Trouve le fichier d'entree a utiliser."""
    if explicit_path:
        p = Path(explicit_path)
        if not p.is_absolute():
            p = BASE_DIR / p
        if p.exists():
            return p
        logger.error("Fichier introuvable : %s", p)
        sys.exit(1)

    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            logger.info("Fichier d'entree trouve : %s", candidate)
            return candidate

    logger.error("Aucun fichier d'entree trouve dans %s", INPUT_DIR)
    logger.error("Candidats testes : %s", [str(c) for c in INPUT_CANDIDATES])
    sys.exit(1)


def load_data(path: Path, logger: logging.Logger) -> List[Dict]:
    """Charge les donnees selon le format du fichier."""
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return load_parquet(path, logger)
    else:
        return load_json(path, logger)


# ===========================================================================
# LABEL GENERATION
# ===========================================================================

def build_join_key(record: Dict) -> str:
    """
    Construit une cle de jointure composite.
    Priorite : partant_uid s'il existe, sinon (date+reunion+course+numPmu).
    """
    uid = record.get("partant_uid")
    if uid:
        return str(uid)

    parts = [
        str(record.get("date_reunion_iso", record.get("date", ""))),
        str(record.get("numReunion", record.get("reunion", ""))),
        str(record.get("numOrdre", record.get("course", record.get("numCourse", "")))),
        str(record.get("numPmu", record.get("numero", ""))),
    ]
    return "|".join(parts)


def safe_float(val: Any) -> Optional[float]:
    """Convert value to float or return None. Rejects NaN/Inf."""
    f = _safe_float(val)
    if f is not None and (math.isnan(f) or math.isinf(f)):
        return None
    return f


def detect_dnf(record: Dict) -> bool:
    """Detect if the horse did not finish (DNF / disqualified / etc.)."""
    # Explicit field
    statut = str(record.get("statut", "")).lower().strip()
    if statut in DNF_STATUTS:
        return True

    # Disqualified flag
    if record.get("is_disqualifie", False):
        return True

    # Position-based heuristic: if no position and not winner
    position = safe_int(record.get("position_arrivee", record.get("ordreArrivee")))
    if position is None and not record.get("is_gagnant", False):
        # Check if there are clues it's truly a DNF vs missing data
        if statut and statut not in {"", "partant"}:
            return True

    return False


def generate_labels_for_course(
    course_records: List[Dict],
) -> List[Dict]:
    """
    Generate labels for all runners in a single course.
    Returns list of label dicts.
    """
    labels = []
    nb_partants = len(course_records)

    # Compute implicit probabilities from final odds (for value_label)
    total_implied_prob = 0.0
    for r in course_records:
        cote = safe_float(r.get("cote_finale", r.get("coteDirect", r.get("rapport_gagnant"))))
        if cote and cote > 0:
            total_implied_prob += 1.0 / cote

    for record in course_records:
        join_key = build_join_key(record)
        position = safe_int(record.get("position_arrivee", record.get("ordreArrivee")))
        cote = safe_float(record.get("cote_finale", record.get("coteDirect", r.get("rapport_gagnant"))))

        is_winner = bool(record.get("is_gagnant", False)) or (position == 1)
        is_place = bool(record.get("is_place", False)) or (position is not None and 1 <= position <= 3)
        is_dnf = detect_dnf(record)

        # ROI if bet at final odds (gagnant)
        roi_final_odds = None
        if cote is not None and cote > 0:
            if is_winner:
                roi_final_odds = round(cote - 1.0, 4)
            else:
                roi_final_odds = -1.0

        # Value label: true if implied probability < fair probability
        # Fair probability approximated from actual outcomes in the race
        value_label = None
        if cote is not None and cote > 0 and total_implied_prob > 0:
            implied_prob = (1.0 / cote) / total_implied_prob  # Normalized
            # If horse won AND implied prob was low -> value
            # More generally: fair_prob = 1/nb_partants for random
            # We define "value" as: cote > nb_partants (i.e. overbet by market)
            # and the horse performed well (top 3)
            if is_winner:
                # Positive EV if cote was higher than 1/(1/nb_partants) = nb_partants
                value_label = cote > nb_partants
            elif is_place and position is not None:
                # For place, value if cote/3 > nb_partants/3
                value_label = cote > nb_partants
            else:
                value_label = False

        label = {
            "join_key": join_key,
            "partant_uid": record.get("partant_uid"),
            "course_uid": record.get("course_uid"),
            "date_reunion_iso": record.get("date_reunion_iso", record.get("date", "")),
            "numReunion": record.get("numReunion", record.get("reunion")),
            "numCourse": record.get("numOrdre", record.get("numCourse", record.get("course"))),
            "numPmu": record.get("numPmu", record.get("numero")),
            "is_winner": is_winner,
            "is_place": is_place,
            "position": position,
            "is_dnf": is_dnf,
            "cote_finale": cote,
            "nb_partants": nb_partants,
            "roi_final_odds": roi_final_odds,
            "value_label": value_label,
        }
        labels.append(label)

    return labels


# ===========================================================================
# SAUVEGARDE
# ===========================================================================

def save_csv(data: List[Dict], path: Path, logger: logging.Logger):
    """Sauvegarde en CSV."""
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(data[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    logger.info("Sauve : %s (%d lignes)", path, len(data))


def save_parquet(data: List[Dict], path: Path, logger: logging.Logger):
    """Sauvegarde en Parquet si pyarrow est disponible."""
    if not HAS_PARQUET or not data:
        return
    try:
        import pyarrow as pa
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path)
        logger.info("Sauve : %s", path)
    except Exception as e:
        logger.warning("Parquet ignore : %s", e)


# ===========================================================================
# STATS
# ===========================================================================

def print_stats(labels: List[Dict], logger: logging.Logger):
    """Print summary statistics of the generated labels."""
    total = len(labels)
    if total == 0:
        logger.info("Aucun label genere.")
        return

    n_winners = sum(1 for l in labels if l["is_winner"])
    n_place = sum(1 for l in labels if l["is_place"])
    n_dnf = sum(1 for l in labels if l["is_dnf"])
    n_with_pos = sum(1 for l in labels if l["position"] is not None)
    n_with_roi = sum(1 for l in labels if l["roi_final_odds"] is not None)
    n_value = sum(1 for l in labels if l["value_label"] is True)

    logger.info("-" * 50)
    logger.info("STATISTIQUES DES LABELS")
    logger.info("-" * 50)
    logger.info("  Total partants  : %d", total)
    logger.info("  Gagnants        : %d (%.1f%%)", n_winners, n_winners / total * 100)
    logger.info("  Places (top 3)  : %d (%.1f%%)", n_place, n_place / total * 100)
    logger.info("  DNF             : %d (%.1f%%)", n_dnf, n_dnf / total * 100)
    logger.info("  Avec position   : %d (%.1f%%)", n_with_pos, n_with_pos / total * 100)
    logger.info("  Avec ROI        : %d", n_with_roi)
    logger.info("  Value (EV+)     : %d", n_value)

    # Average ROI
    roi_values = [l["roi_final_odds"] for l in labels if l["roi_final_odds"] is not None]
    if roi_values:
        avg_roi = sum(roi_values) / len(roi_values)
        logger.info("  ROI moyen       : %.4f", avg_roi)

    # Courses count
    courses = set()
    for l in labels:
        cuid = l.get("course_uid")
        if cuid:
            courses.add(cuid)
    logger.info("  Courses uniques : %d", len(courses))
    logger.info("-" * 50)


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generation des labels d'entrainement a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers le fichier de partants (JSONL)"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie (defaut: output/labels/)"
    )
    parser.add_argument(
        "--format", choices=["jsonl", "csv", "parquet", "all"], default="all",
        help="Format de sortie (defaut: all)"
    )
    args = parser.parse_args()

    logger = setup_logging("generate_labels")
    logger.info("=" * 70)
    logger.info("generate_labels.py — Generation des labels d'entrainement")
    logger.info("=" * 70)

    # Find input
    input_path = find_input(args.input, logger)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Phase 1 : streaming groupby par course_uid ──
    # On lit partants_master.jsonl en streaming et on regroupe par course_uid
    # Comme les records d'une meme course ne sont pas forcement contigus,
    # on accumule par course_uid en memoire (dict of lists), mais seulement
    # les champs necessaires pour les labels (pas tout le record de 140 champs)
    logger.info("Phase 1 : Lecture streaming de %s ...", input_path.name)
    t0 = time.time()

    KEEP_FIELDS = [
        "partant_uid", "course_uid", "date_reunion_iso", "date",
        "numReunion", "reunion", "numOrdre", "numCourse", "course",
        "numPmu", "numero", "position_arrivee", "ordreArrivee",
        "is_gagnant", "is_place", "cote_finale", "coteDirect",
        "rapport_gagnant", "statut", "is_disqualifie",
    ]

    par_course: Dict[str, List[Dict]] = defaultdict(list)
    total_read = 0
    skipped_np = 0

    with open(input_path, "r", encoding="utf-8", errors="replace", buffering=1048576) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_read += 1

            # Skip non-partants
            if str(record.get("statut", "")).lower() == "non_partant":
                skipped_np += 1
                continue

            # Extract only needed fields
            slim = {k: record[k] for k in KEEP_FIELDS if k in record}

            course_uid = slim.get("course_uid", "")
            if not course_uid:
                course_uid = "|".join([
                    str(slim.get("date_reunion_iso", slim.get("date", ""))),
                    str(slim.get("numReunion", slim.get("reunion", ""))),
                    str(slim.get("numOrdre", slim.get("numCourse", ""))),
                ])
            par_course[course_uid].append(slim)

            if total_read % 500000 == 0:
                elapsed = time.time() - t0
                logger.info("  %d lignes lues (%.0f/s), %d courses ...",
                            total_read, total_read / elapsed, len(par_course))

    logger.info("Lecture terminee : %d partants, %d non-partants ignores, %d courses",
                total_read, skipped_np, len(par_course))

    # ── Phase 2 : generation des labels et ecriture streaming ──
    logger.info("Phase 2 : Generation des labels ...")

    jsonl_path = output_dir / "training_labels.jsonl"
    jsonl_tmp = jsonl_path.with_suffix(".tmp")
    csv_path = output_dir / "training_labels.csv"
    csv_tmp = csv_path.with_suffix(".tmp")

    fmt = args.format
    all_labels_for_parquet = []  # Only if parquet requested
    stats = {"total": 0, "winners": 0, "places": 0, "dnf": 0, "with_pos": 0, "with_roi": 0, "value": 0}
    courses_count = 0

    f_jsonl = None
    f_csv = None
    csv_writer_obj = None

    if fmt in ("jsonl", "all"):
        f_jsonl = open(jsonl_tmp, "w", encoding="utf-8", newline="\n")
    if fmt in ("csv", "all"):
        f_csv = open(csv_tmp, "w", newline="", encoding="utf-8")

    try:
        for course_uid, records in par_course.items():
            course_labels = generate_labels_for_course(records)
            courses_count += 1

            for label in course_labels:
                stats["total"] += 1
                if label["is_winner"]:
                    stats["winners"] += 1
                if label["is_place"]:
                    stats["places"] += 1
                if label["is_dnf"]:
                    stats["dnf"] += 1
                if label["position"] is not None:
                    stats["with_pos"] += 1
                if label["roi_final_odds"] is not None:
                    stats["with_roi"] += 1
                if label["value_label"] is True:
                    stats["value"] += 1

                if f_jsonl:
                    f_jsonl.write(json.dumps(label, ensure_ascii=False, default=str) + "\n")

                if f_csv:
                    if csv_writer_obj is None:
                        csv_writer_obj = csv.DictWriter(f_csv, fieldnames=list(label.keys()), extrasaction="ignore")
                        csv_writer_obj.writeheader()
                    csv_writer_obj.writerow(label)

                if fmt in ("parquet", "all"):
                    all_labels_for_parquet.append(label)

            if courses_count % 50000 == 0:
                elapsed = time.time() - t0
                logger.info("  %d courses traitees, %d labels ...", courses_count, stats["total"])

    finally:
        if f_jsonl:
            f_jsonl.close()
        if f_csv:
            f_csv.close()

    # Rename tmp files
    if fmt in ("jsonl", "all") and jsonl_tmp.exists():
        jsonl_tmp.replace(jsonl_path)
        logger.info("Sauve : %s (%d lignes)", jsonl_path, stats["total"])
    if fmt in ("csv", "all") and csv_tmp.exists():
        csv_tmp.replace(csv_path)
        logger.info("Sauve : %s (%d lignes)", csv_path, stats["total"])

    # Parquet
    if fmt in ("parquet", "all") and all_labels_for_parquet:
        save_parquet(all_labels_for_parquet, output_dir / "training_labels.parquet", logger)

    # Stats
    total = stats["total"]
    if total > 0:
        logger.info("-" * 50)
        logger.info("STATISTIQUES DES LABELS")
        logger.info("-" * 50)
        logger.info("  Total partants  : %d", total)
        logger.info("  Gagnants        : %d (%.1f%%)", stats["winners"], stats["winners"] / total * 100)
        logger.info("  Places (top 3)  : %d (%.1f%%)", stats["places"], stats["places"] / total * 100)
        logger.info("  DNF             : %d (%.1f%%)", stats["dnf"], stats["dnf"] / total * 100)
        logger.info("  Avec position   : %d (%.1f%%)", stats["with_pos"], stats["with_pos"] / total * 100)
        logger.info("  Avec ROI        : %d", stats["with_roi"])
        logger.info("  Value (EV+)     : %d", stats["value"])
        logger.info("  Courses uniques : %d", courses_count)
        logger.info("-" * 50)

    elapsed = time.time() - t0
    logger.info("Termine — %d labels, %d courses en %.0fs", total, courses_count, elapsed)


if __name__ == "__main__":
    main()
