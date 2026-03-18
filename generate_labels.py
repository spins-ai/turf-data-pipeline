#!/usr/bin/env python3
"""
generate_labels.py
==================
Genere les labels d'entrainement a partir des donnees de resultats (04_resultats).

Pour chaque partant, produit :
  - is_winner      : bool — a gagne la course
  - is_place       : bool — dans le top 3
  - position       : int  — position d'arrivee (None si DNF/DQ)
  - is_dnf         : bool — did not finish (non-classe, disqualifie, tombe, etc.)
  - roi_final_odds : float — ROI si mise gagnante a la cote finale
  - value_label    : bool — esperance positive (cote finale > 1/proba implicite)

Cle de jointure : partant_uid ou (date + reunion + course + numPmu)

Entree  : output/04_resultats/rapports_normalises.json (ou .parquet)
Sortie  : output/labels/training_labels.jsonl

Usage :
    python generate_labels.py
    python generate_labels.py --input output/04_resultats/rapports_normalises.json
    python generate_labels.py --format parquet
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

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
INPUT_DIR = BASE_DIR / "output" / "04_resultats"
OUTPUT_DIR = BASE_DIR / "output" / "labels"
LOG_DIR = BASE_DIR / "logs"

# Fichiers d'entree possibles (tries par preference)
INPUT_CANDIDATES = [
    INPUT_DIR / "rapports_normalises.json",
    INPUT_DIR / "rapports_normalises.parquet",
    INPUT_DIR / "rapports_brut.json",
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

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("generate_labels")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "generate_labels.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


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
    """Convert value to float or return None."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if not (math.isnan(f) or math.isinf(f)) else None
    except (ValueError, TypeError):
        return None


def safe_int(val: Any) -> Optional[int]:
    """Convert value to int or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


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

def save_jsonl(data: List[Dict], path: Path, logger: logging.Logger):
    """Sauvegarde en JSONL (une ligne JSON par enregistrement)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    tmp.replace(path)
    logger.info("Sauve : %s (%d lignes)", path, len(data))


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
        description="Generation des labels d'entrainement a partir des resultats"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers le fichier de resultats (JSON, JSONL, ou Parquet)"
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

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("generate_labels.py — Generation des labels d'entrainement")
    logger.info("=" * 70)

    # Load data
    input_path = find_input(args.input, logger)
    data = load_data(input_path, logger)

    if not data:
        logger.error("Aucune donnee chargee.")
        sys.exit(1)

    # Group by course
    par_course: Dict[str, List[Dict]] = defaultdict(list)
    for record in data:
        # Skip non-partants
        if str(record.get("statut", "")).lower() == "non_partant":
            continue

        course_uid = record.get("course_uid", "")
        if not course_uid:
            # Build composite key for grouping
            course_uid = "|".join([
                str(record.get("date_reunion_iso", record.get("date", ""))),
                str(record.get("numReunion", record.get("reunion", ""))),
                str(record.get("numOrdre", record.get("numCourse", ""))),
            ])
        par_course[course_uid].append(record)

    logger.info("Courses detectees : %d", len(par_course))

    # Generate labels course by course
    all_labels = []
    for course_uid, records in par_course.items():
        course_labels = generate_labels_for_course(records)
        all_labels.extend(course_labels)

    logger.info("Labels generes : %d", len(all_labels))

    # Stats
    print_stats(all_labels, logger)

    # Save
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    fmt = args.format
    if fmt in ("jsonl", "all"):
        save_jsonl(all_labels, output_dir / "training_labels.jsonl", logger)
    if fmt in ("csv", "all"):
        save_csv(all_labels, output_dir / "training_labels.csv", logger)
    if fmt in ("parquet", "all"):
        save_parquet(all_labels, output_dir / "training_labels.parquet", logger)

    logger.info("Termine — %d labels dans %s", len(all_labels), output_dir)


if __name__ == "__main__":
    main()
