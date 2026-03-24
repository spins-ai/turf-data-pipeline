#!/usr/bin/env python3
"""
labels/advanced_labels.py
=========================
Genere 6 labels avances a partir de partants_master.jsonl (streaming).

Labels produits pour chaque partant (Features oubliees) :
  - y_roi_combine       : float  -- ROI si ce cheval fait partie d'un couple gagnant.
                                    Si position <= 2, utilise rap_couple_gagnant / mise_base - 1,
                                    sinon -1.
  - y_place_top2        : bool   -- 1 si position_arrivee <= 2 (pour paris exacta)
  - y_tierce_part       : bool   -- 1 si position_arrivee <= 3 (coherence de nommage)
  - y_ecart_temps_gagnant : float -- Ecart au gagnant en secondes
                                     (ecart_precedent cumule, converti de ms en s)
  - y_vitesse_normalisee  : float -- reduction_km_ms normalise par la moyenne
                                     hippodrome+distance de la course
  - y_value_bet_retrospectif : bool -- 1 si (resultat * cote_finale) > 1
                                       i.e. pari rentable retrospectivement

Entree  : data_master/partants_master.jsonl (streaming, ~17 GB)
Sortie  : output/labels/advanced_labels.jsonl

Contraintes :
  - RAM < 3 GB : lecture streaming, ne garde que les champs utiles,
    traite course par course puis ecrit immediatement.

Usage :
    python labels/advanced_labels.py
    python labels/advanced_labels.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# PATH SETUP
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.logging_setup import setup_logging
from utils.types import safe_int
from utils.types import safe_float as _safe_float

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BASE_DIR = _PROJECT_ROOT
OUTPUT_DIR = BASE_DIR / "output" / "labels"

INPUT_CANDIDATES = [
    BASE_DIR / "data_master" / "partants_master.jsonl",
    BASE_DIR / "data_master" / "partants_master_enrichi.jsonl",
]

DNF_STATUTS = {
    "non_place", "tombe", "arrete", "disqualifie", "distanced",
    "non_partant", "reste_au_poteau", "dnf", "nr", "pulled_up",
    "fell", "unseated", "refused",
}

# Mise de base PMU (en euros)
MISE_BASE = 1.0

# Champs a extraire de chaque record (slim) pour limiter la RAM
KEEP_FIELDS = [
    "partant_uid", "course_uid", "date_reunion_iso", "date",
    "numReunion", "reunion", "numOrdre", "numCourse", "course",
    "numPmu", "numero",
    # Position / resultat
    "position_arrivee", "ordreArrivee", "is_gagnant", "is_place",
    "statut", "is_disqualifie",
    # Cotes
    "cote_finale", "coteDirect", "rapport_gagnant",
    # Ecart / vitesse
    "ecart_precedent", "reduction_km_ms",
    # Rapports couple / tierce
    "rap_couple_gagnant", "rap_ri_e_couple_gagnant_1_dividende",
    # Hippodrome / distance (pour normalisation)
    "hippodrome_normalise", "distance",
]


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def safe_float(val: Any) -> Optional[float]:
    """Convert to float, reject NaN/Inf."""
    f = _safe_float(val)
    if f is not None and (math.isnan(f) or math.isinf(f)):
        return None
    return f


def find_input(explicit_path: Optional[str], logger: logging.Logger) -> Path:
    """Trouve le fichier d'entree."""
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

    logger.error("Aucun fichier d'entree trouve.")
    logger.error("Candidats testes : %s", [str(c) for c in INPUT_CANDIDATES])
    sys.exit(1)


def build_join_key(record: Dict) -> str:
    """Cle de jointure : partant_uid ou composite."""
    uid = record.get("partant_uid")
    if uid:
        return str(uid)
    parts = [
        str(record.get("date_reunion_iso", record.get("date", ""))),
        str(record.get("numReunion", record.get("reunion", ""))),
        str(record.get("numOrdre", record.get("numCourse", record.get("course", "")))),
        str(record.get("numPmu", record.get("numero", ""))),
    ]
    return "|".join(parts)


def parse_ecart(ecart_str: Any) -> Optional[float]:
    """
    Parse ecart_precedent string into a float (seconds or lengths).
    Exemples : '1L', '2.5L', 'nk', 'shd', '3/4L', '1"5', '0.3s', etc.
    Returns None if unparseable.
    """
    if ecart_str is None:
        return None
    s = str(ecart_str).strip().lower()
    if not s or s in ("", "-", "null", "none"):
        return None

    # Already numeric
    try:
        return float(s)
    except ValueError:
        pass

    # Fractional lengths: "3/4L", "1/2L"
    s_clean = s.replace("l", "").replace("longueur", "").replace("longueurs", "").strip()
    if "/" in s_clean:
        try:
            num, den = s_clean.split("/")
            return float(num) / float(den)
        except (ValueError, ZeroDivisionError):
            pass

    # Named margins
    named = {
        "ct": 0.1, "courte_tete": 0.1, "shd": 0.1, "short_head": 0.1,
        "ce": 0.25, "courte_encolure": 0.25, "snk": 0.25, "short_neck": 0.25,
        "te": 0.3, "tete": 0.3, "hd": 0.3, "head": 0.3,
        "e": 0.5, "enc": 0.5, "encolure": 0.5, "nk": 0.5, "neck": 0.5,
    }
    if s_clean in named:
        return named[s_clean]

    # Strip trailing unit and parse number: "2.5l", "3l", "1"5"
    for suffix in ("l", "s", '"'):
        if s_clean.endswith(suffix):
            try:
                return float(s_clean[:-1])
            except ValueError:
                pass

    # "1"5" -> 1.5 seconds
    if '"' in s:
        try:
            parts = s.replace('"', ".").rstrip(".")
            return float(parts)
        except ValueError:
            pass

    return None


# ---------------------------------------------------------------------------
# LABEL GENERATION (per course)
# ---------------------------------------------------------------------------

def generate_advanced_labels(course_records: List[Dict]) -> List[Dict]:
    """
    Generate 6 advanced labels for all runners in a single course.
    """
    labels = []

    # --- Pre-compute course-level info ---

    # 1. Cumulative ecart to reconstruct margin to winner
    pos_records = []
    for i, r in enumerate(course_records):
        pos = safe_int(r.get("position_arrivee", r.get("ordreArrivee")))
        if pos is not None:
            pos_records.append((pos, i, r))
    pos_records.sort(key=lambda x: x[0])

    cumul_margin: Dict[int, float] = {}  # index -> cumulative margin to winner
    running_margin = 0.0
    for pos, idx, r in pos_records:
        if pos == 1:
            cumul_margin[idx] = 0.0
        else:
            ecart_val = parse_ecart(r.get("ecart_precedent"))
            if ecart_val is not None:
                running_margin += ecart_val
                cumul_margin[idx] = round(running_margin, 4)
            else:
                running_margin = 0.0  # reset — cannot accumulate reliably

    # 2. Average reduction_km_ms for this course (hippodrome+distance grouping)
    speeds = []
    for r in course_records:
        spd = safe_float(r.get("reduction_km_ms"))
        if spd is not None and spd > 0:
            speeds.append(spd)
    avg_speed = (sum(speeds) / len(speeds)) if speeds else None

    # 3. Couple gagnant dividend (course-level, same for all runners)
    couple_dividend = None
    for r in course_records:
        cd = safe_float(r.get("rap_ri_e_couple_gagnant_1_dividende"))
        if cd is None:
            cd = safe_float(r.get("rap_couple_gagnant"))
        if cd is not None and cd > 0:
            couple_dividend = cd
            break

    # --- Build labels per runner ---
    for i, record in enumerate(course_records):
        join_key = build_join_key(record)
        position = safe_int(record.get("position_arrivee", record.get("ordreArrivee")))
        cote = safe_float(record.get("cote_finale",
                                     record.get("coteDirect",
                                                 record.get("rapport_gagnant"))))
        is_gagnant = bool(record.get("is_gagnant", False)) or (position == 1)

        # --- 1. y_roi_combine ---
        #   ROI if this horse is part of the winning couple (top 2).
        #   Approximate: if position <= 2, couple_dividend / mise_base - 1, else -1.
        y_roi_combine = None
        if position is not None:
            if position <= 2 and couple_dividend is not None:
                # Dividend may be in centimes (>50) or euros
                div_euros = couple_dividend / 100.0 if couple_dividend > 50 else couple_dividend
                y_roi_combine = round(div_euros / MISE_BASE - 1.0, 4)
            elif position > 2:
                y_roi_combine = -1.0

        # --- 2. y_place_top2 ---
        y_place_top2 = position is not None and 1 <= position <= 2

        # --- 3. y_tierce_part ---
        y_tierce_part = position is not None and 1 <= position <= 3

        # --- 4. y_ecart_temps_gagnant ---
        #   Cumulative ecart to winner, converted to seconds.
        #   parse_ecart returns values in mixed units (lengths/seconds).
        #   When the raw value is in ms we divide by 1000; otherwise
        #   we keep as-is (already approximate seconds/lengths).
        y_ecart_temps_gagnant = None
        margin_raw = cumul_margin.get(i)
        if margin_raw is not None:
            # Heuristic: if margin > 100, likely milliseconds -> convert to seconds
            if margin_raw > 100:
                y_ecart_temps_gagnant = round(margin_raw / 1000.0, 4)
            else:
                y_ecart_temps_gagnant = round(margin_raw, 4)

        # --- 5. y_vitesse_normalisee ---
        #   reduction_km_ms normalized by the course average speed.
        #   This is the same concept as speed_figure but kept as a target label.
        y_vitesse_normalisee = None
        spd = safe_float(record.get("reduction_km_ms"))
        if spd is not None and spd > 0 and avg_speed is not None and avg_speed > 0:
            y_vitesse_normalisee = round(spd / avg_speed, 4)

        # --- 6. y_value_bet_retrospectif ---
        #   1 if betting on this horse at closing odds was profitable.
        #   (actual_result * cote_finale) > 1
        #   actual_result = 1 if won (is_gagnant), else 0
        y_value_bet_retrospectif = None
        if cote is not None and cote > 0:
            if is_gagnant:
                y_value_bet_retrospectif = (cote * 1.0) > 1.0
            else:
                y_value_bet_retrospectif = False  # 0 * cote = 0, never > 1

        label = {
            "join_key": join_key,
            "partant_uid": record.get("partant_uid"),
            "course_uid": record.get("course_uid"),
            "date_reunion_iso": record.get("date_reunion_iso", record.get("date", "")),
            "numReunion": record.get("numReunion", record.get("reunion")),
            "numCourse": record.get("numOrdre", record.get("numCourse", record.get("course"))),
            "numPmu": record.get("numPmu", record.get("numero")),
            "position": position,
            "y_roi_combine": y_roi_combine,
            "y_place_top2": y_place_top2,
            "y_tierce_part": y_tierce_part,
            "y_ecart_temps_gagnant": y_ecart_temps_gagnant,
            "y_vitesse_normalisee": y_vitesse_normalisee,
            "y_value_bet_retrospectif": y_value_bet_retrospectif,
        }
        labels.append(label)

    return labels


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generation des labels avances a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers le fichier de partants (JSONL)"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie (defaut: output/labels/)"
    )
    args = parser.parse_args()

    logger = setup_logging("advanced_labels")
    logger.info("=" * 70)
    logger.info("advanced_labels.py -- Labels avances (Features oubliees)")
    logger.info("=" * 70)

    # Find input
    input_path = find_input(args.input, logger)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # -- Phase 1 : streaming groupby par course_uid --
    logger.info("Phase 1 : Lecture streaming de %s ...", input_path.name)
    t0 = time.time()

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

            # Extract only needed fields to limit RAM
            slim = {k: record[k] for k in KEEP_FIELDS if k in record}

            course_uid = slim.get("course_uid", "")
            if not course_uid:
                course_uid = "|".join([
                    str(slim.get("date_reunion_iso", slim.get("date", ""))),
                    str(slim.get("numReunion", slim.get("reunion", ""))),
                    str(slim.get("numOrdre", slim.get("numCourse", ""))),
                ])
            par_course[course_uid].append(slim)

            if total_read % 500_000 == 0:
                elapsed = time.time() - t0
                logger.info("  %d lignes lues (%.0f/s), %d courses ...",
                            total_read, total_read / elapsed if elapsed > 0 else 0,
                            len(par_course))

    logger.info("Lecture terminee : %d partants, %d non-partants ignores, %d courses",
                total_read, skipped_np, len(par_course))

    # -- Phase 2 : generation des labels et ecriture streaming --
    logger.info("Phase 2 : Generation des labels avances ...")

    jsonl_path = output_dir / "advanced_labels.jsonl"
    jsonl_tmp = jsonl_path.with_suffix(".tmp")

    stats = {
        "total": 0, "with_roi_combine": 0, "top2": 0, "tierce": 0,
        "with_ecart": 0, "with_vitesse": 0, "value_bet": 0,
    }
    courses_count = 0

    with open(jsonl_tmp, "w", encoding="utf-8", newline="\n") as f_out:
        for course_uid, records in par_course.items():
            course_labels = generate_advanced_labels(records)
            courses_count += 1

            for label in course_labels:
                stats["total"] += 1
                if label["y_roi_combine"] is not None:
                    stats["with_roi_combine"] += 1
                if label["y_place_top2"]:
                    stats["top2"] += 1
                if label["y_tierce_part"]:
                    stats["tierce"] += 1
                if label["y_ecart_temps_gagnant"] is not None:
                    stats["with_ecart"] += 1
                if label["y_vitesse_normalisee"] is not None:
                    stats["with_vitesse"] += 1
                if label["y_value_bet_retrospectif"] is True:
                    stats["value_bet"] += 1

                f_out.write(json.dumps(label, ensure_ascii=False, default=str) + "\n")

            if courses_count % 50_000 == 0:
                logger.info("  %d courses traitees, %d labels ...",
                            courses_count, stats["total"])

    # Rename tmp -> final
    if jsonl_tmp.exists():
        jsonl_tmp.replace(jsonl_path)
        logger.info("Sauve : %s (%d lignes)", jsonl_path, stats["total"])

    # Free course data
    del par_course

    # Stats
    total = stats["total"]
    if total > 0:
        logger.info("-" * 50)
        logger.info("STATISTIQUES DES LABELS AVANCES")
        logger.info("-" * 50)
        logger.info("  Total partants          : %d", total)
        logger.info("  Avec ROI combine        : %d (%.1f%%)", stats["with_roi_combine"],
                     stats["with_roi_combine"] / total * 100)
        logger.info("  Top 2 (y_place_top2)    : %d (%.1f%%)", stats["top2"],
                     stats["top2"] / total * 100)
        logger.info("  Tierce (y_tierce_part)  : %d (%.1f%%)", stats["tierce"],
                     stats["tierce"] / total * 100)
        logger.info("  Avec ecart gagnant      : %d (%.1f%%)", stats["with_ecart"],
                     stats["with_ecart"] / total * 100)
        logger.info("  Avec vitesse normalisee : %d (%.1f%%)", stats["with_vitesse"],
                     stats["with_vitesse"] / total * 100)
        logger.info("  Value bet retrospectif  : %d (%.1f%%)", stats["value_bet"],
                     stats["value_bet"] / total * 100)
        logger.info("  Courses uniques         : %d", courses_count)
        logger.info("-" * 50)

    elapsed = time.time() - t0
    logger.info("Termine -- %d labels, %d courses en %.0fs", total, courses_count, elapsed)


if __name__ == "__main__":
    main()
