#!/usr/bin/env python3
"""
labels/supplementary_labels.py
===============================
Genere 6 labels supplementaires a partir de partants_master.jsonl (streaming).

Labels produits pour chaque partant :
  - is_top5           : bool  — termine dans le top 5
  - is_exacta_part    : bool  — fait partie de l'exacta (top 2)
  - margin_to_winner  : float — ecart au premier (depuis ecart_premier si dispo)
  - speed_rating      : float — vitesse relative vs moyenne du peloton
                                (reduction_km_ms du cheval / moyenne du peloton)
  - beaten_favorite   : bool  — ce cheval a battu le favori
  - roi_place         : float — ROI si pari place (top 3)
                                Utilise rap_rapport_simple_place_N / 100 - 1,
                                sinon approximation cote_finale / 3 - 1.

Entree  : data_master/partants_master.jsonl (streaming, ~17 GB)
Sortie  : output/labels/supplementary_labels.jsonl

Contraintes :
  - RAM < 3 GB : lecture streaming, ne garde que les champs utiles,
    traite course par course puis ecrit immediatement.

Usage :
    python labels/supplementary_labels.py
    python labels/supplementary_labels.py --input data_master/partants_master.jsonl
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
    # Rapports place
    "rap_rapport_simple_place_1", "rap_rapport_simple_place_2",
    "rap_rapport_simple_place_3", "rap_combinaison_place_1",
    "rap_combinaison_place_2", "rap_combinaison_place_3",
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

def generate_supplementary_labels(course_records: List[Dict]) -> List[Dict]:
    """
    Generate 6 supplementary labels for all runners in a single course.
    """
    nb_partants = len(course_records)
    labels = []

    # --- Pre-compute course-level info ---

    # 1. Identify winner and favorite
    winner_idx = None
    favorite_idx = None
    best_cote = float("inf")

    for i, r in enumerate(course_records):
        pos = safe_int(r.get("position_arrivee", r.get("ordreArrivee")))
        is_gagnant = bool(r.get("is_gagnant", False)) or (pos == 1)
        if is_gagnant and winner_idx is None:
            winner_idx = i

        cote = safe_float(r.get("cote_finale", r.get("coteDirect", r.get("rapport_gagnant"))))
        if cote is not None and 0 < cote < best_cote:
            best_cote = cote
            favorite_idx = i

    # 2. Favorite position (for beaten_favorite)
    favorite_pos = None
    if favorite_idx is not None:
        r_fav = course_records[favorite_idx]
        favorite_pos = safe_int(r_fav.get("position_arrivee", r_fav.get("ordreArrivee")))

    # 3. Average reduction_km_ms for speed_rating
    speeds = []
    for r in course_records:
        spd = safe_float(r.get("reduction_km_ms"))
        if spd is not None and spd > 0:
            speeds.append(spd)
    avg_speed = (sum(speeds) / len(speeds)) if speeds else None

    # 4. Cumulative ecart to reconstruct margin_to_winner
    #    ecart_precedent is the gap to the horse just ahead.
    #    We accumulate from position 1 downward.
    #    Sort by position to accumulate.
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
                # Gap broken; set None from here on
                running_margin = 0.0  # reset — cannot accumulate reliably
                # We don't store this index so it stays None

    # 5. Place rapport mapping: numPmu -> rapport_place
    #    rap_rapport_simple_place_N corresponds to rap_combinaison_place_N
    place_rapport_map: Dict[str, float] = {}  # numPmu string -> rapport in euros
    if course_records:
        sample = course_records[0]
        for n in (1, 2, 3):
            combo_key = f"rap_combinaison_place_{n}"
            rap_key = f"rap_rapport_simple_place_{n}"
            combo_val = sample.get(combo_key)
            rap_val = safe_float(sample.get(rap_key))
            if combo_val is not None and rap_val is not None:
                place_rapport_map[str(combo_val).strip()] = rap_val

    # --- Build labels per runner ---
    for i, record in enumerate(course_records):
        join_key = build_join_key(record)
        position = safe_int(record.get("position_arrivee", record.get("ordreArrivee")))
        cote = safe_float(record.get("cote_finale", record.get("coteDirect", record.get("rapport_gagnant"))))
        num_pmu = str(record.get("numPmu", record.get("numero", ""))).strip()

        # --- 1. is_top5 ---
        is_top5 = position is not None and 1 <= position <= 5

        # --- 2. is_exacta_part ---
        is_exacta_part = position is not None and 1 <= position <= 2

        # --- 3. margin_to_winner ---
        margin_to_winner = cumul_margin.get(i)

        # --- 4. speed_rating ---
        speed_rating = None
        spd = safe_float(record.get("reduction_km_ms"))
        if spd is not None and spd > 0 and avg_speed is not None and avg_speed > 0:
            speed_rating = round(spd / avg_speed, 4)

        # --- 5. beaten_favorite ---
        beaten_favorite = None
        if favorite_idx is not None and i != favorite_idx:
            if position is not None and favorite_pos is not None:
                beaten_favorite = position < favorite_pos
            elif position is not None and favorite_pos is None:
                # Favorite DNF, this horse finished -> beat the favorite
                beaten_favorite = True
            else:
                beaten_favorite = None
        elif favorite_idx is not None and i == favorite_idx:
            beaten_favorite = False  # Can't beat yourself

        # --- 6. roi_place ---
        #   Use actual rapport_simple_place if available for this numPmu,
        #   otherwise approximate with cote_finale / 3.
        #   rap values in data dictionary are in centimes (e.g. 260 = 2.60 EUR).
        roi_place = None
        is_place = bool(record.get("is_place", False)) or (position is not None and 1 <= position <= 3)

        if num_pmu in place_rapport_map:
            # Actual rapport available (centimes -> euros)
            rap = place_rapport_map[num_pmu]
            if rap > 0:
                rapport_euros = rap / 100.0 if rap > 50 else rap  # heuristic: >50 likely centimes
                roi_place = round(rapport_euros - 1.0, 4)
        elif cote is not None and cote > 0:
            if is_place:
                roi_place = round(cote / 3.0 - 1.0, 4)
            else:
                roi_place = -1.0

        label = {
            "join_key": join_key,
            "partant_uid": record.get("partant_uid"),
            "course_uid": record.get("course_uid"),
            "date_reunion_iso": record.get("date_reunion_iso", record.get("date", "")),
            "numReunion": record.get("numReunion", record.get("reunion")),
            "numCourse": record.get("numOrdre", record.get("numCourse", record.get("course"))),
            "numPmu": record.get("numPmu", record.get("numero")),
            "position": position,
            "is_top5": is_top5,
            "is_exacta_part": is_exacta_part,
            "margin_to_winner": margin_to_winner,
            "speed_rating": speed_rating,
            "beaten_favorite": beaten_favorite,
            "roi_place": roi_place,
        }
        labels.append(label)

    return labels


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generation des labels supplementaires a partir de partants_master"
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

    logger = setup_logging("supplementary_labels")
    logger.info("=" * 70)
    logger.info("supplementary_labels.py — Labels supplementaires")
    logger.info("=" * 70)

    # Find input
    input_path = find_input(args.input, logger)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Phase 1 : streaming groupby par course_uid ──
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

    # ── Phase 2 : generation des labels et ecriture streaming ──
    logger.info("Phase 2 : Generation des labels supplementaires ...")

    jsonl_path = output_dir / "supplementary_labels.jsonl"
    jsonl_tmp = jsonl_path.with_suffix(".tmp")

    stats = {
        "total": 0, "top5": 0, "exacta": 0,
        "with_margin": 0, "with_speed": 0,
        "beaten_fav": 0, "with_roi_place": 0,
    }
    courses_count = 0

    with open(jsonl_tmp, "w", encoding="utf-8", newline="\n") as f_out:
        for course_uid, records in par_course.items():
            course_labels = generate_supplementary_labels(records)
            courses_count += 1

            for label in course_labels:
                stats["total"] += 1
                if label["is_top5"]:
                    stats["top5"] += 1
                if label["is_exacta_part"]:
                    stats["exacta"] += 1
                if label["margin_to_winner"] is not None:
                    stats["with_margin"] += 1
                if label["speed_rating"] is not None:
                    stats["with_speed"] += 1
                if label["beaten_favorite"] is True:
                    stats["beaten_fav"] += 1
                if label["roi_place"] is not None:
                    stats["with_roi_place"] += 1

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
        logger.info("STATISTIQUES DES LABELS SUPPLEMENTAIRES")
        logger.info("-" * 50)
        logger.info("  Total partants     : %d", total)
        logger.info("  Top 5              : %d (%.1f%%)", stats["top5"], stats["top5"] / total * 100)
        logger.info("  Exacta (top 2)     : %d (%.1f%%)", stats["exacta"], stats["exacta"] / total * 100)
        logger.info("  Avec margin_winner : %d (%.1f%%)", stats["with_margin"], stats["with_margin"] / total * 100)
        logger.info("  Avec speed_rating  : %d (%.1f%%)", stats["with_speed"], stats["with_speed"] / total * 100)
        logger.info("  Beaten favorite    : %d (%.1f%%)", stats["beaten_fav"], stats["beaten_fav"] / total * 100)
        logger.info("  Avec ROI place     : %d (%.1f%%)", stats["with_roi_place"], stats["with_roi_place"] / total * 100)
        logger.info("  Courses uniques    : %d", courses_count)
        logger.info("-" * 50)

    elapsed = time.time() - t0
    logger.info("Termine — %d labels, %d courses en %.0fs", total, courses_count, elapsed)


if __name__ == "__main__":
    main()
