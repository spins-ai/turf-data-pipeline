#!/usr/bin/env python3
"""
feature_builders.age_sex_interaction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Age x sex interaction features for horse racing.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant age/sex interaction features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the historical win-rate statistics -- no future leakage.

Produces:
  - age_sex_interaction.jsonl   in builder_outputs/age_sex_interaction/

Features per partant (10):
  - asi_age_sex_combo          : encoded (age * 10 + sex_code) where M=1,F=2,H=3
  - asi_is_mare                : 1 if sexe == "F" or "JUMENT"
  - asi_is_gelding             : 1 if sexe == "H" or "HONGRE"
  - asi_mare_vs_field_wr       : historical win rate of mares vs non-mares (global)
  - asi_age_group_wr           : win rate for this age group (2, 3, 4-5, 6-7, 8+)
  - asi_sex_discipline_wr      : win rate for this sex in this discipline
  - asi_is_pregnant_mare       : 1 if jument_pleine is truthy
  - asi_young_vs_old           : 1 if age <= 4, 0 otherwise
  - asi_age_x_distance         : age * distance / 1000 (interaction term)
  - asi_breed_discipline_match : 1 if breed matches typical discipline

Usage:
    python feature_builders/age_sex_interaction_builder.py
    python feature_builders/age_sex_interaction_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/age_sex_interaction")

# Progress log every N records
_LOG_EVERY = 500_000

# Sex code mapping
_SEX_CODES = {
    "M": 1, "MALE": 1, "MALES": 1, "ENTIER": 1,
    "F": 2, "FEMELLE": 2, "JUMENT": 2,
    "H": 3, "HONGRE": 3,
}

# Breed-discipline typical matches
_BREED_DISCIPLINE = {
    "TROTTEUR": {"TROT", "ATTELE", "MONTE", "TROT ATTELE", "TROT MONTE"},
    "TROTTEUR FRANCAIS": {"TROT", "ATTELE", "MONTE", "TROT ATTELE", "TROT MONTE"},
    "PUR-SANG": {"PLAT", "OBSTACLE", "HAIES", "STEEPLE", "STEEPLE-CHASE", "CROSS"},
    "PUR SANG": {"PLAT", "OBSTACLE", "HAIES", "STEEPLE", "STEEPLE-CHASE", "CROSS"},
    "AQPS": {"OBSTACLE", "HAIES", "STEEPLE", "STEEPLE-CHASE", "CROSS", "PLAT"},
}


def _age_group(age: int) -> str:
    """Map age to group bucket."""
    if age <= 2:
        return "2"
    elif age == 3:
        return "3"
    elif age <= 5:
        return "4-5"
    elif age <= 7:
        return "6-7"
    else:
        return "8+"


def _sex_code(sexe: str) -> Optional[int]:
    """Normalize sexe string to numeric code. Returns None if unknown."""
    if not sexe:
        return None
    return _SEX_CODES.get(sexe.strip().upper())


def _is_mare(sexe: str) -> bool:
    s = (sexe or "").strip().upper()
    return s in ("F", "FEMELLE", "JUMENT")


def _is_gelding(sexe: str) -> bool:
    s = (sexe or "").strip().upper()
    return s in ("H", "HONGRE")


def _breed_matches_discipline(breed: str, discipline: str) -> Optional[int]:
    """Return 1 if breed matches typical discipline, 0 if not, None if unknown."""
    if not breed or not discipline:
        return None
    breed_upper = breed.strip().upper()
    disc_upper = discipline.strip().upper()
    for breed_key, disciplines in _BREED_DISCIPLINE.items():
        if breed_key in breed_upper:
            return 1 if disc_upper in disciplines else 0
    return None


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# MAIN BUILD (memory-optimised: index+sort + seek-based processing)
# ===========================================================================


def build_age_sex_interaction_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build age x sex interaction features from partants_master.jsonl.

    Two-phase approach:
      1. Read sort keys + file byte offsets into memory (lightweight index).
      2. Sort chronologically.
      3. Seek-based course-by-course processing, streaming output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Age x Sex Interaction Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()

    # Global state: {key -> [wins, total]}
    age_group_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    sex_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # "mare" / "non_mare"
    sex_discipline_stats: dict[tuple[int, str], list[int]] = defaultdict(lambda: [0, 0])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "asi_age_sex_combo": 0,
        "asi_is_mare": 0,
        "asi_is_gelding": 0,
        "asi_mare_vs_field_wr": 0,
        "asi_age_group_wr": 0,
        "asi_sex_discipline_wr": 0,
        "asi_is_pregnant_mare": 0,
        "asi_young_vs_old": 0,
        "asi_age_x_distance": 0,
        "asi_breed_discipline_match": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            age = rec.get("age")
            try:
                age = int(age) if age is not None else None
            except (ValueError, TypeError):
                age = None

            distance = rec.get("distance")
            try:
                distance = int(distance) if distance is not None else None
            except (ValueError, TypeError):
                distance = None

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            breed = rec.get("race") or ""
            breed = breed.strip().upper()

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "cheval": rec.get("nom_cheval") or rec.get("horse_id") or "",
                "sexe": rec.get("sexe") or "",
                "age": age,
                "distance": distance,
                "discipline": discipline,
                "breed": breed,
                "gagnant": bool(rec.get("is_gagnant")),
                "jument_pleine": bool(rec.get("jument_pleine")),
            }

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read only this course's records from disk
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # -- Snapshot pre-race stats and emit features --
            post_updates: list[dict] = []

            for rec in course_group:
                sexe_raw = rec["sexe"]
                age = rec["age"]
                distance = rec["distance"]
                discipline = rec["discipline"]
                breed = rec["breed"]

                sc = _sex_code(sexe_raw)
                mare = _is_mare(sexe_raw)
                gelding = _is_gelding(sexe_raw)

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                # 1. asi_age_sex_combo
                if age is not None and sc is not None:
                    features["asi_age_sex_combo"] = age * 10 + sc
                    fill_counts["asi_age_sex_combo"] += 1
                else:
                    features["asi_age_sex_combo"] = None

                # 2. asi_is_mare
                if sexe_raw:
                    features["asi_is_mare"] = 1 if mare else 0
                    fill_counts["asi_is_mare"] += 1
                else:
                    features["asi_is_mare"] = None

                # 3. asi_is_gelding
                if sexe_raw:
                    features["asi_is_gelding"] = 1 if gelding else 0
                    fill_counts["asi_is_gelding"] += 1
                else:
                    features["asi_is_gelding"] = None

                # 4. asi_mare_vs_field_wr (global: mares vs non-mares)
                mare_s = sex_stats["mare"]
                non_mare_s = sex_stats["non_mare"]
                if mare_s[1] >= 10 and non_mare_s[1] >= 10:
                    mare_wr = mare_s[0] / mare_s[1]
                    non_mare_wr = non_mare_s[0] / non_mare_s[1]
                    features["asi_mare_vs_field_wr"] = round(mare_wr - non_mare_wr, 6)
                    fill_counts["asi_mare_vs_field_wr"] += 1
                else:
                    features["asi_mare_vs_field_wr"] = None

                # 5. asi_age_group_wr
                if age is not None:
                    ag = _age_group(age)
                    ag_s = age_group_stats[ag]
                    if ag_s[1] >= 10:
                        features["asi_age_group_wr"] = round(ag_s[0] / ag_s[1], 6)
                        fill_counts["asi_age_group_wr"] += 1
                    else:
                        features["asi_age_group_wr"] = None
                else:
                    features["asi_age_group_wr"] = None

                # 6. asi_sex_discipline_wr
                if sc is not None and discipline:
                    sd_s = sex_discipline_stats[(sc, discipline)]
                    if sd_s[1] >= 10:
                        features["asi_sex_discipline_wr"] = round(sd_s[0] / sd_s[1], 6)
                        fill_counts["asi_sex_discipline_wr"] += 1
                    else:
                        features["asi_sex_discipline_wr"] = None
                else:
                    features["asi_sex_discipline_wr"] = None

                # 7. asi_is_pregnant_mare
                features["asi_is_pregnant_mare"] = 1 if rec["jument_pleine"] else 0
                fill_counts["asi_is_pregnant_mare"] += 1

                # 8. asi_young_vs_old
                if age is not None:
                    features["asi_young_vs_old"] = 1 if age <= 4 else 0
                    fill_counts["asi_young_vs_old"] += 1
                else:
                    features["asi_young_vs_old"] = None

                # 9. asi_age_x_distance
                if age is not None and distance is not None and distance > 0:
                    features["asi_age_x_distance"] = round(age * distance / 1000.0, 4)
                    fill_counts["asi_age_x_distance"] += 1
                else:
                    features["asi_age_x_distance"] = None

                # 10. asi_breed_discipline_match
                bm = _breed_matches_discipline(breed, discipline)
                features["asi_breed_discipline_match"] = bm
                if bm is not None:
                    fill_counts["asi_breed_discipline_match"] += 1

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Prepare deferred update
                post_updates.append(rec)

            # -- Update global stats after race (post-race, no leakage) --
            for rec in post_updates:
                sexe_raw = rec["sexe"]
                age = rec["age"]
                discipline = rec["discipline"]
                is_winner = rec["gagnant"]
                sc = _sex_code(sexe_raw)
                mare = _is_mare(sexe_raw)

                # Update sex_stats (mare vs non-mare)
                if sexe_raw:
                    key = "mare" if mare else "non_mare"
                    sex_stats[key][1] += 1
                    if is_winner:
                        sex_stats[key][0] += 1

                # Update age_group_stats
                if age is not None:
                    ag = _age_group(age)
                    age_group_stats[ag][1] += 1
                    if is_winner:
                        age_group_stats[ag][0] += 1

                # Update sex_discipline_stats
                if sc is not None and discipline:
                    sex_discipline_stats[(sc, discipline)][1] += 1
                    if is_winner:
                        sex_discipline_stats[(sc, discipline)][0] += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Age x Sex build termine: %d features en %.1fs",
        n_written, elapsed,
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info(
            "  %s: %d/%d (%.1f%%)",
            k, v, n_written, 100 * v / n_written if n_written else 0,
        )

    return n_written


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features age x sexe a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/age_sex_interaction/)",
    )
    args = parser.parse_args()

    logger = setup_logging("age_sex_interaction_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "age_sex_interaction.jsonl"
    build_age_sex_interaction_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
