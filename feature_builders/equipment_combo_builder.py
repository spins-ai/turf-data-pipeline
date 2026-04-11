#!/usr/bin/env python3
"""
feature_builders.equipment_combo_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Equipment combination interaction features.

Reads partants_master.jsonl in streaming mode, computes per-partant
equipment features including static flags, combo encodings, and
temporal tracking of equipment changes and performance impacts.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the horse equipment stats -- no future leakage.

Produces:
  - equipment_combo.jsonl   in builder_outputs/equipment_combo/

Features per partant (10):
  - eq_oeilleres_flag           : 1 if oeilleres is not null/empty, 0 otherwise
  - eq_deferre_flag             : 1 if deferre is not null/empty, 0 otherwise
  - eq_deferre_type_encoded     : DA=1, DP=2, D4=3, DAP=4, other=5, none=0
  - eq_surcharge_decharge       : surcharge_decharge_kg as float
  - eq_poids_delta              : poids_porte_kg - poids_base_kg
  - eq_equipment_combo_hash     : 0=none, 1=oeilleres_only, 2=deferre_only, 3=both
  - eq_horse_oeilleres_first_time : 1 if first time horse wears oeilleres
  - eq_horse_oeilleres_win_rate : win rate WITH oeilleres vs WITHOUT (delta)
  - eq_horse_deferre_change     : 1 if deferre config changed from last race
  - eq_weight_vs_field_avg      : poids_porte_kg / avg poids_porte_kg in race

Two-pass approach:
  Pass 1: Build lightweight index + compute avg weight per course_uid.
  Pass 2: Sort chronologically, process course-by-course with seek,
           snapshot temporal state BEFORE updating.

Usage:
    python feature_builders/equipment_combo_builder.py
    python feature_builders/equipment_combo_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/equipment_combo")

_LOG_EVERY = 500_000

# Deferre type encoding
_DEFERRE_ENCODING = {
    "DA": 1,
    "DP": 2,
    "D4": 3,
    "DAP": 4,
}

# ===========================================================================
# HELPERS
# ===========================================================================


def _is_nonempty(val: Any) -> bool:
    """Return True if val is a non-empty, non-null string."""
    if val is None:
        return False
    s = str(val).strip()
    return s != "" and s.lower() not in ("none", "nan", "null")


def _safe_float(val: Any) -> Optional[float]:
    """Convert value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _encode_deferre(val: Any) -> int:
    """Encode deferre type: DA=1, DP=2, D4=3, DAP=4, other=5, none=0."""
    if not _is_nonempty(val):
        return 0
    s = str(val).strip().upper()
    return _DEFERRE_ENCODING.get(s, 5)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_equipment_combo_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build equipment combo features from partants_master.jsonl.

    Two-pass approach:
      Pass 1: Build lightweight index (date, course_uid, offset)
              + accumulate avg weight per course_uid.
      Pass 2: Sort index chronologically, process course-by-course
              with seek, stream output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Equipment Combo Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1: Build index + compute average weight per course_uid
    # ------------------------------------------------------------------
    index: list[tuple[str, str, int, int]] = []  # (date, course_uid, num_pmu, offset)
    course_weight_sum: dict[str, float] = defaultdict(float)
    course_weight_count: dict[str, int] = defaultdict(int)
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
                logger.info("  Pass 1: indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

            # Accumulate weight for field average
            poids = _safe_float(rec.get("poids_porte_kg"))
            if poids is not None and poids > 0 and course_uid:
                course_weight_sum[course_uid] += poids
                course_weight_count[course_uid] += 1

    logger.info(
        "Pass 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # Compute average weight per course
    course_avg_weight: dict[str, float] = {}
    for cuid, total_w in course_weight_sum.items():
        cnt = course_weight_count[cuid]
        if cnt > 0:
            course_avg_weight[cuid] = total_w / cnt
    del course_weight_sum, course_weight_count
    gc.collect()

    logger.info("Poids moyen calcule pour %d courses", len(course_avg_weight))

    # ------------------------------------------------------------------
    # Pass 1b: Sort index chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Pass 2: Process course-by-course with temporal tracking
    # ------------------------------------------------------------------
    t2 = time.time()

    # Per-horse temporal state
    # Each horse tracks: {
    #   "ever_oeilleres": bool,  # has horse ever worn oeilleres before?
    #   "oeilleres_wins": int,   # wins with oeilleres
    #   "oeilleres_total": int,  # races with oeilleres
    #   "no_oeilleres_wins": int,
    #   "no_oeilleres_total": int,
    #   "last_deferre": str or None,  # deferre config in last race
    # }
    horse_state: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "ever_oeilleres": False,
        "oeilleres_wins": 0,
        "oeilleres_total": 0,
        "no_oeilleres_wins": 0,
        "no_oeilleres_total": 0,
        "last_deferre": None,
    })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "eq_oeilleres_flag",
        "eq_deferre_flag",
        "eq_deferre_type_encoded",
        "eq_surcharge_decharge",
        "eq_poids_delta",
        "eq_equipment_combo_hash",
        "eq_horse_oeilleres_first_time",
        "eq_horse_oeilleres_win_rate",
        "eq_horse_deferre_change",
        "eq_weight_vs_field_avg",
    ]
    fill_counts = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

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

            # Read records from disk for this course
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # Get avg weight for this race
            avg_weight = course_avg_weight.get(course_uid)

            # -- Snapshot pre-race state, compute features --
            updates: list[tuple[str, bool, bool, str]] = []  # (horse_id, has_oeilleres, is_winner, deferre)

            for rec in course_records:
                horse_id = rec.get("horse_id") or rec.get("nom_cheval") or ""
                partant_uid = rec.get("partant_uid", "")
                oeilleres_raw = rec.get("oeilleres")
                deferre_raw = rec.get("deferre")

                has_oeilleres = _is_nonempty(oeilleres_raw)
                has_deferre = _is_nonempty(deferre_raw)

                features: dict[str, Any] = {"partant_uid": partant_uid}

                # 1. eq_oeilleres_flag
                oeilleres_flag = 1 if has_oeilleres else 0
                features["eq_oeilleres_flag"] = oeilleres_flag
                fill_counts["eq_oeilleres_flag"] += 1

                # 2. eq_deferre_flag
                deferre_flag = 1 if has_deferre else 0
                features["eq_deferre_flag"] = deferre_flag
                fill_counts["eq_deferre_flag"] += 1

                # 3. eq_deferre_type_encoded
                deferre_encoded = _encode_deferre(deferre_raw)
                features["eq_deferre_type_encoded"] = deferre_encoded
                fill_counts["eq_deferre_type_encoded"] += 1

                # 4. eq_surcharge_decharge
                surcharge = _safe_float(rec.get("surcharge_decharge_kg"))
                features["eq_surcharge_decharge"] = surcharge
                if surcharge is not None:
                    fill_counts["eq_surcharge_decharge"] += 1

                # 5. eq_poids_delta
                poids_porte = _safe_float(rec.get("poids_porte_kg"))
                poids_base = _safe_float(rec.get("poids_base_kg"))
                if poids_porte is not None and poids_base is not None:
                    features["eq_poids_delta"] = round(poids_porte - poids_base, 2)
                    fill_counts["eq_poids_delta"] += 1
                else:
                    features["eq_poids_delta"] = None

                # 6. eq_equipment_combo_hash
                if has_oeilleres and has_deferre:
                    combo = 3
                elif has_oeilleres:
                    combo = 1
                elif has_deferre:
                    combo = 2
                else:
                    combo = 0
                features["eq_equipment_combo_hash"] = combo
                fill_counts["eq_equipment_combo_hash"] += 1

                # --- Temporal features (snapshot BEFORE update) ---
                if horse_id:
                    state = horse_state[horse_id]

                    # 7. eq_horse_oeilleres_first_time
                    if has_oeilleres and not state["ever_oeilleres"]:
                        features["eq_horse_oeilleres_first_time"] = 1
                        fill_counts["eq_horse_oeilleres_first_time"] += 1
                    elif has_oeilleres and state["ever_oeilleres"]:
                        features["eq_horse_oeilleres_first_time"] = 0
                        fill_counts["eq_horse_oeilleres_first_time"] += 1
                    else:
                        # Not wearing oeilleres -> not applicable, but set 0
                        features["eq_horse_oeilleres_first_time"] = 0
                        fill_counts["eq_horse_oeilleres_first_time"] += 1

                    # 8. eq_horse_oeilleres_win_rate
                    #    Delta: win_rate_with - win_rate_without
                    wr_with = None
                    wr_without = None
                    if state["oeilleres_total"] > 0:
                        wr_with = state["oeilleres_wins"] / state["oeilleres_total"]
                    if state["no_oeilleres_total"] > 0:
                        wr_without = state["no_oeilleres_wins"] / state["no_oeilleres_total"]
                    if wr_with is not None and wr_without is not None:
                        features["eq_horse_oeilleres_win_rate"] = round(wr_with - wr_without, 4)
                        fill_counts["eq_horse_oeilleres_win_rate"] += 1
                    elif wr_with is not None:
                        features["eq_horse_oeilleres_win_rate"] = round(wr_with, 4)
                        fill_counts["eq_horse_oeilleres_win_rate"] += 1
                    elif wr_without is not None:
                        features["eq_horse_oeilleres_win_rate"] = round(-wr_without, 4)
                        fill_counts["eq_horse_oeilleres_win_rate"] += 1
                    else:
                        features["eq_horse_oeilleres_win_rate"] = None

                    # 9. eq_horse_deferre_change
                    last_def = state["last_deferre"]
                    current_def = str(deferre_raw).strip().upper() if _is_nonempty(deferre_raw) else ""
                    if last_def is not None:
                        features["eq_horse_deferre_change"] = 1 if current_def != last_def else 0
                        fill_counts["eq_horse_deferre_change"] += 1
                    else:
                        # No previous race known
                        features["eq_horse_deferre_change"] = None

                    # Track info for post-race update
                    is_winner = bool(rec.get("is_gagnant"))
                    updates.append((horse_id, has_oeilleres, is_winner,
                                    current_def))
                else:
                    features["eq_horse_oeilleres_first_time"] = None
                    features["eq_horse_oeilleres_win_rate"] = None
                    features["eq_horse_deferre_change"] = None

                # 10. eq_weight_vs_field_avg
                if poids_porte is not None and avg_weight is not None and avg_weight > 0:
                    features["eq_weight_vs_field_avg"] = round(poids_porte / avg_weight, 4)
                    fill_counts["eq_weight_vs_field_avg"] += 1
                else:
                    features["eq_weight_vs_field_avg"] = None

                # Write feature record
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update horse states AFTER snapshotting all partants --
            for update_tuple in updates:
                horse_id, had_oeilleres, was_winner, cur_def = update_tuple
                state = horse_state[horse_id]

                if had_oeilleres:
                    state["ever_oeilleres"] = True
                    state["oeilleres_total"] += 1
                    if was_winner:
                        state["oeilleres_wins"] += 1
                else:
                    state["no_oeilleres_total"] += 1
                    if was_winner:
                        state["no_oeilleres_wins"] += 1

                state["last_deferre"] = cur_def

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Equipment combo build termine: %d features en %.1fs (chevaux suivis: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
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
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features equipment combo a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/equipment_combo/)",
    )
    args = parser.parse_args()

    logger = setup_logging("equipment_combo_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "equipment_combo.jsonl"
    build_equipment_combo_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
