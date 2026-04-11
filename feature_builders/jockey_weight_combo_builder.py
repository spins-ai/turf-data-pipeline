#!/usr/bin/env python3
"""
feature_builders.jockey_weight_combo_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Jockey x weight combination features -- how well jockeys perform at
different weight ranges.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant jockey-weight combo features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage. Snapshot BEFORE update.

Produces:
  - jockey_weight_combo.jsonl   in builder_outputs/jockey_weight_combo/

Features per partant (8):
  - jwc_jockey_weight_bracket_wr      : jockey's win rate at this weight bracket
  - jwc_jockey_preferred_weight       : weight bracket where jockey has best win rate
  - jwc_is_jockey_preferred_weight    : 1 if current weight matches preferred bracket
  - jwc_jockey_light_wr               : jockey's win rate when carrying < 54kg
  - jwc_jockey_heavy_wr               : jockey's win rate when carrying > 58kg
  - jwc_jockey_weight_range_wr_diff   : best_bracket_wr - worst_bracket_wr (versatility)
  - jwc_jockey_distance_x_weight      : jockey's wr at this distance bucket AND weight bracket
  - jwc_weight_advantage              : jockey's wr at this bracket vs overall jockey wr

Weight brackets: light (<54), normal (54-58), heavy (58-62), top (>62).
Distance buckets: sprint (<1400), mile (1400-1900), inter (1900-2400), stayer (>=2400).

Memory-optimised version:
  - Phase 1 reads only sort keys + file byte offsets (not full dicts)
  - Phase 2 streams output to disk via seek-based re-reads
  - gc.collect() called every 500K records
  - Write to .tmp then atomic rename

Usage:
    python feature_builders/jockey_weight_combo_builder.py
    python feature_builders/jockey_weight_combo_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_weight_combo")

# Progress log every N records
_LOG_EVERY = 500_000

# Weight bracket boundaries (kg)
_LIGHT_MAX = 54.0
_NORMAL_MAX = 58.0
_HEAVY_MAX = 62.0

# Distance bucket boundaries (m)
_SPRINT_MAX = 1400
_MILE_MAX = 1900
_INTER_MAX = 2400

# Minimum races in a bracket to produce a win rate
_MIN_BRACKET_RACES = 3


# ===========================================================================
# HELPERS
# ===========================================================================


def _weight_bracket(kg: float) -> str:
    """Classify weight into light/normal/heavy/top."""
    if kg < _LIGHT_MAX:
        return "light"
    elif kg <= _NORMAL_MAX:
        return "normal"
    elif kg <= _HEAVY_MAX:
        return "heavy"
    else:
        return "top"


def _distance_bucket(dist: float) -> str:
    """Classify distance into sprint/mile/inter/stayer."""
    if dist < _SPRINT_MAX:
        return "sprint"
    elif dist <= _MILE_MAX:
        return "mile"
    elif dist <= _INTER_MAX:
        return "inter"
    else:
        return "stayer"


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v and v > 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _JockeyState:
    """Tracks jockey performance across weight brackets and distance-weight combos.

    State fields:
      - weight_bracket_stats : {bracket -> [wins, total]}
      - dist_weight_stats    : {(dist_bucket, weight_bracket) -> [wins, total]}
      - overall              : [wins, total]
    """

    __slots__ = ("weight_bracket_stats", "dist_weight_stats", "overall")

    def __init__(self) -> None:
        self.weight_bracket_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.dist_weight_stats: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.overall: list[int] = [0, 0]  # [wins, total]

    def snapshot(
        self,
        weight_bracket: Optional[str],
        dist_bucket: Optional[str],
    ) -> dict[str, Any]:
        """Compute features using only past data (strict temporal)."""
        feats: dict[str, Any] = {
            "jwc_jockey_weight_bracket_wr": None,
            "jwc_jockey_preferred_weight": None,
            "jwc_is_jockey_preferred_weight": None,
            "jwc_jockey_light_wr": None,
            "jwc_jockey_heavy_wr": None,
            "jwc_jockey_weight_range_wr_diff": None,
            "jwc_jockey_distance_x_weight": None,
            "jwc_weight_advantage": None,
        }

        total_overall = self.overall[1]
        if total_overall == 0:
            return feats

        overall_wr = self.overall[0] / total_overall

        # --- Win rate at current weight bracket ---
        if weight_bracket is not None:
            stats = self.weight_bracket_stats.get(weight_bracket)
            if stats is not None and stats[1] >= _MIN_BRACKET_RACES:
                bracket_wr = stats[0] / stats[1]
                feats["jwc_jockey_weight_bracket_wr"] = round(bracket_wr, 4)
                # Weight advantage vs overall
                feats["jwc_weight_advantage"] = round(bracket_wr - overall_wr, 4)

        # --- Light / heavy WR ---
        light_stats = self.weight_bracket_stats.get("light")
        if light_stats is not None and light_stats[1] >= _MIN_BRACKET_RACES:
            feats["jwc_jockey_light_wr"] = round(light_stats[0] / light_stats[1], 4)

        heavy_stats = self.weight_bracket_stats.get("heavy")
        if heavy_stats is not None and heavy_stats[1] >= _MIN_BRACKET_RACES:
            feats["jwc_jockey_heavy_wr"] = round(heavy_stats[0] / heavy_stats[1], 4)

        # --- Preferred weight bracket (best WR among brackets with enough data) ---
        best_bracket: Optional[str] = None
        best_wr = -1.0
        worst_wr = 2.0
        brackets_with_data = 0

        for bkt in ("light", "normal", "heavy", "top"):
            s = self.weight_bracket_stats.get(bkt)
            if s is None or s[1] < _MIN_BRACKET_RACES:
                continue
            brackets_with_data += 1
            wr = s[0] / s[1]
            if wr > best_wr:
                best_wr = wr
                best_bracket = bkt
            if wr < worst_wr:
                worst_wr = wr

        if best_bracket is not None:
            feats["jwc_jockey_preferred_weight"] = best_bracket
            if weight_bracket is not None:
                feats["jwc_is_jockey_preferred_weight"] = (
                    1 if weight_bracket == best_bracket else 0
                )

        # --- Versatility: best - worst bracket WR ---
        if brackets_with_data >= 2:
            feats["jwc_jockey_weight_range_wr_diff"] = round(best_wr - worst_wr, 4)

        # --- Distance x weight combo WR ---
        if dist_bucket is not None and weight_bracket is not None:
            dw_key = (dist_bucket, weight_bracket)
            dw_stats = self.dist_weight_stats.get(dw_key)
            if dw_stats is not None and dw_stats[1] >= _MIN_BRACKET_RACES:
                feats["jwc_jockey_distance_x_weight"] = round(
                    dw_stats[0] / dw_stats[1], 4
                )

        return feats

    def update(
        self,
        weight_bracket: Optional[str],
        dist_bucket: Optional[str],
        is_winner: bool,
    ) -> None:
        """Update state with a new race result (post-race)."""
        win_int = 1 if is_winner else 0

        self.overall[0] += win_int
        self.overall[1] += 1

        if weight_bracket is not None:
            s = self.weight_bracket_stats[weight_bracket]
            s[0] += win_int
            s[1] += 1

        if dist_bucket is not None and weight_bracket is not None:
            dw = self.dist_weight_stats[(dist_bucket, weight_bracket)]
            dw[0] += win_int
            dw[1] += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_jockey_weight_combo_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build jockey-weight combo features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Jockey Weight Combo Builder ===")
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
    jockey_state: dict[str, _JockeyState] = defaultdict(_JockeyState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "jwc_jockey_weight_bracket_wr",
        "jwc_jockey_preferred_weight",
        "jwc_is_jockey_preferred_weight",
        "jwc_jockey_light_wr",
        "jwc_jockey_heavy_wr",
        "jwc_jockey_weight_range_wr_diff",
        "jwc_jockey_distance_x_weight",
        "jwc_weight_advantage",
    ]
    fill_counts = {k: 0 for k in feature_names}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
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

            # Read only this course's records from disk
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                course_records.append(rec)

            # -- Snapshot pre-race stats & emit features (temporal integrity) --
            post_updates: list[tuple[str, Optional[str], Optional[str], bool]] = []

            for rec in course_records:
                jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
                partant_uid = rec.get("partant_uid")
                is_winner = bool(rec.get("is_gagnant"))

                poids = _safe_float(rec.get("poids_porte_kg"))
                distance = _safe_float(rec.get("distance"))

                wb = _weight_bracket(poids) if poids is not None else None
                db = _distance_bucket(distance) if distance is not None else None

                features: dict[str, Any] = {"partant_uid": partant_uid}

                if jockey:
                    st = jockey_state[jockey]
                    snap = st.snapshot(wb, db)
                    features.update(snap)

                    for fname in feature_names:
                        if features.get(fname) is not None:
                            fill_counts[fname] += 1
                else:
                    for fname in feature_names:
                        features[fname] = None

                # Write to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Queue post-race update
                post_updates.append((jockey, wb, db, is_winner))

            # -- Update states AFTER race (no leakage) --
            for jockey, wb, db, is_winner in post_updates:
                if not jockey:
                    continue
                jockey_state[jockey].update(wb, db, is_winner)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Jockey weight combo build termine: %d features en %.1fs (jockeys: %d)",
        n_written, elapsed, len(jockey_state),
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


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features jockey-weight combo a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_weight_combo_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {input_path}")

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "jockey_weight_combo.jsonl"
    build_jockey_weight_combo_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
