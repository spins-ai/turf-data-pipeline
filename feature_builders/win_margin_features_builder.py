#!/usr/bin/env python3
"""
feature_builders.win_margin_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Win margin and finishing gap features per partant.

Temporal integrity: for feature 7 (wmf_horse_avg_time_behind_pct), only
races with date < D contribute to the running average -- no future leakage.

Produces:
  - win_margin_features.jsonl  in builder_outputs/win_margin_features/

Features per partant (8):
  - wmf_time_behind_winner      : (horse_temps - winner_temps) in ms
  - wmf_time_behind_pct         : time_behind / winner_temps * 100
  - wmf_position_margin         : position_arrivee - 1 (0 for winner)
  - wmf_field_time_spread       : (slowest - fastest) / fastest * 100
  - wmf_horse_in_top_quarter    : 1 if horse in fastest 25% of the field
  - wmf_relative_speed_figure   : (winner_temps / horse_temps) * 100
  - wmf_horse_avg_time_behind_pct : horse's running average of time_behind_pct
  - wmf_has_timing              : 1 if temps_ms is available

Two-pass approach:
  Pass 1 — stream input, group by course, compute per-course timing aggregates
  Pass 2 — compute per-partant features, stream output to disk

Usage:
    python feature_builders/win_margin_features_builder.py
    python feature_builders/win_margin_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/win_margin_features")

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_win_margin_features(input_path: Path, output_path: Path, logger) -> int:
    """Build win margin features from partants_master.jsonl.

    Two-pass approach:
      Pass 1: read all records, extract slim data, sort chronologically,
              group by course, compute per-course timing aggregates.
      Pass 2: compute per-partant features with temporal tracking for
              wmf_horse_avg_time_behind_pct, stream output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Win Margin Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1: read minimal data, sort chronologically, group by course
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "course": rec.get("course_uid", "") or "",
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "temps_ms": _safe_float(rec.get("temps_ms")),
            "position": _safe_int(rec.get("position_arrivee")),
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Pass 1 lecture terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # Sort chronologically
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Pass 2: compute per-course aggregates + per-partant features
    # ------------------------------------------------------------------
    t2 = time.time()

    # Temporal tracking for wmf_horse_avg_time_behind_pct
    # horse -> (running_sum, running_count)
    horse_state: dict[str, list[float]] = {}  # horse -> [sum, count]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    n_processed = 0
    total = len(slim_records)

    fill_counts: dict[str, int] = {
        "wmf_time_behind_winner": 0,
        "wmf_time_behind_pct": 0,
        "wmf_position_margin": 0,
        "wmf_field_time_spread": 0,
        "wmf_horse_in_top_quarter": 0,
        "wmf_relative_speed_figure": 0,
        "wmf_horse_avg_time_behind_pct": 0,
        "wmf_has_timing": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        i = 0
        while i < total:
            # Collect all records for this course
            course_uid = slim_records[i]["course"]
            course_date = slim_records[i]["date"]
            course_group: list[dict] = []

            while (
                i < total
                and slim_records[i]["course"] == course_uid
                and slim_records[i]["date"] == course_date
            ):
                course_group.append(slim_records[i])
                i += 1

            if not course_group:
                continue

            # --- Per-course timing aggregates ---
            # Find winner temps
            winner_temps: Optional[float] = None
            for rec in course_group:
                if rec["is_gagnant"] and rec["temps_ms"] is not None:
                    winner_temps = rec["temps_ms"]
                    break
            # Fallback: if no explicit winner, use the horse with position 1
            if winner_temps is None:
                for rec in course_group:
                    if rec["position"] == 1 and rec["temps_ms"] is not None:
                        winner_temps = rec["temps_ms"]
                        break

            # Collect all valid temps for field spread / top quarter
            all_temps = [rec["temps_ms"] for rec in course_group if rec["temps_ms"] is not None]
            all_temps_sorted = sorted(all_temps) if all_temps else []

            fastest_temps = all_temps_sorted[0] if all_temps_sorted else None
            slowest_temps = all_temps_sorted[-1] if all_temps_sorted else None

            # Field time spread
            field_time_spread: Optional[float] = None
            if fastest_temps is not None and slowest_temps is not None and fastest_temps > 0:
                field_time_spread = round(
                    (slowest_temps - fastest_temps) / fastest_temps * 100, 4
                )

            # Top quarter threshold
            top_quarter_threshold: Optional[float] = None
            if all_temps_sorted:
                q_idx = max(0, len(all_temps_sorted) // 4 - 1)
                # The fastest 25% means the lowest temps values
                top_quarter_threshold = all_temps_sorted[min(q_idx, len(all_temps_sorted) - 1)]

            # --- Snapshot pre-race temporal features, then compute per-partant ---
            for rec in course_group:
                uid = rec["uid"]
                cheval = rec["cheval"]
                temps = rec["temps_ms"]
                position = rec["position"]

                feats: dict[str, Any] = {"partant_uid": uid}

                # wmf_has_timing
                has_timing = 1 if temps is not None else 0
                feats["wmf_has_timing"] = has_timing
                fill_counts["wmf_has_timing"] += 1  # always filled

                # wmf_time_behind_winner
                if temps is not None and winner_temps is not None:
                    time_behind = round(temps - winner_temps, 2)
                    feats["wmf_time_behind_winner"] = time_behind
                    fill_counts["wmf_time_behind_winner"] += 1
                else:
                    feats["wmf_time_behind_winner"] = None

                # wmf_time_behind_pct
                if temps is not None and winner_temps is not None and winner_temps > 0:
                    behind_pct = round((temps - winner_temps) / winner_temps * 100, 4)
                    feats["wmf_time_behind_pct"] = behind_pct
                    fill_counts["wmf_time_behind_pct"] += 1
                else:
                    behind_pct = None
                    feats["wmf_time_behind_pct"] = None

                # wmf_position_margin
                if position is not None and position > 0:
                    feats["wmf_position_margin"] = position - 1
                    fill_counts["wmf_position_margin"] += 1
                else:
                    feats["wmf_position_margin"] = None

                # wmf_field_time_spread
                feats["wmf_field_time_spread"] = field_time_spread
                if field_time_spread is not None:
                    fill_counts["wmf_field_time_spread"] += 1

                # wmf_horse_in_top_quarter
                if temps is not None and top_quarter_threshold is not None:
                    feats["wmf_horse_in_top_quarter"] = int(temps <= top_quarter_threshold)
                    fill_counts["wmf_horse_in_top_quarter"] += 1
                else:
                    feats["wmf_horse_in_top_quarter"] = None

                # wmf_relative_speed_figure
                if winner_temps is not None and temps is not None and temps > 0:
                    feats["wmf_relative_speed_figure"] = round(
                        (winner_temps / temps) * 100, 4
                    )
                    fill_counts["wmf_relative_speed_figure"] += 1
                else:
                    feats["wmf_relative_speed_figure"] = None

                # wmf_horse_avg_time_behind_pct (temporal: snapshot BEFORE update)
                if cheval and cheval in horse_state:
                    s, c = horse_state[cheval]
                    feats["wmf_horse_avg_time_behind_pct"] = round(s / c, 4)
                    fill_counts["wmf_horse_avg_time_behind_pct"] += 1
                else:
                    feats["wmf_horse_avg_time_behind_pct"] = None

                fout.write(json.dumps(feats, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # --- Update temporal state AFTER snapshotting ---
            for rec in course_group:
                cheval = rec["cheval"]
                temps = rec["temps_ms"]
                if cheval is None or temps is None or winner_temps is None or winner_temps <= 0:
                    continue
                pct = (temps - winner_temps) / winner_temps * 100
                if cheval in horse_state:
                    horse_state[cheval][0] += pct
                    horse_state[cheval][1] += 1
                else:
                    horse_state[cheval] = [pct, 1]

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Win margin features build termine: %d features en %.1fs (horses tracked: %d)",
        n_written, elapsed, len(horse_state),
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
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features win margin a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=str(INPUT_PARTANTS),
        help="Chemin vers partants_master.jsonl",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("win_margin_features_builder")
    logger.info("=" * 70)
    logger.info("win_margin_features_builder.py — Win Margin & Finishing Gap Features")
    logger.info("=" * 70)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "win_margin_features.jsonl"

    build_win_margin_features(input_path, out_path, logger)

    logger.info("Termine.")


if __name__ == "__main__":
    main()
