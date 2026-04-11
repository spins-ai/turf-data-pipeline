#!/usr/bin/env python3
"""
feature_builders.cumulative_earnings_rank_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cumulative earnings ranking features -- tracking horse's financial standing
relative to competitors over time.

Two-pass streaming builder:
  Pass 1: aggregate per course (gains lists, annual gains, num_pmu)
  Pass 2: compute per-partant ranking features

Temporal integrity: features are intra-race (comparing horses within the same
field using pre-race career/annual earnings) -- no future leakage.

Produces:
  - cumulative_earnings_rank.jsonl  in builder_outputs/cumulative_earnings_rank/

Features per partant (8):
  - cer_gains_rank_in_field       : rank of horse's gains_carriere within race (1=richest)
  - cer_gains_percentile_field    : gains rank / nombre_partants
  - cer_gains_vs_field_median     : horse's gains / field median gains
  - cer_gains_vs_field_max        : horse's gains / max gains in field
  - cer_is_richest                : 1 if horse has highest gains in the race
  - cer_is_poorest                : 1 if horse has lowest gains in the race
  - cer_gains_spread_field        : (max - min gains) / mean gains in field
  - cer_annual_gains_rank         : rank of horse's gains_annee within field (1=highest)

Usage:
    python feature_builders/cumulative_earnings_rank_builder.py
    python feature_builders/cumulative_earnings_rank_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/cumulative_earnings_rank")

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
    """Convert to float, return None on failure or NaN."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _rank_descending(values: list[Optional[float]], value: Optional[float]) -> Optional[int]:
    """Return 1-based rank of *value* in *values* (descending, 1 = highest).

    None values are excluded from ranking.  Returns None if value is None
    or no valid values exist.
    """
    if value is None:
        return None
    valid = sorted((v for v in values if v is not None), reverse=True)
    if not valid:
        return None
    # Find position (ties share the best rank)
    for i, v in enumerate(valid):
        if value >= v:
            return i + 1
    return len(valid)


# ===========================================================================
# MAIN BUILD (two-pass)
# ===========================================================================


def build_cumulative_earnings_rank_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build cumulative earnings rank features in two passes.

    Pass 1: read all records, group lightweight data by course_uid.
    Pass 2: compute per-partant features and write to disk.
    Returns total records written.
    """
    logger.info("=== Cumulative Earnings Rank Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Pass 1: aggregate per course
    # ------------------------------------------------------------------
    # course_uid -> list of {uid, gains_carriere, gains_annee, num_pmu}
    course_data: dict[str, list[dict]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Pass 1 - Lu %d records...", n_read)
            gc.collect()

        course_key = rec.get("course_uid", "")

        gains_carriere = _safe_float(rec.get("gains_carriere_euros"))
        if gains_carriere is None:
            gains_carriere = _safe_float(rec.get("gains_carriere"))

        gains_annee = _safe_float(rec.get("gains_annee_euros"))
        if gains_annee is None:
            gains_annee = _safe_float(rec.get("gains_annee"))

        course_data[course_key].append({
            "uid": rec.get("partant_uid"),
            "gains_carriere": gains_carriere,
            "gains_annee": gains_annee,
            "num_pmu": rec.get("num_pmu"),
        })

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        n_read, len(course_data), time.time() - t0,
    )
    gc.collect()

    # ------------------------------------------------------------------
    # Pass 2: compute per-partant features, write to disk
    # ------------------------------------------------------------------
    t1 = time.time()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    fill_counts: dict[str, int] = {
        "cer_gains_rank_in_field": 0,
        "cer_gains_percentile_field": 0,
        "cer_gains_vs_field_median": 0,
        "cer_gains_vs_field_max": 0,
        "cer_is_richest": 0,
        "cer_is_poorest": 0,
        "cer_gains_spread_field": 0,
        "cer_annual_gains_rank": 0,
    }

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        for course_uid, runners in course_data.items():
            # Collect valid career gains for field-level stats
            career_gains = [r["gains_carriere"] for r in runners if r["gains_carriere"] is not None]
            annual_gains = [r["gains_annee"] for r in runners if r["gains_annee"] is not None]

            field_size = len(runners)
            field_median = median(career_gains) if career_gains else None
            field_max = max(career_gains) if career_gains else None
            field_min = min(career_gains) if career_gains else None
            field_mean = (sum(career_gains) / len(career_gains)) if career_gains else None

            # Gains spread: (max - min) / mean
            gains_spread: Optional[float] = None
            if field_max is not None and field_min is not None and field_mean is not None and field_mean > 0:
                gains_spread = round((field_max - field_min) / field_mean, 4)

            for r in runners:
                gc_val = r["gains_carriere"]
                ga_val = r["gains_annee"]

                feats: dict[str, Any] = {"partant_uid": r["uid"]}

                # --- cer_gains_rank_in_field ---
                rank = _rank_descending(career_gains, gc_val)
                feats["cer_gains_rank_in_field"] = rank

                # --- cer_gains_percentile_field ---
                if rank is not None and field_size > 0:
                    feats["cer_gains_percentile_field"] = round(rank / field_size, 4)
                else:
                    feats["cer_gains_percentile_field"] = None

                # --- cer_gains_vs_field_median ---
                if gc_val is not None and field_median is not None and field_median > 0:
                    feats["cer_gains_vs_field_median"] = round(gc_val / field_median, 4)
                else:
                    feats["cer_gains_vs_field_median"] = None

                # --- cer_gains_vs_field_max ---
                if gc_val is not None and field_max is not None and field_max > 0:
                    feats["cer_gains_vs_field_max"] = round(gc_val / field_max, 4)
                else:
                    feats["cer_gains_vs_field_max"] = None

                # --- cer_is_richest ---
                if gc_val is not None and field_max is not None:
                    feats["cer_is_richest"] = int(abs(gc_val - field_max) < 0.01)
                else:
                    feats["cer_is_richest"] = None

                # --- cer_is_poorest ---
                if gc_val is not None and field_min is not None:
                    feats["cer_is_poorest"] = int(abs(gc_val - field_min) < 0.01)
                else:
                    feats["cer_is_poorest"] = None

                # --- cer_gains_spread_field ---
                feats["cer_gains_spread_field"] = gains_spread

                # --- cer_annual_gains_rank ---
                feats["cer_annual_gains_rank"] = _rank_descending(annual_gains, ga_val)

                # Track fill rates
                for k in fill_counts:
                    if feats.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(feats, ensure_ascii=False))
                fout.write("\n")
                n_written += 1

            if n_written % _LOG_EVERY == 0 and n_written > 0:
                logger.info("  Pass 2 - Ecrit %d records...", n_written)
                gc.collect()

    # Atomic rename
    if output_path.exists():
        output_path.unlink()
    tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Cumulative earnings rank build termine: %d features en %.1fs (courses: %d)",
        n_written, elapsed, len(course_data),
    )

    # Fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written > 0 else 0.0
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
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features cumulative earnings rank a partir de partants_master"
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

    logger = setup_logging("cumulative_earnings_rank_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "cumulative_earnings_rank.jsonl"
    build_cumulative_earnings_rank_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
