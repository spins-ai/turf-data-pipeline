#!/usr/bin/env python3
"""
feature_builders.uncertainty_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Uncertainty and variability features for horses.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant uncertainty/dispersion features
from historical race results.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the features -- no future leakage.

Produces:
  - uncertainty_features.jsonl   in output/uncertainty_features/

Features per partant:
  - prediction_variance   : variance of horse's recent positions (last 10)
  - result_entropy         : Shannon entropy of position distribution
  - upset_potential        : how often this horse beats the favorite
  - consistency_vs_class   : std_positions / mean_position (CV)
  - form_uncertainty       : max - min position in last 5 (range)

Usage:
    python feature_builders/uncertainty_builder.py
    python feature_builders/uncertainty_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/uncertainty")

# Window sizes
_RECENT_WINDOW = 10   # for prediction_variance
_FORM_WINDOW = 5      # for form_uncertainty
_ENTROPY_MAX_BINS = 20  # max distinct positions for entropy

# Progress log every N records
_LOG_EVERY = 500_000


# ===========================================================================
# STATS HELPERS (no numpy)
# ===========================================================================


def _mean(vals: list[float]) -> float:
    """Arithmetic mean."""
    return sum(vals) / len(vals) if vals else 0.0


def _variance(vals: list[float]) -> float:
    """Population variance."""
    if len(vals) < 2:
        return 0.0
    m = _mean(vals)
    return sum((x - m) ** 2 for x in vals) / len(vals)


def _std(vals: list[float]) -> float:
    """Population standard deviation."""
    return math.sqrt(_variance(vals))


def _shannon_entropy(vals: list[float]) -> float:
    """Shannon entropy of value distribution.

    Groups values into integer bins (position 1, 2, 3, ...) and computes
    the entropy of the resulting distribution. Higher entropy means more
    unpredictable results.
    """
    if not vals:
        return 0.0
    counts: dict[int, int] = defaultdict(int)
    for v in vals:
        counts[int(round(v))] += 1
    n = len(vals)
    entropy = 0.0
    for c in counts.values():
        if c > 0:
            p = c / n
            entropy -= p * math.log2(p)
    return entropy


# ===========================================================================
# HORSE HISTORY TRACKER
# ===========================================================================


class _HorseState:
    """Per-horse accumulator for uncertainty features."""

    __slots__ = (
        "recent_positions",   # deque(maxlen=10) for variance
        "form_positions",     # deque(maxlen=5) for form_uncertainty
        "all_positions",      # all positions for entropy
        "nb_races",
        "nb_beat_favorite",   # times horse beat the race favorite
        "nb_not_favorite",    # times horse was NOT the favorite
    )

    def __init__(self) -> None:
        self.recent_positions: deque[float] = deque(maxlen=_RECENT_WINDOW)
        self.form_positions: deque[float] = deque(maxlen=_FORM_WINDOW)
        self.all_positions: list[float] = []
        self.nb_races: int = 0
        self.nb_beat_favorite: int = 0
        self.nb_not_favorite: int = 0


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
# MAIN BUILD
# ===========================================================================


def build_uncertainty_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build uncertainty features from partants_master.jsonl.

    Single-pass approach: read minimal fields, sort chronologically,
    then process course-by-course. For each partant, features are
    computed from the horse's history BEFORE the current race.
    After emitting features, update the horse's history with the
    current race results (including upset detection).
    """
    logger.info("=== Uncertainty Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields into memory --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "position": rec.get("position_arrivee"),
            "cote": rec.get("rapport_pmu", rec.get("cote_probable")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        # Collect all partants for this course
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

        # -- Emit features from pre-race history (no leakage) --
        for rec in course_group:
            cheval = rec["cheval"]

            if not cheval or cheval not in horse_state:
                # No history yet
                results.append({
                    "partant_uid": rec["uid"],
                    "prediction_variance": None,
                    "result_entropy": None,
                    "upset_potential": None,
                    "consistency_vs_class": None,
                    "form_uncertainty": None,
                })
                continue

            st = horse_state[cheval]

            # prediction_variance: variance of recent positions (last 10)
            recent = list(st.recent_positions)
            pred_var = (
                round(_variance(recent), 4)
                if len(recent) >= 2 else None
            )

            # result_entropy: Shannon entropy of all historical positions
            ent = (
                round(_shannon_entropy(st.all_positions), 4)
                if st.all_positions else None
            )

            # upset_potential: fraction of times horse beat the favorite
            # (only counted when horse was NOT the favorite)
            upset = (
                round(st.nb_beat_favorite / st.nb_not_favorite, 4)
                if st.nb_not_favorite > 0 else None
            )

            # consistency_vs_class: CV = std / mean of positions
            cv = None
            if len(recent) >= 2:
                m = _mean(recent)
                if m > 0:
                    cv = round(_std(recent) / m, 4)

            # form_uncertainty: range of last 5 positions
            form = list(st.form_positions)
            form_unc = (
                round(max(form) - min(form), 2)
                if len(form) >= 2 else None
            )

            results.append({
                "partant_uid": rec["uid"],
                "prediction_variance": pred_var,
                "result_entropy": ent,
                "upset_potential": upset,
                "consistency_vs_class": cv,
                "form_uncertainty": form_unc,
            })

        # -- Determine favorite for this course (lowest cote) --
        favorite_cheval = None
        best_cote = float("inf")
        for rec in course_group:
            cote = rec["cote"]
            if cote is not None:
                try:
                    cote_f = float(cote)
                    if 0 < cote_f < best_cote:
                        best_cote = cote_f
                        favorite_cheval = rec["cheval"]
                except (ValueError, TypeError):
                    pass

        # Determine favorite's finishing position
        favorite_position: Optional[float] = None
        if favorite_cheval:
            for rec in course_group:
                if rec["cheval"] == favorite_cheval and rec["position"] is not None:
                    try:
                        favorite_position = float(rec["position"])
                    except (ValueError, TypeError):
                        pass
                    break

        # -- Update histories AFTER emitting (temporal integrity) --
        for rec in course_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            st = horse_state[cheval]
            st.nb_races += 1

            pos = rec["position"]
            if pos is not None:
                try:
                    pos_f = float(pos)
                    if pos_f > 0:
                        st.recent_positions.append(pos_f)
                        st.form_positions.append(pos_f)
                        st.all_positions.append(pos_f)

                        # Track upsets: did this horse beat the favorite?
                        if (
                            favorite_cheval
                            and cheval != favorite_cheval
                            and favorite_position is not None
                        ):
                            st.nb_not_favorite += 1
                            if pos_f < favorite_position:
                                st.nb_beat_favorite += 1
                except (ValueError, TypeError):
                    pass

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Uncertainty build termine: %d features en %.1fs (chevaux suivis: %d)",
        len(results), elapsed, len(horse_state),
    )

    return results


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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features d'incertitude a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/uncertainty_features/)",
    )
    args = parser.parse_args()

    logger = setup_logging("uncertainty_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_uncertainty_features(input_path, logger)

    # Save
    out_path = output_dir / "uncertainty_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
