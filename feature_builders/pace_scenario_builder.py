#!/usr/bin/env python3
"""
feature_builders.pace_scenario_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pace-scenario features for horse racing prediction.

Classifies each horse's running style (front-runner / stalker / closer)
from historical early-position data, then evaluates the pace dynamics
of the current field.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant pace-scenario features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the running-style classification -- no future leakage.

Produces:
  - pace_scenario.jsonl   in output/pace_scenario/

Features per partant (5):
  - pace_early_leader_prob : probability this horse leads early
                             (fraction of past races where horse was in top-25%
                             of early positions)
  - pace_finisher_type     : 1=front-runner, 2=stalker, 3=closer
                             (derived from average normalised early-position rank)
  - pace_collapse_risk     : fraction of field that are front-runners (type=1);
                             high value signals likely pace collapse
  - nb_front_runners       : count of horses with pace_finisher_type=1 in this
                             race field
  - pace_advantage         : 1 if horse's type matches the favourable scenario
                             (closer when pace is hot, front-runner when pace is
                             cold), else 0

Usage:
    python feature_builders/pace_scenario_builder.py
    python feature_builders/pace_scenario_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
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
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "pace_scenario"

# Thresholds for running-style classification
# avg_early_rank <= 0.33 => front-runner (1)
# avg_early_rank <= 0.66 => stalker (2)
# else => closer (3)
FRONT_RUNNER_CUTOFF = 0.33
STALKER_CUTOFF = 0.66

# Pace-collapse threshold: if this fraction of field is front-runners
PACE_HOT_THRESHOLD = 0.35

# Minimum past races needed for a confident classification
MIN_HISTORY = 3

# Progress log every N records
_LOG_EVERY = 500_000

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
# PACE HISTORY TRACKER
# ===========================================================================


class _PaceHistory:
    """Track a horse's early-position history across past races."""

    __slots__ = ("early_ranks", "nb_races")

    def __init__(self) -> None:
        self.early_ranks: list[float] = []
        self.nb_races: int = 0

    def avg_early_rank(self) -> Optional[float]:
        if not self.early_ranks:
            return None
        return sum(self.early_ranks) / len(self.early_ranks)

    def early_leader_prob(self) -> Optional[float]:
        """Fraction of races where horse was in top 25% of early positions."""
        if not self.early_ranks:
            return None
        top_quarter = sum(1 for r in self.early_ranks if r <= 0.25)
        return top_quarter / len(self.early_ranks)

    def finisher_type(self) -> Optional[int]:
        """1=front-runner, 2=stalker, 3=closer."""
        avg = self.avg_early_rank()
        if avg is None:
            return None
        if avg <= FRONT_RUNNER_CUTOFF:
            return 1
        if avg <= STALKER_CUTOFF:
            return 2
        return 3


def _extract_early_rank(rec: dict, n_runners: int) -> Optional[float]:
    """Extract a normalised early-position rank from a record.

    Uses 'position_cordee' (position at first checkpoint / early call)
    or falls back to 'position_au_poteau' fields if available.
    Normalised to [0, 1] where 0 = front, 1 = back.
    """
    # Try multiple fields that might indicate early position
    early_pos = (
        rec.get("position_cordee")
        or rec.get("place_cordee")
        or rec.get("place_au_800")
    )
    if early_pos is not None:
        try:
            early_pos = int(early_pos)
            if n_runners > 1 and early_pos > 0:
                return (early_pos - 1) / (n_runners - 1)
        except (ValueError, TypeError):
            pass

    # Fallback: use final position as weak proxy (front-runners often finish
    # near their early position in historical average)
    pos = rec.get("position_arrivee")
    if pos is not None:
        try:
            pos = int(pos)
            if n_runners > 1 and pos > 0:
                return (pos - 1) / (n_runners - 1)
        except (ValueError, TypeError):
            pass

    return None


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_pace_scenario_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build pace-scenario features from partants_master.jsonl.

    Single-pass approach: read all records with needed fields, sort
    chronologically, then process course-by-course.
    """
    logger.info("=== Pace Scenario Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ── Phase 1: Read minimal fields ──
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
            "position_cordee": rec.get("position_cordee"),
            "place_cordee": rec.get("place_cordee"),
            "place_au_800": rec.get("place_au_800"),
            "nb_partants": rec.get("nb_partants"),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # ── Phase 2: Sort chronologically ──
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ── Phase 3: Process course by course ──
    t2 = time.time()
    horse_pace: dict[str, _PaceHistory] = defaultdict(_PaceHistory)
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

        n_runners = len(course_group)
        nb_partants_field = course_group[0].get("nb_partants")
        if nb_partants_field is not None:
            try:
                n_runners_from_data = int(nb_partants_field)
                if n_runners_from_data > 0:
                    n_runners = max(n_runners, n_runners_from_data)
            except (ValueError, TypeError):
                pass

        # ── Snapshot pre-race pace types for all partants ──
        pre_race: list[dict[str, Any]] = []
        for rec in course_group:
            h = rec["cheval"]
            hist = horse_pace[h] if h else _PaceHistory()

            pace_type = hist.finisher_type() if hist.nb_races >= MIN_HISTORY else None
            leader_prob = hist.early_leader_prob()

            pre_race.append({
                "rec": rec,
                "pace_type": pace_type,
                "leader_prob": leader_prob,
            })

        # Count front-runners in this field
        # Use only classified runners as denominator (not total field size),
        # otherwise the fraction is always near 0 when most horses lack history
        types_in_field = [p["pace_type"] for p in pre_race if p["pace_type"] is not None]
        nb_fr = sum(1 for t in types_in_field if t == 1)
        nb_classified = len(types_in_field)
        fr_fraction = nb_fr / nb_classified if nb_classified > 0 else 0.0
        pace_is_hot = fr_fraction >= PACE_HOT_THRESHOLD

        # Emit features
        for pr in pre_race:
            pace_type = pr["pace_type"]
            leader_prob = pr["leader_prob"]

            # pace_advantage: closer benefits from hot pace, front-runner from cold
            pace_adv = None
            if pace_type is not None:
                if pace_type == 3 and pace_is_hot:
                    pace_adv = 1
                elif pace_type == 1 and not pace_is_hot:
                    pace_adv = 1
                else:
                    pace_adv = 0
            elif leader_prob is not None and nb_classified > 0:
                # Fallback for horses without full classification:
                # Use leader_prob as a soft proxy for running style
                is_likely_closer = leader_prob < 0.15
                is_likely_front = leader_prob > 0.50
                if is_likely_closer and pace_is_hot:
                    pace_adv = 1
                elif is_likely_front and not pace_is_hot:
                    pace_adv = 1
                else:
                    pace_adv = 0

            results.append({
                "partant_uid": pr["rec"]["uid"],
                "pace_early_leader_prob": (
                    round(pr["leader_prob"], 4) if pr["leader_prob"] is not None else None
                ),
                "pace_finisher_type": pace_type,
                "pace_collapse_risk": round(fr_fraction, 4),
                "nb_front_runners": nb_fr,
                "pace_advantage": pace_adv,
            })

        # ── Update pace history after race ──
        for rec in course_group:
            h = rec["cheval"]
            if not h:
                continue
            early_rank = _extract_early_rank(rec, n_runners)
            if early_rank is not None:
                horse_pace[h].early_ranks.append(early_rank)
            horse_pace[h].nb_races += 1

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Pace scenario build termine: %d features en %.1fs (chevaux suivis: %d)",
        len(results), elapsed, len(horse_pace),
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
        description="Construction des features pace-scenario a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/pace_scenario/)",
    )
    args = parser.parse_args()

    logger = setup_logging("pace_scenario_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_pace_scenario_features(input_path, logger)

    # Save
    out_path = output_dir / "pace_scenario.jsonl"
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
