#!/usr/bin/env python3
"""
feature_builders.race_rhythm_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Hippodrome/discipline-level rhythm features that capture how predictable
or surprising races tend to be at a given venue.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant race-rhythm features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the rhythm statistics -- no future leakage.

Produces:
  - race_rhythm.jsonl   in output/race_rhythm/

Features per partant (5):
  - nb_favoris_battus_hippo    : how often favorites (lowest odds) lose at this hippodrome
  - avg_winning_cote_hippo     : average winning odds at hippodrome (high = unpredictable)
  - discipline_predictability  : 1/entropy of winner distribution in discipline
  - course_surprise_index      : how often the winner was >10.0 odds at hippo+distance
  - repeat_winner_rate         : how often same horse wins back-to-back at same hippodrome

Usage:
    python feature_builders/race_rhythm_builder.py
    python feature_builders/race_rhythm_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "race_rhythm"

_LOG_EVERY = 500_000
_MIN_OBS = 3  # Minimum races for stats to be meaningful

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


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# DISTANCE BUCKETING
# ===========================================================================


def _distance_bucket(distance: Any) -> str:
    """Bucket distance into categories for hippo+distance key."""
    if distance is None:
        return "unknown"
    try:
        d = int(distance)
    except (ValueError, TypeError):
        return "unknown"
    if d <= 1300:
        return "sprint"
    if d <= 1700:
        return "mile"
    if d <= 2200:
        return "inter"
    if d <= 3000:
        return "long"
    return "marathon"


# ===========================================================================
# ACCUMULATORS
# ===========================================================================


class _HippoStats:
    """Track hippodrome-level race outcomes for favorites/odds stats."""

    __slots__ = ("nb_races", "fav_losses", "winning_cotes")

    def __init__(self) -> None:
        self.nb_races: int = 0
        self.fav_losses: int = 0
        self.winning_cotes: list[float] = []

    def snapshot_fav_loss_rate(self) -> Optional[float]:
        if self.nb_races < _MIN_OBS:
            return None
        return round(self.fav_losses / self.nb_races, 4)

    def snapshot_avg_winning_cote(self) -> Optional[float]:
        if len(self.winning_cotes) < _MIN_OBS:
            return None
        return round(sum(self.winning_cotes) / len(self.winning_cotes), 4)


class _DisciplineStats:
    """Track discipline-level winner distribution for entropy."""

    __slots__ = ("winner_counts", "total_races")

    def __init__(self) -> None:
        self.winner_counts: dict[str, int] = defaultdict(int)  # horse -> nb wins
        self.total_races: int = 0

    def snapshot_predictability(self) -> Optional[float]:
        """1 / entropy of winner distribution. Higher = more predictable."""
        if self.total_races < _MIN_OBS:
            return None
        total = sum(self.winner_counts.values())
        if total == 0:
            return None
        entropy = 0.0
        for count in self.winner_counts.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)
        if entropy <= 0:
            return None  # Only one winner ever = degenerate
        return round(1.0 / entropy, 4)


class _HippoDistStats:
    """Track hippo+distance surprise index."""

    __slots__ = ("nb_races", "nb_surprises")

    def __init__(self) -> None:
        self.nb_races: int = 0
        self.nb_surprises: int = 0  # winner had odds > 10.0

    def snapshot_surprise_index(self) -> Optional[float]:
        if self.nb_races < _MIN_OBS:
            return None
        return round(self.nb_surprises / self.nb_races, 4)


class _HorseHippoRepeat:
    """Track whether a horse won its last race at this hippodrome."""

    __slots__ = ("last_won",)

    def __init__(self) -> None:
        self.last_won: bool = False


class _HippoRepeatStats:
    """Track repeat-winner rate at hippodrome level."""

    __slots__ = ("total_returning_winners", "total_returning_runners")

    def __init__(self) -> None:
        self.total_returning_winners: int = 0  # prev winner who won again
        self.total_returning_runners: int = 0  # prev winner returning

    def snapshot_repeat_winner_rate(self) -> Optional[float]:
        if self.total_returning_runners < _MIN_OBS:
            return None
        return round(self.total_returning_winners / self.total_returning_runners, 4)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_race_rhythm_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build race rhythm features from partants_master.jsonl.

    Single-pass approach:
      1. Read minimal fields into memory.
      2. Sort chronologically.
      3. Process course-by-course, snapshotting pre-race then updating.
    """
    logger.info("=== Race Rhythm Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote = _safe_float(
            rec.get("rapport_simple_gagnant")
            or rec.get("cote_probable")
            or rec.get("rapport_probable")
        )

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "hippo": (rec.get("hippodrome_normalise") or "").strip(),
            "discipline": (rec.get("discipline") or "").lower().strip(),
            "distance": rec.get("distance"),
            "cote": cote,
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()

    hippo_stats: dict[str, _HippoStats] = defaultdict(_HippoStats)
    discipline_stats: dict[str, _DisciplineStats] = defaultdict(_DisciplineStats)
    hippo_dist_stats: dict[tuple[str, str], _HippoDistStats] = defaultdict(_HippoDistStats)
    horse_hippo_repeat: dict[tuple[str, str], _HorseHippoRepeat] = defaultdict(_HorseHippoRepeat)
    hippo_repeat_stats: dict[str, _HippoRepeatStats] = defaultdict(_HippoRepeatStats)

    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (i < total
               and slim_records[i]["course"] == course_uid
               and slim_records[i]["date"] == course_date):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        hippo = course_group[0]["hippo"]
        discipline = course_group[0]["discipline"]
        dist_bucket = _distance_bucket(course_group[0]["distance"])
        hippo_dist_key = (hippo, dist_bucket)

        # -- Snapshot pre-race features for all partants in this course --
        for rec in course_group:
            if not hippo:
                results.append({
                    "partant_uid": rec["uid"],
                    "nb_favoris_battus_hippo": None,
                    "avg_winning_cote_hippo": None,
                    "discipline_predictability": None,
                    "course_surprise_index": None,
                    "repeat_winner_rate": None,
                })
                continue

            h_stats = hippo_stats[hippo]
            d_stats = discipline_stats[discipline] if discipline else None
            hd_stats = hippo_dist_stats[hippo_dist_key]
            hr_stats = hippo_repeat_stats[hippo]

            results.append({
                "partant_uid": rec["uid"],
                "nb_favoris_battus_hippo": h_stats.snapshot_fav_loss_rate(),
                "avg_winning_cote_hippo": h_stats.snapshot_avg_winning_cote(),
                "discipline_predictability": d_stats.snapshot_predictability() if d_stats else None,
                "course_surprise_index": hd_stats.snapshot_surprise_index(),
                "repeat_winner_rate": hr_stats.snapshot_repeat_winner_rate(),
            })

        # -- Update stats after snapshotting (post-race) --
        # Determine race-level outcomes
        winner = None
        winner_cote: Optional[float] = None
        fav_cote: Optional[float] = None

        for rec in course_group:
            cote = rec["cote"]
            if cote is not None:
                if fav_cote is None or cote < fav_cote:
                    fav_cote = cote
            if rec["gagnant"]:
                winner = rec["cheval"]
                winner_cote = cote

        if hippo:
            h_stats = hippo_stats[hippo]
            h_stats.nb_races += 1

            # Favorite beaten?
            if winner_cote is not None and fav_cote is not None:
                if winner_cote > fav_cote:
                    h_stats.fav_losses += 1

            # Winning odds
            if winner_cote is not None:
                h_stats.winning_cotes.append(winner_cote)

            # Discipline winner distribution
            if discipline and winner:
                d_stats = discipline_stats[discipline]
                d_stats.winner_counts[winner] += 1
                d_stats.total_races += 1

            # Surprise index (winner odds > 10.0)
            hd_stats = hippo_dist_stats[hippo_dist_key]
            hd_stats.nb_races += 1
            if winner_cote is not None and winner_cote > 10.0:
                hd_stats.nb_surprises += 1

            # Repeat winner tracking
            hr_stats = hippo_repeat_stats[hippo]
            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    continue
                key = (cheval, hippo)
                repeat_state = horse_hippo_repeat[key]

                if repeat_state.last_won:
                    # This horse won last time at this hippo
                    hr_stats.total_returning_runners += 1
                    if rec["gagnant"]:
                        hr_stats.total_returning_winners += 1

                # Update last_won status
                repeat_state.last_won = rec["gagnant"]

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Race rhythm build termine: %d features en %.1fs "
        "(hippodromes: %d, disciplines: %d)",
        len(results), elapsed,
        len(hippo_stats), len(discipline_stats),
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
        description="Construction des features rythme de course a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: output/race_rhythm/)",
    )
    args = parser.parse_args()

    logger = setup_logging("race_rhythm_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_race_rhythm_features(input_path, logger)

    # Save
    out_path = output_dir / "race_rhythm.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_count, 100 * v / total_count)


if __name__ == "__main__":
    main()
