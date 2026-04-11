#!/usr/bin/env python3
"""
feature_builders.hippodrome_profile_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep hippodrome profiling features -- characteristics of each racecourse
that affect race outcomes.

Index + chronological sort + seek architecture:
  Phase 1 - Build lightweight index (sort keys + byte offsets) from JSONL.
  Phase 2 - Sort the index chronologically.
  Phase 3 - Seek to each record on disk, process course by course,
            stream output directly to disk.

Temporal integrity: for any partant at date D, only races with date < D
at the same hippodrome contribute to the hippodrome profile -- no future
leakage. States are updated AFTER emitting features for the current race.

Per-hippodrome state:
  - deque(maxlen=100) of race results (winner_cote, winner_pos_num,
    field_size, allocation, speed_figure)
  - draw stats: inside_wins, outside_wins, total_draws
  - trainer performance tracking (top-5 most frequent trainers' win rates)

Produces:
  - hippodrome_profile_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippodrome_profile/

Features per partant (10):
  - hip_avg_field_size         : rolling avg field size at this hippo (last 100 races)
  - hip_favorite_win_rate      : favorite (lowest cote) win rate at this hippo (rolling 100)
  - hip_avg_winning_odds       : avg winning odds at this hippo (rolling 100)
  - hip_upset_frequency        : proportion of races won by cote > 10 at this hippo
  - hip_draw_bias_strength     : abs difference in win rates (inside vs outside draw)
  - hip_nb_races_year          : number of races at this hippo in last 365 days
  - hip_prestige_score         : avg allocation at this hippo (rolling)
  - hip_home_advantage         : rolling win rate of top 5 most frequent trainers
  - hip_speed_favoring         : avg speed_figure of winners at this hippo
  - hip_predictability_score   : 1 - normalised entropy of winner odds (how predictable)

Usage:
    python feature_builders/hippodrome_profile_builder.py
    python feature_builders/hippodrome_profile_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippodrome_profile"
)

# Rolling window for race-level stats
_ROLLING_RACES = 100

# Progress / GC
_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# Draw bias: "inside" = corde <= median, "outside" = corde > median
# We split the field in half by corde number.


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    try:
        f = float(v)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string to datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# PER-HIPPODROME STATE
# ===========================================================================


class _RaceResult:
    """Minimal result of one race at a hippodrome."""
    __slots__ = (
        "winner_cote", "winner_was_favorite", "field_size",
        "allocation", "winner_speed", "date",
    )

    def __init__(
        self,
        winner_cote: Optional[float],
        winner_was_favorite: bool,
        field_size: int,
        allocation: Optional[float],
        winner_speed: Optional[float],
        date: Optional[datetime],
    ):
        self.winner_cote = winner_cote
        self.winner_was_favorite = winner_was_favorite
        self.field_size = field_size
        self.allocation = allocation
        self.winner_speed = winner_speed
        self.date = date


class _HippoState:
    """Rolling state for one hippodrome."""

    __slots__ = (
        "race_results",
        "inside_wins", "outside_wins", "total_draw_races",
        "trainer_runs", "trainer_wins",
    )

    def __init__(self):
        self.race_results: deque[_RaceResult] = deque(maxlen=_ROLLING_RACES)
        # Draw bias tracking (cumulative, not windowed -- reflects full history)
        self.inside_wins: int = 0
        self.outside_wins: int = 0
        self.total_draw_races: int = 0
        # Trainer tracking: trainer_name -> [runs, wins]
        self.trainer_runs: dict[str, int] = defaultdict(int)
        self.trainer_wins: dict[str, int] = defaultdict(int)

    def snapshot(self, race_date: Optional[datetime]) -> dict[str, Any]:
        """Capture current hippodrome profile features BEFORE updating."""
        results = self.race_results
        n = len(results)

        if n == 0:
            return {
                "hip_avg_field_size": None,
                "hip_favorite_win_rate": None,
                "hip_avg_winning_odds": None,
                "hip_upset_frequency": None,
                "hip_draw_bias_strength": None,
                "hip_nb_races_year": None,
                "hip_prestige_score": None,
                "hip_home_advantage": None,
                "hip_speed_favoring": None,
                "hip_predictability_score": None,
            }

        # hip_avg_field_size: rolling avg field size
        avg_field = round(sum(r.field_size for r in results) / n, 2)

        # hip_favorite_win_rate: proportion of races won by the favorite
        fav_wins = sum(1 for r in results if r.winner_was_favorite)
        fav_wr = round(fav_wins / n, 4)

        # hip_avg_winning_odds: avg cote of winners
        winner_cotes = [r.winner_cote for r in results if r.winner_cote is not None]
        avg_win_odds = round(sum(winner_cotes) / len(winner_cotes), 2) if winner_cotes else None

        # hip_upset_frequency: proportion of races won by cote > 10
        if winner_cotes:
            upsets = sum(1 for c in winner_cotes if c > 10.0)
            upset_freq = round(upsets / len(winner_cotes), 4)
        else:
            upset_freq = None

        # hip_draw_bias_strength: |inside_wr - outside_wr|
        draw_bias = None
        if self.total_draw_races >= 10:
            # We track inside/outside wins separately
            # Total wins = inside_wins + outside_wins
            total_wins = self.inside_wins + self.outside_wins
            if total_wins > 0:
                inside_wr = self.inside_wins / total_wins
                outside_wr = self.outside_wins / total_wins
                draw_bias = round(abs(inside_wr - outside_wr), 4)

        # hip_nb_races_year: races at this hippo in last 365 days
        nb_year = None
        if race_date is not None:
            cutoff = race_date - timedelta(days=365)
            nb_year = sum(1 for r in results if r.date is not None and r.date >= cutoff)

        # hip_prestige_score: avg allocation at this hippo
        allocs = [r.allocation for r in results if r.allocation is not None]
        prestige = round(sum(allocs) / len(allocs), 2) if allocs else None

        # hip_home_advantage: win rate of top 5 most frequent trainers
        home_adv = None
        if self.trainer_runs:
            # Top 5 trainers by number of runs at this hippo
            top5 = sorted(
                self.trainer_runs.items(),
                key=lambda x: x[1],
                reverse=True,
            )[:5]
            top5_runs = sum(runs for _, runs in top5)
            top5_wins = sum(self.trainer_wins.get(t, 0) for t, _ in top5)
            if top5_runs >= 5:
                home_adv = round(top5_wins / top5_runs, 4)

        # hip_speed_favoring: avg speed_figure of winners
        winner_speeds = [r.winner_speed for r in results if r.winner_speed is not None]
        speed_fav = round(sum(winner_speeds) / len(winner_speeds), 2) if winner_speeds else None

        # hip_predictability_score: 1 - normalised entropy of winner odds
        # Bin winner cotes into buckets and compute entropy
        predictability = None
        if len(winner_cotes) >= 10:
            # Buckets: [1-3), [3-5), [5-8), [8-12), [12-20), [20+)
            bins = [0] * 6
            for c in winner_cotes:
                if c < 3:
                    bins[0] += 1
                elif c < 5:
                    bins[1] += 1
                elif c < 8:
                    bins[2] += 1
                elif c < 12:
                    bins[3] += 1
                elif c < 20:
                    bins[4] += 1
                else:
                    bins[5] += 1
            total_c = len(winner_cotes)
            # Shannon entropy
            entropy = 0.0
            for b in bins:
                if b > 0:
                    p = b / total_c
                    entropy -= p * math.log2(p)
            # Max entropy for 6 bins = log2(6) ~ 2.585
            max_entropy = math.log2(6)
            if max_entropy > 0:
                predictability = round(1.0 - entropy / max_entropy, 4)

        return {
            "hip_avg_field_size": avg_field,
            "hip_favorite_win_rate": fav_wr,
            "hip_avg_winning_odds": avg_win_odds,
            "hip_upset_frequency": upset_freq,
            "hip_draw_bias_strength": draw_bias,
            "hip_nb_races_year": nb_year,
            "hip_prestige_score": prestige,
            "hip_home_advantage": home_adv,
            "hip_speed_favoring": speed_fav,
            "hip_predictability_score": predictability,
        }

    def update(self, race_result: _RaceResult, draw_info: list[tuple[int, bool]],
               trainers: list[tuple[str, bool]]) -> None:
        """Update state AFTER emitting features for the current race.

        Parameters
        ----------
        race_result : _RaceResult
            Summary of the race outcome.
        draw_info : list of (corde, is_winner)
            Draw/position info for each runner. Used for inside/outside bias.
        trainers : list of (trainer_name, is_winner)
            Trainer info for each runner.
        """
        self.race_results.append(race_result)

        # Update draw bias stats
        if draw_info:
            cordes = [c for c, _ in draw_info if c is not None]
            if cordes:
                median_corde = sorted(cordes)[len(cordes) // 2]
                self.total_draw_races += 1
                for corde, is_winner in draw_info:
                    if corde is not None and is_winner:
                        if corde <= median_corde:
                            self.inside_wins += 1
                        else:
                            self.outside_wins += 1

        # Update trainer stats
        for trainer, is_winner in trainers:
            if trainer:
                self.trainer_runs[trainer] += 1
                if is_winner:
                    self.trainer_wins[trainer] += 1


# ===========================================================================
# MAIN BUILD (index + sort + seek)
# ===========================================================================


def build_hippodrome_profile_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build hippodrome profile features using index + seek architecture.

    Returns the total number of feature records written.
    """
    logger.info("=== Hippodrome Profile Builder (index + seek) ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Build lightweight index (date, course_uid, num_pmu, offset)
    # ------------------------------------------------------------------
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
                logger.info("  Phase 1 - Indexe %d records...", n_read)

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

    # ------------------------------------------------------------------
    # Phase 2: Sort the lightweight index chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Phase 2 - Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3: Process course by course, streaming output
    # ------------------------------------------------------------------
    t2 = time.time()
    hippo_states: dict[str, _HippoState] = defaultdict(_HippoState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "hip_avg_field_size": 0,
        "hip_favorite_win_rate": 0,
        "hip_avg_winning_odds": 0,
        "hip_upset_frequency": 0,
        "hip_draw_bias_strength": 0,
        "hip_nb_races_year": 0,
        "hip_prestige_score": 0,
        "hip_home_advantage": 0,
        "hip_speed_favoring": 0,
        "hip_predictability_score": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            cote = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
            cote_ref = _safe_float(rec.get("cote_reference"))
            allocation = _safe_float(rec.get("allocation") or rec.get("montant_prix"))
            speed = _safe_float(rec.get("speed_figure") or rec.get("spd_speed_figure"))
            corde = _safe_int(rec.get("corde") or rec.get("numero_corde"))
            nb_partants = _safe_int(rec.get("nombre_partants"))
            position = _safe_int(rec.get("position_arrivee") or rec.get("place_arrivee"))

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "hippo": (
                    rec.get("hippodrome_normalise")
                    or rec.get("hippodrome")
                    or rec.get("nom_hippodrome")
                    or ""
                ),
                "cote": cote,
                "cote_ref": cote_ref,
                "is_gagnant": bool(rec.get("is_gagnant")),
                "position": position,
                "allocation": allocation,
                "speed": speed,
                "corde": corde,
                "nb_partants": nb_partants or 0,
                "trainer": (
                    rec.get("entraineur_normalise")
                    or rec.get("nom_entraineur")
                    or rec.get("entraineur")
                    or ""
                ),
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

            race_date = _parse_date(course_date_str)

            # Determine hippodrome (take from first record with a value)
            hippo_name = ""
            for rec in course_group:
                if rec["hippo"]:
                    hippo_name = rec["hippo"]
                    break

            if not hippo_name:
                # No hippodrome info -- emit nulls
                for rec in course_group:
                    features = {
                        "partant_uid": rec["uid"],
                        "course_uid": rec["course"],
                        "date_reunion_iso": rec["date"],
                        "hip_avg_field_size": None,
                        "hip_favorite_win_rate": None,
                        "hip_avg_winning_odds": None,
                        "hip_upset_frequency": None,
                        "hip_draw_bias_strength": None,
                        "hip_nb_races_year": None,
                        "hip_prestige_score": None,
                        "hip_home_advantage": None,
                        "hip_speed_favoring": None,
                        "hip_predictability_score": None,
                    }
                    fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                    n_written += 1
                n_processed += len(course_group)
                continue

            # -- Snapshot pre-race hippodrome profile (temporal integrity) --
            state = hippo_states[hippo_name]
            profile = state.snapshot(race_date)

            # Emit features for each partant in this course
            for rec in course_group:
                features = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }
                features.update(profile)

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Track fill counts
                for k in fill_counts:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

            # -- Determine race outcome for state update --
            field_size = len(course_group)

            # Find winner and favorite
            winner_cote: Optional[float] = None
            winner_speed: Optional[float] = None
            favorite_cote: Optional[float] = None
            winner_was_favorite = False

            # Determine favorite (lowest cote)
            cotes = [(rec["uid"], rec["cote"]) for rec in course_group if rec["cote"] is not None and rec["cote"] > 0]
            if cotes:
                fav_uid, favorite_cote = min(cotes, key=lambda x: x[1])

            for rec in course_group:
                if rec["is_gagnant"]:
                    winner_cote = rec["cote"]
                    winner_speed = rec["speed"]
                    if favorite_cote is not None and rec["cote"] is not None:
                        winner_was_favorite = (rec["cote"] == favorite_cote)
                    break

            # Allocation: take from any record (same for all in the course)
            allocation = None
            for rec in course_group:
                if rec["allocation"] is not None:
                    allocation = rec["allocation"]
                    break

            race_result = _RaceResult(
                winner_cote=winner_cote,
                winner_was_favorite=winner_was_favorite,
                field_size=field_size,
                allocation=allocation,
                winner_speed=winner_speed,
                date=race_date,
            )

            # Draw info for bias tracking
            draw_info = [
                (rec["corde"], rec["is_gagnant"])
                for rec in course_group
            ]

            # Trainer info
            trainers = [
                (rec["trainer"], rec["is_gagnant"])
                for rec in course_group
            ]

            state.update(race_result, draw_info, trainers)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Phase 3 - Traite %d / %d records...", n_processed, total)

            if n_processed % _GC_EVERY < len(course_group):
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Hippodrome profile build termine: %d features en %.1fs (hippodromes: %d)",
        n_written, elapsed, len(hippo_states),
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features hippodrome profile a partir de partants_master"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: builder_outputs/hippodrome_profile/)",
    )
    args = parser.parse_args()

    logger = setup_logging("hippodrome_profile_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "hippodrome_profile_features.jsonl"
    build_hippodrome_profile_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
