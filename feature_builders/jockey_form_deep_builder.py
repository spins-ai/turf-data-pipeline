#!/usr/bin/env python3
"""
feature_builders.jockey_form_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep jockey form analysis features tracking jockey performance patterns
over time.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - jockey_form_deep_features.jsonl  in builder_outputs/jockey_form_deep/

Features per partant (10):
  - jfd_jockey_win_rate_7d        : jockey's win rate in last 7 days
  - jfd_jockey_win_rate_30d       : jockey's win rate in last 30 days
  - jfd_jockey_rides_today        : number of rides for this jockey in the same reunion
  - jfd_jockey_hot_streak         : current consecutive wins streak
  - jfd_jockey_cold_streak        : current consecutive losses streak (no win)
  - jfd_jockey_discipline_wr      : jockey win rate in this specific discipline
  - jfd_jockey_distance_wr        : jockey win rate at similar distance bucket
  - jfd_jockey_hippo_wr           : jockey win rate at this hippodrome
  - jfd_jockey_fav_conversion     : jockey's win rate when riding favorites (cote < 5)
  - jfd_jockey_longshot_wr        : jockey's win rate when riding longshots (cote > 15)

Usage:
    python feature_builders/jockey_form_deep_builder.py
    python feature_builders/jockey_form_deep_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_form_deep")

_LOG_EVERY = 500_000


# ===========================================================================
# DISTANCE BUCKET HELPER
# ===========================================================================

def _distance_bucket(dist_m) -> Optional[str]:
    """Map distance in metres to short/mid/long bucket."""
    if dist_m is None:
        return None
    try:
        d = int(dist_m)
    except (ValueError, TypeError):
        return None
    if d <= 0:
        return None
    if d < 1600:
        return "short"
    if d < 2200:
        return "mid"
    return "long"


def _parse_date_days(date_str: str) -> Optional[int]:
    """Convert YYYY-MM-DD to an integer day count for gap calculations."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


def _safe_float(v) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_rate(wins: int, total: int) -> Optional[float]:
    """Win rate with minimum-sample guard."""
    if total < 1:
        return None
    return round(wins / total, 4)


# ===========================================================================
# PER-JOCKEY STATE (memory-efficient with __slots__)
# ===========================================================================


class _JockeyState:
    """Track rolling state for one jockey."""

    __slots__ = (
        "recent_results",      # deque of (date_days, is_gagnant) maxlen=100
        "discipline_stats",    # {discipline -> [wins, total]}
        "distance_stats",      # {bucket -> [wins, total]}
        "hippo_stats",         # {hippodrome -> [wins, total]}
        "fav_stats",           # [wins, total]
        "longshot_stats",      # [wins, total]
        "current_streak",      # int: length of current streak
        "streak_type",         # str: "win" or "loss" or None
    )

    def __init__(self) -> None:
        self.recent_results: deque = deque(maxlen=100)
        self.discipline_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.distance_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.hippo_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.fav_stats: list[int] = [0, 0]
        self.longshot_stats: list[int] = [0, 0]
        self.current_streak: int = 0
        self.streak_type: Optional[str] = None

    def snapshot(
        self,
        current_date_days: Optional[int],
        discipline: Optional[str],
        dist_bucket: Optional[str],
        hippodrome: Optional[str],
        cote: Optional[float],
    ) -> dict[str, Any]:
        """Snapshot all 10 features BEFORE updating state."""
        feats: dict[str, Any] = {}

        # 1. jfd_jockey_win_rate_7d
        # 2. jfd_jockey_win_rate_30d
        if current_date_days is not None and self.recent_results:
            wins_7d = 0
            total_7d = 0
            wins_30d = 0
            total_30d = 0
            for rd, won in self.recent_results:
                if rd is None:
                    continue
                gap = current_date_days - rd
                if gap <= 7:
                    total_7d += 1
                    if won:
                        wins_7d += 1
                if gap <= 30:
                    total_30d += 1
                    if won:
                        wins_30d += 1
            feats["jfd_jockey_win_rate_7d"] = _safe_rate(wins_7d, total_7d)
            feats["jfd_jockey_win_rate_30d"] = _safe_rate(wins_30d, total_30d)
        else:
            feats["jfd_jockey_win_rate_7d"] = None
            feats["jfd_jockey_win_rate_30d"] = None

        # 3. jfd_jockey_rides_today -- will be filled at course level
        feats["jfd_jockey_rides_today"] = None

        # 4. jfd_jockey_hot_streak
        if self.streak_type == "win" and self.current_streak > 0:
            feats["jfd_jockey_hot_streak"] = self.current_streak
        else:
            feats["jfd_jockey_hot_streak"] = 0

        # 5. jfd_jockey_cold_streak
        if self.streak_type == "loss" and self.current_streak > 0:
            feats["jfd_jockey_cold_streak"] = self.current_streak
        else:
            feats["jfd_jockey_cold_streak"] = 0

        # 6. jfd_jockey_discipline_wr
        if discipline and discipline in self.discipline_stats:
            w, t = self.discipline_stats[discipline]
            feats["jfd_jockey_discipline_wr"] = _safe_rate(w, t)
        else:
            feats["jfd_jockey_discipline_wr"] = None

        # 7. jfd_jockey_distance_wr
        if dist_bucket and dist_bucket in self.distance_stats:
            w, t = self.distance_stats[dist_bucket]
            feats["jfd_jockey_distance_wr"] = _safe_rate(w, t)
        else:
            feats["jfd_jockey_distance_wr"] = None

        # 8. jfd_jockey_hippo_wr
        if hippodrome and hippodrome in self.hippo_stats:
            w, t = self.hippo_stats[hippodrome]
            feats["jfd_jockey_hippo_wr"] = _safe_rate(w, t)
        else:
            feats["jfd_jockey_hippo_wr"] = None

        # 9. jfd_jockey_fav_conversion (cote < 5)
        if self.fav_stats[1] > 0:
            feats["jfd_jockey_fav_conversion"] = _safe_rate(
                self.fav_stats[0], self.fav_stats[1]
            )
        else:
            feats["jfd_jockey_fav_conversion"] = None

        # 10. jfd_jockey_longshot_wr (cote > 15)
        if self.longshot_stats[1] > 0:
            feats["jfd_jockey_longshot_wr"] = _safe_rate(
                self.longshot_stats[0], self.longshot_stats[1]
            )
        else:
            feats["jfd_jockey_longshot_wr"] = None

        return feats

    def update(
        self,
        date_days: Optional[int],
        is_gagnant: bool,
        discipline: Optional[str],
        dist_bucket: Optional[str],
        hippodrome: Optional[str],
        cote: Optional[float],
    ) -> None:
        """Update state AFTER feature extraction (post-race)."""
        self.recent_results.append((date_days, is_gagnant))

        # Streak tracking
        if is_gagnant:
            if self.streak_type == "win":
                self.current_streak += 1
            else:
                self.streak_type = "win"
                self.current_streak = 1
        else:
            if self.streak_type == "loss":
                self.current_streak += 1
            else:
                self.streak_type = "loss"
                self.current_streak = 1

        # Discipline stats
        if discipline:
            s = self.discipline_stats[discipline]
            s[1] += 1
            if is_gagnant:
                s[0] += 1

        # Distance stats
        if dist_bucket:
            s = self.distance_stats[dist_bucket]
            s[1] += 1
            if is_gagnant:
                s[0] += 1

        # Hippodrome stats
        if hippodrome:
            s = self.hippo_stats[hippodrome]
            s[1] += 1
            if is_gagnant:
                s[0] += 1

        # Favourite stats (cote < 5)
        if cote is not None and cote < 5:
            self.fav_stats[1] += 1
            if is_gagnant:
                self.fav_stats[0] += 1

        # Longshot stats (cote > 15)
        if cote is not None and cote > 15:
            self.longshot_stats[1] += 1
            if is_gagnant:
                self.longshot_stats[0] += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_jockey_form_deep_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build deep jockey form features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Jockey Form Deep Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
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

    # -- Phase 2: Sort chronologically --
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

    feature_keys = [
        "jfd_jockey_win_rate_7d",
        "jfd_jockey_win_rate_30d",
        "jfd_jockey_rides_today",
        "jfd_jockey_hot_streak",
        "jfd_jockey_cold_streak",
        "jfd_jockey_discipline_wr",
        "jfd_jockey_distance_wr",
        "jfd_jockey_hippo_wr",
        "jfd_jockey_fav_conversion",
        "jfd_jockey_longshot_wr",
    ]
    fill_counts = {k: 0 for k in feature_keys}

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

            current_date_days = _parse_date_days(course_date_str)

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # -- Count jockey rides today (same date) for each jockey --
            jockey_rides_today: dict[str, int] = defaultdict(int)
            for rec in course_records:
                jk = (rec.get("jockey_driver") or "").strip().upper()
                if jk:
                    jockey_rides_today[jk] += 1

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                jockey = (rec.get("jockey_driver") or "").strip().upper()
                partant_uid = rec.get("partant_uid")

                discipline = (rec.get("discipline") or "").strip().lower() or None
                dist_bucket = _distance_bucket(rec.get("distance"))
                hippodrome = (rec.get("hippodrome_normalise") or "").strip().lower() or None
                cote = _safe_float(rec.get("cote_finale"))
                is_gagnant = bool(rec.get("is_gagnant"))

                out_rec: dict[str, Any] = {
                    "partant_uid": partant_uid,
                    "course_uid": rec.get("course_uid"),
                    "date_reunion_iso": rec.get("date_reunion_iso"),
                }

                if not jockey:
                    # No jockey -- emit Nones
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                js = jockey_state[jockey]

                # Snapshot features BEFORE update
                feats = js.snapshot(
                    current_date_days, discipline, dist_bucket, hippodrome, cote
                )

                # Override rides_today with actual count from this reunion
                feats["jfd_jockey_rides_today"] = jockey_rides_today.get(jockey, 0)

                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # Prepare deferred state update
                post_updates.append((
                    jockey, current_date_days, is_gagnant,
                    discipline, dist_bucket, hippodrome, cote,
                ))

            # -- Update jockey states after race (no leakage) --
            for (
                jockey, date_days, is_gagnant,
                discipline, dist_bucket, hippodrome, cote,
            ) in post_updates:
                jockey_state[jockey].update(
                    date_days, is_gagnant,
                    discipline, dist_bucket, hippodrome, cote,
                )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Jockey form deep build termine: %d features en %.1fs (jockeys suivis: %d)",
        n_written, elapsed, len(jockey_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
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
        description="Construction des features jockey form deep a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/jockey_form_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_form_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "jockey_form_deep_features.jsonl"
    build_jockey_form_deep_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
