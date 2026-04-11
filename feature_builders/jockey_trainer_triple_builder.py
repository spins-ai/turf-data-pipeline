#!/usr/bin/env python3
"""
feature_builders.jockey_trainer_triple_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Advanced jockey-trainer-horse triple interaction features.

Reads partants_master.jsonl in streaming mode, processes chronologically,
computes per-partant jockey/trainer/triple features using only past data
(no future leakage).

Features (10):
  - jtt_jockey_current_streak       : jockey's current consecutive winning streak
  - jtt_trainer_current_streak      : trainer's current consecutive winning streak
  - jtt_jockey_hippo_specialist     : jockey win rate at THIS hippodrome (historical)
  - jtt_trainer_hippo_specialist    : trainer win rate at THIS hippodrome (historical)
  - jtt_jockey_distance_winrate     : jockey win rate at THIS distance band (+/- 200m)
  - jtt_trainer_discipline_winrate  : trainer win rate in THIS discipline
  - jtt_trio_combo_count            : # times (jockey, trainer, horse) combo has raced
  - jtt_trio_combo_winrate          : win rate of the exact trio
  - jtt_jockey_rides_today          : # rides jockey has in THIS reunion (overload)
  - jtt_trainer_runners_today       : # runners trainer has in THIS reunion

Memory: defaultdict + __slots__ trackers, gc.collect() every 500K records.

Usage:
    python feature_builders/jockey_trainer_triple_builder.py
    python feature_builders/jockey_trainer_triple_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_trainer_triple")
_LOG_EVERY = 500_000

_FEATURE_NAMES = [
    "jtt_jockey_current_streak",
    "jtt_trainer_current_streak",
    "jtt_jockey_hippo_specialist",
    "jtt_trainer_hippo_specialist",
    "jtt_jockey_distance_winrate",
    "jtt_trainer_discipline_winrate",
    "jtt_trio_combo_count",
    "jtt_trio_combo_winrate",
    "jtt_jockey_rides_today",
    "jtt_trainer_runners_today",
]


# ===========================================================================
# STATE TRACKERS (memory-optimised with __slots__)
# ===========================================================================


class _StreakTracker:
    """Tracks current winning streak for a jockey or trainer."""
    __slots__ = ("current_streak",)

    def __init__(self):
        self.current_streak: int = 0


class _WinRateCounter:
    """Tracks wins/total for a (entity, context) pair."""
    __slots__ = ("wins", "total")

    def __init__(self):
        self.wins: int = 0
        self.total: int = 0

    def rate(self) -> Optional[float]:
        if self.total == 0:
            return None
        return round(self.wins / self.total, 4)


class _TrioStats:
    """Tracks combo count and wins for a (jockey, trainer, horse) triple."""
    __slots__ = ("count", "wins")

    def __init__(self):
        self.count: int = 0
        self.wins: int = 0

    def win_rate(self) -> Optional[float]:
        if self.count == 0:
            return None
        return round(self.wins / self.count, 4)


def _distance_band(distance: int) -> int:
    """Round distance to nearest 200m band center for grouping."""
    return round(distance / 200) * 200


def _is_winner(rec: dict) -> bool:
    """Check if record represents a winner."""
    gagnant = rec.get("is_gagnant")
    if gagnant is True or gagnant == 1:
        return True
    pos = rec.get("position_arrivee")
    if pos is None:
        return False
    try:
        return int(pos) == 1
    except (ValueError, TypeError):
        return False


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_jockey_trainer_triple_features(
    input_path: Path, output_dir: Path, logger
) -> int:
    """Build jockey-trainer-triple features from partants_master.jsonl.

    Three-phase approach:
      1. Index: read sort keys + byte offsets (lightweight).
      2. Sort chronologically.
      3. Stream through sorted records, compute features, write output.

    Returns the total number of feature records written.
    """
    logger.info("=== Jockey-Trainer-Triple Builder ===")
    logger.info("Input: %s", input_path)
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
            num_pmu = 0
            try:
                num_pmu = int(rec.get("num_pmu", 0) or 0)
            except (ValueError, TypeError):
                pass
            index.append((date_str, course_uid, num_pmu, offset))

    logger.info("Phase 1: %d records indexes en %.1fs", len(index), time.time() - t0)

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Pre-scan: count jockey/trainer per reunion for rides_today/runners_today --
    # reunion_uid = date_reunion_iso (all races on same day at same venue share a date)
    # We need to group by reunion. A reunion is identified by (date, hippodrome).
    # But we don't have hippodrome in the index. We'll compute rides_today on-the-fly
    # during phase 3 by tracking per-reunion counters that reset when reunion changes.
    # Actually, we can precompute by scanning once more.
    logger.info("Pre-scan: comptage rides/runners par reunion...")
    t_pre = time.time()

    # reunion = (date_str, hippodrome) -> jockey -> count, trainer -> count
    reunion_jockey_counts: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    reunion_trainer_counts: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))

    with open(input_path, "r", encoding="utf-8") as f:
        for date_str, course_uid, num_pmu, offset in index:
            f.seek(offset)
            rec = json.loads(f.readline())
            date_r = rec.get("date_reunion_iso", "") or ""
            hippo = rec.get("hippodrome_normalise", "") or ""
            jockey = rec.get("jockey_driver", "") or ""
            trainer = rec.get("entraineur", "") or ""
            reunion_key = (date_r, hippo)
            if jockey:
                reunion_jockey_counts[reunion_key][jockey] += 1
            if trainer:
                reunion_trainer_counts[reunion_key][trainer] += 1

    logger.info("Pre-scan termine en %.1fs (%d reunions)", time.time() - t_pre, len(reunion_jockey_counts))

    # -- Phase 3: Compute features and stream to output --
    t2 = time.time()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "jockey_trainer_triple_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    # State trackers
    jockey_streaks: dict[str, _StreakTracker] = defaultdict(_StreakTracker)
    trainer_streaks: dict[str, _StreakTracker] = defaultdict(_StreakTracker)
    jockey_hippo: dict[tuple[str, str], _WinRateCounter] = defaultdict(_WinRateCounter)
    trainer_hippo: dict[tuple[str, str], _WinRateCounter] = defaultdict(_WinRateCounter)
    jockey_dist: dict[tuple[str, int], _WinRateCounter] = defaultdict(_WinRateCounter)
    trainer_disc: dict[tuple[str, str], _WinRateCounter] = defaultdict(_WinRateCounter)
    trio_stats: dict[tuple[str, str, str], _TrioStats] = defaultdict(_TrioStats)

    n_processed = 0
    n_written = 0
    total = len(index)
    fill = {k: 0 for k in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(off: int) -> dict:
            fin.seek(off)
            return json.loads(fin.readline())

        for date_str, course_uid, num_pmu, offset in index:
            rec = _read_at(offset)
            n_processed += 1

            if n_processed % _LOG_EVERY == 0:
                elapsed = time.time() - t2
                pct = n_processed / total * 100
                logger.info(
                    "  Phase 3: %d/%d (%.1f%%) en %.0fs",
                    n_processed, total, pct, elapsed,
                )
                gc.collect()

            partant_uid = rec.get("partant_uid", "")
            jockey = rec.get("jockey_driver", "") or ""
            trainer = rec.get("entraineur", "") or ""
            horse_id = rec.get("horse_id", "") or ""
            hippo = rec.get("hippodrome_normalise", "") or ""
            discipline = rec.get("discipline", "") or ""

            distance_raw = rec.get("distance")
            distance = None
            if distance_raw is not None:
                try:
                    distance = int(distance_raw)
                except (ValueError, TypeError):
                    pass

            # --- Compute features (snapshot BEFORE update) ---
            out: dict = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            # 1. Jockey current streak
            if jockey:
                out["jtt_jockey_current_streak"] = jockey_streaks[jockey].current_streak
                fill["jtt_jockey_current_streak"] += 1
            else:
                out["jtt_jockey_current_streak"] = None

            # 2. Trainer current streak
            if trainer:
                out["jtt_trainer_current_streak"] = trainer_streaks[trainer].current_streak
                fill["jtt_trainer_current_streak"] += 1
            else:
                out["jtt_trainer_current_streak"] = None

            # 3. Jockey hippodrome specialist
            if jockey and hippo:
                wr = jockey_hippo[(jockey, hippo)].rate()
                out["jtt_jockey_hippo_specialist"] = wr
                if wr is not None:
                    fill["jtt_jockey_hippo_specialist"] += 1
            else:
                out["jtt_jockey_hippo_specialist"] = None

            # 4. Trainer hippodrome specialist
            if trainer and hippo:
                wr = trainer_hippo[(trainer, hippo)].rate()
                out["jtt_trainer_hippo_specialist"] = wr
                if wr is not None:
                    fill["jtt_trainer_hippo_specialist"] += 1
            else:
                out["jtt_trainer_hippo_specialist"] = None

            # 5. Jockey distance win rate
            if jockey and distance is not None:
                band = _distance_band(distance)
                wr = jockey_dist[(jockey, band)].rate()
                out["jtt_jockey_distance_winrate"] = wr
                if wr is not None:
                    fill["jtt_jockey_distance_winrate"] += 1
            else:
                out["jtt_jockey_distance_winrate"] = None

            # 6. Trainer discipline win rate
            if trainer and discipline:
                wr = trainer_disc[(trainer, discipline)].rate()
                out["jtt_trainer_discipline_winrate"] = wr
                if wr is not None:
                    fill["jtt_trainer_discipline_winrate"] += 1
            else:
                out["jtt_trainer_discipline_winrate"] = None

            # 7. Trio combo count
            if jockey and trainer and horse_id:
                trio_key = (jockey, trainer, horse_id)
                out["jtt_trio_combo_count"] = trio_stats[trio_key].count
                fill["jtt_trio_combo_count"] += 1
            else:
                out["jtt_trio_combo_count"] = None

            # 8. Trio combo win rate
            if jockey and trainer and horse_id:
                trio_key = (jockey, trainer, horse_id)
                wr = trio_stats[trio_key].win_rate()
                out["jtt_trio_combo_winrate"] = wr
                if wr is not None:
                    fill["jtt_trio_combo_winrate"] += 1
            else:
                out["jtt_trio_combo_winrate"] = None

            # 9. Jockey rides today (precomputed)
            if jockey and date_str and hippo:
                reunion_key = (date_str, hippo)
                cnt = reunion_jockey_counts.get(reunion_key, {}).get(jockey, 0)
                out["jtt_jockey_rides_today"] = cnt
                fill["jtt_jockey_rides_today"] += 1
            else:
                out["jtt_jockey_rides_today"] = None

            # 10. Trainer runners today (precomputed)
            if trainer and date_str and hippo:
                reunion_key = (date_str, hippo)
                cnt = reunion_trainer_counts.get(reunion_key, {}).get(trainer, 0)
                out["jtt_trainer_runners_today"] = cnt
                fill["jtt_trainer_runners_today"] += 1
            else:
                out["jtt_trainer_runners_today"] = None

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

            # --- UPDATE state (after snapshot) ---
            won = _is_winner(rec)

            # Update streaks
            if jockey:
                if won:
                    jockey_streaks[jockey].current_streak += 1
                else:
                    jockey_streaks[jockey].current_streak = 0

            if trainer:
                if won:
                    trainer_streaks[trainer].current_streak += 1
                else:
                    trainer_streaks[trainer].current_streak = 0

            # Update jockey-hippo win rate
            if jockey and hippo:
                tracker = jockey_hippo[(jockey, hippo)]
                tracker.total += 1
                if won:
                    tracker.wins += 1

            # Update trainer-hippo win rate
            if trainer and hippo:
                tracker = trainer_hippo[(trainer, hippo)]
                tracker.total += 1
                if won:
                    tracker.wins += 1

            # Update jockey-distance win rate
            if jockey and distance is not None:
                band = _distance_band(distance)
                tracker = jockey_dist[(jockey, band)]
                tracker.total += 1
                if won:
                    tracker.wins += 1

            # Update trainer-discipline win rate
            if trainer and discipline:
                tracker = trainer_disc[(trainer, discipline)]
                tracker.total += 1
                if won:
                    tracker.wins += 1

            # Update trio stats
            if jockey and trainer and horse_id:
                trio_key = (jockey, trainer, horse_id)
                trio_stats[trio_key].count += 1
                if won:
                    trio_stats[trio_key].wins += 1

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d features ecrites en %.1fs", n_written, elapsed)
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %-40s: %8d / %d  (%.1f%%)", k, v, n_written, pct)

    gc.collect()
    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(description="Jockey-Trainer-Triple feature builder")
    parser.add_argument("--input", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("jockey_trainer_triple_builder")

    if args.input:
        input_path = Path(args.input)
    else:
        input_path = None
        for candidate in INPUT_CANDIDATES:
            if candidate.exists():
                input_path = candidate
                break
        if input_path is None:
            logger.error(
                "Aucun fichier partants_master trouve. Candidats testes: %s",
                [str(c) for c in INPUT_CANDIDATES],
            )
            sys.exit(1)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    logger.info("Input: %s", input_path)
    n = build_jockey_trainer_triple_features(input_path, OUTPUT_DIR, logger)
    logger.info("Total features ecrites: %d", n)


if __name__ == "__main__":
    main()
