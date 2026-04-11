#!/usr/bin/env python3
"""
feature_builders.market_psychology_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Betting psychology and smart-money pattern features.

Models public betting biases, identifies smart money signals, and computes
course-level market structure metrics.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the rolling hippodrome statistics -- no future leakage.
Course-level market structure (overround, concentration, etc.) uses the
race's own odds (observed at race time, not future data).

Architecture:
  Pass 1 — Stream JSONL, keep slim records, build per-course cote lists.
  Pass 1b — Compute course-level market stats from cote lists.
  Pass 2 — Sort chronologically (index + sort + seek), process course by
            course with rolling per-hippodrome state for bias/trap/value
            features, merge course-level stats.

Produces:
  - market_psychology_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_psychology/

Features per partant (10):
  - mkt_public_bias_score     : how much the public overweights this horse
                                type (historical cote vs actual win rate
                                for similar cote ranges at this hippodrome)
  - mkt_favorite_trap         : 1 if strong favorite (cote < 3) at hippo
                                where favorites underperform
  - mkt_longshot_value        : 1 if cote > 10 but historical win rate
                                exceeds implied probability
  - mkt_steam_move_proxy      : 1 if cote_finale < cote_reference (money
                                came in = smart money signal)
  - mkt_drift_signal          : 1 if cote_finale > cote_reference * 1.2
                                (money went out = negative signal)
  - mkt_market_overround      : sum(1/cote) for all horses in field
  - mkt_horse_share_of_market : (1/cote) / sum(1/cote_all) — normalised
                                implied probability
  - mkt_favorite_vs_second    : cote of favorite / cote of second favorite
  - mkt_top3_concentration    : sum of top-3 implied probs / total
  - mkt_outsider_count        : nb horses in field with cote > 20

Usage:
    python feature_builders/market_psychology_builder.py
    python feature_builders/market_psychology_builder.py --input path/to/partants_master.jsonl
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_psychology"
)

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# Cote range buckets for public bias tracking
_COTE_RANGES = [
    (0, 3, "fav"),       # strong favorites
    (3, 6, "mid_fav"),   # mid favorites
    (6, 10, "mid"),      # middle range
    (10, 20, "outsider"),  # outsiders
    (20, 999, "longshot"),  # longshots
]

# Minimum observations before computing hippodrome stats
_MIN_OBS = 30


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


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _cote_bucket(cote: float) -> Optional[str]:
    """Return the cote range bucket label."""
    for lo, hi, label in _COTE_RANGES:
        if lo <= cote < hi:
            return label
    return None


# ===========================================================================
# ROLLING HIPPODROME STATS
# ===========================================================================


class _HippoRollingStats:
    """Track per-hippodrome rolling statistics for cote-range accuracy
    and favorite performance.

    Tracks:
      - Per cote bucket: (nb_races, nb_wins) to compute actual win rate
      - Favorite performance: (nb_fav_races, nb_fav_wins)
    """

    __slots__ = ("bucket_races", "bucket_wins", "fav_races", "fav_wins")

    def __init__(self) -> None:
        self.bucket_races: dict[str, int] = defaultdict(int)
        self.bucket_wins: dict[str, int] = defaultdict(int)
        self.fav_races: int = 0
        self.fav_wins: int = 0

    def total_obs(self) -> int:
        return sum(self.bucket_races.values())

    def bucket_win_rate(self, bucket: str) -> Optional[float]:
        """Actual win rate for a cote bucket at this hippodrome."""
        n = self.bucket_races.get(bucket, 0)
        if n < _MIN_OBS:
            return None
        return self.bucket_wins.get(bucket, 0) / n

    def fav_win_rate(self) -> Optional[float]:
        """Win rate of strong favorites (cote < 3) at this hippodrome."""
        if self.fav_races < _MIN_OBS:
            return None
        return self.fav_wins / self.fav_races

    def snapshot_bias(self, cote: float) -> Optional[float]:
        """Public bias score: ratio of implied probability to actual win rate.

        > 1 means public overestimates this type, < 1 means underestimates.
        """
        bucket = _cote_bucket(cote)
        if bucket is None:
            return None
        actual_wr = self.bucket_win_rate(bucket)
        if actual_wr is None or actual_wr == 0:
            return None
        implied_prob = 1.0 / cote
        return round(implied_prob / actual_wr, 4)

    def snapshot_fav_trap(self, cote: float) -> Optional[int]:
        """1 if strong favorite at hippo where favorites underperform."""
        if cote >= 3:
            return 0
        fwr = self.fav_win_rate()
        if fwr is None:
            return None
        # Expected win rate for cote < 3 is > 33%; if actual is < 25%, trap
        if fwr < 0.25:
            return 1
        return 0

    def snapshot_longshot_value(self, cote: float) -> Optional[int]:
        """1 if longshot (cote > 10) with better actual win rate than implied."""
        if cote <= 10:
            return 0
        bucket = _cote_bucket(cote)
        if bucket is None:
            return None
        actual_wr = self.bucket_win_rate(bucket)
        if actual_wr is None:
            return None
        implied_prob = 1.0 / cote
        if actual_wr > implied_prob:
            return 1
        return 0

    def update(self, cote: float, is_winner: bool) -> None:
        """Update stats after a race (post-race, no leakage)."""
        bucket = _cote_bucket(cote)
        if bucket is not None:
            self.bucket_races[bucket] += 1
            if is_winner:
                self.bucket_wins[bucket] += 1
        # Track favorite performance
        if cote < 3:
            self.fav_races += 1
            if is_winner:
                self.fav_wins += 1


# ===========================================================================
# COURSE-LEVEL MARKET STATS
# ===========================================================================


def _compute_course_market_stats(
    cotes: list[tuple[int, float]],
) -> dict[str, Any]:
    """Compute course-level market structure stats from list of (num_pmu, cote).

    Returns a dict with:
      - overround, per-horse share lookup, fav_vs_second, top3_concentration,
        outsider_count.
    """
    stats: dict[str, Any] = {
        "overround": None,
        "shares": {},           # num_pmu -> share
        "fav_vs_second": None,
        "top3_concentration": None,
        "outsider_count": None,
    }
    if not cotes:
        return stats

    # Implied probabilities
    implied = [(num, 1.0 / c) for num, c in cotes if c > 0]
    if not implied:
        return stats

    total_implied = sum(ip for _, ip in implied)

    # Overround
    stats["overround"] = round(total_implied, 4)

    # Per-horse share
    if total_implied > 0:
        stats["shares"] = {
            num: round(ip / total_implied, 6) for num, ip in implied
        }

    # Sort by implied prob descending (favorite first)
    sorted_imp = sorted(implied, key=lambda x: x[1], reverse=True)

    # Favorite vs second
    if len(sorted_imp) >= 2:
        fav_cote = 1.0 / sorted_imp[0][1]  # back to cote from implied
        sec_cote = 1.0 / sorted_imp[1][1]
        if sec_cote > 0:
            stats["fav_vs_second"] = round(fav_cote / sec_cote, 4)

    # Top-3 concentration
    if total_implied > 0 and len(sorted_imp) >= 3:
        top3_sum = sum(ip for _, ip in sorted_imp[:3])
        stats["top3_concentration"] = round(top3_sum / total_implied, 4)

    # Outsider count (cote > 20)
    stats["outsider_count"] = sum(1 for _, c in cotes if c > 20)

    return stats


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_market_psychology_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build market psychology features using 2-pass architecture."""
    logger.info("=== Market Psychology Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -----------------------------------------------------------------------
    # Pass 1: Stream JSONL, collect slim records + per-course cote lists
    # -----------------------------------------------------------------------
    slim_records: list[dict] = []
    course_cotes: dict[str, list[tuple[int, float]]] = defaultdict(list)
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote = _safe_float(rec.get("cote_finale"))
        cote_ref = _safe_float(rec.get("cote_reference"))
        course_uid = rec.get("course_uid", "")
        num_pmu = rec.get("num_pmu", 0) or 0

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": course_uid,
            "num": num_pmu,
            "hippo": (rec.get("hippodrome_normalise") or "").strip().lower(),
            "cote": cote,
            "cote_ref": cote_ref,
            "is_gagnant": bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

        # Collect cotes for course-level stats
        if cote is not None and course_uid:
            course_cotes[course_uid].append((num_pmu, cote))

        if n_read % _GC_EVERY == 0:
            gc.collect()

    logger.info(
        "Pass 1 terminee: %d records, %d courses en %.1fs",
        len(slim_records),
        len(course_cotes),
        time.time() - t0,
    )

    # -----------------------------------------------------------------------
    # Pass 1b: Compute course-level market stats
    # -----------------------------------------------------------------------
    t1 = time.time()
    course_stats: dict[str, dict[str, Any]] = {}
    for cuid, clist in course_cotes.items():
        course_stats[cuid] = _compute_course_market_stats(clist)
    logger.info(
        "Pass 1b: %d course market stats en %.1fs",
        len(course_stats),
        time.time() - t1,
    )

    # Free the raw cote lists
    del course_cotes
    gc.collect()

    # -----------------------------------------------------------------------
    # Pass 2: Sort chronologically + process course by course
    # -----------------------------------------------------------------------
    t2 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t2)

    hippo_stats: dict[str, _HippoRollingStats] = defaultdict(_HippoRollingStats)
    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Seek: collect all records for this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # Get pre-computed course-level stats
        cs = course_stats.get(course_uid, {})
        shares = cs.get("shares", {})

        # --- Snapshot pre-race features, then update post-race ---
        post_updates: list[tuple[str, float, bool]] = []  # (hippo, cote, is_winner)

        for rec in course_group:
            hippo = rec["hippo"]
            cote = rec["cote"]
            cote_ref = rec["cote_ref"]
            num = rec["num"]

            feats: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "course_uid": course_uid,
                "date_reunion_iso": course_date,
            }

            # -- Horse-level features using rolling hippo stats --
            if hippo and cote is not None:
                hs = hippo_stats[hippo]
                feats["mkt_public_bias_score"] = hs.snapshot_bias(cote)
                feats["mkt_favorite_trap"] = hs.snapshot_fav_trap(cote)
                feats["mkt_longshot_value"] = hs.snapshot_longshot_value(cote)
            else:
                feats["mkt_public_bias_score"] = None
                feats["mkt_favorite_trap"] = None
                feats["mkt_longshot_value"] = None

            # -- Steam move / drift (cote_finale vs cote_reference) --
            if cote is not None and cote_ref is not None:
                feats["mkt_steam_move_proxy"] = int(cote < cote_ref)
                feats["mkt_drift_signal"] = int(cote > cote_ref * 1.2)
            else:
                feats["mkt_steam_move_proxy"] = None
                feats["mkt_drift_signal"] = None

            # -- Course-level features --
            feats["mkt_market_overround"] = cs.get("overround")
            feats["mkt_horse_share_of_market"] = shares.get(num)
            feats["mkt_favorite_vs_second"] = cs.get("fav_vs_second")
            feats["mkt_top3_concentration"] = cs.get("top3_concentration")
            feats["mkt_outsider_count"] = cs.get("outsider_count")

            results.append(feats)

            # Prepare deferred update
            if hippo and cote is not None:
                post_updates.append((hippo, cote, rec["is_gagnant"]))

        # -- Update hippo stats post-race (no leakage) --
        for hippo, cote, is_winner in post_updates:
            hippo_stats[hippo].update(cote, is_winner)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

        if n_processed % _GC_EVERY == 0:
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Market psychology build termine: %d features en %.1fs (hippodromes: %d)",
        len(results),
        elapsed,
        len(hippo_stats),
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
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Market psychology features a partir de partants_master"
    )
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("market_psychology_builder")
    logger.info("=" * 70)
    logger.info("market_psychology_builder.py")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_market_psychology_features(input_path, logger)

    # Save (save_jsonl handles .tmp + rename + newline="\n")
    out_path = output_dir / "market_psychology_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rates
    if results:
        feature_keys = [
            k for k in results[0]
            if k not in ("partant_uid", "course_uid", "date_reunion_iso")
        ]
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info(
                "  %s: %d/%d (%.1f%%)", k, filled, total_count,
                100 * filled / total_count
            )

    logger.info("Termine — %d partants ecrits dans %s", len(results), out_path)


if __name__ == "__main__":
    main()
