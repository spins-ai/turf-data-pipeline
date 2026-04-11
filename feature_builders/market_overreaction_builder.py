#!/usr/bin/env python3
"""
feature_builders.market_overreaction_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Betting market overreaction features per partant.

Detects when the market over-adjusts odds based on recent results, tracks
horse-level calibration of odds vs actual performance, and identifies
smart-money signals.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically (course by course, snapshot before update), and computes
per-partant market overreaction features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 re-reads records from disk via seek, streams output to .tmp
  - defaultdict with deque(maxlen=50) for bounded state
  - gc.collect() every 500K records

Produces:
  - market_overreaction.jsonl   in builder_outputs/market_overreaction/

Features per partant (12):
  - mkor_odds_drift             : cote_finale - cote_reference (>0 = drifted out)
  - mkor_odds_drift_ratio       : cote_finale / cote_reference (>1 = less popular)
  - mkor_last_race_effect       : avg odds shortening after a win for this horse
  - mkor_overreaction_score     : |odds change| vs |perf change|; >1 = overreaction
  - mkor_calibration_error      : horse's historical |implied_prob - actual_win_rate|
  - mkor_beaten_fav_rate        : % of races this horse lost when favourite (cote < 5)
  - mkor_outsider_upset_rate    : % of races this horse won when outsider (cote > 10)
  - mkor_implied_prob_accuracy  : win rate at similar implied probability range
  - mkor_odds_rank_vs_finish    : rolling correlation of odds rank vs finish position
  - mkor_smart_money            : late drift toward horse (cote_finale << cote_reference)
  - mkor_false_fav_rate         : % of races this horse was fav (cote < 5) and lost
  - mkor_odds_cat_perf          : horse's win rate in current odds category

Usage:
    python feature_builders/market_overreaction_builder.py
    python feature_builders/market_overreaction_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/market_overreaction")

_LOG_EVERY = 500_000

# Odds category boundaries
_ODDS_CATEGORIES = [
    (0.0, 3.0, "heavy_fav"),       # 1-3
    (3.0, 5.0, "fav"),             # 3-5
    (5.0, 10.0, "mid"),            # 5-10
    (10.0, 20.0, "outsider"),      # 10-20
    (20.0, 999999.0, "big_out"),   # 20+
]

# Implied probability buckets (for accuracy feature)
_PROBA_BUCKETS = [
    (0.00, 0.05, "p0_5"),
    (0.05, 0.10, "p5_10"),
    (0.10, 0.15, "p10_15"),
    (0.15, 0.20, "p15_20"),
    (0.20, 0.30, "p20_30"),
    (0.30, 0.50, "p30_50"),
    (0.50, 1.01, "p50_100"),
]

# Minimum observations for meaningful statistics
_MIN_RACES_RATE = 3
_MIN_RACES_CORR = 5

# Deque max length for bounded memory
_DEQUE_MAXLEN = 50


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or None. Rejects NaN, inf, non-positive for odds."""
    if val is None:
        return None
    try:
        v = float(val)
        if v != v or math.isinf(v):  # NaN or inf
            return None
        return v
    except (TypeError, ValueError):
        return None


def _safe_positive_float(val: Any) -> Optional[float]:
    """Convert to positive float or None."""
    v = _safe_float(val)
    if v is not None and v > 0:
        return v
    return None


def _odds_category(cote: float) -> Optional[str]:
    """Map odds value to a category label."""
    for lo, hi, label in _ODDS_CATEGORIES:
        if lo <= cote < hi:
            return label
    return None


def _proba_bucket(p: float) -> Optional[str]:
    """Map implied probability to a bucket label."""
    for lo, hi, label in _PROBA_BUCKETS:
        if lo <= p < hi:
            return label
    return None


def _pearson_corr(xs: list[float], ys: list[float]) -> Optional[float]:
    """Compute Pearson correlation. Returns None if not enough data or zero variance."""
    n = len(xs)
    if n < _MIN_RACES_CORR or len(ys) != n:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sx = sum((x - mx) ** 2 for x in xs)
    sy = sum((y - my) ** 2 for y in ys)
    if sx == 0 or sy == 0:
        return None
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    return round(sxy / (sx * sy) ** 0.5, 4)


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseMarketState:
    """Tracks per-horse market overreaction state with bounded deques."""

    __slots__ = (
        # Recent odds: list of (cote_finale, cote_reference, is_gagnant, position, proba_impl)
        "recent_races",
        # Favourite outcomes: list of (is_gagnant,) when cote < 5
        "fav_outcomes",
        # Outsider outcomes: list of (is_gagnant,) when cote > 10
        "outsider_outcomes",
        # Odds category -> (wins, total)
        "odds_cat_record",
        # Implied probability bucket -> (wins, total)
        "proba_bucket_record",
        # (odds_rank, finish_position) pairs for rolling correlation
        "rank_finish_pairs",
        # Post-win odds changes: (next_cote / prev_cote) after a win
        "post_win_odds_changes",
        # Performance changes vs odds changes for overreaction detection
        "perf_vs_odds",
    )

    def __init__(self) -> None:
        self.recent_races: deque = deque(maxlen=_DEQUE_MAXLEN)
        self.fav_outcomes: deque = deque(maxlen=_DEQUE_MAXLEN)
        self.outsider_outcomes: deque = deque(maxlen=_DEQUE_MAXLEN)
        self.odds_cat_record: dict[str, list[int]] = defaultdict(lambda: [0, 0])  # [wins, total]
        self.proba_bucket_record: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.rank_finish_pairs: deque = deque(maxlen=_DEQUE_MAXLEN)
        self.post_win_odds_changes: deque = deque(maxlen=_DEQUE_MAXLEN)
        self.perf_vs_odds: deque = deque(maxlen=_DEQUE_MAXLEN)

    def snapshot(
        self,
        cote_finale: Optional[float],
        cote_reference: Optional[float],
        proba_implicite: Optional[float],
    ) -> dict[str, Any]:
        """Compute features BEFORE updating state (temporal integrity)."""
        feats: dict[str, Any] = {
            "mkor_odds_drift": None,
            "mkor_odds_drift_ratio": None,
            "mkor_last_race_effect": None,
            "mkor_overreaction_score": None,
            "mkor_calibration_error": None,
            "mkor_beaten_fav_rate": None,
            "mkor_outsider_upset_rate": None,
            "mkor_implied_prob_accuracy": None,
            "mkor_odds_rank_vs_finish": None,
            "mkor_smart_money": None,
            "mkor_false_fav_rate": None,
            "mkor_odds_cat_perf": None,
        }

        # --- 1. Odds drift (current race, no history needed) ---
        if cote_finale is not None and cote_reference is not None:
            feats["mkor_odds_drift"] = round(cote_finale - cote_reference, 4)
            if cote_reference > 0:
                feats["mkor_odds_drift_ratio"] = round(cote_finale / cote_reference, 4)

        # --- 2. Last-race-effect on odds ---
        # After a win, how much do odds shorten next time?
        if self.post_win_odds_changes:
            feats["mkor_last_race_effect"] = round(
                sum(self.post_win_odds_changes) / len(self.post_win_odds_changes), 4
            )

        # --- 3. Overreaction score ---
        # avg |odds_change_ratio| / avg |perf_change| ; >1 = market overreacts
        if len(self.perf_vs_odds) >= _MIN_RACES_RATE:
            odds_changes = [abs(oc) for oc, pc in self.perf_vs_odds if oc is not None]
            perf_changes = [abs(pc) for oc, pc in self.perf_vs_odds if pc is not None]
            if odds_changes and perf_changes:
                avg_oc = sum(odds_changes) / len(odds_changes)
                avg_pc = sum(perf_changes) / len(perf_changes)
                if avg_pc > 0.01:  # avoid division by near-zero
                    feats["mkor_overreaction_score"] = round(avg_oc / avg_pc, 4)

        # --- 4. Calibration error ---
        # |average implied probability - actual win rate| for this horse
        if len(self.recent_races) >= _MIN_RACES_RATE:
            probas = [r[4] for r in self.recent_races if r[4] is not None]
            wins = sum(1 for r in self.recent_races if r[2])
            total = len(self.recent_races)
            if probas and total > 0:
                avg_prob = sum(probas) / len(probas)
                actual_wr = wins / total
                feats["mkor_calibration_error"] = round(abs(avg_prob - actual_wr), 4)

        # --- 5. Beaten favourite rate ---
        if len(self.fav_outcomes) >= _MIN_RACES_RATE:
            losses = sum(1 for g in self.fav_outcomes if not g)
            feats["mkor_beaten_fav_rate"] = round(losses / len(self.fav_outcomes), 4)

        # --- 6. Outsider upset rate ---
        if len(self.outsider_outcomes) >= _MIN_RACES_RATE:
            wins_out = sum(1 for g in self.outsider_outcomes if g)
            feats["mkor_outsider_upset_rate"] = round(wins_out / len(self.outsider_outcomes), 4)

        # --- 7. Implied probability accuracy ---
        # Win rate at similar implied probability range
        if proba_implicite is not None:
            bucket = _proba_bucket(proba_implicite)
            if bucket and bucket in self.proba_bucket_record:
                rec = self.proba_bucket_record[bucket]
                if rec[1] >= _MIN_RACES_RATE:
                    feats["mkor_implied_prob_accuracy"] = round(rec[0] / rec[1], 4)

        # --- 8. Odds rank vs finish position (rolling correlation) ---
        if len(self.rank_finish_pairs) >= _MIN_RACES_CORR:
            ranks = [r[0] for r in self.rank_finish_pairs]
            finishes = [r[1] for r in self.rank_finish_pairs]
            feats["mkor_odds_rank_vs_finish"] = _pearson_corr(ranks, finishes)

        # --- 9. Smart money indicator ---
        # Late drift toward horse: cote_finale << cote_reference means money came in
        # Expressed as % shortening; big negative = strong smart money signal
        if cote_finale is not None and cote_reference is not None and cote_reference > 0:
            drift_pct = (cote_finale - cote_reference) / cote_reference
            # Only flag as smart money if odds shortened significantly (> 15%)
            if drift_pct < -0.15:
                feats["mkor_smart_money"] = round(drift_pct, 4)
            else:
                feats["mkor_smart_money"] = 0.0

        # --- 10. False favourite rate ---
        # Same as beaten_fav_rate but framed differently for clarity
        # (historical rate this horse was favourite AND lost)
        if len(self.fav_outcomes) >= _MIN_RACES_RATE:
            losses = sum(1 for g in self.fav_outcomes if not g)
            feats["mkor_false_fav_rate"] = round(losses / len(self.fav_outcomes), 4)

        # --- 11. Odds category performance ---
        if cote_finale is not None:
            cat = _odds_category(cote_finale)
            if cat and cat in self.odds_cat_record:
                rec = self.odds_cat_record[cat]
                if rec[1] >= _MIN_RACES_RATE:
                    feats["mkor_odds_cat_perf"] = round(rec[0] / rec[1], 4)

        return feats

    def update(
        self,
        cote_finale: Optional[float],
        cote_reference: Optional[float],
        is_gagnant: bool,
        position: Optional[int],
        proba_implicite: Optional[float],
        odds_rank: Optional[int],
    ) -> None:
        """Update state AFTER snapshot (post-race)."""

        # Store full race info
        self.recent_races.append((
            cote_finale, cote_reference, is_gagnant, position, proba_implicite,
        ))

        # Track favourite outcomes (cote < 5)
        if cote_finale is not None and cote_finale < 5.0:
            self.fav_outcomes.append(is_gagnant)

        # Track outsider outcomes (cote > 10)
        if cote_finale is not None and cote_finale > 10.0:
            self.outsider_outcomes.append(is_gagnant)

        # Track odds category performance
        if cote_finale is not None:
            cat = _odds_category(cote_finale)
            if cat:
                self.odds_cat_record[cat][1] += 1
                if is_gagnant:
                    self.odds_cat_record[cat][0] += 1

        # Track implied probability bucket performance
        if proba_implicite is not None:
            bucket = _proba_bucket(proba_implicite)
            if bucket:
                self.proba_bucket_record[bucket][1] += 1
                if is_gagnant:
                    self.proba_bucket_record[bucket][0] += 1

        # Track rank vs finish
        if odds_rank is not None and position is not None:
            self.rank_finish_pairs.append((float(odds_rank), float(position)))

        # Track post-win odds effect: if the PREVIOUS race was a win,
        # record how much odds changed this time
        if len(self.recent_races) >= 2:
            prev = self.recent_races[-2]
            prev_gagnant = prev[2]
            prev_cote = prev[0]
            if prev_gagnant and prev_cote is not None and prev_cote > 0 and cote_finale is not None:
                # Ratio < 1 means odds shortened (market expected repeat win)
                self.post_win_odds_changes.append(
                    round(cote_finale / prev_cote, 4)
                )

        # Track performance vs odds changes for overreaction score
        if len(self.recent_races) >= 2:
            prev = self.recent_races[-2]
            prev_cote = prev[0]
            prev_pos = prev[3]
            if (
                prev_cote is not None
                and prev_cote > 0
                and cote_finale is not None
                and cote_finale > 0
                and prev_pos is not None
                and position is not None
            ):
                odds_change_ratio = (cote_finale - prev_cote) / prev_cote
                # Normalise position change (negative = improved)
                perf_change = (prev_pos - position) / max(prev_pos, 1)
                self.perf_vs_odds.append((odds_change_ratio, perf_change))


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_market_overreaction(input_path: Path, output_path: Path, logger) -> int:
    """Build market overreaction features from partants_master.jsonl.

    Two-phase approach:
      1. Index: read sort keys + byte offsets (lightweight).
      2. Sort chronologically, then seek-read records course by course,
         streaming output to .tmp, then atomic rename.

    Returns the total number of feature records written.
    """
    logger.info("=== Market Overreaction Builder (memory-optimised) ===")
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

    # -- Phase 3: Seek-based processing, streaming output to .tmp --
    t2 = time.time()

    horse_states: dict[str, _HorseMarketState] = defaultdict(_HorseMarketState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    _FEATURE_KEYS = [
        "mkor_odds_drift",
        "mkor_odds_drift_ratio",
        "mkor_last_race_effect",
        "mkor_overreaction_score",
        "mkor_calibration_error",
        "mkor_beaten_fav_rate",
        "mkor_outsider_upset_rate",
        "mkor_implied_prob_accuracy",
        "mkor_odds_rank_vs_finish",
        "mkor_smart_money",
        "mkor_false_fav_rate",
        "mkor_odds_cat_perf",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            horse_id = rec.get("horse_id") or rec.get("nom_cheval") or rec.get("cheval_nom")
            return {
                "uid": rec.get("partant_uid"),
                "horse_id": horse_id.strip().upper() if horse_id and isinstance(horse_id, str) else None,
                "cote_finale": _safe_positive_float(rec.get("cote_finale")),
                "cote_reference": _safe_positive_float(rec.get("cote_reference")),
                "proba_implicite": _safe_float(rec.get("proba_implicite")),
                "is_gagnant": bool(rec.get("is_gagnant")),
                "position": None,
                "nombre_partants": None,
            }

        def _extract_position(rec: dict) -> Optional[int]:
            try:
                return int(rec.get("position_arrivee"))
            except (TypeError, ValueError):
                return None

        def _extract_nb_partants(rec: dict) -> Optional[int]:
            try:
                return int(rec.get("nombre_partants"))
            except (TypeError, ValueError):
                return None

        i = 0
        while i < total:
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

            # Read this course's records from disk
            course_raw = [_read_record_at(index[ci][3]) for ci in course_indices]
            course_group = []
            for raw_rec in course_raw:
                slim = _extract_slim(raw_rec)
                slim["position"] = _extract_position(raw_rec)
                slim["nombre_partants"] = _extract_nb_partants(raw_rec)
                course_group.append(slim)

            # Compute odds ranks for this course (ascending odds = rank 1 = favourite)
            cote_pairs = [
                (idx, r["cote_finale"])
                for idx, r in enumerate(course_group)
                if r["cote_finale"] is not None
            ]
            cote_pairs.sort(key=lambda x: x[1])
            odds_ranks: dict[int, int] = {}
            for rank, (idx, _) in enumerate(cote_pairs, 1):
                odds_ranks[idx] = rank

            # -- Snapshot BEFORE update for all partants --
            post_updates: list[tuple[int, dict]] = []

            for idx, rec in enumerate(course_group):
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid:
                    state = horse_states[hid]
                    feats = state.snapshot(
                        cote_finale=rec["cote_finale"],
                        cote_reference=rec["cote_reference"],
                        proba_implicite=rec["proba_implicite"],
                    )
                    features.update(feats)
                else:
                    for k in _FEATURE_KEYS:
                        features[k] = None

                # Track fill rates
                for k in _FEATURE_KEYS:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                post_updates.append((idx, rec))

            # -- Update states AFTER all snapshots for this course --
            for idx, rec in post_updates:
                hid = rec["horse_id"]
                if not hid:
                    continue

                horse_states[hid].update(
                    cote_finale=rec["cote_finale"],
                    cote_reference=rec["cote_reference"],
                    is_gagnant=rec["is_gagnant"],
                    position=rec["position"],
                    proba_implicite=rec["proba_implicite"],
                    odds_rank=odds_ranks.get(idx),
                )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Market overreaction build termine: %d features en %.1fs",
        n_written, elapsed,
    )
    logger.info(
        "Chevaux trackes: %d", len(horse_states),
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
        description="Construction des features market overreaction a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/market_overreaction/)",
    )
    args = parser.parse_args()

    logger = setup_logging("market_overreaction_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "market_overreaction.jsonl"
    build_market_overreaction(input_path, out_path, logger)


if __name__ == "__main__":
    main()
