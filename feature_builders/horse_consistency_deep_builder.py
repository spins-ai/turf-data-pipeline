#!/usr/bin/env python3
"""
feature_builders.horse_consistency_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep horse consistency features -- measuring reliability/predictability
of each horse based on historical race patterns.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant consistency features.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.  Snapshot BEFORE update.

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 re-reads records from disk via seek, streams output to .tmp
  - gc.collect() every 500K records

Produces:
  - horse_consistency_deep.jsonl   in builder_outputs/horse_consistency_deep/

Features per partant (10):
  - hcd_position_cv               : coefficient of variation of positions (std/mean) -- lower = more consistent
  - hcd_beaten_lengths_cv         : CV of (position/partants) normalised finish
  - hcd_odds_vs_finish_correlation: correlation between odds rank and finish rank over last 10 races
  - hcd_consecutive_form_match    : count of consecutive races where result matched odds expectation (within 2 positions)
  - hcd_upset_frequency           : fraction of races where horse finished much better than odds implied (>3 positions)
  - hcd_disappointment_frequency  : fraction of races where horse finished much worse than expected
  - hcd_place_rate_stability      : std of rolling 5-race place rates over career (lower = more stable form)
  - hcd_is_banker_type            : 1 if position_cv < 0.3 and place_rate > 40% -- reliable placer
  - hcd_is_volatile               : 1 if position_cv > 0.6 -- unpredictable outcomes
  - hcd_form_cycle_length         : estimated average number of races between wins (for regular winners)

Usage:
    python feature_builders/horse_consistency_deep_builder.py
    python feature_builders/horse_consistency_deep_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/horse_consistency_deep")

_LOG_EVERY = 500_000

_POS_WINDOW = 20       # rolling window for positions / partants
_ODDS_WINDOW = 10      # rolling window for odds/finish rank correlation
_PLACE_ROLL = 5        # rolling window for place rate stability


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseConsistencyState:
    """Per-horse consistency state, memory-optimised with __slots__."""

    __slots__ = (
        "positions", "partants", "odds_ranks", "finish_ranks",
        "form_match_streak", "upsets", "disappointments", "total",
        "rolling_place_rates", "wins", "win_indices",
    )

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=_POS_WINDOW)
        self.partants: deque = deque(maxlen=_POS_WINDOW)
        self.odds_ranks: deque = deque(maxlen=_ODDS_WINDOW)
        self.finish_ranks: deque = deque(maxlen=_ODDS_WINDOW)

        self.form_match_streak: int = 0
        self.upsets: int = 0
        self.disappointments: int = 0
        self.total: int = 0

        # Rolling 5-race place rates computed at each race end
        self.rolling_place_rates: deque = deque(maxlen=10)

        self.wins: int = 0
        self.win_indices: list = []  # race indices (0-based) where horse won

    # -----------------------------------------------------------------
    # Snapshot BEFORE update
    # -----------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        """Return features from CURRENT state (before this race's update)."""
        feats: dict[str, Any] = {}

        # 1. hcd_position_cv: CV of positions (std / mean)
        if len(self.positions) >= 3:
            vals = list(self.positions)
            mean_p = sum(vals) / len(vals)
            if mean_p > 0:
                var_p = sum((v - mean_p) ** 2 for v in vals) / len(vals)
                std_p = math.sqrt(var_p)
                feats["hcd_position_cv"] = round(std_p / mean_p, 4)
            else:
                feats["hcd_position_cv"] = None
        else:
            feats["hcd_position_cv"] = None

        # 2. hcd_beaten_lengths_cv: CV of (position / partants) normalised finish
        if len(self.positions) >= 3 and len(self.partants) >= 3:
            norm_vals = []
            for pos, nb in zip(self.positions, self.partants):
                if nb and nb > 0:
                    norm_vals.append(pos / nb)
            if len(norm_vals) >= 3:
                mean_n = sum(norm_vals) / len(norm_vals)
                if mean_n > 0:
                    var_n = sum((v - mean_n) ** 2 for v in norm_vals) / len(norm_vals)
                    std_n = math.sqrt(var_n)
                    feats["hcd_beaten_lengths_cv"] = round(std_n / mean_n, 4)
                else:
                    feats["hcd_beaten_lengths_cv"] = None
            else:
                feats["hcd_beaten_lengths_cv"] = None
        else:
            feats["hcd_beaten_lengths_cv"] = None

        # 3. hcd_odds_vs_finish_correlation: Pearson r between odds rank and finish rank
        if len(self.odds_ranks) >= 5 and len(self.finish_ranks) >= 5:
            n = min(len(self.odds_ranks), len(self.finish_ranks))
            or_list = list(self.odds_ranks)[:n]
            fr_list = list(self.finish_ranks)[:n]
            feats["hcd_odds_vs_finish_correlation"] = _pearson_r(or_list, fr_list)
        else:
            feats["hcd_odds_vs_finish_correlation"] = None

        # 4. hcd_consecutive_form_match
        feats["hcd_consecutive_form_match"] = self.form_match_streak if self.total > 0 else None

        # 5. hcd_upset_frequency
        if self.total > 0:
            feats["hcd_upset_frequency"] = round(self.upsets / self.total, 4)
        else:
            feats["hcd_upset_frequency"] = None

        # 6. hcd_disappointment_frequency
        if self.total > 0:
            feats["hcd_disappointment_frequency"] = round(
                self.disappointments / self.total, 4
            )
        else:
            feats["hcd_disappointment_frequency"] = None

        # 7. hcd_place_rate_stability: std of rolling 5-race place rates
        if len(self.rolling_place_rates) >= 3:
            rates = list(self.rolling_place_rates)
            mean_r = sum(rates) / len(rates)
            var_r = sum((r - mean_r) ** 2 for r in rates) / len(rates)
            feats["hcd_place_rate_stability"] = round(math.sqrt(var_r), 4)
        else:
            feats["hcd_place_rate_stability"] = None

        # 8. hcd_is_banker_type: position_cv < 0.3 and place_rate > 40%
        pos_cv = feats.get("hcd_position_cv")
        place_rate = self._current_place_rate()
        if pos_cv is not None and place_rate is not None:
            feats["hcd_is_banker_type"] = int(
                pos_cv < 0.3 and place_rate > 0.40
            )
        else:
            feats["hcd_is_banker_type"] = None

        # 9. hcd_is_volatile: position_cv > 0.6
        if pos_cv is not None:
            feats["hcd_is_volatile"] = int(pos_cv > 0.6)
        else:
            feats["hcd_is_volatile"] = None

        # 10. hcd_form_cycle_length: avg races between wins
        if len(self.win_indices) >= 2:
            gaps = [
                self.win_indices[j] - self.win_indices[j - 1]
                for j in range(1, len(self.win_indices))
            ]
            feats["hcd_form_cycle_length"] = round(sum(gaps) / len(gaps), 2)
        else:
            feats["hcd_form_cycle_length"] = None

        return feats

    # -----------------------------------------------------------------
    # Update AFTER snapshot
    # -----------------------------------------------------------------
    def update(
        self,
        finish_pos: Optional[int],
        nb_partants: int,
        odds_rank: Optional[int],
        is_gagnant: bool,
        is_place: bool,
    ) -> None:
        """Update state AFTER snapshot has been taken."""
        if finish_pos is not None:
            self.positions.append(finish_pos)
        if nb_partants > 0:
            self.partants.append(nb_partants)

        # Odds rank and finish rank for correlation
        finish_rank = finish_pos  # finish rank = place_arrivee
        if odds_rank is not None:
            self.odds_ranks.append(odds_rank)
        if finish_rank is not None:
            self.finish_ranks.append(finish_rank)

        # Form match: did result match odds expectation (within 2 positions)?
        if odds_rank is not None and finish_rank is not None:
            diff = finish_rank - odds_rank
            if abs(diff) <= 2:
                self.form_match_streak += 1
            else:
                self.form_match_streak = 0

            # Upset: finished much better than expected (>3 positions better)
            if diff < -3:
                self.upsets += 1

            # Disappointment: finished much worse than expected (>3 positions worse)
            if diff > 3:
                self.disappointments += 1

        self.total += 1

        # Rolling place rate: compute over last _PLACE_ROLL races in positions
        if len(self.positions) >= _PLACE_ROLL:
            recent = list(self.positions)[-_PLACE_ROLL:]
            recent_nb = list(self.partants)[-_PLACE_ROLL:] if len(self.partants) >= _PLACE_ROLL else None
            if recent_nb:
                places = sum(
                    1 for pos, nb in zip(recent, recent_nb)
                    if nb > 0 and pos <= max(3, int(nb * 0.3))
                )
                self.rolling_place_rates.append(places / _PLACE_ROLL)

        if is_gagnant:
            self.wins += 1
            self.win_indices.append(self.total - 1)

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------
    def _current_place_rate(self) -> Optional[float]:
        """Place rate over the current positions window."""
        if not self.positions or not self.partants:
            return None
        places = 0
        counted = 0
        for pos, nb in zip(self.positions, self.partants):
            if nb and nb > 0:
                if pos <= max(3, int(nb * 0.3)):
                    places += 1
                counted += 1
        if counted == 0:
            return None
        return places / counted


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float or return None."""
    if v is None:
        return None
    try:
        val = float(v)
        return val if val == val else None  # NaN check
    except (TypeError, ValueError):
        return None


def _pearson_r(xs: list, ys: list) -> Optional[float]:
    """Compute Pearson correlation coefficient. Returns None if undefined."""
    n = len(xs)
    if n < 3:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    denom = math.sqrt(var_x * var_y)
    if denom < 1e-12:
        return None
    return round(cov / denom, 4)


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
# MAIN BUILD (memory-optimised: index+sort+seek)
# ===========================================================================


def build_horse_consistency_deep(input_path: Path, output_path: Path, logger) -> int:
    """Build deep horse consistency features from partants_master.jsonl.

    Two-phase approach:
      1. Index: read sort keys + byte offsets (lightweight).
      2. Sort chronologically, then seek-read records course by course,
         streaming output to .tmp, then atomic rename.

    Returns the total number of feature records written.
    """
    logger.info("=== Horse Consistency Deep Builder (memory-optimised) ===")
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

    # -- Phase 3: Seek-based processing, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseConsistencyState] = defaultdict(_HorseConsistencyState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    _FEATURE_KEYS = [
        "hcd_position_cv",
        "hcd_beaten_lengths_cv",
        "hcd_odds_vs_finish_correlation",
        "hcd_consecutive_form_match",
        "hcd_upset_frequency",
        "hcd_disappointment_frequency",
        "hcd_place_rate_stability",
        "hcd_is_banker_type",
        "hcd_is_volatile",
        "hcd_form_cycle_length",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            nb_partants = _safe_int(rec.get("nombre_partants")) or 0
            num_pmu = _safe_int(rec.get("num_pmu")) or 0
            finish_pos = _safe_int(rec.get("place_arrivee"))
            if finish_pos is None:
                finish_pos = _safe_int(rec.get("arrivee_ordre"))

            cote = _safe_float(rec.get("cote_finale"))
            if cote is None:
                cote = _safe_float(rec.get("cote_reference"))

            is_place = bool(rec.get("is_place"))

            return {
                "uid": rec.get("partant_uid"),
                "cheval": rec.get("nom_cheval"),
                "gagnant": bool(rec.get("is_gagnant")),
                "num_pmu": num_pmu,
                "finish_pos": finish_pos,
                "nb_partants": nb_partants,
                "cote": cote,
                "is_place": is_place,
            }

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
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # Compute odds ranks for this course (lower cote = lower rank = more favoured)
            cotes_in_course = []
            for rec in course_group:
                cotes_in_course.append((rec["cote"], len(cotes_in_course)))
            valid_cotes = [(c, idx) for c, idx in cotes_in_course if c is not None and c > 0]
            valid_cotes.sort(key=lambda x: x[0])
            odds_rank_map: dict[int, int] = {}
            for rank, (_, orig_idx) in enumerate(valid_cotes, 1):
                odds_rank_map[orig_idx] = rank

            # -- Snapshot BEFORE update for all partants --
            post_updates: list[tuple[str, Optional[int], int, Optional[int], bool, bool]] = []

            for rec_idx, rec in enumerate(course_group):
                cheval = rec["cheval"]
                if not cheval:
                    # Write empty features
                    features = {"partant_uid": rec["uid"]}
                    for k in _FEATURE_KEYS:
                        features[k] = None
                    fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                # Snapshot
                hs = horse_state[cheval]
                features = hs.snapshot()
                features["partant_uid"] = rec["uid"]

                # Track fill rates
                for k in _FEATURE_KEYS:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Defer update
                odds_rank = odds_rank_map.get(rec_idx)
                post_updates.append((
                    cheval,
                    rec["finish_pos"],
                    rec["nb_partants"],
                    odds_rank,
                    rec["gagnant"],
                    rec["is_place"],
                ))

            # -- Update states AFTER all snapshots --
            for cheval, finish_pos, nb_partants, odds_rank, is_gagnant, is_place in post_updates:
                horse_state[cheval].update(
                    finish_pos, nb_partants, odds_rank, is_gagnant, is_place
                )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Horse consistency deep build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
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
        description="Construction des features horse consistency deep a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/horse_consistency_deep/)",
    )
    args = parser.parse_args()

    logger = setup_logging("horse_consistency_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "horse_consistency_deep.jsonl"
    build_horse_consistency_deep(input_path, out_path, logger)


if __name__ == "__main__":
    main()
