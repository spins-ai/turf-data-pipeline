#!/usr/bin/env python3
"""
feature_builders.betting_value_advanced_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Advanced betting value features for the betting strategy modules
(Kelly, Value Hunter RL, ROI predictor, ticket optimizer).

Reads partants_master.jsonl in streaming mode, builds an index +
chronological sort + seek architecture, and computes per-partant
advanced betting value features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to all statistics -- no future leakage. State is snapshotted
BEFORE update.

State tracking:
  - Per horse: wins, runs, total_return
  - Per jockey: rolling 100 rides with returns
  - Per trainer: rolling 100 runners with returns
  - Per (hippodrome, cote_range): wins, runs
  - cote_range = floor(cote / 2) * 2 (buckets of 2 EUR)

Produces:
  - betting_value_advanced_features.jsonl

Features per partant (12):
  - bva_historical_roi_horse       : horse's historical ROI (sum returns / sum bets)
  - bva_historical_roi_jockey      : jockey's rolling ROI (last 100 rides)
  - bva_historical_roi_trainer     : trainer's rolling ROI (last 100 runners)
  - bva_cote_range_win_rate        : historical win rate for this odds range at this hippodrome
  - bva_expected_value             : EV = (estimated_prob * cote) - 1
  - bva_kelly_optimal              : Kelly fraction = (p*b - q) / b
  - bva_overbet_signal             : 1 if cote < historical avg for similar profile
  - bva_underbet_signal            : 1 if cote > historical avg for similar profile
  - bva_smart_money_proxy          : abs(cote_finale - cote_reference) / cote_reference
  - bva_place_value                : estimated place prob / place_odds_implied
  - bva_each_way_edge              : max(0, EV, place_value)
  - bva_bankroll_risk              : 1/cote * (1 - estimated_win_prob)

Usage:
    python feature_builders/betting_value_advanced_builder.py
    python feature_builders/betting_value_advanced_builder.py --input path/to/partants_master.jsonl
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

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/betting_value_advanced")
OUTPUT_FILENAME = "betting_value_advanced_features.jsonl"

# Bayesian prior for win-rate shrinkage
PRIOR_WEIGHT = 10
GLOBAL_WIN_RATE = 0.08  # ~8% baseline

# Rolling window for jockey/trainer
ROLLING_WINDOW = 100

# Kelly parameters
KELLY_CAP = 0.25  # quarter-Kelly maximum fraction

# Progress log every N records
_LOG_EVERY = 500_000

# Feature names for fill rate tracking
_FEATURE_NAMES = [
    "bva_historical_roi_horse",
    "bva_historical_roi_jockey",
    "bva_historical_roi_trainer",
    "bva_cote_range_win_rate",
    "bva_expected_value",
    "bva_kelly_optimal",
    "bva_overbet_signal",
    "bva_underbet_signal",
    "bva_smart_money_proxy",
    "bva_place_value",
    "bva_each_way_edge",
    "bva_bankroll_risk",
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _cote_range(cote: float) -> int:
    """Compute odds bucket: floor(cote / 2) * 2. E.g. 3.5 -> 2, 7.1 -> 6."""
    return int(math.floor(cote / 2.0)) * 2


def _safe_float(val) -> Optional[float]:
    """Convert value to float, return None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _HorseROI:
    """Tracks per-horse cumulative betting ROI."""

    __slots__ = ("wins", "runs", "total_return")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0
        self.total_return: float = 0.0  # sum of (cote if won, else 0)

    def roi(self) -> Optional[float]:
        """ROI = (total_return - total_staked) / total_staked = total_return/runs - 1."""
        if self.runs == 0:
            return None
        return round(self.total_return / self.runs - 1.0, 6)

    def win_rate(self) -> Optional[float]:
        if self.runs == 0:
            return None
        return self.wins / self.runs

    def avg_cote(self) -> Optional[float]:
        """Average cote seen by this horse (proxy for profile avg odds)."""
        # Not tracked here; we track per (hippo, cote_range) instead
        return None


class _RollingROI:
    """Tracks rolling ROI over last N entries for jockeys/trainers.

    Each entry is (return_for_bet,) where return = cote if won, else 0.
    """

    __slots__ = ("history",)

    def __init__(self) -> None:
        self.history: deque = deque(maxlen=ROLLING_WINDOW)

    def roi(self) -> Optional[float]:
        if not self.history:
            return None
        total_return = sum(self.history)
        n = len(self.history)
        return round(total_return / n - 1.0, 6)


class _CoteRangeStats:
    """Tracks win rate per (hippodrome, cote_range) bucket."""

    __slots__ = ("wins", "runs", "sum_cote")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0
        self.sum_cote: float = 0.0  # sum of cotes seen, for average

    def win_rate(self) -> Optional[float]:
        if self.runs < 3:
            return None
        return round(self.wins / self.runs, 6)

    def avg_cote(self) -> Optional[float]:
        if self.runs == 0:
            return None
        return self.sum_cote / self.runs


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_betting_value_advanced(input_path: Path, output_path: Path, logger) -> int:
    """Build advanced betting value features.

    Architecture:
      1. Index: read sort keys + byte offsets into memory.
      2. Sort chronologically.
      3. Seek + process course by course, streaming output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Betting Value Advanced Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
    index: list[tuple[str, str, int, int]] = []  # (date, course_uid, num_pmu, offset)
    n_read = 0
    n_errors = 0

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
                n_errors += 1
                if n_errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", n_errors)
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
        "Phase 1 terminee: %d records indexes, %d erreurs JSON en %.1fs",
        len(index), n_errors, time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()

    # State trackers
    horse_roi: dict[str, _HorseROI] = defaultdict(_HorseROI)
    jockey_roi: dict[str, _RollingROI] = defaultdict(_RollingROI)
    trainer_roi: dict[str, _RollingROI] = defaultdict(_RollingROI)
    cote_range_stats: dict[tuple[str, int], _CoteRangeStats] = defaultdict(_CoteRangeStats)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {name: 0 for name in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(off: int) -> dict:
            fin.seek(off)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            cote_finale = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
            cote_reference = _safe_float(rec.get("cote_reference"))
            cote_place = _safe_float(rec.get("rapport_place"))

            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper()

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "cheval": rec.get("nom_cheval"),
                "jockey": rec.get("nom_jockey") or rec.get("driver"),
                "entraineur": rec.get("entraineur") or rec.get("nom_entraineur"),
                "hippo": rec.get("hippodrome_normalise", ""),
                "discipline": discipline,
                "gagnant": bool(rec.get("is_gagnant")),
                "place": bool(rec.get("is_place")),
                "cote_finale": cote_finale,
                "cote_reference": cote_reference,
                "cote_place": cote_place,
                "nb_partants": rec.get("nombre_partants"),
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
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            # -- Snapshot pre-race stats and emit features --
            post_updates: list[dict] = []

            for rec in course_group:
                cheval = rec["cheval"]
                jockey = rec["jockey"]
                entraineur = rec["entraineur"]
                hippo = rec["hippo"]
                cote = rec["cote_finale"]
                cote_ref = rec["cote_reference"]

                features: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": rec["course"],
                    "date_reunion_iso": rec["date"],
                }

                # --- 1. bva_historical_roi_horse ---
                roi_h = horse_roi[cheval].roi() if cheval else None
                features["bva_historical_roi_horse"] = roi_h
                if roi_h is not None:
                    fill_counts["bva_historical_roi_horse"] += 1

                # --- 2. bva_historical_roi_jockey ---
                roi_j = jockey_roi[jockey].roi() if jockey else None
                features["bva_historical_roi_jockey"] = roi_j
                if roi_j is not None:
                    fill_counts["bva_historical_roi_jockey"] += 1

                # --- 3. bva_historical_roi_trainer ---
                roi_t = trainer_roi[entraineur].roi() if entraineur else None
                features["bva_historical_roi_trainer"] = roi_t
                if roi_t is not None:
                    fill_counts["bva_historical_roi_trainer"] += 1

                # --- 4. bva_cote_range_win_rate ---
                cr_wr = None
                cr_avg_cote = None
                if cote is not None and cote > 0 and hippo:
                    cr = _cote_range(cote)
                    key = (hippo, cr)
                    cr_wr = cote_range_stats[key].win_rate()
                    cr_avg_cote = cote_range_stats[key].avg_cote()
                features["bva_cote_range_win_rate"] = cr_wr
                if cr_wr is not None:
                    fill_counts["bva_cote_range_win_rate"] += 1

                # --- Estimated probability (Bayesian) ---
                # Prior: hippodrome + cote_range win rate (or global)
                prior_p = cr_wr if cr_wr is not None else GLOBAL_WIN_RATE
                # Update with horse's personal win rate
                horse_wr = horse_roi[cheval].win_rate() if cheval else None
                if horse_wr is not None and horse_roi[cheval].runs >= 3:
                    # Bayesian combination: weight personal data
                    n_horse = horse_roi[cheval].runs
                    est_prob = (PRIOR_WEIGHT * prior_p + n_horse * horse_wr) / (PRIOR_WEIGHT + n_horse)
                else:
                    est_prob = prior_p

                # --- 5. bva_expected_value ---
                ev = None
                if cote is not None and cote > 0:
                    ev = round(est_prob * cote - 1.0, 6)
                features["bva_expected_value"] = ev
                if ev is not None:
                    fill_counts["bva_expected_value"] += 1

                # --- 6. bva_kelly_optimal ---
                kelly = None
                if cote is not None and cote > 1.0:
                    b = cote - 1.0
                    q = 1.0 - est_prob
                    raw_kelly = (est_prob * b - q) / b
                    kelly = round(max(0.0, min(raw_kelly, KELLY_CAP)), 6)
                features["bva_kelly_optimal"] = kelly
                if kelly is not None:
                    fill_counts["bva_kelly_optimal"] += 1

                # --- 7. bva_overbet_signal ---
                # 1 if cote is lower than historical avg for this (hippo, cote_range)
                overbet = None
                if cote is not None and cr_avg_cote is not None:
                    overbet = 1 if cote < cr_avg_cote else 0
                features["bva_overbet_signal"] = overbet
                if overbet is not None:
                    fill_counts["bva_overbet_signal"] += 1

                # --- 8. bva_underbet_signal ---
                underbet = None
                if cote is not None and cr_avg_cote is not None:
                    underbet = 1 if cote > cr_avg_cote else 0
                features["bva_underbet_signal"] = underbet
                if underbet is not None:
                    fill_counts["bva_underbet_signal"] += 1

                # --- 9. bva_smart_money_proxy ---
                smart_money = None
                if cote is not None and cote_ref is not None and cote_ref > 0:
                    smart_money = round(abs(cote - cote_ref) / cote_ref, 6)
                features["bva_smart_money_proxy"] = smart_money
                if smart_money is not None:
                    fill_counts["bva_smart_money_proxy"] += 1

                # --- 10. bva_place_value ---
                place_value = None
                # Estimate place probability from historical data
                # Approximate: top ~30% of field places (depends on field size)
                nb_p = rec.get("nb_partants")
                try:
                    nb_p = int(nb_p) if nb_p else 0
                except (ValueError, TypeError):
                    nb_p = 0

                if nb_p >= 4 and cote is not None and cote > 0:
                    # Place probability: rough prior = 3/field_size for top3
                    place_slots = 3 if nb_p >= 8 else 2
                    base_place_prob = place_slots / nb_p
                    # Adjust: better horses (lower cote) place more often
                    implied_win = 1.0 / cote
                    est_place_prob = min(1.0, base_place_prob + implied_win * 0.5)

                    # Place odds implied probability
                    cote_pl = rec.get("cote_place")
                    if cote_pl is not None and cote_pl > 0:
                        place_odds_implied = 1.0 / cote_pl
                        if place_odds_implied > 0:
                            place_value = round(est_place_prob / place_odds_implied, 6)
                features["bva_place_value"] = place_value
                if place_value is not None:
                    fill_counts["bva_place_value"] += 1

                # --- 11. bva_each_way_edge ---
                each_way = None
                candidates = [v for v in [ev, place_value] if v is not None]
                if candidates:
                    candidates.append(0.0)
                    each_way = round(max(candidates), 6)
                features["bva_each_way_edge"] = each_way
                if each_way is not None:
                    fill_counts["bva_each_way_edge"] += 1

                # --- 12. bva_bankroll_risk ---
                risk = None
                if cote is not None and cote > 0:
                    risk = round((1.0 / cote) * (1.0 - est_prob), 6)
                features["bva_bankroll_risk"] = risk
                if risk is not None:
                    fill_counts["bva_bankroll_risk"] += 1

                # Write to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Save for post-race update
                post_updates.append(rec)

            # -- Update state trackers AFTER race (no leakage) --
            for rec in post_updates:
                cheval = rec["cheval"]
                jockey = rec["jockey"]
                entraineur = rec["entraineur"]
                hippo = rec["hippo"]
                cote = rec["cote_finale"]
                is_winner = rec["gagnant"]

                # Return for this bet: cote if won, else 0
                bet_return = cote if (is_winner and cote is not None and cote > 0) else 0.0

                # Horse ROI tracker
                if cheval:
                    horse_roi[cheval].runs += 1
                    horse_roi[cheval].total_return += bet_return
                    if is_winner:
                        horse_roi[cheval].wins += 1

                # Jockey rolling ROI
                if jockey:
                    jockey_roi[jockey].history.append(bet_return)

                # Trainer rolling ROI
                if entraineur:
                    trainer_roi[entraineur].history.append(bet_return)

                # Cote range stats
                if cote is not None and cote > 0 and hippo:
                    cr = _cote_range(cote)
                    key = (hippo, cr)
                    cote_range_stats[key].runs += 1
                    cote_range_stats[key].sum_cote += cote
                    if is_winner:
                        cote_range_stats[key].wins += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Betting value advanced build termine: %d features en %.1fs "
        "(chevaux: %d, jockeys: %d, entraineurs: %d, cote_ranges: %d)",
        n_written, elapsed,
        len(horse_roi), len(jockey_roi), len(trainer_roi), len(cote_range_stats),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k in _FEATURE_NAMES:
        v = fill_counts[k]
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
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features betting value advanced a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("betting_value_advanced_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_betting_value_advanced(input_path, out_path, logger)


if __name__ == "__main__":
    main()
