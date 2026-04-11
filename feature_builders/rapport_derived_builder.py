#!/usr/bin/env python3
"""
feature_builders.rapport_derived_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Historical rapport (payout/dividend) derived features.

The rap_ fields contain actual race payouts. Since these are POST-HOC
data (known only after race results), we aggregate them HISTORICALLY:
rolling averages of past race characteristics per hippodrome, discipline,
horse, jockey, and trainer.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the rolling stats -- no future leakage.

Produces:
  - rapport_derived_features.jsonl   in builder_outputs/rapport_derived/

Features per partant (12):
  - rpd_hippo_avg_gagnant_payout     : rolling avg of simple_gagnant at this hippodrome (last 50 races)
  - rpd_hippo_avg_place_spread       : rolling avg of place payout spread at hippodrome
  - rpd_hippo_upset_rate             : proportion of recent races at hippo where gagnant > 1000
  - rpd_hippo_market_concentration_avg : rolling avg market_concentration at hippodrome
  - rpd_discipline_avg_gagnant       : rolling avg simple_gagnant for this discipline
  - rpd_discipline_surprise_rate     : upset rate by discipline
  - rpd_field_size_x_concentration   : nombre_partants * historical market_concentration
  - rpd_horse_avg_dividend_when_placed : horse's historical avg dividende when placed
  - rpd_horse_upset_maker            : proportion of horse's past wins that were upsets
  - rpd_jockey_roi_rolling           : jockey rolling ROI (last 30 rides)
  - rpd_trainer_roi_rolling          : trainer rolling ROI (last 30 rides)
  - rpd_course_predictability        : 1 - hippo_upset_rate

Usage:
    python feature_builders/rapport_derived_builder.py
    python feature_builders/rapport_derived_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
INPUT_CANDIDATES = [
    INPUT_DEFAULT,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rapport_derived")

# Rolling window sizes
HIPPO_WINDOW = 50       # last 50 races per hippodrome
DISCIPLINE_WINDOW = 100  # last 100 races per discipline
JOCKEY_WINDOW = 30       # last 30 rides per jockey
TRAINER_WINDOW = 30      # last 30 rides per trainer

# Upset threshold: simple_gagnant > 1000 centimes = payout > 10 EUR
UPSET_THRESHOLD = 1000

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


def _parse_date(date_str: str) -> Optional[str]:
    """Validate and return ISO date string (YYYY-MM-DD). Returns None on failure."""
    if not date_str or len(date_str) < 10:
        return None
    try:
        # Basic validation
        y, m, d = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
        if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
            return date_str[:10]
    except (ValueError, TypeError, IndexError):
        pass
    return None


def _safe_int(val) -> Optional[int]:
    """Convert value to int, return None if impossible."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    """Convert value to float, return None if impossible."""
    if val is None:
        return None
    try:
        v = float(val)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


# ===========================================================================
# ROLLING TRACKERS
# ===========================================================================


class _RollingStats:
    """Fixed-size deque tracker for numeric values with rolling avg."""

    __slots__ = ("values",)

    def __init__(self, maxlen: int) -> None:
        self.values: deque = deque(maxlen=maxlen)

    def add(self, val: float) -> None:
        self.values.append(val)

    def avg(self) -> Optional[float]:
        if not self.values:
            return None
        return round(sum(self.values) / len(self.values), 4)

    def rate_above(self, threshold: float) -> Optional[float]:
        """Proportion of values above threshold."""
        if not self.values:
            return None
        count = sum(1 for v in self.values if v > threshold)
        return round(count / len(self.values), 4)

    def count(self) -> int:
        return len(self.values)


class _RollingROI:
    """Track ROI as sum(dividende) / count(rides) over a rolling window."""

    __slots__ = ("dividends", "window")

    def __init__(self, maxlen: int) -> None:
        self.dividends: deque = deque(maxlen=maxlen)
        self.window = maxlen

    def add(self, dividende: float) -> None:
        """Add a ride. dividende=0 if not placed, else the actual dividend."""
        self.dividends.append(dividende)

    def roi(self) -> Optional[float]:
        if not self.dividends:
            return None
        total_div = sum(self.dividends)
        # ROI = (total_return - total_cost) / total_cost
        # cost = 1 EUR per ride (unit bet)
        n = len(self.dividends)
        return round((total_div - n) / n, 4)


class _HorsePayoutTracker:
    """Track horse's dividends when placed (non-zero dividende)."""

    __slots__ = ("placed_dividends", "wins", "upset_wins")

    def __init__(self) -> None:
        self.placed_dividends: list[float] = []
        self.wins: int = 0
        self.upset_wins: int = 0  # wins where gagnant > UPSET_THRESHOLD

    def add_placed(self, dividende: float) -> None:
        self.placed_dividends.append(dividende)

    def add_win(self, gagnant_centimes: int) -> None:
        self.wins += 1
        if gagnant_centimes > UPSET_THRESHOLD:
            self.upset_wins += 1

    def avg_dividend_when_placed(self) -> Optional[float]:
        if not self.placed_dividends:
            return None
        return round(sum(self.placed_dividends) / len(self.placed_dividends), 4)

    def upset_maker_rate(self) -> Optional[float]:
        if self.wins == 0:
            return None
        return round(self.upset_wins / self.wins, 4)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_rapport_derived_features(input_path: Path, output_path: Path, logger) -> int:
    """Build rapport-derived features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Rapport Derived Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (sort_key, byte_offset) --
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

    # Rolling trackers per hippodrome
    hippo_gagnant: dict[str, _RollingStats] = {}
    hippo_place_spread: dict[str, _RollingStats] = {}
    hippo_concentration: dict[str, _RollingStats] = {}
    # We track upset rate via hippo_gagnant.rate_above(UPSET_THRESHOLD)

    # Rolling trackers per discipline
    disc_gagnant: dict[str, _RollingStats] = {}
    # discipline upset via disc_gagnant.rate_above(UPSET_THRESHOLD)

    # Per-horse trackers
    horse_tracker: dict[str, _HorsePayoutTracker] = {}

    # Per-jockey/trainer ROI
    jockey_roi: dict[str, _RollingROI] = {}
    trainer_roi: dict[str, _RollingROI] = {}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "rpd_hippo_avg_gagnant_payout",
        "rpd_hippo_avg_place_spread",
        "rpd_hippo_upset_rate",
        "rpd_hippo_market_concentration_avg",
        "rpd_discipline_avg_gagnant",
        "rpd_discipline_surprise_rate",
        "rpd_field_size_x_concentration",
        "rpd_horse_avg_dividend_when_placed",
        "rpd_horse_upset_maker",
        "rpd_jockey_roi_rolling",
        "rpd_trainer_roi_rolling",
        "rpd_course_predictability",
    ]
    fill_counts = {k: 0 for k in feature_names}

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

            # Read this course's records from disk
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            date_iso = _parse_date(course_date_str)

            # Extract race-level rapport data (same for all partants in a course)
            first = course_records[0]
            race_gagnant = _safe_int(first.get("rap_rapport_simple_gagnant"))
            place_1 = _safe_int(first.get("rap_rapport_simple_place_1"))
            place_2 = _safe_int(first.get("rap_rapport_simple_place_2"))
            place_3 = _safe_int(first.get("rap_rapport_simple_place_3"))
            race_concentration = _safe_float(first.get("rap_market_concentration"))
            hippo = (first.get("hippodrome_normalise") or "").strip()
            discipline = (first.get("discipline") or first.get("type_course") or "").strip().upper()
            nb_partants = _safe_int(first.get("nombre_partants")) or 0

            # Compute race-level place spread
            place_vals = [v for v in [place_1, place_2, place_3] if v is not None]
            race_place_spread = (max(place_vals) - min(place_vals)) if len(place_vals) >= 2 else None

            # -- Snapshot pre-race stats for all partants --
            for rec in course_records:
                partant_uid = rec.get("partant_uid")
                course_uid_r = rec.get("course_uid")
                cheval = rec.get("nom_cheval") or ""
                jockey = rec.get("nom_jockey") or ""
                entraineur = rec.get("nom_entraineur") or ""

                features: dict[str, Any] = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_r,
                    "date_reunion_iso": date_iso,
                }

                # --- Hippodrome features ---
                if hippo and hippo in hippo_gagnant:
                    val = hippo_gagnant[hippo].avg()
                    features["rpd_hippo_avg_gagnant_payout"] = val
                    if val is not None:
                        fill_counts["rpd_hippo_avg_gagnant_payout"] += 1
                else:
                    features["rpd_hippo_avg_gagnant_payout"] = None

                if hippo and hippo in hippo_place_spread:
                    val = hippo_place_spread[hippo].avg()
                    features["rpd_hippo_avg_place_spread"] = val
                    if val is not None:
                        fill_counts["rpd_hippo_avg_place_spread"] += 1
                else:
                    features["rpd_hippo_avg_place_spread"] = None

                if hippo and hippo in hippo_gagnant:
                    val = hippo_gagnant[hippo].rate_above(UPSET_THRESHOLD)
                    features["rpd_hippo_upset_rate"] = val
                    if val is not None:
                        fill_counts["rpd_hippo_upset_rate"] += 1
                else:
                    features["rpd_hippo_upset_rate"] = None

                if hippo and hippo in hippo_concentration:
                    val = hippo_concentration[hippo].avg()
                    features["rpd_hippo_market_concentration_avg"] = val
                    if val is not None:
                        fill_counts["rpd_hippo_market_concentration_avg"] += 1
                else:
                    features["rpd_hippo_market_concentration_avg"] = None

                # --- Discipline features ---
                if discipline and discipline in disc_gagnant:
                    val = disc_gagnant[discipline].avg()
                    features["rpd_discipline_avg_gagnant"] = val
                    if val is not None:
                        fill_counts["rpd_discipline_avg_gagnant"] += 1
                else:
                    features["rpd_discipline_avg_gagnant"] = None

                if discipline and discipline in disc_gagnant:
                    val = disc_gagnant[discipline].rate_above(UPSET_THRESHOLD)
                    features["rpd_discipline_surprise_rate"] = val
                    if val is not None:
                        fill_counts["rpd_discipline_surprise_rate"] += 1
                else:
                    features["rpd_discipline_surprise_rate"] = None

                # --- Field size x concentration ---
                conc_avg = None
                if hippo and hippo in hippo_concentration:
                    conc_avg = hippo_concentration[hippo].avg()
                if conc_avg is not None and nb_partants > 0:
                    features["rpd_field_size_x_concentration"] = round(nb_partants * conc_avg, 4)
                    fill_counts["rpd_field_size_x_concentration"] += 1
                else:
                    features["rpd_field_size_x_concentration"] = None

                # --- Horse features ---
                if cheval and cheval in horse_tracker:
                    ht = horse_tracker[cheval]
                    val = ht.avg_dividend_when_placed()
                    features["rpd_horse_avg_dividend_when_placed"] = val
                    if val is not None:
                        fill_counts["rpd_horse_avg_dividend_when_placed"] += 1

                    val = ht.upset_maker_rate()
                    features["rpd_horse_upset_maker"] = val
                    if val is not None:
                        fill_counts["rpd_horse_upset_maker"] += 1
                else:
                    features["rpd_horse_avg_dividend_when_placed"] = None
                    features["rpd_horse_upset_maker"] = None

                # --- Jockey ROI ---
                if jockey and jockey in jockey_roi:
                    val = jockey_roi[jockey].roi()
                    features["rpd_jockey_roi_rolling"] = val
                    if val is not None:
                        fill_counts["rpd_jockey_roi_rolling"] += 1
                else:
                    features["rpd_jockey_roi_rolling"] = None

                # --- Trainer ROI ---
                if entraineur and entraineur in trainer_roi:
                    val = trainer_roi[entraineur].roi()
                    features["rpd_trainer_roi_rolling"] = val
                    if val is not None:
                        fill_counts["rpd_trainer_roi_rolling"] += 1
                else:
                    features["rpd_trainer_roi_rolling"] = None

                # --- Course predictability = 1 - hippo_upset_rate ---
                upset_rate = features.get("rpd_hippo_upset_rate")
                if upset_rate is not None:
                    features["rpd_course_predictability"] = round(1.0 - upset_rate, 4)
                    fill_counts["rpd_course_predictability"] += 1
                else:
                    features["rpd_course_predictability"] = None

                # Write to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER snapshotting (temporal integrity) --
            # Race-level updates (once per course)
            if hippo:
                if hippo not in hippo_gagnant:
                    hippo_gagnant[hippo] = _RollingStats(HIPPO_WINDOW)
                if hippo not in hippo_place_spread:
                    hippo_place_spread[hippo] = _RollingStats(HIPPO_WINDOW)
                if hippo not in hippo_concentration:
                    hippo_concentration[hippo] = _RollingStats(HIPPO_WINDOW)

                if race_gagnant is not None:
                    hippo_gagnant[hippo].add(race_gagnant)
                if race_place_spread is not None:
                    hippo_place_spread[hippo].add(race_place_spread)
                if race_concentration is not None:
                    hippo_concentration[hippo].add(race_concentration)

            if discipline:
                if discipline not in disc_gagnant:
                    disc_gagnant[discipline] = _RollingStats(DISCIPLINE_WINDOW)
                if race_gagnant is not None:
                    disc_gagnant[discipline].add(race_gagnant)

            # Per-partant updates
            for rec in course_records:
                cheval = rec.get("nom_cheval") or ""
                jockey = rec.get("nom_jockey") or ""
                entraineur = rec.get("nom_entraineur") or ""
                is_gagnant = bool(rec.get("is_gagnant"))
                dividende = _safe_float(rec.get("rap_dividende_euros")) or 0.0
                is_place = bool(rec.get("est_place"))

                # Horse tracker
                if cheval:
                    if cheval not in horse_tracker:
                        horse_tracker[cheval] = _HorsePayoutTracker()
                    if is_place and dividende > 0:
                        horse_tracker[cheval].add_placed(dividende)
                    if is_gagnant and race_gagnant is not None:
                        horse_tracker[cheval].add_win(race_gagnant)

                # Jockey ROI
                if jockey:
                    if jockey not in jockey_roi:
                        jockey_roi[jockey] = _RollingROI(JOCKEY_WINDOW)
                    jockey_roi[jockey].add(dividende)

                # Trainer ROI
                if entraineur:
                    if entraineur not in trainer_roi:
                        trainer_roi[entraineur] = _RollingROI(TRAINER_WINDOW)
                    trainer_roi[entraineur].add(dividende)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Rapport derived build termine: %d features en %.1fs",
        n_written, elapsed,
    )
    logger.info(
        "Trackers: %d hippodromes, %d disciplines, %d chevaux, %d jockeys, %d entraineurs",
        len(hippo_gagnant), len(disc_gagnant), len(horse_tracker),
        len(jockey_roi), len(trainer_roi),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k in feature_names:
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features derivees des rapports a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/rapport_derived/)",
    )
    args = parser.parse_args()

    logger = setup_logging("rapport_derived_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "rapport_derived_features.jsonl"
    build_rapport_derived_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
