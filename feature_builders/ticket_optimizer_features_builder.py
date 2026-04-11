#!/usr/bin/env python3
"""
feature_builders.ticket_optimizer_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Features for the ticket optimizer and combined tickets modules --
predicting which bet types are most profitable in given contexts.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically with index + seek, and computes per-hippodrome
historical returns by bet type.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the per-hippodrome stats -- no future leakage.

Produces:
  - ticket_optimizer_features.jsonl

Features per partant (10):
  - tkt_simple_gagnant_avg_return : rolling avg return on simple_gagnant at this hippo (last 100 races)
  - tkt_simple_place_avg_return   : rolling avg return on simple_place at this hippo
  - tkt_couple_avg_return         : rolling avg return on couple_gagnant at this hippo
  - tkt_tierce_avg_return         : rolling avg return on tierce at this hippo
  - tkt_best_bet_type             : which bet type has highest avg return (0=gagnant, 1=place, 2=couple, 3=tierce)
  - tkt_favorite_simple_roi       : rolling ROI betting favorites (cote < 5) to win at this hippo
  - tkt_outsider_place_roi        : rolling ROI betting outsiders (cote > 10) to place at this hippo
  - tkt_multi_profitability       : rolling avg multi payout / nb_partants (normalised)
  - tkt_optimal_stake_fraction    : Kelly-based: (hippo_win_rate * avg_return - 1) / (avg_return - 1)
  - tkt_nb_profitable_bet_types   : count of bet types with positive rolling ROI at this hippo

Per-hippodrome state:
  - deque(maxlen=100) of race-level payout snapshots
  - Uses rap_ fields: rap_rapport_simple_gagnant/100 as return (euros per euro bet)

Usage:
    python feature_builders/ticket_optimizer_features_builder.py
    python feature_builders/ticket_optimizer_features_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/ticket_optimizer_features")

_LOG_EVERY = 500_000
_ROLLING_WINDOW = 100


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(v) -> Optional[float]:
    """Convert to float, return None on failure."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if f >= 0 else None
    except (ValueError, TypeError):
        return None


def _centimes_to_return(v) -> Optional[float]:
    """Convert centimes rapport to return per euro bet.

    PMU rapports are in centimes (e.g., 350 = 3.50 EUR for 1 EUR bet).
    Return = rapport / 100.
    """
    f = _safe_float(v)
    if f is None or f <= 0:
        return None
    return f / 100.0


# ===========================================================================
# PER-HIPPODROME STATE TRACKER
# ===========================================================================


class _HippoRaceRecord:
    """Minimal per-race payout record stored in the deque."""

    __slots__ = (
        "simple_gagnant_return", "simple_place_return",
        "couple_return", "tierce_return",
        "cote_winner", "is_favorite_win", "is_outsider_place",
        "multi_return", "nb_partants",
    )

    def __init__(
        self,
        simple_gagnant_return: Optional[float],
        simple_place_return: Optional[float],
        couple_return: Optional[float],
        tierce_return: Optional[float],
        cote_winner: Optional[float],
        is_favorite_win: Optional[bool],
        is_outsider_place: Optional[bool],
        multi_return: Optional[float],
        nb_partants: int,
    ):
        self.simple_gagnant_return = simple_gagnant_return
        self.simple_place_return = simple_place_return
        self.couple_return = couple_return
        self.tierce_return = tierce_return
        self.cote_winner = cote_winner
        self.is_favorite_win = is_favorite_win
        self.is_outsider_place = is_outsider_place
        self.multi_return = multi_return
        self.nb_partants = nb_partants


class _HippoState:
    """Rolling state per hippodrome: deque of last N race records."""

    __slots__ = ("history", "total_races", "total_wins")

    def __init__(self) -> None:
        self.history: deque[_HippoRaceRecord] = deque(maxlen=_ROLLING_WINDOW)
        self.total_races: int = 0
        self.total_wins: int = 0

    def _avg_return(self, attr: str) -> Optional[float]:
        """Average of a return field across history, ignoring None."""
        vals = [getattr(r, attr) for r in self.history if getattr(r, attr) is not None]
        if not vals:
            return None
        return round(sum(vals) / len(vals), 4)

    def avg_simple_gagnant(self) -> Optional[float]:
        return self._avg_return("simple_gagnant_return")

    def avg_simple_place(self) -> Optional[float]:
        return self._avg_return("simple_place_return")

    def avg_couple(self) -> Optional[float]:
        return self._avg_return("couple_return")

    def avg_tierce(self) -> Optional[float]:
        return self._avg_return("tierce_return")

    def best_bet_type(self) -> Optional[int]:
        """0=gagnant, 1=place, 2=couple, 3=tierce. None if no data."""
        avgs = [
            self.avg_simple_gagnant(),
            self.avg_simple_place(),
            self.avg_couple(),
            self.avg_tierce(),
        ]
        valid = [(i, v) for i, v in enumerate(avgs) if v is not None]
        if not valid:
            return None
        return max(valid, key=lambda x: x[1])[0]

    def favorite_simple_roi(self) -> Optional[float]:
        """ROI when betting favorites (cote < 5) to win.

        ROI = (sum of returns - nb bets) / nb bets.
        For favorites that won: return = simple_gagnant_return.
        For favorites that lost: return = 0.
        """
        bets = 0
        total_return = 0.0
        for r in self.history:
            if r.cote_winner is not None and r.cote_winner < 5.0:
                bets += 1
                if r.is_favorite_win:
                    ret = r.simple_gagnant_return
                    if ret is not None:
                        total_return += ret
        if bets == 0:
            return None
        return round((total_return - bets) / bets, 4)

    def outsider_place_roi(self) -> Optional[float]:
        """ROI when betting outsiders (cote > 10) to place.

        For outsiders that placed: return = simple_place_return.
        For outsiders that did not place: return = 0.
        """
        bets = 0
        total_return = 0.0
        for r in self.history:
            if r.cote_winner is not None and r.cote_winner > 10.0:
                bets += 1
                if r.is_outsider_place:
                    ret = r.simple_place_return
                    if ret is not None:
                        total_return += ret
        if bets == 0:
            return None
        return round((total_return - bets) / bets, 4)

    def multi_profitability(self) -> Optional[float]:
        """Avg multi return normalised by nb_partants."""
        vals = []
        for r in self.history:
            if r.multi_return is not None and r.nb_partants > 0:
                vals.append(r.multi_return / r.nb_partants)
        if not vals:
            return None
        return round(sum(vals) / len(vals), 4)

    def win_rate(self) -> Optional[float]:
        if self.total_races == 0:
            return None
        return self.total_wins / self.total_races

    def optimal_stake_fraction(self) -> Optional[float]:
        """Kelly-based: (win_rate * avg_return - 1) / (avg_return - 1)."""
        wr = self.win_rate()
        avg_ret = self.avg_simple_gagnant()
        if wr is None or avg_ret is None or avg_ret <= 1.0:
            return None
        numerator = wr * avg_ret - 1.0
        denominator = avg_ret - 1.0
        if denominator <= 0:
            return None
        kelly = numerator / denominator
        return round(max(0.0, min(kelly, 0.25)), 6)

    def nb_profitable_bet_types(self) -> Optional[int]:
        """Count of bet types with positive rolling ROI (return > 1.0 means profitable)."""
        avgs = [
            self.avg_simple_gagnant(),
            self.avg_simple_place(),
            self.avg_couple(),
            self.avg_tierce(),
        ]
        valid = [v for v in avgs if v is not None]
        if not valid:
            return None
        return sum(1 for v in valid if v > 1.0)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_ticket_optimizer_features(input_path: Path, output_path: Path, logger) -> int:
    """Build ticket optimizer features from partants_master.jsonl.

    Index + chronological sort + seek approach for memory efficiency.
    Returns the total number of feature records written.
    """
    logger.info("=== Ticket Optimizer Features Builder ===")
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
                gc.collect()

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
    hippo_state: dict[str, _HippoState] = defaultdict(_HippoState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_names = [
        "tkt_simple_gagnant_avg_return",
        "tkt_simple_place_avg_return",
        "tkt_couple_avg_return",
        "tkt_tierce_avg_return",
        "tkt_best_bet_type",
        "tkt_favorite_simple_roi",
        "tkt_outsider_place_roi",
        "tkt_multi_profitability",
        "tkt_optimal_stake_fraction",
        "tkt_nb_profitable_bet_types",
    ]
    fill_counts = {name: 0 for name in feature_names}

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

            # Extract race-level payout info (same for all partants in a course)
            first = course_records[0]
            hippo = (first.get("hippodrome_normalise") or "").strip()
            nb_partants = 0
            try:
                nb_partants = int(first.get("nombre_partants") or 0)
            except (ValueError, TypeError):
                nb_partants = 0

            # Race-level rapport fields (centimes -> euros return per euro bet)
            simple_gagnant_ret = _centimes_to_return(first.get("rap_rapport_simple_gagnant"))

            # Simple place: average of the 3 place rapports
            sp1 = _centimes_to_return(first.get("rap_rapport_simple_place_1"))
            sp2 = _centimes_to_return(first.get("rap_rapport_simple_place_2"))
            sp3 = _centimes_to_return(first.get("rap_rapport_simple_place_3"))
            place_vals = [v for v in (sp1, sp2, sp3) if v is not None]
            simple_place_ret = round(sum(place_vals) / len(place_vals), 4) if place_vals else None

            couple_ret = _centimes_to_return(first.get("rap_rapport_couple_gagnant"))
            tierce_ret = _centimes_to_return(first.get("rap_rapport_tierce_ordre"))

            # Multi return: use multi_4 as the most common multi payout
            multi_ret = _centimes_to_return(first.get("rap_rapport_multi_4"))

            # Find winner cote and whether favorite won / outsider placed
            winner_cote: Optional[float] = None
            is_favorite_win: Optional[bool] = None
            is_outsider_place: Optional[bool] = None

            for rec in course_records:
                cote = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
                is_gagnant = bool(rec.get("is_gagnant"))
                is_place = bool(rec.get("is_place"))

                if is_gagnant and cote is not None:
                    winner_cote = cote
                    is_favorite_win = (cote < 5.0)

                if cote is not None and cote > 10.0 and is_place:
                    is_outsider_place = True

            if is_outsider_place is None:
                # Check if any outsider ran but none placed
                has_outsider = any(
                    _safe_float(r.get("cote_finale") or r.get("rapport_final")) is not None
                    and _safe_float(r.get("cote_finale") or r.get("rapport_final")) > 10.0
                    for r in course_records
                )
                if has_outsider:
                    is_outsider_place = False

            # -- Snapshot BEFORE update: emit features for all partants --
            hs = hippo_state[hippo] if hippo else None

            for rec in course_records:
                features: dict[str, Any] = {
                    "partant_uid": rec.get("partant_uid"),
                    "course_uid": rec.get("course_uid"),
                    "date_reunion_iso": rec.get("date_reunion_iso"),
                }

                if hs is not None and len(hs.history) > 0:
                    v = hs.avg_simple_gagnant()
                    features["tkt_simple_gagnant_avg_return"] = v
                    if v is not None:
                        fill_counts["tkt_simple_gagnant_avg_return"] += 1

                    v = hs.avg_simple_place()
                    features["tkt_simple_place_avg_return"] = v
                    if v is not None:
                        fill_counts["tkt_simple_place_avg_return"] += 1

                    v = hs.avg_couple()
                    features["tkt_couple_avg_return"] = v
                    if v is not None:
                        fill_counts["tkt_couple_avg_return"] += 1

                    v = hs.avg_tierce()
                    features["tkt_tierce_avg_return"] = v
                    if v is not None:
                        fill_counts["tkt_tierce_avg_return"] += 1

                    v = hs.best_bet_type()
                    features["tkt_best_bet_type"] = v
                    if v is not None:
                        fill_counts["tkt_best_bet_type"] += 1

                    v = hs.favorite_simple_roi()
                    features["tkt_favorite_simple_roi"] = v
                    if v is not None:
                        fill_counts["tkt_favorite_simple_roi"] += 1

                    v = hs.outsider_place_roi()
                    features["tkt_outsider_place_roi"] = v
                    if v is not None:
                        fill_counts["tkt_outsider_place_roi"] += 1

                    v = hs.multi_profitability()
                    features["tkt_multi_profitability"] = v
                    if v is not None:
                        fill_counts["tkt_multi_profitability"] += 1

                    v = hs.optimal_stake_fraction()
                    features["tkt_optimal_stake_fraction"] = v
                    if v is not None:
                        fill_counts["tkt_optimal_stake_fraction"] += 1

                    v = hs.nb_profitable_bet_types()
                    features["tkt_nb_profitable_bet_types"] = v
                    if v is not None:
                        fill_counts["tkt_nb_profitable_bet_types"] += 1
                else:
                    for name in feature_names:
                        features[name] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update state AFTER snapshot (temporal integrity) --
            if hippo:
                race_rec = _HippoRaceRecord(
                    simple_gagnant_return=simple_gagnant_ret,
                    simple_place_return=simple_place_ret,
                    couple_return=couple_ret,
                    tierce_return=tierce_ret,
                    cote_winner=winner_cote,
                    is_favorite_win=is_favorite_win if is_favorite_win is not None else False,
                    is_outsider_place=is_outsider_place if is_outsider_place is not None else False,
                    multi_return=multi_ret,
                    nb_partants=nb_partants,
                )
                hippo_state[hippo].history.append(race_rec)
                hippo_state[hippo].total_races += 1
                # Count win for hippo win rate (used in Kelly)
                if any(bool(r.get("is_gagnant")) for r in course_records):
                    hippo_state[hippo].total_wins += 1

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Ticket optimizer build termine: %d features en %.1fs (hippodromes: %d)",
        n_written, elapsed, len(hippo_state),
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
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features ticket optimizer a partir de partants_master"
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

    logger = setup_logging("ticket_optimizer_features_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "ticket_optimizer_features.jsonl"
    build_ticket_optimizer_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
