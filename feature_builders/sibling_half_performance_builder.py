#!/usr/bin/env python3
"""
feature_builders.sibling_half_performance_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tracks performance of half-siblings (same sire or same dam) to predict
horse quality.

Temporal integrity: index + sort + seek. Features are computed BEFORE the
current race is added to sire/dam state (no future leakage).

Features (8):
  - shp_sire_progeny_win_rate   : win rate of all progeny of this horse's sire
                                   (before this date)
  - shp_sire_progeny_count      : number of progeny that have raced (same sire)
  - shp_dam_progeny_win_rate    : win rate of all progeny of this horse's dam
  - shp_dam_progeny_count       : number of dam's progeny that have raced
  - shp_sire_avg_earnings       : average earnings per runner for this sire's
                                   progeny
  - shp_best_sibling_position   : best finishing position ever achieved by any
                                   half-sibling (sire side), excluding self
  - shp_sibling_at_distance_wr  : win rate of sire's progeny at this specific
                                   distance bucket (<1600 / 1600-2400 / >2400)
  - shp_dam_improving           : 1 if dam's recent progeny (last 2 years) have
                                   a better win rate than older ones, 0 if not,
                                   None if insufficient data

Key fields used:
  nom_pere, nom_mere, distance, date_reunion_iso, position_arrivee,
  gains_carriere_euros, partant_uid, course_uid, num_pmu, nom_cheval
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

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/sibling_half_performance"
)
_LOG_EVERY = 500_000

# Distance bucket boundaries (metres)
_DIST_SHORT = 1600   # < 1600 m  → "short"
_DIST_LONG  = 2400   # > 2400 m  → "long"  else "mid"

# Minimum number of progeny races before we report a win rate
_MIN_PROGENY = 3
# Minimum recent progeny runs to declare dam improving
_MIN_RECENT = 2
_MIN_OLD    = 2

# How many years define "recent" for dam_improving
_RECENT_YEARS = 2


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------


def _safe_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f   # reject NaN
    except (ValueError, TypeError):
        return None


def _year_of(date_str: str) -> Optional[int]:
    """Extract 4-digit year from ISO date string like '2019-06-15'."""
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


def _dist_bucket(distance: Optional[int]) -> Optional[str]:
    if distance is None:
        return None
    if distance < _DIST_SHORT:
        return "short"
    elif distance > _DIST_LONG:
        return "long"
    return "mid"


# ---------------------------------------------------------------------------
# PER-SIRE STATE
# ---------------------------------------------------------------------------


class _SireState:
    """Accumulates progeny racing statistics for a single sire."""

    __slots__ = (
        "total",      # int: number of starts by progeny
        "wins",       # int: number of wins by progeny
        "earnings",   # float: sum of gains_carriere_euros for runners at start
        "best_pos",   # int: best finishing position by any progeny (lower = better)
        "dist",       # dict[str, {"w": int, "t": int}] distance-bucket stats
        "horses",     # set[str]: horse names that have contributed
    )

    def __init__(self) -> None:
        self.total: int = 0
        self.wins: int = 0
        self.earnings: float = 0.0
        self.best_pos: Optional[int] = None
        self.dist: dict[str, dict] = {
            "short": {"w": 0, "t": 0},
            "mid":   {"w": 0, "t": 0},
            "long":  {"w": 0, "t": 0},
        }
        self.horses: set[str] = set()

    def snapshot(
        self,
        horse_name: str,
        distance: Optional[int],
    ) -> dict:
        """
        Return feature dict using state BEFORE this race is included.
        horse_name is excluded from best_pos to avoid self-inclusion.
        """
        out: dict = {}

        if self.total >= _MIN_PROGENY:
            out["shp_sire_progeny_win_rate"] = round(self.wins / self.total, 4)
            out["shp_sire_avg_earnings"] = (
                round(self.earnings / self.total, 2) if self.total > 0 else None
            )
        else:
            out["shp_sire_progeny_win_rate"] = None
            out["shp_sire_avg_earnings"] = None

        out["shp_sire_progeny_count"] = self.total if self.total > 0 else None

        out["shp_best_sibling_position"] = self.best_pos  # None if nothing yet

        # Distance-bucket win rate
        bucket = _dist_bucket(distance)
        if bucket and self.dist[bucket]["t"] >= _MIN_PROGENY:
            b = self.dist[bucket]
            out["shp_sibling_at_distance_wr"] = round(b["w"] / b["t"], 4)
        else:
            out["shp_sibling_at_distance_wr"] = None

        return out

    def update(
        self,
        horse_name: str,
        is_winner: bool,
        position: Optional[int],
        earnings: Optional[float],
        distance: Optional[int],
    ) -> None:
        """Update state AFTER features have been emitted."""
        self.total += 1
        if is_winner:
            self.wins += 1
        if earnings is not None:
            self.earnings += earnings
        if position is not None and position > 0:
            if self.best_pos is None or position < self.best_pos:
                self.best_pos = position
        self.horses.add(horse_name)
        bucket = _dist_bucket(distance)
        if bucket:
            self.dist[bucket]["t"] += 1
            if is_winner:
                self.dist[bucket]["w"] += 1


# ---------------------------------------------------------------------------
# PER-DAM STATE
# ---------------------------------------------------------------------------


class _DamState:
    """Accumulates progeny racing statistics for a single dam."""

    __slots__ = (
        "total",         # int: total starts
        "wins",          # int: total wins
        "yearly",        # dict[int, {"w": int, "t": int}]: stats by year of race
        "horses",        # set[str]
    )

    def __init__(self) -> None:
        self.total: int = 0
        self.wins: int = 0
        self.yearly: dict[int, dict] = defaultdict(lambda: {"w": 0, "t": 0})
        self.horses: set[str] = set()

    def snapshot(self, current_date_str: str) -> dict:
        out: dict = {}

        if self.total >= _MIN_PROGENY:
            out["shp_dam_progeny_win_rate"] = round(self.wins / self.total, 4)
        else:
            out["shp_dam_progeny_win_rate"] = None

        out["shp_dam_progeny_count"] = self.total if self.total > 0 else None

        # dam improving: compare recent 2 years vs older
        current_year = _year_of(current_date_str)
        if current_year is not None:
            recent_w, recent_t = 0, 0
            old_w, old_t = 0, 0
            cutoff = current_year - _RECENT_YEARS
            for yr, yd in self.yearly.items():
                if yr >= cutoff:
                    recent_w += yd["w"]
                    recent_t += yd["t"]
                else:
                    old_w += yd["w"]
                    old_t += yd["t"]
            if recent_t >= _MIN_RECENT and old_t >= _MIN_OLD:
                recent_wr = recent_w / recent_t
                old_wr = old_w / old_t
                out["shp_dam_improving"] = 1 if recent_wr > old_wr else 0
            else:
                out["shp_dam_improving"] = None
        else:
            out["shp_dam_improving"] = None

        return out

    def update(
        self,
        horse_name: str,
        is_winner: bool,
        race_year: Optional[int],
    ) -> None:
        self.total += 1
        if is_winner:
            self.wins += 1
        if race_year is not None:
            self.yearly[race_year]["t"] += 1
            if is_winner:
                self.yearly[race_year]["w"] += 1
        self.horses.add(horse_name)


# ---------------------------------------------------------------------------
# BUILD
# ---------------------------------------------------------------------------

_FEATURE_KEYS = [
    "shp_sire_progeny_win_rate",
    "shp_sire_progeny_count",
    "shp_dam_progeny_win_rate",
    "shp_dam_progeny_count",
    "shp_sire_avg_earnings",
    "shp_best_sibling_position",
    "shp_sibling_at_distance_wr",
    "shp_dam_improving",
]


def build(input_path: Path, logger) -> None:
    t0 = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Phase 1: Build index (offset + sort keys) without loading all data
    # ------------------------------------------------------------------
    logger.info("Phase 1: Construction de l'index temporel...")
    index: list[tuple[str, str, int, int]] = []   # (date, course_uid, num_pmu, offset)
    n_idx = 0
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
            n_idx += 1
            if n_idx % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_idx)

            date_str  = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = 0
            try:
                num_pmu = int(rec.get("num_pmu", 0) or 0)
            except (ValueError, TypeError):
                pass
            index.append((date_str, course_uid, num_pmu, offset))

    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("  Index: %d records tries", len(index))

    # ------------------------------------------------------------------
    # Phase 2: Process chronologically — snapshot then update
    # ------------------------------------------------------------------
    logger.info("Phase 2: Calcul features (snapshot avant update)...")

    sire_states: dict[str, _SireState] = defaultdict(_SireState)
    dam_states:  dict[str, _DamState]  = defaultdict(_DamState)

    output_path = OUTPUT_DIR / "sibling_half_performance.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    fill = {k: 0 for k in _FEATURE_KEYS}
    n_written = 0
    total = len(index)

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        i = 0
        while i < total:
            # Gather all partants in the same course (same date + course_uid)
            cur_date   = index[i][0]
            cur_course = index[i][1]
            group_end  = i

            while (
                group_end < total
                and index[group_end][0] == cur_date
                and index[group_end][1] == cur_course
            ):
                group_end += 1

            group = index[i:group_end]
            i = group_end

            # --- Load records for this group ---
            records_in_group: list[dict] = []
            for _, _, _, offset in group:
                fin.seek(offset)
                try:
                    rec = json.loads(fin.readline())
                except json.JSONDecodeError:
                    continue
                records_in_group.append(rec)

            # --- Snapshot BEFORE update ---
            for rec in records_in_group:
                partant_uid = rec.get("partant_uid", "")
                course_uid  = rec.get("course_uid", "")

                nom   = (rec.get("nom_cheval") or "").upper().strip()
                sire  = (rec.get("nom_pere")   or "").upper().strip()
                dam   = (rec.get("nom_mere")   or "").upper().strip()
                dist  = _safe_int(rec.get("distance"))

                out: dict = {
                    "partant_uid":     partant_uid,
                    "course_uid":      course_uid,
                    "date_reunion_iso": cur_date,
                }

                # --- Sire features ---
                if sire:
                    ss = sire_states[sire]
                    sire_snap = ss.snapshot(nom, dist)
                else:
                    sire_snap = {
                        "shp_sire_progeny_win_rate":  None,
                        "shp_sire_progeny_count":     None,
                        "shp_sire_avg_earnings":      None,
                        "shp_best_sibling_position":  None,
                        "shp_sibling_at_distance_wr": None,
                    }
                out.update(sire_snap)

                # --- Dam features ---
                if dam:
                    ds = dam_states[dam]
                    dam_snap = ds.snapshot(cur_date)
                else:
                    dam_snap = {
                        "shp_dam_progeny_win_rate": None,
                        "shp_dam_progeny_count":    None,
                        "shp_dam_improving":        None,
                    }
                out.update(dam_snap)

                # Fill-rate accounting
                for k in _FEATURE_KEYS:
                    if out.get(k) is not None:
                        fill[k] += 1

                fout.write(json.dumps(out, ensure_ascii=False) + "\n")
                n_written += 1

            # --- Update AFTER snapshot ---
            for rec in records_in_group:
                nom  = (rec.get("nom_cheval") or "").upper().strip()
                sire = (rec.get("nom_pere")   or "").upper().strip()
                dam  = (rec.get("nom_mere")   or "").upper().strip()
                dist = _safe_int(rec.get("distance"))

                pos_raw = rec.get("position_arrivee")
                position = _safe_int(pos_raw)
                is_winner = position == 1

                earnings = _safe_float(rec.get("gains_carriere_euros"))
                race_year = _year_of(cur_date)

                if sire and nom:
                    sire_states[sire].update(nom, is_winner, position, earnings, dist)

                if dam and nom:
                    dam_states[dam].update(nom, is_winner, race_year)

            if n_written % _LOG_EVERY < len(records_in_group):
                pct = n_written / total * 100
                logger.info("  Phase 2: %d/%d (%.1f%%)", n_written, total, pct)
                gc.collect()

    # Atomic rename
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Termine: %d features ecrites en %.1fs",
        n_written, elapsed,
    )
    logger.info(
        "Peres uniques: %d  |  Meres uniques: %d",
        len(sire_states), len(dam_states),
    )
    logger.info("Fill rates:")
    for k in _FEATURE_KEYS:
        pct = fill[k] / n_written * 100 if n_written > 0 else 0.0
        logger.info("  %-40s %.1f%%", k, pct)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sibling half-performance feature builder"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: %(default)s)",
    )
    args = parser.parse_args()

    logger = setup_logging("sibling_half_performance_builder")

    input_path = Path(args.input) if args.input else INPUT_PARTANTS
    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", OUTPUT_DIR)

    build(input_path, logger)


if __name__ == "__main__":
    main()
