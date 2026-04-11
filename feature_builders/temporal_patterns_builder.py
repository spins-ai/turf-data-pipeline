#!/usr/bin/env python3
"""
feature_builders.temporal_patterns_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Temporal pattern features exploiting date/time/calendar signals.

Reads partants_master.jsonl in streaming mode, processes chronologically,
computes per-partant temporal features using only past data (no leakage).

Features (8):
  - temp_day_of_week          : jour de la semaine (0=lundi..6=dimanche)
  - temp_is_weekend           : 1 si samedi/dimanche
  - temp_hour_bucket          : tranche horaire (0=matin, 1=aprem, 2=soir)
  - temp_month_sin/cos        : encodage cyclique du mois
  - temp_days_since_last_race : jours depuis derniere course du cheval
  - temp_races_last_30d       : nb courses du cheval dans les 30 derniers jours
  - temp_season_form_delta    : delta win_rate saison actuelle vs saison precedente

Memory: ~4 GB max (horse history = dict of last race dates + season stats)
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/temporal_patterns")
_LOG_EVERY = 500_000

_MONTH_TO_SEASON = {
    1: 4, 2: 4, 3: 1, 4: 1, 5: 1,
    6: 2, 7: 2, 8: 2,
    9: 3, 10: 3, 11: 3,
    12: 4,
}


def _parse_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _parse_hour(s: str) -> Optional[int]:
    """Extract hour from heure_depart like '13h50' or '13:50'."""
    if not s:
        return None
    try:
        s = str(s).replace("h", ":").replace("H", ":")
        return int(s.split(":")[0])
    except (ValueError, TypeError, IndexError):
        return None


class _HorseTemporalState:
    __slots__ = ("last_race_date", "race_dates_30d", "season_wins", "season_total")

    def __init__(self):
        self.last_race_date: Optional[datetime] = None
        self.race_dates_30d: list[datetime] = []
        self.season_wins = [0, 0, 0, 0]
        self.season_total = [0, 0, 0, 0]

    def win_rate(self, season_idx: int) -> Optional[float]:
        t = self.season_total[season_idx]
        if t == 0:
            return None
        return self.season_wins[season_idx] / t

    def prev_season_idx(self, current: int) -> int:
        return (current - 1) % 4


def build(input_path: Path, output_dir: Path, logger) -> None:
    t0 = time.time()
    logger.info("Phase 1: Indexation de %s", input_path)

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

    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    t2 = time.time()
    horse_state: dict[str, _HorseTemporalState] = defaultdict(_HorseTemporalState)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "temporal_patterns.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill = {k: 0 for k in [
        "temp_day_of_week", "temp_is_weekend", "temp_hour_bucket",
        "temp_month_sin", "temp_month_cos",
        "temp_days_since_last_race", "temp_races_last_30d",
        "temp_season_form_delta",
    ]}

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
                logger.info("  Phase 3: %d/%d (%.1f%%) en %.0fs", n_processed, total, pct, elapsed)
                gc.collect()

            dt = _parse_date(date_str)
            if dt is None:
                continue

            partant_uid = rec.get("partant_uid", "")
            horse_id = rec.get("horse_id") or rec.get("nom_cheval", "")
            if not horse_id:
                continue

            heure = rec.get("heure_depart", "")
            hour = _parse_hour(heure)

            # --- Compute features (snapshot BEFORE update) ---
            out = {
                "partant_uid": partant_uid,
                "course_uid": course_uid,
                "date_reunion_iso": date_str,
            }

            # Day of week
            dow = dt.weekday()
            out["temp_day_of_week"] = dow
            fill["temp_day_of_week"] += 1

            out["temp_is_weekend"] = 1 if dow >= 5 else 0
            fill["temp_is_weekend"] += 1

            # Hour bucket
            if hour is not None:
                if hour < 12:
                    out["temp_hour_bucket"] = 0
                elif hour < 17:
                    out["temp_hour_bucket"] = 1
                else:
                    out["temp_hour_bucket"] = 2
                fill["temp_hour_bucket"] += 1
            else:
                out["temp_hour_bucket"] = None

            # Cyclic month encoding
            month = dt.month
            out["temp_month_sin"] = round(math.sin(2 * math.pi * month / 12), 4)
            out["temp_month_cos"] = round(math.cos(2 * math.pi * month / 12), 4)
            fill["temp_month_sin"] += 1
            fill["temp_month_cos"] += 1

            # Horse-specific temporal features
            hs = horse_state[horse_id]

            # Days since last race
            if hs.last_race_date is not None:
                delta = (dt - hs.last_race_date).days
                out["temp_days_since_last_race"] = delta
                fill["temp_days_since_last_race"] += 1
            else:
                out["temp_days_since_last_race"] = None

            # Races in last 30 days
            cutoff = dt - timedelta(days=30)
            hs.race_dates_30d = [d for d in hs.race_dates_30d if d >= cutoff]
            out["temp_races_last_30d"] = len(hs.race_dates_30d)
            fill["temp_races_last_30d"] += 1

            # Season form delta
            season_idx = _MONTH_TO_SEASON[month] - 1
            prev_idx = hs.prev_season_idx(season_idx)
            wr_current = hs.win_rate(season_idx)
            wr_prev = hs.win_rate(prev_idx)
            if wr_current is not None and wr_prev is not None:
                out["temp_season_form_delta"] = round(wr_current - wr_prev, 4)
                fill["temp_season_form_delta"] += 1
            else:
                out["temp_season_form_delta"] = None

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_written += 1

            # --- UPDATE state (after snapshot) ---
            hs.last_race_date = dt
            hs.race_dates_30d.append(dt)

            is_winner = False
            pos = rec.get("position_arrivee")
            try:
                is_winner = int(pos) == 1
            except (ValueError, TypeError):
                pass
            if is_winner:
                hs.season_wins[season_idx] += 1
            hs.season_total[season_idx] += 1

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
        logger.info("  %s: %.1f%%", k, pct)


def main():
    parser = argparse.ArgumentParser(description="Temporal patterns feature builder")
    parser.add_argument("--input", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("temporal_patterns_builder")

    if args.input:
        input_path = Path(args.input)
    else:
        input_path = None
        for p in INPUT_CANDIDATES:
            if p.exists():
                input_path = p
                break
        if input_path is None:
            logger.error("Aucun fichier partants_master trouve")
            sys.exit(1)

    logger.info("Input: %s", input_path)
    build(input_path, OUTPUT_DIR, logger)


if __name__ == "__main__":
    main()
