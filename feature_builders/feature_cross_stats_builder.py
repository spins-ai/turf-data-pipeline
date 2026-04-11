#!/usr/bin/env python3
"""
feature_builders.feature_cross_stats_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-statistics features — combining multiple dimensions to create rich
context features from rolling historical data.

Reads partants_master.jsonl in streaming mode, builds a lightweight
index for chronological sorting, then seeks back to read full records
course by course. Output is streamed directly to disk.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the cross-stats — no future leakage.  Snapshot BEFORE update.

Produces:
  - feature_cross_stats_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/feature_cross_stats/

Features per partant (10):
  - fcs_hippo_discipline_winrate        : horse win rate at (hippodrome, discipline)
  - fcs_jockey_discipline_winrate       : jockey win rate for this discipline
  - fcs_trainer_terrain_winrate         : trainer win rate on this terrain type
  - fcs_age_distance_winrate            : win rate for this age at this distance band (rolling)
  - fcs_sex_discipline_winrate          : win rate for this sex in this discipline
  - fcs_breed_distance_winrate          : win rate for this breed at this distance band
  - fcs_cote_range_discipline_winrate   : win rate for this cote range in this discipline
  - fcs_hippo_month_winrate             : seasonal effect at hippodrome (win rate by month)
  - fcs_jockey_age_combo_winrate        : jockey win rate with horses of this age group
  - fcs_trainer_distance_combo_winrate  : trainer win rate at this distance band

Usage:
    python feature_builders/feature_cross_stats_builder.py
    python feature_builders/feature_cross_stats_builder.py --input path/to/partants_master.jsonl
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

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PATH = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/feature_cross_stats")
OUTPUT_FILENAME = "feature_cross_stats_features.jsonl"

# Minimum observations before emitting a rate (else None)
MIN_OBS = 5

# Progress / gc interval
_LOG_EVERY = 500_000

# Feature names for fill-rate tracking
_FEATURE_NAMES = [
    "fcs_hippo_discipline_winrate",
    "fcs_jockey_discipline_winrate",
    "fcs_trainer_terrain_winrate",
    "fcs_age_distance_winrate",
    "fcs_sex_discipline_winrate",
    "fcs_breed_distance_winrate",
    "fcs_cote_range_discipline_winrate",
    "fcs_hippo_month_winrate",
    "fcs_jockey_age_combo_winrate",
    "fcs_trainer_distance_combo_winrate",
]

# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_band(distance) -> Optional[str]:
    """Classify distance into bands: <1400, 1400-1800, 1800-2400, 2400+."""
    if distance is None:
        return None
    try:
        d = int(distance)
    except (ValueError, TypeError):
        return None
    if d <= 0:
        return None
    if d < 1400:
        return "short"
    if d < 1800:
        return "mile"
    if d < 2400:
        return "inter"
    return "long"


def _age_group(age) -> Optional[str]:
    """Classify age into groups: 2, 3, 4-5, 6+."""
    if age is None:
        return None
    try:
        a = int(age)
    except (ValueError, TypeError):
        return None
    if a <= 0:
        return None
    if a <= 3:
        return str(a)
    if a <= 5:
        return "4-5"
    return "6+"


def _cote_range(cote) -> Optional[str]:
    """Classify cote into ranges: <3, 3-6, 6-10, 10-20, 20+."""
    if cote is None:
        return None
    try:
        c = float(cote)
    except (ValueError, TypeError):
        return None
    if c <= 0:
        return None
    if c < 3:
        return "fav"
    if c < 6:
        return "3-6"
    if c < 10:
        return "6-10"
    if c < 20:
        return "10-20"
    return "20+"


def _parse_month(date_str: str) -> Optional[int]:
    """Extract month (1-12) from ISO date string."""
    if not date_str or len(date_str) < 7:
        return None
    try:
        return int(date_str[5:7])
    except (ValueError, TypeError):
        return None


# ===========================================================================
# COUNTER STATE — compact (wins, total) per combination key
# ===========================================================================


class _WinCounter:
    """Ultra-compact win/total counter using __slots__."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def rate(self) -> Optional[float]:
        """Return win rate if total >= MIN_OBS, else None."""
        if self.total < MIN_OBS:
            return None
        return round(self.wins / self.total, 6)


class _CounterStore:
    """Dictionary of _WinCounter instances keyed by arbitrary tuples."""

    __slots__ = ("_data",)

    def __init__(self) -> None:
        self._data: dict[tuple, _WinCounter] = {}

    def get_rate(self, key: tuple) -> Optional[float]:
        """Snapshot current rate for key (None if not enough data)."""
        counter = self._data.get(key)
        if counter is None:
            return None
        return counter.rate()

    def update(self, key: tuple, is_winner: bool) -> None:
        """Increment counters for key after the race."""
        counter = self._data.get(key)
        if counter is None:
            counter = _WinCounter()
            self._data[key] = counter
        counter.total += 1
        if is_winner:
            counter.wins += 1

    def __len__(self) -> int:
        return len(self._data)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_cross_stats_features(input_path: Path, output_path: Path, logger) -> int:
    """Build cross-statistics features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Feature Cross Stats Builder (index + seek) ===")
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

    # -- Phase 2: Sort the lightweight index --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()

    # 10 counter stores, one per feature dimension
    cs_hippo_disc = _CounterStore()       # (horse, hippo, discipline)
    cs_jockey_disc = _CounterStore()      # (jockey, discipline)
    cs_trainer_terrain = _CounterStore()   # (trainer, terrain)
    cs_age_dist = _CounterStore()          # (age_group, distance_band)
    cs_sex_disc = _CounterStore()          # (sex, discipline)
    cs_breed_dist = _CounterStore()        # (breed, distance_band)
    cs_cote_disc = _CounterStore()         # (cote_range, discipline)
    cs_hippo_month = _CounterStore()       # (hippo, month)
    cs_jockey_age = _CounterStore()        # (jockey, age_group)
    cs_trainer_dist = _CounterStore()      # (trainer, distance_band)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {name: 0 for name in _FEATURE_NAMES}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_fields(rec: dict) -> dict:
            """Extract minimal fields needed for cross-stats."""
            cote = rec.get("cote_finale") or rec.get("rapport_final")
            distance = rec.get("distance") or rec.get("distance_course")
            discipline = rec.get("discipline") or rec.get("type_course") or ""
            discipline = discipline.strip().upper() if discipline else ""
            terrain = rec.get("etat_terrain") or rec.get("terrain") or ""
            terrain = terrain.strip().upper() if terrain else ""
            sex = rec.get("sexe") or ""
            sex = sex.strip().upper() if sex else ""
            breed = rec.get("race") or rec.get("race_cheval") or ""
            breed = breed.strip().upper() if breed else ""

            return {
                "partant_uid": rec.get("partant_uid"),
                "course_uid": rec.get("course_uid", ""),
                "date": rec.get("date_reunion_iso", ""),
                "cheval": rec.get("nom_cheval") or "",
                "jockey": rec.get("nom_jockey") or rec.get("jockey") or "",
                "entraineur": rec.get("nom_entraineur") or rec.get("entraineur") or "",
                "hippo": rec.get("hippodrome_normalise") or rec.get("hippodrome") or "",
                "discipline": discipline,
                "terrain": terrain,
                "distance": distance,
                "age": rec.get("age") or rec.get("age_cheval"),
                "sex": sex,
                "breed": breed,
                "cote": cote,
                "gagnant": bool(rec.get("is_gagnant")),
            }

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
            course_group = [
                _extract_fields(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            month = _parse_month(course_date_str)

            # -- Snapshot pre-race stats and emit features (temporal integrity) --
            post_updates: list[dict] = []

            for rec in course_group:
                cheval = rec["cheval"]
                jockey = rec["jockey"]
                entraineur = rec["entraineur"]
                hippo = rec["hippo"]
                discipline = rec["discipline"]
                terrain = rec["terrain"]
                dist_band = _distance_band(rec["distance"])
                age_grp = _age_group(rec["age"])
                sex = rec["sex"]
                breed = rec["breed"]
                cote_rng = _cote_range(rec["cote"])

                features: dict[str, Any] = {
                    "partant_uid": rec["partant_uid"],
                    "course_uid": rec["course_uid"],
                    "date_reunion_iso": rec["date"],
                }

                # 1. fcs_hippo_discipline_winrate: horse at (hippo, discipline)
                val = None
                if cheval and hippo and discipline:
                    val = cs_hippo_disc.get_rate((cheval, hippo, discipline))
                features["fcs_hippo_discipline_winrate"] = val
                if val is not None:
                    fill_counts["fcs_hippo_discipline_winrate"] += 1

                # 2. fcs_jockey_discipline_winrate
                val = None
                if jockey and discipline:
                    val = cs_jockey_disc.get_rate((jockey, discipline))
                features["fcs_jockey_discipline_winrate"] = val
                if val is not None:
                    fill_counts["fcs_jockey_discipline_winrate"] += 1

                # 3. fcs_trainer_terrain_winrate
                val = None
                if entraineur and terrain:
                    val = cs_trainer_terrain.get_rate((entraineur, terrain))
                features["fcs_trainer_terrain_winrate"] = val
                if val is not None:
                    fill_counts["fcs_trainer_terrain_winrate"] += 1

                # 4. fcs_age_distance_winrate
                val = None
                if age_grp and dist_band:
                    val = cs_age_dist.get_rate((age_grp, dist_band))
                features["fcs_age_distance_winrate"] = val
                if val is not None:
                    fill_counts["fcs_age_distance_winrate"] += 1

                # 5. fcs_sex_discipline_winrate
                val = None
                if sex and discipline:
                    val = cs_sex_disc.get_rate((sex, discipline))
                features["fcs_sex_discipline_winrate"] = val
                if val is not None:
                    fill_counts["fcs_sex_discipline_winrate"] += 1

                # 6. fcs_breed_distance_winrate
                val = None
                if breed and dist_band:
                    val = cs_breed_dist.get_rate((breed, dist_band))
                features["fcs_breed_distance_winrate"] = val
                if val is not None:
                    fill_counts["fcs_breed_distance_winrate"] += 1

                # 7. fcs_cote_range_discipline_winrate
                val = None
                if cote_rng and discipline:
                    val = cs_cote_disc.get_rate((cote_rng, discipline))
                features["fcs_cote_range_discipline_winrate"] = val
                if val is not None:
                    fill_counts["fcs_cote_range_discipline_winrate"] += 1

                # 8. fcs_hippo_month_winrate
                val = None
                if hippo and month is not None:
                    val = cs_hippo_month.get_rate((hippo, month))
                features["fcs_hippo_month_winrate"] = val
                if val is not None:
                    fill_counts["fcs_hippo_month_winrate"] += 1

                # 9. fcs_jockey_age_combo_winrate
                val = None
                if jockey and age_grp:
                    val = cs_jockey_age.get_rate((jockey, age_grp))
                features["fcs_jockey_age_combo_winrate"] = val
                if val is not None:
                    fill_counts["fcs_jockey_age_combo_winrate"] += 1

                # 10. fcs_trainer_distance_combo_winrate
                val = None
                if entraineur and dist_band:
                    val = cs_trainer_dist.get_rate((entraineur, dist_band))
                features["fcs_trainer_distance_combo_winrate"] = val
                if val is not None:
                    fill_counts["fcs_trainer_distance_combo_winrate"] += 1

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Stash for deferred update
                post_updates.append({
                    "cheval": cheval,
                    "jockey": jockey,
                    "entraineur": entraineur,
                    "hippo": hippo,
                    "discipline": discipline,
                    "terrain": terrain,
                    "dist_band": dist_band,
                    "age_grp": age_grp,
                    "sex": sex,
                    "breed": breed,
                    "cote_rng": cote_rng,
                    "month": month,
                    "gagnant": rec["gagnant"],
                })

            # -- Update all counters AFTER the race (no leakage) --
            for u in post_updates:
                win = u["gagnant"]

                if u["cheval"] and u["hippo"] and u["discipline"]:
                    cs_hippo_disc.update((u["cheval"], u["hippo"], u["discipline"]), win)

                if u["jockey"] and u["discipline"]:
                    cs_jockey_disc.update((u["jockey"], u["discipline"]), win)

                if u["entraineur"] and u["terrain"]:
                    cs_trainer_terrain.update((u["entraineur"], u["terrain"]), win)

                if u["age_grp"] and u["dist_band"]:
                    cs_age_dist.update((u["age_grp"], u["dist_band"]), win)

                if u["sex"] and u["discipline"]:
                    cs_sex_disc.update((u["sex"], u["discipline"]), win)

                if u["breed"] and u["dist_band"]:
                    cs_breed_dist.update((u["breed"], u["dist_band"]), win)

                if u["cote_rng"] and u["discipline"]:
                    cs_cote_disc.update((u["cote_rng"], u["discipline"]), win)

                if u["hippo"] and u["month"] is not None:
                    cs_hippo_month.update((u["hippo"], u["month"]), win)

                if u["jockey"] and u["age_grp"]:
                    cs_jockey_age.update((u["jockey"], u["age_grp"]), win)

                if u["entraineur"] and u["dist_band"]:
                    cs_trainer_dist.update((u["entraineur"], u["dist_band"]), win)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Cross stats build termine: %d features en %.1fs",
        n_written, elapsed,
    )
    logger.info(
        "Counter sizes: hippo_disc=%d, jockey_disc=%d, trainer_terrain=%d, "
        "age_dist=%d, sex_disc=%d, breed_dist=%d, cote_disc=%d, "
        "hippo_month=%d, jockey_age=%d, trainer_dist=%d",
        len(cs_hippo_disc), len(cs_jockey_disc), len(cs_trainer_terrain),
        len(cs_age_dist), len(cs_sex_disc), len(cs_breed_dist),
        len(cs_cote_disc), len(cs_hippo_month), len(cs_jockey_age),
        len(cs_trainer_dist),
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
    if INPUT_PATH.exists():
        return INPUT_PATH
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PATH}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features cross-stats a partir de partants_master"
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

    logger = setup_logging("feature_cross_stats_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_cross_stats_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
