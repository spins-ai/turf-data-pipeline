#!/usr/bin/env python3
"""
feature_builders.bayesian_shrinkage_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Bayesian shrinkage win-rate estimates for jockeys, trainers, combos,
horses, and sires across multiple dimensions.

Raw win rates are noisy for small samples.  Bayesian shrinkage pulls
estimates toward the global mean, giving more stable predictions:

    shrunk_rate = (n * raw_rate + k * global_rate) / (n + k)

where k is the shrinkage factor and n is the number of observations.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant shrinkage features.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.  Snapshot BEFORE update.

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 re-reads records from disk via seek, streams output to .tmp
  - gc.collect() every 500K records

Produces:
  - bayesian_shrinkage.jsonl   in builder_outputs/bayesian_shrinkage/

Features per partant (12):
  - bshr_jockey_wr              : jockey shrunk win rate (overall)
  - bshr_jockey_hippo_wr        : jockey shrunk win rate at this hippodrome
  - bshr_jockey_disc_wr         : jockey shrunk win rate in this discipline
  - bshr_trainer_wr             : trainer shrunk win rate (overall)
  - bshr_trainer_hippo_wr       : trainer shrunk win rate at this hippodrome
  - bshr_trainer_disc_wr        : trainer shrunk win rate in this discipline
  - bshr_jock_train_combo_wr    : jockey-trainer combo shrunk win rate
  - bshr_horse_hippo_wr         : horse at this hippodrome shrunk win rate
  - bshr_horse_disc_wr          : horse in this discipline shrunk win rate
  - bshr_horse_dist_wr          : horse at this distance bucket shrunk win rate
  - bshr_sire_dist_wr           : sire progeny shrunk win rate at this distance
  - bshr_sire_surface_wr        : sire progeny shrunk win rate on this surface

Shrinkage factors:
  - k=20 for jockey, trainer (many observations)
  - k=10 for combo, horse-specific (fewer observations)
  - k=30 for sire (high variance, needs more shrinkage)

Usage:
    python feature_builders/bayesian_shrinkage_builder.py
    python feature_builders/bayesian_shrinkage_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/bayesian_shrinkage")

_LOG_EVERY = 500_000

# Shrinkage factors
K_JOCKEY = 20
K_TRAINER = 20
K_COMBO = 10
K_HORSE = 10
K_SIRE = 30

# Distance buckets (metres)
_DIST_BUCKETS = [
    (0, 1300, "sprint"),
    (1300, 1600, "mile"),
    (1600, 2000, "inter"),
    (2000, 2500, "moyen"),
    (2500, 3200, "long"),
    (3200, 99999, "marathon"),
]


# ===========================================================================
# HELPERS
# ===========================================================================


def _norm_name(name: Optional[str]) -> Optional[str]:
    """Normalise a jockey/trainer/horse name."""
    if not name or not isinstance(name, str):
        return None
    return name.strip().upper()


def _norm_hippo(name: Optional[str]) -> Optional[str]:
    """Normalise a hippodrome name."""
    if not name or not isinstance(name, str):
        return None
    return name.strip().lower()


def _norm_disc(disc: Optional[str]) -> Optional[str]:
    """Normalise discipline."""
    if not disc or not isinstance(disc, str):
        return None
    return disc.strip().lower()


def _norm_surface(surf: Optional[str]) -> Optional[str]:
    """Normalise surface (piste)."""
    if not surf or not isinstance(surf, str):
        return None
    return surf.strip().lower()


def _distance_bucket(dist_val: Any) -> Optional[str]:
    """Map distance (metres) to a bucket label."""
    try:
        d = float(dist_val)
    except (TypeError, ValueError):
        return None
    for lo, hi, label in _DIST_BUCKETS:
        if lo <= d < hi:
            return label
    return None


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
# GLOBAL WIN-RATE TRACKER
# ===========================================================================


class _GlobalRate:
    """Running global win rate across all starters."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def rate(self) -> float:
        if self.total == 0:
            return 0.08  # prior fallback ~1/12
        return self.wins / self.total

    def update(self, is_gagnant: bool) -> None:
        self.total += 1
        if is_gagnant:
            self.wins += 1


# ===========================================================================
# ENTITY WIN/TOTAL COUNTERS
# ===========================================================================


class _Counter:
    """Minimal (wins, total) counter."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def update(self, is_gagnant: bool) -> None:
        self.total += 1
        if is_gagnant:
            self.wins += 1


def _shrunk_rate(counter: Optional[_Counter], global_rate: float, k: int) -> Optional[float]:
    """Compute shrinkage estimate. Returns None if counter is None or has 0 obs."""
    if counter is None or counter.total == 0:
        return None
    raw = counter.wins / counter.total
    return round((counter.total * raw + k * global_rate) / (counter.total + k), 6)


# ===========================================================================
# STATE CONTAINERS
# ===========================================================================


class _EntityState:
    """Per-entity counters: overall + per-dimension."""

    __slots__ = ("overall", "by_dim")

    def __init__(self) -> None:
        self.overall = _Counter()
        self.by_dim: dict[str, _Counter] = defaultdict(_Counter)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_bayesian_shrinkage(input_path: Path, output_path: Path, logger) -> int:
    """Build Bayesian shrinkage features from partants_master.jsonl.

    Two-phase approach:
      1. Index: read sort keys + byte offsets (lightweight).
      2. Sort chronologically, then seek-read records course by course,
         streaming output to .tmp, then atomic rename.

    Returns the total number of feature records written.
    """
    logger.info("=== Bayesian Shrinkage Builder (memory-optimised) ===")
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

    global_rate = _GlobalRate()

    # Entity states: key -> _EntityState
    jockey_state: dict[str, _EntityState] = defaultdict(_EntityState)
    trainer_state: dict[str, _EntityState] = defaultdict(_EntityState)
    combo_state: dict[str, _Counter] = defaultdict(_Counter)
    horse_state: dict[str, _EntityState] = defaultdict(_EntityState)
    sire_state: dict[str, _EntityState] = defaultdict(_EntityState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    _FEATURE_KEYS = [
        "bshr_jockey_wr",
        "bshr_jockey_hippo_wr",
        "bshr_jockey_disc_wr",
        "bshr_trainer_wr",
        "bshr_trainer_hippo_wr",
        "bshr_trainer_disc_wr",
        "bshr_jock_train_combo_wr",
        "bshr_horse_hippo_wr",
        "bshr_horse_disc_wr",
        "bshr_horse_dist_wr",
        "bshr_sire_dist_wr",
        "bshr_sire_surface_wr",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(byte_offset: int) -> dict:
            fin.seek(byte_offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            return {
                "uid": rec.get("partant_uid"),
                "jockey": _norm_name(rec.get("jockey_driver")),
                "trainer": _norm_name(rec.get("entraineur")),
                "horse": _norm_name(rec.get("cheval_nom")),
                "hippo": _norm_hippo(rec.get("hippodrome_normalise")),
                "disc": _norm_disc(rec.get("discipline")),
                "surface": _norm_surface(rec.get("surface")),
                "dist_bucket": _distance_bucket(rec.get("distance")),
                "sire": _norm_name(rec.get("pere")),
                "gagnant": bool(rec.get("is_gagnant")),
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

            gr = global_rate.rate()

            # -- Snapshot BEFORE update for all partants --
            post_updates: list[dict] = []

            for rec in course_group:
                jockey = rec["jockey"]
                trainer = rec["trainer"]
                horse = rec["horse"]
                hippo = rec["hippo"]
                disc = rec["disc"]
                surface = rec["surface"]
                dist_bucket = rec["dist_bucket"]
                sire = rec["sire"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                # 1. Jockey shrunk win rate (overall)
                if jockey:
                    js = jockey_state[jockey]
                    features["bshr_jockey_wr"] = _shrunk_rate(js.overall, gr, K_JOCKEY)
                else:
                    features["bshr_jockey_wr"] = None

                # 2. Jockey shrunk win rate at this hippodrome
                if jockey and hippo:
                    js = jockey_state[jockey]
                    features["bshr_jockey_hippo_wr"] = _shrunk_rate(
                        js.by_dim.get(f"h:{hippo}"), gr, K_JOCKEY
                    )
                else:
                    features["bshr_jockey_hippo_wr"] = None

                # 3. Jockey shrunk win rate in this discipline
                if jockey and disc:
                    js = jockey_state[jockey]
                    features["bshr_jockey_disc_wr"] = _shrunk_rate(
                        js.by_dim.get(f"d:{disc}"), gr, K_JOCKEY
                    )
                else:
                    features["bshr_jockey_disc_wr"] = None

                # 4. Trainer shrunk win rate (overall)
                if trainer:
                    ts = trainer_state[trainer]
                    features["bshr_trainer_wr"] = _shrunk_rate(ts.overall, gr, K_TRAINER)
                else:
                    features["bshr_trainer_wr"] = None

                # 5. Trainer shrunk win rate at this hippodrome
                if trainer and hippo:
                    ts = trainer_state[trainer]
                    features["bshr_trainer_hippo_wr"] = _shrunk_rate(
                        ts.by_dim.get(f"h:{hippo}"), gr, K_TRAINER
                    )
                else:
                    features["bshr_trainer_hippo_wr"] = None

                # 6. Trainer shrunk win rate in this discipline
                if trainer and disc:
                    ts = trainer_state[trainer]
                    features["bshr_trainer_disc_wr"] = _shrunk_rate(
                        ts.by_dim.get(f"d:{disc}"), gr, K_TRAINER
                    )
                else:
                    features["bshr_trainer_disc_wr"] = None

                # 7. Jockey-trainer combo shrunk win rate
                if jockey and trainer:
                    combo_key = f"{jockey}|{trainer}"
                    features["bshr_jock_train_combo_wr"] = _shrunk_rate(
                        combo_state.get(combo_key), gr, K_COMBO
                    )
                else:
                    features["bshr_jock_train_combo_wr"] = None

                # 8. Horse at this hippodrome shrunk win rate
                if horse and hippo:
                    hs = horse_state[horse]
                    features["bshr_horse_hippo_wr"] = _shrunk_rate(
                        hs.by_dim.get(f"h:{hippo}"), gr, K_HORSE
                    )
                else:
                    features["bshr_horse_hippo_wr"] = None

                # 9. Horse in this discipline shrunk win rate
                if horse and disc:
                    hs = horse_state[horse]
                    features["bshr_horse_disc_wr"] = _shrunk_rate(
                        hs.by_dim.get(f"d:{disc}"), gr, K_HORSE
                    )
                else:
                    features["bshr_horse_disc_wr"] = None

                # 10. Horse at this distance bucket shrunk win rate
                if horse and dist_bucket:
                    hs = horse_state[horse]
                    features["bshr_horse_dist_wr"] = _shrunk_rate(
                        hs.by_dim.get(f"dist:{dist_bucket}"), gr, K_HORSE
                    )
                else:
                    features["bshr_horse_dist_wr"] = None

                # 11. Sire progeny shrunk win rate at this distance
                if sire and dist_bucket:
                    ss = sire_state[sire]
                    features["bshr_sire_dist_wr"] = _shrunk_rate(
                        ss.by_dim.get(f"dist:{dist_bucket}"), gr, K_SIRE
                    )
                else:
                    features["bshr_sire_dist_wr"] = None

                # 12. Sire progeny shrunk win rate on this surface
                if sire and surface:
                    ss = sire_state[sire]
                    features["bshr_sire_surface_wr"] = _shrunk_rate(
                        ss.by_dim.get(f"surf:{surface}"), gr, K_SIRE
                    )
                else:
                    features["bshr_sire_surface_wr"] = None

                # Track fill rates
                for k in _FEATURE_KEYS:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Defer update data
                post_updates.append(rec)

            # -- Update states AFTER all snapshots for this course --
            for rec in post_updates:
                is_g = rec["gagnant"]
                jockey = rec["jockey"]
                trainer = rec["trainer"]
                horse = rec["horse"]
                hippo = rec["hippo"]
                disc = rec["disc"]
                surface = rec["surface"]
                dist_bucket = rec["dist_bucket"]
                sire = rec["sire"]

                # Global rate
                global_rate.update(is_g)

                # Jockey
                if jockey:
                    js = jockey_state[jockey]
                    js.overall.update(is_g)
                    if hippo:
                        js.by_dim[f"h:{hippo}"].update(is_g)
                    if disc:
                        js.by_dim[f"d:{disc}"].update(is_g)

                # Trainer
                if trainer:
                    ts = trainer_state[trainer]
                    ts.overall.update(is_g)
                    if hippo:
                        ts.by_dim[f"h:{hippo}"].update(is_g)
                    if disc:
                        ts.by_dim[f"d:{disc}"].update(is_g)

                # Jockey-trainer combo
                if jockey and trainer:
                    combo_state[f"{jockey}|{trainer}"].update(is_g)

                # Horse
                if horse:
                    hs = horse_state[horse]
                    if hippo:
                        hs.by_dim[f"h:{hippo}"].update(is_g)
                    if disc:
                        hs.by_dim[f"d:{disc}"].update(is_g)
                    if dist_bucket:
                        hs.by_dim[f"dist:{dist_bucket}"].update(is_g)

                # Sire
                if sire:
                    ss = sire_state[sire]
                    if dist_bucket:
                        ss.by_dim[f"dist:{dist_bucket}"].update(is_g)
                    if surface:
                        ss.by_dim[f"surf:{surface}"].update(is_g)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Bayesian shrinkage build termine: %d features en %.1fs",
        n_written, elapsed,
    )
    logger.info(
        "Entites trackees: jockeys=%d, trainers=%d, combos=%d, horses=%d, sires=%d",
        len(jockey_state), len(trainer_state), len(combo_state),
        len(horse_state), len(sire_state),
    )
    logger.info("Global win rate final: %.4f (%d/%d)", global_rate.rate(), global_rate.wins, global_rate.total)

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
        description="Construction des features Bayesian shrinkage a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/bayesian_shrinkage/)",
    )
    args = parser.parse_args()

    logger = setup_logging("bayesian_shrinkage_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "bayesian_shrinkage.jsonl"
    build_bayesian_shrinkage(input_path, out_path, logger)


if __name__ == "__main__":
    main()
