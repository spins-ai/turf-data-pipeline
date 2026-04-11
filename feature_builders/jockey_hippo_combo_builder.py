#!/usr/bin/env python3
"""
feature_builders.jockey_hippo_combo_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Jockey x hippodrome combination features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant jockey-hippodrome features.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.  Snapshot BEFORE update.

Memory-optimised version:
  - Phase 1 reads only minimal tuples (sort keys + byte offsets)
  - Phase 2 sorts the lightweight index chronologically
  - Phase 3 re-reads records from disk via seek, streams output to .tmp
  - gc.collect() every 500K records

Produces:
  - jockey_hippo_combo.jsonl   in builder_outputs/jockey_hippo_combo/

Features per partant (8):
  - jhc_jockey_hippo_wr         : jockey's win rate at this hippodrome
  - jhc_jockey_hippo_runs       : number of runs at this hippodrome
  - jhc_jockey_hippo_place_rate : jockey's place rate at this hippodrome
  - jhc_jockey_is_hippo_specialist : 1 if hippo wr > 1.5x overall wr and 10+ runs
  - jhc_jockey_hippo_first_time : 1 if jockey has never ridden at this hippodrome
  - jhc_jockey_hippo_recent_form: win rate at this hippo in last 10 rides there
  - jhc_jockey_hippo_advantage  : hippo wr - overall wr (positive = performs better here)
  - jhc_trainer_hippo_wr        : trainer's win rate at this hippodrome

Usage:
    python feature_builders/jockey_hippo_combo_builder.py
    python feature_builders/jockey_hippo_combo_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
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

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_hippo_combo")

_LOG_EVERY = 500_000

_MIN_RUNS_FOR_RATE = 3   # minimum runs to compute win/place rate
_SPECIALIST_MIN = 10     # minimum runs for specialist flag
_SPECIALIST_MULT = 1.5   # hippo wr must exceed overall wr * this factor
_RECENT_MAXLEN = 10      # last N rides at this hippodrome for recent form


# ===========================================================================
# STATE TRACKERS
# ===========================================================================


class _JockeyState:
    """Per-jockey state: overall stats + per-hippodrome stats."""

    __slots__ = ("overall_wins", "overall_total", "hippo_stats", "hippo_recent")

    def __init__(self) -> None:
        # overall [wins, total]
        self.overall_wins: int = 0
        self.overall_total: int = 0
        # hippo_stats: {hippo -> [wins, places, total]}
        self.hippo_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
        # hippo_recent: {hippo -> deque(10) of is_gagnant}
        self.hippo_recent: dict[str, deque] = defaultdict(lambda: deque(maxlen=_RECENT_MAXLEN))

    def snapshot(self, hippo: str) -> dict[str, Any]:
        """Return features from CURRENT state (before this race's update)."""
        feats: dict[str, Any] = {}

        hs = self.hippo_stats.get(hippo)
        total_at_hippo = hs[2] if hs else 0
        wins_at_hippo = hs[0] if hs else 0
        places_at_hippo = hs[1] if hs else 0

        # 1. jhc_jockey_hippo_wr
        if total_at_hippo >= _MIN_RUNS_FOR_RATE:
            feats["jhc_jockey_hippo_wr"] = round(wins_at_hippo / total_at_hippo, 4)
        else:
            feats["jhc_jockey_hippo_wr"] = None

        # 2. jhc_jockey_hippo_runs
        feats["jhc_jockey_hippo_runs"] = total_at_hippo if total_at_hippo > 0 else None

        # 3. jhc_jockey_hippo_place_rate
        if total_at_hippo >= _MIN_RUNS_FOR_RATE:
            feats["jhc_jockey_hippo_place_rate"] = round(places_at_hippo / total_at_hippo, 4)
        else:
            feats["jhc_jockey_hippo_place_rate"] = None

        # 4. jhc_jockey_is_hippo_specialist
        overall_wr = (self.overall_wins / self.overall_total) if self.overall_total > 0 else 0
        hippo_wr = (wins_at_hippo / total_at_hippo) if total_at_hippo > 0 else 0
        if total_at_hippo >= _SPECIALIST_MIN and self.overall_total > 0:
            feats["jhc_jockey_is_hippo_specialist"] = int(
                hippo_wr > overall_wr * _SPECIALIST_MULT
            )
        else:
            feats["jhc_jockey_is_hippo_specialist"] = None

        # 5. jhc_jockey_hippo_first_time
        feats["jhc_jockey_hippo_first_time"] = int(total_at_hippo == 0)

        # 6. jhc_jockey_hippo_recent_form
        recent = self.hippo_recent.get(hippo)
        if recent and len(recent) >= _MIN_RUNS_FOR_RATE:
            feats["jhc_jockey_hippo_recent_form"] = round(sum(recent) / len(recent), 4)
        else:
            feats["jhc_jockey_hippo_recent_form"] = None

        # 7. jhc_jockey_hippo_advantage
        if total_at_hippo >= _MIN_RUNS_FOR_RATE and self.overall_total >= _MIN_RUNS_FOR_RATE:
            feats["jhc_jockey_hippo_advantage"] = round(hippo_wr - overall_wr, 4)
        else:
            feats["jhc_jockey_hippo_advantage"] = None

        return feats

    def update(self, hippo: str, is_gagnant: bool, is_place: bool) -> None:
        """Update state AFTER snapshot has been taken."""
        self.overall_total += 1
        if is_gagnant:
            self.overall_wins += 1

        hs = self.hippo_stats[hippo]
        hs[2] += 1
        if is_gagnant:
            hs[0] += 1
        if is_place:
            hs[1] += 1

        self.hippo_recent[hippo].append(int(is_gagnant))


class _TrainerState:
    """Per-trainer state: per-hippodrome stats."""

    __slots__ = ("hippo_stats",)

    def __init__(self) -> None:
        # hippo_stats: {hippo -> [wins, total]}
        self.hippo_stats: dict[str, list[int]] = defaultdict(lambda: [0, 0])

    def snapshot(self, hippo: str) -> Optional[float]:
        """Return trainer hippo win rate (before this race)."""
        hs = self.hippo_stats.get(hippo)
        if not hs or hs[1] < _MIN_RUNS_FOR_RATE:
            return None
        return round(hs[0] / hs[1], 4)

    def update(self, hippo: str, is_gagnant: bool) -> None:
        """Update state AFTER snapshot."""
        hs = self.hippo_stats[hippo]
        hs[1] += 1
        if is_gagnant:
            hs[0] += 1


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


def _norm_name(name: Optional[str]) -> Optional[str]:
    """Normalise a jockey/trainer name."""
    if not name or not isinstance(name, str):
        return None
    return name.strip().upper()


def _norm_hippo(name: Optional[str]) -> Optional[str]:
    """Normalise a hippodrome name."""
    if not name or not isinstance(name, str):
        return None
    return name.strip().lower()


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


def build_jockey_hippo_combo(input_path: Path, output_path: Path, logger) -> int:
    """Build jockey x hippodrome combo features from partants_master.jsonl.

    Two-phase approach:
      1. Index: read sort keys + byte offsets (lightweight).
      2. Sort chronologically, then seek-read records course by course,
         streaming output to .tmp, then atomic rename.

    Returns the total number of feature records written.
    """
    logger.info("=== Jockey Hippo Combo Builder (memory-optimised) ===")
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
    jockey_state: dict[str, _JockeyState] = defaultdict(_JockeyState)
    trainer_state: dict[str, _TrainerState] = defaultdict(_TrainerState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    _FEATURE_KEYS = [
        "jhc_jockey_hippo_wr",
        "jhc_jockey_hippo_runs",
        "jhc_jockey_hippo_place_rate",
        "jhc_jockey_is_hippo_specialist",
        "jhc_jockey_hippo_first_time",
        "jhc_jockey_hippo_recent_form",
        "jhc_jockey_hippo_advantage",
        "jhc_trainer_hippo_wr",
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
                "hippo": _norm_hippo(rec.get("hippodrome_normalise")),
                "gagnant": bool(rec.get("is_gagnant")),
                "is_place": bool(rec.get("is_place")),
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

            # -- Snapshot BEFORE update for all partants --
            post_updates: list[tuple[Optional[str], Optional[str], Optional[str], bool, bool]] = []

            for rec in course_group:
                jockey = rec["jockey"]
                trainer = rec["trainer"]
                hippo = rec["hippo"]

                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if jockey and hippo:
                    js = jockey_state[jockey]
                    features.update(js.snapshot(hippo))
                else:
                    for k in _FEATURE_KEYS:
                        if k != "jhc_trainer_hippo_wr":
                            features[k] = None

                # Trainer hippo wr
                if trainer and hippo:
                    ts = trainer_state[trainer]
                    features["jhc_trainer_hippo_wr"] = ts.snapshot(hippo)
                else:
                    features["jhc_trainer_hippo_wr"] = None

                # Track fill rates
                for k in _FEATURE_KEYS:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(features, ensure_ascii=False) + "\n")
                n_written += 1

                # Defer update
                post_updates.append((
                    jockey, trainer, hippo,
                    rec["gagnant"], rec["is_place"],
                ))

            # -- Update states AFTER all snapshots --
            for jockey, trainer, hippo, is_gagnant, is_place in post_updates:
                if jockey and hippo:
                    jockey_state[jockey].update(hippo, is_gagnant, is_place)
                if trainer and hippo:
                    trainer_state[trainer].update(hippo, is_gagnant)

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Jockey hippo combo build termine: %d features en %.1fs (jockeys: %d, trainers: %d)",
        n_written, elapsed, len(jockey_state), len(trainer_state),
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
        description="Construction des features jockey x hippodrome combo a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/jockey_hippo_combo/)",
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_hippo_combo_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "jockey_hippo_combo.jsonl"
    build_jockey_hippo_combo(input_path, out_path, logger)


if __name__ == "__main__":
    main()
