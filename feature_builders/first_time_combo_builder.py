#!/usr/bin/env python3
"""
feature_builders.first_time_combo_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
First-time combination features -- detecting novel experiences for the horse.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - first_time_combo.jsonl  in builder_outputs/first_time_combo/

Features per partant (8):
  - ftc_first_time_hippo          : 1 if horse has never raced at this hippodrome before
  - ftc_first_time_distance       : 1 if horse has never run this exact distance
  - ftc_first_time_discipline     : 1 if horse has never run in this discipline
  - ftc_first_time_surface        : 1 if horse has never run on this type_piste
  - ftc_first_time_jockey         : 1 if this jockey has never ridden this horse
  - ftc_novelty_score             : sum of all first_time flags above (0-5)
  - ftc_horse_hippo_experience    : number of prior runs at this hippodrome
  - ftc_first_time_combo_dist_hippo : 1 if horse has never run this distance at this hippodrome

State per horse:
  hippos_set, distances_set, disciplines_set, surfaces_set,
  jockeys_set, hippo_counts dict, dist_hippo_set.

Snapshot BEFORE update (strict temporal integrity).

Usage:
    python feature_builders/first_time_combo_builder.py
    python feature_builders/first_time_combo_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/first_time_combo")

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


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _HorseFirstTimeState:
    """Per-horse state for first-time combination tracking.

    Tracks sets of previously seen hippodromes, distances, disciplines,
    surfaces, jockeys, and distance+hippodrome combos.
    """

    __slots__ = (
        "hippos_set", "distances_set", "disciplines_set",
        "surfaces_set", "jockeys_set", "hippo_counts", "dist_hippo_set",
    )

    def __init__(self) -> None:
        self.hippos_set: set[str] = set()
        self.distances_set: set[int] = set()
        self.disciplines_set: set[str] = set()
        self.surfaces_set: set[str] = set()
        self.jockeys_set: set[str] = set()
        self.hippo_counts: dict[str, int] = {}
        self.dist_hippo_set: set[tuple[int, str]] = set()

    def snapshot(
        self,
        hippo: Optional[str],
        distance: Optional[int],
        discipline: Optional[str],
        surface: Optional[str],
        jockey: Optional[str],
    ) -> dict[str, Any]:
        """Return feature dict BEFORE updating state."""
        feats: dict[str, Any] = {}

        # --- ftc_first_time_hippo ---
        if hippo is not None:
            feats["ftc_first_time_hippo"] = 1 if hippo not in self.hippos_set else 0
        else:
            feats["ftc_first_time_hippo"] = None

        # --- ftc_first_time_distance ---
        if distance is not None:
            feats["ftc_first_time_distance"] = 1 if distance not in self.distances_set else 0
        else:
            feats["ftc_first_time_distance"] = None

        # --- ftc_first_time_discipline ---
        if discipline is not None:
            feats["ftc_first_time_discipline"] = 1 if discipline not in self.disciplines_set else 0
        else:
            feats["ftc_first_time_discipline"] = None

        # --- ftc_first_time_surface ---
        if surface is not None:
            feats["ftc_first_time_surface"] = 1 if surface not in self.surfaces_set else 0
        else:
            feats["ftc_first_time_surface"] = None

        # --- ftc_first_time_jockey ---
        if jockey is not None:
            feats["ftc_first_time_jockey"] = 1 if jockey not in self.jockeys_set else 0
        else:
            feats["ftc_first_time_jockey"] = None

        # --- ftc_novelty_score ---
        flags = [
            feats["ftc_first_time_hippo"],
            feats["ftc_first_time_distance"],
            feats["ftc_first_time_discipline"],
            feats["ftc_first_time_surface"],
            feats["ftc_first_time_jockey"],
        ]
        non_none = [f for f in flags if f is not None]
        if non_none:
            feats["ftc_novelty_score"] = sum(non_none)
        else:
            feats["ftc_novelty_score"] = None

        # --- ftc_horse_hippo_experience ---
        if hippo is not None:
            feats["ftc_horse_hippo_experience"] = self.hippo_counts.get(hippo, 0)
        else:
            feats["ftc_horse_hippo_experience"] = None

        # --- ftc_first_time_combo_dist_hippo ---
        if distance is not None and hippo is not None:
            feats["ftc_first_time_combo_dist_hippo"] = (
                1 if (distance, hippo) not in self.dist_hippo_set else 0
            )
        else:
            feats["ftc_first_time_combo_dist_hippo"] = None

        return feats

    def update(
        self,
        hippo: Optional[str],
        distance: Optional[int],
        discipline: Optional[str],
        surface: Optional[str],
        jockey: Optional[str],
    ) -> None:
        """Update state AFTER snapshot."""
        if hippo is not None:
            self.hippos_set.add(hippo)
            self.hippo_counts[hippo] = self.hippo_counts.get(hippo, 0) + 1
        if distance is not None:
            self.distances_set.add(distance)
        if discipline is not None:
            self.disciplines_set.add(discipline)
        if surface is not None:
            self.surfaces_set.add(surface)
        if jockey is not None:
            self.jockeys_set.add(jockey)
        if distance is not None and hippo is not None:
            self.dist_hippo_set.add((distance, hippo))


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(v: Any) -> Optional[int]:
    """Convert to int or return None."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _norm_str(v: Any) -> Optional[str]:
    """Normalize a string field: strip + upper. None if empty."""
    if not v or not isinstance(v, str):
        return None
    s = v.strip().upper()
    return s if s else None


# ===========================================================================
# MAIN BUILD (index + sort + seek-based streaming output)
# ===========================================================================


def build_first_time_combo_features(input_path: Path, output_path: Path, logger) -> int:
    """Build first-time combination features from partants_master.jsonl.

    Memory-optimised approach:
      Phase 1: Read only sort keys + file byte offsets (lightweight index).
      Phase 2: Sort chronologically.
      Phase 3: Seek-based re-read, snapshot BEFORE update, stream output to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== First-Time Combo Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date_str, course_uid, num_pmu, byte_offset) --
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
    horse_states: dict[str, _HorseFirstTimeState] = defaultdict(_HorseFirstTimeState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts = {
        "ftc_first_time_hippo": 0,
        "ftc_first_time_distance": 0,
        "ftc_first_time_discipline": 0,
        "ftc_first_time_surface": 0,
        "ftc_first_time_jockey": 0,
        "ftc_novelty_score": 0,
        "ftc_horse_hippo_experience": 0,
        "ftc_first_time_combo_dist_hippo": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
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

            # Read records from disk for this course
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_record_at(index[ci][3])
                horse_id = rec.get("horse_id") or rec.get("nom_cheval")
                hippo = _norm_str(rec.get("hippodrome_normalise"))
                distance = _safe_int(rec.get("distance"))
                discipline = _norm_str(rec.get("discipline"))
                surface = _norm_str(rec.get("type_piste"))
                jockey = _norm_str(rec.get("jockey_driver"))

                course_records.append({
                    "uid": rec.get("partant_uid"),
                    "horse_id": horse_id,
                    "hippo": hippo,
                    "distance": distance,
                    "discipline": discipline,
                    "surface": surface,
                    "jockey": jockey,
                })

            # -- Snapshot BEFORE update (temporal integrity) --
            for rec in course_records:
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid:
                    feats = horse_states[hid].snapshot(
                        rec["hippo"], rec["distance"], rec["discipline"],
                        rec["surface"], rec["jockey"],
                    )
                    features.update(feats)
                    for k in fill_counts:
                        if feats.get(k) is not None:
                            fill_counts[k] += 1
                else:
                    features["ftc_first_time_hippo"] = None
                    features["ftc_first_time_distance"] = None
                    features["ftc_first_time_discipline"] = None
                    features["ftc_first_time_surface"] = None
                    features["ftc_first_time_jockey"] = None
                    features["ftc_novelty_score"] = None
                    features["ftc_horse_hippo_experience"] = None
                    features["ftc_first_time_combo_dist_hippo"] = None

                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states AFTER race --
            for rec in course_records:
                hid = rec["horse_id"]
                if hid:
                    horse_states[hid].update(
                        rec["hippo"], rec["distance"], rec["discipline"],
                        rec["surface"], rec["jockey"],
                    )

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "First-time combo build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_states),
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
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features first-time combo a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/first_time_combo/)",
    )
    args = parser.parse_args()

    logger = setup_logging("first_time_combo_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "first_time_combo.jsonl"
    build_first_time_combo_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
