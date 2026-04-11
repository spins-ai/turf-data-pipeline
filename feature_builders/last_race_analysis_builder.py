#!/usr/bin/env python3
"""
feature_builders.last_race_analysis_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Last race detailed analysis features -- deep dive into what happened
in the horse's most recent race.

Reads partants_master.jsonl in streaming mode (index + seek), processes
all records chronologically, and computes per-partant last-race features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.  Snapshot BEFORE update.

Produces:
  - last_race_analysis.jsonl   in output/last_race_analysis/

Features per partant (10):
  - lra_last_position        : finish position in last race
  - lra_last_beaten_pct      : (partants - position) / (partants - 1) in last race
  - lra_last_odds            : cote_finale in last race
  - lra_last_distance        : distance of last race
  - lra_last_discipline      : discipline of last race (encoded integer)
  - lra_last_field_size      : nombre_partants in last race
  - lra_last_was_favorite    : 1 if last race odds were lowest in that race
  - lra_last_outperformed    : 1 if finished better than odds implied in last race
  - lra_same_distance        : 1 if current distance == last race distance
  - lra_same_hippo           : 1 if current hippodrome == last race hippodrome

Usage:
    python feature_builders/last_race_analysis_builder.py
    python feature_builders/last_race_analysis_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/last_race_analysis")

# Progress log every N records
_LOG_EVERY = 500_000

# Discipline encoding map
_DISCIPLINE_CODES: dict[str, int] = {
    "PLAT": 1,
    "TROT": 2,
    "ATTELE": 3,
    "TROT ATTELE": 3,
    "MONTE": 4,
    "TROT MONTE": 4,
    "OBSTACLE": 5,
    "HAIES": 6,
    "STEEPLE": 7,
    "STEEPLE-CHASE": 7,
    "CROSS": 8,
    "CROSS-COUNTRY": 8,
}


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _encode_discipline(raw: Optional[str]) -> Optional[int]:
    if not raw or not isinstance(raw, str):
        return None
    return _DISCIPLINE_CODES.get(raw.strip().upper())


# ===========================================================================
# STATE TRACKER
# ===========================================================================


class _LastRaceState:
    """Per-horse state: stores data from the most recent race (deque of 1)."""

    __slots__ = ("last_race",)

    def __init__(self) -> None:
        # deque(maxlen=1) storing a single dict with keys:
        # position, beaten_pct, odds, distance, discipline, partants, hippo,
        # was_fav, outperformed
        self.last_race: deque[dict[str, Any]] = deque(maxlen=1)

    def snapshot(self, current_distance: Optional[int],
                 current_hippo: Optional[str]) -> dict[str, Any]:
        """Return features from the last race (snapshot BEFORE update)."""
        feats: dict[str, Any] = {
            "lra_last_position": None,
            "lra_last_beaten_pct": None,
            "lra_last_odds": None,
            "lra_last_distance": None,
            "lra_last_discipline": None,
            "lra_last_field_size": None,
            "lra_last_was_favorite": None,
            "lra_last_outperformed": None,
            "lra_same_distance": None,
            "lra_same_hippo": None,
        }

        if not self.last_race:
            return feats

        lr = self.last_race[0]
        feats["lra_last_position"] = lr["position"]
        feats["lra_last_beaten_pct"] = lr["beaten_pct"]
        feats["lra_last_odds"] = lr["odds"]
        feats["lra_last_distance"] = lr["distance"]
        feats["lra_last_discipline"] = lr["discipline"]
        feats["lra_last_field_size"] = lr["partants"]
        feats["lra_last_was_favorite"] = lr["was_fav"]
        feats["lra_last_outperformed"] = lr["outperformed"]

        # Same distance / same hippodrome
        if current_distance is not None and lr["distance"] is not None:
            feats["lra_same_distance"] = int(current_distance == lr["distance"])

        if current_hippo and lr["hippo"]:
            feats["lra_same_hippo"] = int(current_hippo == lr["hippo"])

        return feats

    def update(self, data: dict[str, Any]) -> None:
        """Push new race data (post-race update)."""
        self.last_race.append(data)


# ===========================================================================
# MAIN BUILD (memory-optimised: index + seek + streaming output)
# ===========================================================================


def build_last_race_analysis(input_path: Path, output_path: Path, logger) -> int:
    """Build last-race analysis features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         and stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Last Race Analysis Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index (date, course_uid, num_pmu, offset) --
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

    # -- Phase 2: Sort the lightweight index chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _LastRaceState] = defaultdict(_LastRaceState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)
    fill_counts: dict[str, int] = {
        "lra_last_position": 0,
        "lra_last_beaten_pct": 0,
        "lra_last_odds": 0,
        "lra_last_distance": 0,
        "lra_last_discipline": 0,
        "lra_last_field_size": 0,
        "lra_last_was_favorite": 0,
        "lra_last_outperformed": 0,
        "lra_same_distance": 0,
        "lra_same_hippo": 0,
    }

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            """Seek to offset and read one JSON record."""
            fin.seek(offset)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            cote = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
            nb_partants = _safe_int(rec.get("nombre_partants"))
            position = _safe_int(rec.get("position_arrivee"))
            distance = _safe_int(rec.get("distance"))
            discipline_raw = rec.get("discipline") or rec.get("type_course") or ""
            hippo = rec.get("hippodrome_normalise") or rec.get("hippodrome") or ""
            if isinstance(hippo, str):
                hippo = hippo.strip().upper()

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "num": rec.get("num_pmu", 0) or 0,
                "cheval": rec.get("nom_cheval") or rec.get("horse_id"),
                "position": position,
                "cote": cote,
                "distance": distance,
                "discipline_raw": discipline_raw,
                "discipline_code": _encode_discipline(discipline_raw),
                "nb_partants": nb_partants,
                "hippo": hippo,
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
            course_group = [_extract_slim(_read_record_at(index[ci][3])) for ci in course_indices]

            # -- Determine favorite for this course (lowest odds) --
            min_cote: Optional[float] = None
            for rec in course_group:
                c = rec["cote"]
                if c is not None and c > 1.0:
                    if min_cote is None or c < min_cote:
                        min_cote = c

            # -- Snapshot pre-race features for all partants --
            for rec in course_group:
                cheval = rec["cheval"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if cheval:
                    feats = horse_state[cheval].snapshot(
                        current_distance=rec["distance"],
                        current_hippo=rec["hippo"],
                    )
                    features.update(feats)
                else:
                    features.update({
                        "lra_last_position": None,
                        "lra_last_beaten_pct": None,
                        "lra_last_odds": None,
                        "lra_last_distance": None,
                        "lra_last_discipline": None,
                        "lra_last_field_size": None,
                        "lra_last_was_favorite": None,
                        "lra_last_outperformed": None,
                        "lra_same_distance": None,
                        "lra_same_hippo": None,
                    })

                # Track fill counts
                for k in fill_counts:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                # Stream to output
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # -- Update states after race (post-race, no leakage) --
            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    continue

                position = rec["position"]
                cote = rec["cote"]
                nb_partants = rec["nb_partants"]
                distance = rec["distance"]
                discipline_code = rec["discipline_code"]
                hippo = rec["hippo"]

                # Compute beaten_pct: (partants - position) / (partants - 1)
                beaten_pct: Optional[float] = None
                if position is not None and nb_partants is not None and nb_partants > 1 and position > 0:
                    beaten_pct = round((nb_partants - position) / (nb_partants - 1), 4)

                # Was favorite: 1 if this horse had the lowest odds in the race
                was_fav: Optional[int] = None
                if cote is not None and min_cote is not None and cote > 1.0:
                    was_fav = int(abs(cote - min_cote) < 0.01)

                # Outperformed: finished better than odds implied
                # Odds-implied position = 1 / cote * nb_partants (rank by market)
                outperformed: Optional[int] = None
                if (position is not None and position > 0
                        and cote is not None and cote > 1.0
                        and nb_partants is not None and nb_partants > 0):
                    implied_rank = (1.0 / cote) * nb_partants
                    # Lower position = better; outperformed if actual position < implied
                    outperformed = int(position < implied_rank)

                horse_state[cheval].update({
                    "position": position,
                    "beaten_pct": beaten_pct,
                    "odds": cote,
                    "distance": distance,
                    "discipline": discipline_code,
                    "partants": nb_partants,
                    "hippo": hippo,
                    "was_fav": was_fav,
                    "outperformed": outperformed,
                })

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Last race analysis build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, 100 * v / n_written if n_written else 0)

    return n_written


# ===========================================================================
# SAUVEGARDE & CLI
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
    raise FileNotFoundError(f"Fichier d'entree introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features last-race analysis a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/last_race_analysis/)",
    )
    args = parser.parse_args()

    logger = setup_logging("last_race_analysis_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "last_race_analysis.jsonl"
    build_last_race_analysis(input_path, out_path, logger)


if __name__ == "__main__":
    main()
