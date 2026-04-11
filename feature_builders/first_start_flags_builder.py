#!/usr/bin/env python3
"""
feature_builders.first_start_flags_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detects "first time" / debut conditions for each partant.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant debut flags.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the horse's accumulated experience -- no future leakage.
Features are snapshotted BEFORE the current race updates the state.

Produces:
  - first_start_flags.jsonl   in output/first_start_flags/

Features per partant (8):
  - fs_is_debut           : 1 if this is the horse's first ever race
  - fs_first_at_distance  : 1 if horse has never raced at this distance bucket
  - fs_first_at_hippo     : 1 if horse has never raced at this hippodrome
  - fs_first_with_jockey  : 1 if this horse-jockey combo has never raced together
  - fs_first_in_discipline: 1 if horse has never raced in this discipline
  - fs_first_on_terrain   : 1 if horse has never raced on this terrain type
  - fs_novelty_score      : sum of the 5 first_* flags above (0-5; fs_is_debut excluded)
  - fs_experience_score   : total unique (hippo, distance, discipline, terrain) combos seen

Distance buckets (metres):
  <1000, 1000-1299, 1300-1499, 1500-1699, 1700-1899, 1900-2099,
  2100-2399, 2400-2799, 2800-3499, >=3500

Usage:
    python feature_builders/first_start_flags_builder.py
    python feature_builders/first_start_flags_builder.py --input data_master/partants_master.jsonl
    python feature_builders/first_start_flags_builder.py --output-dir output/first_start_flags
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
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/first_start_flags")

_LOG_EVERY = 500_000

# Distance bucket boundaries in metres (upper-exclusive except last)
_DISTANCE_BUCKETS = [1000, 1300, 1500, 1700, 1900, 2100, 2400, 2800, 3500]


# ===========================================================================
# HELPERS
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


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _distance_bucket(distance_m) -> Optional[str]:
    """Map a distance in metres to a string bucket label."""
    d = _safe_int(distance_m)
    if d is None or d <= 0:
        return None
    if d < _DISTANCE_BUCKETS[0]:
        return f"<{_DISTANCE_BUCKETS[0]}"
    for i in range(len(_DISTANCE_BUCKETS) - 1):
        if _DISTANCE_BUCKETS[i] <= d < _DISTANCE_BUCKETS[i + 1]:
            return f"{_DISTANCE_BUCKETS[i]}-{_DISTANCE_BUCKETS[i + 1] - 1}"
    return f">={_DISTANCE_BUCKETS[-1]}"


def _normalise_str(val) -> Optional[str]:
    """Strip, lowercase and return None for empty / missing values."""
    if val is None:
        return None
    s = str(val).strip().lower()
    return s if s else None


def _get_horse_id(rec: dict) -> Optional[str]:
    """Return a stable horse identifier from a record."""
    return (
        _normalise_str(rec.get("horse_id"))
        or _normalise_str(rec.get("nom_cheval"))
    )


def _get_jockey(rec: dict) -> Optional[str]:
    """Return jockey identifier from a record."""
    return (
        _normalise_str(rec.get("jockey"))
        or _normalise_str(rec.get("nom_jockey"))
    )


def _get_discipline(rec: dict) -> Optional[str]:
    """Return discipline / specialite from a record."""
    return (
        _normalise_str(rec.get("discipline"))
        or _normalise_str(rec.get("specialite"))
    )


def _get_terrain(rec: dict) -> Optional[str]:
    """Return terrain type from a record."""
    return _normalise_str(rec.get("etat_terrain"))


def _get_hippo(rec: dict) -> Optional[str]:
    """Return hippodrome identifier from a record."""
    return _normalise_str(rec.get("hippodrome"))


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseState:
    """Accumulates the contexts a horse has experienced before the current race."""

    __slots__ = (
        "total_races",
        "hippos",
        "distances",
        "disciplines",
        "terrains",
        "jockeys",
        "combo_set",
    )

    def __init__(self) -> None:
        self.total_races: int = 0
        self.hippos: set[str] = set()
        self.distances: set[str] = set()
        self.disciplines: set[str] = set()
        self.terrains: set[str] = set()
        self.jockeys: set[str] = set()
        # Each combo is a tuple (hippo, distance_bucket, discipline, terrain)
        # used to compute fs_experience_score
        self.combo_set: set[tuple] = set()

    def snapshot_flags(
        self,
        hippo: Optional[str],
        dist_bucket: Optional[str],
        jockey: Optional[str],
        discipline: Optional[str],
        terrain: Optional[str],
    ) -> dict[str, Any]:
        """Return feature dict based on state BEFORE this race (no update yet)."""
        is_debut = 1 if self.total_races == 0 else 0

        fs_first_at_distance = (
            1 if (dist_bucket is not None and dist_bucket not in self.distances) else 0
        )
        fs_first_at_hippo = (
            1 if (hippo is not None and hippo not in self.hippos) else 0
        )
        fs_first_with_jockey = (
            1 if (jockey is not None and jockey not in self.jockeys) else 0
        )
        fs_first_in_discipline = (
            1 if (discipline is not None and discipline not in self.disciplines) else 0
        )
        fs_first_on_terrain = (
            1 if (terrain is not None and terrain not in self.terrains) else 0
        )

        novelty_score = (
            fs_first_at_distance
            + fs_first_at_hippo
            + fs_first_with_jockey
            + fs_first_in_discipline
            + fs_first_on_terrain
        )

        experience_score = len(self.combo_set)

        return {
            "fs_is_debut": is_debut,
            "fs_first_at_distance": fs_first_at_distance,
            "fs_first_at_hippo": fs_first_at_hippo,
            "fs_first_with_jockey": fs_first_with_jockey,
            "fs_first_in_discipline": fs_first_in_discipline,
            "fs_first_on_terrain": fs_first_on_terrain,
            "fs_novelty_score": novelty_score,
            "fs_experience_score": experience_score,
        }

    def update(
        self,
        hippo: Optional[str],
        dist_bucket: Optional[str],
        jockey: Optional[str],
        discipline: Optional[str],
        terrain: Optional[str],
    ) -> None:
        """Update state after snapshotting features for this race."""
        self.total_races += 1
        if hippo:
            self.hippos.add(hippo)
        if dist_bucket:
            self.distances.add(dist_bucket)
        if jockey:
            self.jockeys.add(jockey)
        if discipline:
            self.disciplines.add(discipline)
        if terrain:
            self.terrains.add(terrain)
        # Record the combo (use None placeholders for missing fields)
        self.combo_set.add((hippo, dist_bucket, discipline, terrain))


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_first_start_flags(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build first-start debut flags from partants_master.jsonl.

    Two-pass approach (index + sort + seek):
      1. Stream the file once, reading only the fields needed. Keep slim records
         in memory.
      2. Sort chronologically (date, course_uid, num_pmu).
      3. Walk the sorted list: for each race group, snapshot features BEFORE
         updating horse states, then update states.

    Returns a list of dicts [{partant_uid, fs_*}, ...].
    """
    logger.info("=== First-Start Flags Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1 – Stream and keep slim records
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        horse_id = _get_horse_id(rec)
        if not horse_id:
            horse_id = None  # will result in unknown horse, skip flag computation

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "course": rec.get("course_uid", "") or "",
            "num": _safe_int(rec.get("num_pmu")) or 0,
            "horse": horse_id,
            "hippo": _get_hippo(rec),
            "dist_bucket": _distance_bucket(rec.get("distance")),
            "jockey": _get_jockey(rec),
            "discipline": _get_discipline(rec),
            "terrain": _get_terrain(rec),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        n_read, time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2 – Sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3 – Walk sorted records, group by course_uid, snapshot then update
    # ------------------------------------------------------------------
    t2 = time.time()
    horse_states: dict[str, _HorseState] = defaultdict(_HorseState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all runners of this race
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot features (BEFORE update) --
        for rec in course_group:
            horse = rec["horse"]
            feats: dict[str, Any] = {"partant_uid": rec["uid"]}

            if horse:
                state = horse_states[horse]
                flags = state.snapshot_flags(
                    hippo=rec["hippo"],
                    dist_bucket=rec["dist_bucket"],
                    jockey=rec["jockey"],
                    discipline=rec["discipline"],
                    terrain=rec["terrain"],
                )
                feats.update(flags)
            else:
                # Unknown horse: cannot compute reliable debut flags
                feats.update(
                    {
                        "fs_is_debut": None,
                        "fs_first_at_distance": None,
                        "fs_first_at_hippo": None,
                        "fs_first_with_jockey": None,
                        "fs_first_in_discipline": None,
                        "fs_first_on_terrain": None,
                        "fs_novelty_score": None,
                        "fs_experience_score": None,
                    }
                )

            results.append(feats)

        # -- Update state (AFTER snapshot) --
        for rec in course_group:
            horse = rec["horse"]
            if horse:
                horse_states[horse].update(
                    hippo=rec["hippo"],
                    dist_bucket=rec["dist_bucket"],
                    jockey=rec["jockey"],
                    discipline=rec["discipline"],
                    terrain=rec["terrain"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info(
                "  Traite %d / %d records (chevaux suivis: %d)...",
                n_processed, total, len(horse_states),
            )

    elapsed = time.time() - t0
    logger.info(
        "First-start flags build termine: %d features en %.1fs (chevaux: %d)",
        len(results), elapsed, len(horse_states),
    )

    # Free slim records early
    del slim_records
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI arg or default candidates."""
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
        description=(
            "Construction des flags 'premiere fois' (debut) a partir de "
            "partants_master.jsonl"
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/.../first_start_flags/)",
    )
    args = parser.parse_args()

    logger = setup_logging("first_start_flags_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_first_start_flags(input_path, logger)

    # Save
    out_path = output_dir / "first_start_flags.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k in feature_keys:
            v = filled[k]
            logger.info(
                "  %-30s: %d/%d (%.1f%%)",
                k, v, total_count, 100.0 * v / total_count,
            )

        # Debut stats
        debutants = sum(1 for r in results if r.get("fs_is_debut") == 1)
        logger.info(
            "Debutants detectes: %d (%.1f%%)",
            debutants, 100.0 * debutants / total_count,
        )
        avg_novelty = (
            sum(r.get("fs_novelty_score") or 0 for r in results) / total_count
        )
        logger.info("Novelty score moyen: %.3f", avg_novelty)


if __name__ == "__main__":
    main()
