#!/usr/bin/env python3
"""
feature_builders.discipline_specialist_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Horse specialization within racing discipline (trot attele/monte, plat, obstacle, etc.).

Temporal integrity: for any partant at date D, only races with date < D contribute
to the statistics -- no future leakage.

Produces:
  - discipline_specialist.jsonl  in builder_outputs/discipline_specialist/

Features per partant:
  - ds_discipline_code          : encoded discipline (0=trot_attele, 1=trot_monte,
                                  2=plat, 3=obstacle, 4=cross, 5=haies)
  - ds_horse_discipline_wr      : horse's win rate in this specific discipline
  - ds_horse_discipline_runs    : number of runs in this discipline
  - ds_horse_main_discipline    : the discipline where horse has most races
  - ds_is_main_discipline       : 1 if current discipline matches horse's main discipline
  - ds_cross_discipline_switch  : 1 if horse switched discipline from last race
  - ds_discipline_specialist    : 1 if >80% of horse's races are in this discipline
  - ds_discipline_population_wr : overall population win rate in this discipline

State per horse  : dict[discipline] -> {wins, total}; plus last_discipline
State population : dict[discipline] -> {wins, total}

Usage:
    python feature_builders/discipline_specialist_builder.py
    python feature_builders/discipline_specialist_builder.py --input /path/to/partants_master.jsonl
    python feature_builders/discipline_specialist_builder.py --output-dir /path/to/output
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/discipline_specialist")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_LOCAL_INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000

# Discipline encoding map -- canonical names to integer codes
# Aliases handled in _normalize_discipline()
_DISCIPLINE_CODES: dict[str, int] = {
    "trot_attele": 0,
    "trot_monte": 1,
    "plat": 2,
    "obstacle": 3,
    "cross": 4,
    "haies": 5,
}

# Threshold for "specialist": horse runs >80% of races in one discipline
_SPECIALIST_THRESHOLD = 0.80


# ===========================================================================
# DISCIPLINE NORMALISATION
# ===========================================================================


def _normalize_discipline(value: Optional[str]) -> Optional[str]:
    """
    Map raw discipline/specialite field values to canonical discipline keys.
    Returns None if unrecognised or empty.
    """
    if not value:
        return None
    v = str(value).strip().lower()

    # Remove accents for comparison
    v = (
        v.replace("é", "e")
         .replace("è", "e")
         .replace("ê", "e")
         .replace("à", "a")
         .replace("â", "a")
         .replace("ô", "o")
         .replace("î", "i")
         .replace("û", "u")
         .replace("ç", "c")
    )

    if v in ("trot_attele", "trot attele", "attele", "a", "ta"):
        return "trot_attele"
    if v in ("trot_monte", "trot monte", "monte", "m", "tm"):
        return "trot_monte"
    if v in ("plat", "p", "flat"):
        return "plat"
    if v in ("obstacle", "obs", "o", "steeple", "steeple-chase", "steeplechase"):
        return "obstacle"
    if v in ("cross", "cross-country", "cross country", "cx"):
        return "cross"
    if v in ("haies", "hurdle", "hurdling", "h"):
        return "haies"

    return None


def _discipline_code(discipline: Optional[str]) -> Optional[int]:
    """Return integer code for a canonical discipline name, or None."""
    if discipline is None:
        return None
    return _DISCIPLINE_CODES.get(discipline)


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file one line at a time (streaming)."""
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


def _safe_bool(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "oui", "yes")
    return False


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseDisciplineState:
    """Accumulates per-horse, per-discipline statistics."""

    __slots__ = ("wins", "runs", "total_runs", "last_discipline")

    def __init__(self) -> None:
        # {discipline: count}
        self.wins: dict[str, int] = defaultdict(int)
        self.runs: dict[str, int] = defaultdict(int)
        self.total_runs: int = 0
        self.last_discipline: Optional[str] = None

    # ------------------------------------------------------------------
    # SNAPSHOT -- features computed BEFORE updating with this race
    # ------------------------------------------------------------------

    def snapshot(self, discipline: Optional[str]) -> dict[str, Any]:
        """
        Compute all horse-level discipline features using only past races.
        discipline: canonical discipline of the current race (not yet in state).
        """
        feats: dict[str, Any] = {
            "ds_horse_discipline_wr": None,
            "ds_horse_discipline_runs": None,
            "ds_horse_main_discipline": None,
            "ds_is_main_discipline": None,
            "ds_cross_discipline_switch": None,
            "ds_discipline_specialist": None,
        }

        total = self.total_runs
        if total == 0:
            # No past races yet -- all features None / missing
            if discipline is not None:
                # Still record 0 runs for discipline
                feats["ds_horse_discipline_runs"] = 0
                feats["ds_horse_discipline_wr"] = None
            return feats

        # -- Main discipline (most races) --
        main_disc = max(self.runs, key=lambda d: self.runs[d])
        feats["ds_horse_main_discipline"] = main_disc

        # -- Is main discipline --
        if discipline is not None:
            feats["ds_is_main_discipline"] = int(discipline == main_disc)

        # -- Cross-discipline switch --
        if self.last_discipline is not None and discipline is not None:
            feats["ds_cross_discipline_switch"] = int(discipline != self.last_discipline)

        # -- Stats in current discipline --
        if discipline is not None:
            disc_runs = self.runs.get(discipline, 0)
            disc_wins = self.wins.get(discipline, 0)
            feats["ds_horse_discipline_runs"] = disc_runs
            feats["ds_horse_discipline_wr"] = (
                round(disc_wins / disc_runs, 4) if disc_runs > 0 else None
            )

        # -- Specialist: >80% of races in current discipline --
        if discipline is not None and total > 0:
            disc_runs = self.runs.get(discipline, 0)
            feats["ds_discipline_specialist"] = int(disc_runs / total > _SPECIALIST_THRESHOLD)

        return feats

    # ------------------------------------------------------------------
    # UPDATE -- called AFTER snapshotting (post-race)
    # ------------------------------------------------------------------

    def update(self, discipline: Optional[str], is_winner: bool) -> None:
        if discipline is None:
            return
        self.runs[discipline] += 1
        self.total_runs += 1
        if is_winner:
            self.wins[discipline] += 1
        self.last_discipline = discipline


# ===========================================================================
# POPULATION STATE
# ===========================================================================


class _PopulationState:
    """Global win-rate tracker per discipline across all horses."""

    __slots__ = ("wins", "runs")

    def __init__(self) -> None:
        self.wins: dict[str, int] = defaultdict(int)
        self.runs: dict[str, int] = defaultdict(int)

    def win_rate(self, discipline: Optional[str]) -> Optional[float]:
        if discipline is None:
            return None
        r = self.runs.get(discipline, 0)
        if r == 0:
            return None
        return round(self.wins.get(discipline, 0) / r, 4)

    def update(self, discipline: Optional[str], is_winner: bool) -> None:
        if discipline is None:
            return
        self.runs[discipline] += 1
        if is_winner:
            self.wins[discipline] += 1


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_discipline_specialist_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build discipline specialist features from partants_master.jsonl."""
    logger.info("=== Discipline Specialist Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Read minimal fields into memory
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Resolve discipline from multiple possible field names
        raw_disc = rec.get("discipline") or rec.get("specialite") or rec.get("type_course")
        discipline = _normalize_discipline(raw_disc)

        horse_id = rec.get("horse_id") or rec.get("nom_cheval")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", "") or "",
            "course": rec.get("course_uid", "") or "",
            "num": int(rec.get("num_pmu") or 0),
            "horse_id": str(horse_id).strip() if horse_id else None,
            "discipline": discipline,
            "is_winner": _safe_bool(rec.get("is_gagnant")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # ------------------------------------------------------------------
    # Phase 3: Process race by race (group by course_uid + date)
    # ------------------------------------------------------------------
    t2 = time.time()

    horse_states: dict[str, _HorseDisciplineState] = defaultdict(_HorseDisciplineState)
    pop_state = _PopulationState()
    results: list[dict[str, Any]] = []
    n_processed = 0
    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot: compute features BEFORE updating states --
        for rec in course_group:
            disc = rec["discipline"]
            hid = rec["horse_id"]

            feat: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "ds_discipline_code": _discipline_code(disc),
                "ds_horse_discipline_wr": None,
                "ds_horse_discipline_runs": None,
                "ds_horse_main_discipline": None,
                "ds_is_main_discipline": None,
                "ds_cross_discipline_switch": None,
                "ds_discipline_specialist": None,
                "ds_discipline_population_wr": pop_state.win_rate(disc),
            }

            if hid:
                horse_snap = horse_states[hid].snapshot(disc)
                feat.update(horse_snap)

            results.append(feat)

        # -- Update states after snapshotting (strictly post-race) --
        for rec in course_group:
            disc = rec["discipline"]
            hid = rec["horse_id"]
            is_w = rec["is_winner"]

            if hid:
                horse_states[hid].update(disc, is_w)
            pop_state.update(disc, is_w)

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info(
                "  Traite %d / %d records (%.0f%%)",
                n_processed, total, 100 * n_processed / total,
            )

    elapsed = time.time() - t0
    logger.info(
        "Discipline specialist build termine: %d features en %.1fs "
        "(chevaux uniques: %d, disciplines vues: %s)",
        len(results),
        elapsed,
        len(horse_states),
        list(pop_state.runs.keys()),
    )

    gc.collect()
    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file: CLI arg > D:/ canonical path > local candidates."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    for candidate in _LOCAL_INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Aucun fichier d'entree trouve. Essayez --input /chemin/vers/partants_master.jsonl"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features discipline_specialist a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/discipline_specialist/)",
    )
    args = parser.parse_args()

    logger = setup_logging("discipline_specialist_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_discipline_specialist_features(input_path, logger)

    out_path = output_dir / "discipline_specialist.jsonl"
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
                "  %-35s: %d/%d (%.1f%%)",
                k, v, total_count, 100 * v / total_count,
            )

    logger.info("Done.")


if __name__ == "__main__":
    main()
