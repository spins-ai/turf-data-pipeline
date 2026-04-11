#!/usr/bin/env python3
"""
feature_builders.trainer_horse_match_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Measures how well-suited a trainer is for a specific horse type.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant trainer/horse-type affinity
features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Features per partant:
  - thm_trainer_age_group_wr        : trainer's win rate with this horse's age group
  - thm_trainer_sex_wr              : trainer's win rate with horses of this sex
  - thm_trainer_young_specialist    : 1 if trainer has >60% of runners aged 2-3
  - thm_trainer_distance_match_wr   : trainer's win rate at this distance bucket
  - thm_trainer_surface_match_wr    : trainer's win rate on this surface type
  - thm_trainer_versatility         : nb distinct distance buckets where trainer has won
  - thm_trainer_stable_size         : total number of distinct horses trainer has run
  - thm_trainer_recent_wr_30d       : trainer's win rate in last 30 days

Usage:
    python feature_builders/trainer_horse_match_builder.py
    python feature_builders/trainer_horse_match_builder.py --input /path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/trainer_horse_match")

_LOG_EVERY = 500_000

# Distance buckets (metres)
_DISTANCE_BUCKETS = [
    ("sprint",   0,     1300),
    ("mile",     1300,  1900),
    ("middle",   1900,  2500),
    ("staying",  2500,  float("inf")),
]

# Surface normalisation map
_SURFACE_MAP = {
    "plat":         "turf",
    "herbe":        "turf",
    "gazon":        "turf",
    "psg":          "allweather",
    "polytrack":    "allweather",
    "sable":        "allweather",
    "piste":        "allweather",
    "obstacle":     "jumps",
    "haie":         "jumps",
    "steeple":      "jumps",
    "cross":        "jumps",
    "trot":         "trot",
    "attelé":       "trot",
    "attele":       "trot",
    "monté":        "trot",
    "monte":        "trot",
}

# "Young" age groups (2-3 year-olds)
_YOUNG_AGES = {2, 3}

# Recent-form window
_RECENT_DAYS = 30

# Minimum young-specialist threshold
_YOUNG_SPECIALIST_THRESHOLD = 0.60


# ===========================================================================
# HELPERS
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # filter NaN
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _distance_bucket(distance_m: Optional[float]) -> Optional[str]:
    """Return the bucket label for a distance in metres."""
    if distance_m is None or distance_m <= 0:
        return None
    for label, lo, hi in _DISTANCE_BUCKETS:
        if lo <= distance_m < hi:
            return label
    return "staying"


def _normalise_surface(raw: Optional[str]) -> Optional[str]:
    """Normalise raw surface/discipline string to a canonical surface label."""
    if not raw:
        return None
    key = raw.strip().lower()
    # Try exact key first, then check if any canonical key is contained in raw
    if key in _SURFACE_MAP:
        return _SURFACE_MAP[key]
    for k, v in _SURFACE_MAP.items():
        if k in key:
            return v
    return key  # keep raw lowercased if unknown


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO date string (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS...) to datetime."""
    if not date_str:
        return None
    s = str(date_str).strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return None


def _wr(wins: int, total: int) -> Optional[float]:
    """Win rate. Returns None if no observations."""
    if total == 0:
        return None
    return round(wins / total, 4)


# ===========================================================================
# PER-TRAINER STATE
# ===========================================================================


class _TrainerState:
    """Accumulated state for one trainer.

    All counters are updated AFTER the snapshot for a race is taken, ensuring
    strict temporal integrity.
    """

    __slots__ = (
        "per_age_group",
        "per_sex",
        "per_distance",
        "per_surface",
        "horses",
        "recent_races",
    )

    def __init__(self) -> None:
        # {group_key: [wins, total]}
        self.per_age_group: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.per_sex: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.per_distance: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        self.per_surface: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        # Distinct horse identifiers seen by this trainer
        self.horses: set[str] = set()
        # Sliding window of (date, won) for recent_wr_30d
        self.recent_races: deque = deque(maxlen=200)

    # ------------------------------------------------------------------
    # SNAPSHOT (features BEFORE update)
    # ------------------------------------------------------------------

    def snapshot(
        self,
        age: Optional[int],
        sex: Optional[str],
        dist_bucket: Optional[str],
        surface: Optional[str],
        race_date: Optional[datetime],
    ) -> dict[str, Any]:
        """Return feature dict using only past data (strict temporal leakage-free)."""

        # 1. Trainer win rate for this age group
        age_group = _age_to_group(age)
        thm_trainer_age_group_wr: Optional[float] = None
        if age_group is not None:
            w, t = self.per_age_group.get(age_group, [0, 0])
            thm_trainer_age_group_wr = _wr(w, t)

        # 2. Trainer win rate for this sex
        thm_trainer_sex_wr: Optional[float] = None
        if sex:
            w, t = self.per_sex.get(sex, [0, 0])
            thm_trainer_sex_wr = _wr(w, t)

        # 3. Young specialist flag
        thm_trainer_young_specialist: Optional[int] = None
        total_young = sum(
            self.per_age_group.get(g, [0, 0])[1]
            for g in ("2", "3")
        )
        total_all = sum(v[1] for v in self.per_age_group.values())
        if total_all >= 10:
            thm_trainer_young_specialist = (
                1 if (total_young / total_all) > _YOUNG_SPECIALIST_THRESHOLD else 0
            )

        # 4. Trainer win rate at this distance bucket
        thm_trainer_distance_match_wr: Optional[float] = None
        if dist_bucket is not None:
            w, t = self.per_distance.get(dist_bucket, [0, 0])
            thm_trainer_distance_match_wr = _wr(w, t)

        # 5. Trainer win rate on this surface
        thm_trainer_surface_match_wr: Optional[float] = None
        if surface is not None:
            w, t = self.per_surface.get(surface, [0, 0])
            thm_trainer_surface_match_wr = _wr(w, t)

        # 6. Versatility: distinct distance buckets where trainer has at least 1 win
        thm_trainer_versatility: int = sum(
            1 for v in self.per_distance.values() if v[0] > 0
        )

        # 7. Stable size: distinct horses trained
        thm_trainer_stable_size: int = len(self.horses)

        # 8. Recent win rate (last 30 days)
        thm_trainer_recent_wr_30d: Optional[float] = None
        if race_date is not None and self.recent_races:
            cutoff = race_date - timedelta(days=_RECENT_DAYS)
            recent = [won for dt, won in self.recent_races if dt >= cutoff]
            if recent:
                thm_trainer_recent_wr_30d = round(sum(recent) / len(recent), 4)

        return {
            "thm_trainer_age_group_wr": thm_trainer_age_group_wr,
            "thm_trainer_sex_wr": thm_trainer_sex_wr,
            "thm_trainer_young_specialist": thm_trainer_young_specialist,
            "thm_trainer_distance_match_wr": thm_trainer_distance_match_wr,
            "thm_trainer_surface_match_wr": thm_trainer_surface_match_wr,
            "thm_trainer_versatility": thm_trainer_versatility if thm_trainer_versatility > 0 else None,
            "thm_trainer_stable_size": thm_trainer_stable_size if thm_trainer_stable_size > 0 else None,
            "thm_trainer_recent_wr_30d": thm_trainer_recent_wr_30d,
        }

    # ------------------------------------------------------------------
    # UPDATE (post-race)
    # ------------------------------------------------------------------

    def update(
        self,
        age: Optional[int],
        sex: Optional[str],
        dist_bucket: Optional[str],
        surface: Optional[str],
        horse_id: Optional[str],
        race_date: Optional[datetime],
        won: bool,
    ) -> None:
        """Update state with race result. Called AFTER snapshot."""

        age_group = _age_to_group(age)
        if age_group is not None:
            cell = self.per_age_group[age_group]
            cell[1] += 1
            if won:
                cell[0] += 1

        if sex:
            cell = self.per_sex[sex]
            cell[1] += 1
            if won:
                cell[0] += 1

        if dist_bucket is not None:
            cell = self.per_distance[dist_bucket]
            cell[1] += 1
            if won:
                cell[0] += 1

        if surface is not None:
            cell = self.per_surface[surface]
            cell[1] += 1
            if won:
                cell[0] += 1

        if horse_id:
            self.horses.add(horse_id)

        if race_date is not None:
            self.recent_races.append((race_date, 1 if won else 0))


def _age_to_group(age: Optional[int]) -> Optional[str]:
    """Convert raw age integer to a string group key."""
    if age is None:
        return None
    return str(age)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_trainer_horse_match_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build trainer/horse-type affinity features from partants_master.jsonl."""
    logger.info("=== Trainer Horse Match Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -----------------------------------------------------------------------
    # Phase 1: Read minimal fields
    # -----------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Trainer identification (try multiple field names)
        entraineur = (
            rec.get("entraineur")
            or rec.get("nom_entraineur")
            or ""
        )
        entraineur = entraineur.strip() if entraineur else ""

        # Horse identification
        horse_id = (
            rec.get("horse_id")
            or rec.get("nom_cheval")
            or rec.get("partant_uid")
            or ""
        )
        horse_id = str(horse_id).strip() if horse_id else ""

        # Distance
        distance_raw = _safe_float(rec.get("distance"))
        dist_bucket = _distance_bucket(distance_raw)

        # Surface/discipline
        surface_raw = rec.get("etat_terrain") or rec.get("discipline") or None
        surface = _normalise_surface(surface_raw)

        # Age (horse age at time of race)
        age = _safe_int(rec.get("age"))

        # Sex
        sex_raw = rec.get("sexe") or None
        sex = sex_raw.strip().lower() if sex_raw else None

        # Is winner
        pos = rec.get("position_arrivee")
        is_gagnant = bool(rec.get("is_gagnant")) or (pos is not None and str(pos).strip() == "1")

        # Date
        date_str = rec.get("date_reunion_iso", "")
        race_date = _parse_date(date_str)

        slim_records.append({
            "uid": rec.get("partant_uid"),
            "date_str": date_str,
            "date": race_date,
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "entraineur": entraineur,
            "horse_id": horse_id,
            "age": age,
            "sex": sex,
            "dist_bucket": dist_bucket,
            "surface": surface,
            "won": is_gagnant,
        })

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -----------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # -----------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date_str"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -----------------------------------------------------------------------
    # Phase 3: Process course by course (group by course_uid + date)
    # -----------------------------------------------------------------------
    t2 = time.time()
    trainer_states: dict[str, _TrainerState] = defaultdict(_TrainerState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)
    i = 0

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date_str"]
        course_group: list[dict] = []

        # Collect all partants for this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date_str"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # -- Snapshot pre-race features BEFORE update --
        for rec in course_group:
            entraineur = rec["entraineur"]
            feat: dict[str, Any] = {"partant_uid": rec["uid"]}

            if entraineur:
                state = trainer_states[entraineur]
                snap = state.snapshot(
                    age=rec["age"],
                    sex=rec["sex"],
                    dist_bucket=rec["dist_bucket"],
                    surface=rec["surface"],
                    race_date=rec["date"],
                )
                feat.update(snap)
            else:
                feat.update({
                    "thm_trainer_age_group_wr": None,
                    "thm_trainer_sex_wr": None,
                    "thm_trainer_young_specialist": None,
                    "thm_trainer_distance_match_wr": None,
                    "thm_trainer_surface_match_wr": None,
                    "thm_trainer_versatility": None,
                    "thm_trainer_stable_size": None,
                    "thm_trainer_recent_wr_30d": None,
                })

            results.append(feat)

        # -- Update states AFTER snapshotting --
        for rec in course_group:
            entraineur = rec["entraineur"]
            if entraineur:
                trainer_states[entraineur].update(
                    age=rec["age"],
                    sex=rec["sex"],
                    dist_bucket=rec["dist_bucket"],
                    surface=rec["surface"],
                    horse_id=rec["horse_id"],
                    race_date=rec["date"],
                    won=rec["won"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Build termine: %d features en %.1fs (entraineurs uniques: %d)",
        len(results), elapsed, len(trainer_states),
    )

    # Free memory
    del slim_records
    del trainer_states
    gc.collect()

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path from CLI arg or default."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construction des features trainer/horse-type affinity"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: D:/turf-data-pipeline/...)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie",
    )
    args = parser.parse_args()

    logger = setup_logging("trainer_horse_match_builder")
    logger.info("=" * 70)
    logger.info("trainer_horse_match_builder.py — Trainer/Horse-type affinity")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_trainer_horse_match_features(input_path, logger)

    # Save
    out_path = output_dir / "trainer_horse_match.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill rate summary
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        total_count = len(results)
        logger.info("=== Fill rates (%d partants) ===", total_count)
        for k in feature_keys:
            filled = sum(1 for r in results if r.get(k) is not None)
            logger.info(
                "  %-40s %d/%d (%.1f%%)",
                k, filled, total_count, 100.0 * filled / total_count,
            )

    logger.info("Termine — %d partants traites", len(results))


if __name__ == "__main__":
    main()
