#!/usr/bin/env python3
"""
feature_builders.owner_investment_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Owner / proprietaire investment and strategy features.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant owner investment features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - owner_investment_features.jsonl  in
    D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/owner_investment/

Features per partant (8):
  - own_nb_horses_active       : nb of distinct horses this owner has raced (up to now)
  - own_win_rate               : owner's overall win rate up to this point
  - own_avg_earnings_per_horse : owner's total earnings / nb_horses (proxy for investment return)
  - own_multi_entry_race       : 1 if owner has multiple horses in this same race
  - own_investment_level       : 0=low / 1=medium / 2=high (nb_horses * avg_earnings categorised)
  - own_trainer_loyalty        : fraction of owner's horses trained by this horse's current trainer
  - own_recent_form_30d        : owner's win rate in the last 30 days (rolling)
  - own_class_tendency         : average race value of owner's runners (allocation or prix_course)

Usage:
    python feature_builders/owner_investment_builder.py
    python feature_builders/owner_investment_builder.py --input /path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections import Counter, defaultdict
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Primary input: absolute production path
_INPUT_PRIMARY = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")

# Fallback candidates relative to project root
INPUT_CANDIDATES = [
    _INPUT_PRIMARY,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

OUTPUT_DIR = Path(
    "D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/owner_investment"
)
OUTPUT_FILENAME = "owner_investment_features.jsonl"

_LOG_EVERY = 500_000

# Investment level thresholds (score = nb_horses * avg_earnings_per_horse)
_INV_MEDIUM_THRESHOLD = 50_000   # score >= this  => medium
_INV_HIGH_THRESHOLD   = 500_000  # score >= this  => high

# Rolling window for recent form
_RECENT_FORM_DAYS = 30

# Minimum horses trained by the same trainer to compute loyalty meaningfully
_MIN_TRAINER_LOYALTY_RUNS = 1


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


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN guard
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


def _investment_level(nb_horses: int, avg_earnings: Optional[float]) -> Optional[int]:
    """
    Categorise investment level as 0 (low) / 1 (medium) / 2 (high).

    Score = nb_horses * avg_earnings_per_horse.
    Returns None when there are no past horses yet.
    """
    if nb_horses == 0:
        return None
    e = avg_earnings if avg_earnings is not None else 0.0
    score = nb_horses * e
    if score >= _INV_HIGH_THRESHOLD:
        return 2
    if score >= _INV_MEDIUM_THRESHOLD:
        return 1
    return 0


# ===========================================================================
# PER-OWNER STATE
# ===========================================================================


class _OwnerState:
    """Per-owner accumulated state for investment and strategy features."""

    __slots__ = (
        "horses",
        "wins",
        "total",
        "earnings",
        "trainers",
        "recent_races",
        "class_values",
    )

    def __init__(self) -> None:
        self.horses: set[str] = set()
        self.wins: int = 0
        self.total: int = 0
        self.earnings: float = 0.0
        # Counter of trainer names across all of this owner's horses
        self.trainers: Counter = Counter()
        # deque of (date: datetime, is_win: bool) for recent_form_30d
        self.recent_races: deque = deque()
        # list of race class/value figures for own_class_tendency
        self.class_values: list[float] = []

    # ------------------------------------------------------------------
    # Snapshot (features BEFORE updating with the current race)
    # ------------------------------------------------------------------

    def snapshot(
        self,
        current_trainer: Optional[str],
        current_date: Optional[datetime],
        class_val: Optional[float],
    ) -> dict[str, Any]:
        """Return feature dict using only past races (strict temporal integrity)."""

        nb_horses = len(self.horses)

        # own_win_rate
        own_win_rate: Optional[float] = None
        if self.total > 0:
            own_win_rate = round(self.wins / self.total, 4)

        # own_avg_earnings_per_horse
        own_avg_earnings: Optional[float] = None
        if nb_horses > 0:
            own_avg_earnings = round(self.earnings / nb_horses, 2)

        # own_investment_level
        own_inv_level = _investment_level(nb_horses, own_avg_earnings)

        # own_trainer_loyalty
        own_trainer_loyalty: Optional[float] = None
        if current_trainer and self.total > 0:
            trainer_runs = self.trainers.get(current_trainer, 0)
            own_trainer_loyalty = round(trainer_runs / self.total, 4)

        # own_recent_form_30d  (already pruned on update, but prune once more for safety)
        own_recent_form_30d: Optional[float] = None
        if current_date is not None:
            cutoff = current_date - timedelta(days=_RECENT_FORM_DAYS)
            recent_list = [
                entry for entry in self.recent_races if entry[0] >= cutoff
            ]
            if recent_list:
                wins_recent = sum(1 for _, w in recent_list if w)
                own_recent_form_30d = round(wins_recent / len(recent_list), 4)

        # own_class_tendency
        own_class_tendency: Optional[float] = None
        if self.class_values:
            own_class_tendency = round(
                sum(self.class_values) / len(self.class_values), 2
            )

        return {
            "own_nb_horses_active": nb_horses if nb_horses > 0 else None,
            "own_win_rate": own_win_rate,
            "own_avg_earnings_per_horse": own_avg_earnings,
            "own_investment_level": own_inv_level,
            "own_trainer_loyalty": own_trainer_loyalty,
            "own_recent_form_30d": own_recent_form_30d,
            "own_class_tendency": own_class_tendency,
        }

    # ------------------------------------------------------------------
    # Update (post-race, called after snapshotting)
    # ------------------------------------------------------------------

    def update(
        self,
        horse_id: Optional[str],
        trainer: Optional[str],
        is_win: bool,
        earnings_delta: float,
        race_date: Optional[datetime],
        class_val: Optional[float],
    ) -> None:
        """Incorporate a completed race into the owner's history."""
        if horse_id:
            self.horses.add(horse_id)
        self.total += 1
        if is_win:
            self.wins += 1
        self.earnings += earnings_delta
        if trainer:
            self.trainers[trainer] += 1
        if race_date is not None:
            self.recent_races.append((race_date, is_win))
            # Prune entries older than 30 days to keep deque bounded
            cutoff = race_date - timedelta(days=_RECENT_FORM_DAYS)
            while self.recent_races and self.recent_races[0][0] < cutoff:
                self.recent_races.popleft()
        if class_val is not None and class_val > 0:
            self.class_values.append(class_val)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_owner_investment_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build owner investment features from partants_master.jsonl."""
    logger.info("=== Owner Investment Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # ------------------------------------------------------------------
    # Phase 1: Read & slim records (streaming)
    # ------------------------------------------------------------------
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        # Resolve field aliases
        owner = (
            rec.get("proprietaire")
            or rec.get("proprietaire_nom")
            or ""
        ).strip()

        trainer = (
            rec.get("entraineur")
            or rec.get("nom_entraineur")
            or ""
        ).strip()

        horse_id = (
            rec.get("horse_id")
            or rec.get("nom_cheval")
            or ""
        ).strip()

        # Race class / value
        class_val = _safe_float(
            rec.get("allocation") or rec.get("prix_course")
        )

        # Earnings (gains_carriere_euros is career-total; we use gains_course if available,
        # else 0 -- it will be filled only when the record captures it)
        earnings_delta = _safe_float(
            rec.get("gains_course_euros")
            or rec.get("gains_course")
            or 0
        ) or 0.0

        position = _safe_int(rec.get("position_arrivee"))
        is_win = position == 1

        date_str = rec.get("date_reunion_iso", "")

        slim = {
            "uid": rec.get("partant_uid"),
            "date": date_str,
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "owner": owner,
            "trainer": trainer,
            "horse_id": horse_id if horse_id else None,
            "class_val": class_val,
            "earnings_delta": earnings_delta,
            "is_win": is_win,
            "date_obj": _parse_date(date_str),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # ------------------------------------------------------------------
    # Phase 2: Sort chronologically (date, course, num_pmu)
    # ------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)
    gc.collect()

    # ------------------------------------------------------------------
    # Phase 3: Process race-by-race (group by course_uid + date)
    # ------------------------------------------------------------------
    t2 = time.time()
    owner_states: dict[str, _OwnerState] = defaultdict(_OwnerState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date_str = slim_records[i]["date"]
        course_group: list[dict] = []

        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date_str
        ):
            course_group.append(slim_records[i])
            i += 1

        # ---- Compute own_multi_entry_race per owner within this race ----
        owner_horse_count: Counter = Counter(
            rec["owner"] for rec in course_group if rec["owner"]
        )

        # ---- Snapshot pre-race features for each runner ----
        for rec in course_group:
            owner = rec["owner"]
            feats: dict[str, Any] = {
                "partant_uid": rec["uid"],
                "own_nb_horses_active": None,
                "own_win_rate": None,
                "own_avg_earnings_per_horse": None,
                "own_multi_entry_race": None,
                "own_investment_level": None,
                "own_trainer_loyalty": None,
                "own_recent_form_30d": None,
                "own_class_tendency": None,
            }

            if owner:
                state = owner_states[owner]
                snap = state.snapshot(
                    current_trainer=rec["trainer"] or None,
                    current_date=rec["date_obj"],
                    class_val=rec["class_val"],
                )
                feats.update(snap)

                # own_multi_entry_race (can be computed from current race, not future)
                feats["own_multi_entry_race"] = (
                    1 if owner_horse_count[owner] > 1 else 0
                )

            results.append(feats)

        # ---- Update owner states after snapshotting (post-race) ----
        for rec in course_group:
            owner = rec["owner"]
            if owner:
                owner_states[owner].update(
                    horse_id=rec["horse_id"],
                    trainer=rec["trainer"] or None,
                    is_win=rec["is_win"],
                    earnings_delta=rec["earnings_delta"],
                    race_date=rec["date_obj"],
                    class_val=rec["class_val"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info(
                "  Traite %d / %d records...", n_processed, total
            )
            gc.collect()

    elapsed = time.time() - t0
    logger.info(
        "Owner investment build termine: %d features en %.1fs"
        " (proprietaires uniques: %d)",
        len(results),
        elapsed,
        len(owner_states),
    )

    return results


# ===========================================================================
# SAVE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Construction des features proprietaire/investissement "
            "a partir de partants_master"
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
        help=(
            "Repertoire de sortie "
            "(defaut: D:/turf-data-pipeline/02_DONNEES_BRUTES/"
            "builder_outputs/owner_investment/)"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("owner_investment_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_owner_investment_features(input_path, logger)

    # Atomic write via save_jsonl (tmp + replace)
    out_path = output_dir / OUTPUT_FILENAME
    save_jsonl(results, out_path, logger)

    # --- Fill-rate summary ---
    if results:
        feature_keys = [k for k in results[0] if k != "partant_uid"]
        filled: dict[str, int] = {k: 0 for k in feature_keys}
        for r in results:
            for k in feature_keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_count = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info(
                "  %-35s : %d/%d (%.1f%%)",
                k,
                v,
                total_count,
                100.0 * v / total_count,
            )


if __name__ == "__main__":
    main()
