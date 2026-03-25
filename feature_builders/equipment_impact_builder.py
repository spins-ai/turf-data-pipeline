#!/usr/bin/env python3
"""
feature_builders.equipment_impact_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Equipment change impact features measuring how oeilleres and deferre changes
correlate with performance improvements.

Temporal integrity: for any partant at date D, only races with date < D
contribute to equipment history -- no future leakage.

Produces:
  - equipment_impact_features.jsonl   in output/equipment_impact/

Features per partant (4):
  - oeilleres_change_impact   : win rate WITH oeilleres minus win rate WITHOUT
                                 for this horse. Positive = oeilleres help.
  - first_time_oeilleres_boost: 1 if this is the first race with oeilleres
                                 after previously racing without. 0 otherwise.
  - deferre_change_impact     : win rate with current deferre config minus
                                 win rate with previous config. Positive = helps.
  - equipment_stability_score : fraction of recent races (last 5) where the
                                 horse had the same equipment config as today.
                                 1.0 = very stable, 0.0 = always changing.

Usage:
    python feature_builders/equipment_impact_builder.py
    python feature_builders/equipment_impact_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "equipment_impact"

_LOG_EVERY = 500_000

# Stability window
_STABILITY_WINDOW = 5


# ===========================================================================
# EQUIPMENT STATE TRACKER
# ===========================================================================


class _EquipState:
    """Per-horse equipment performance tracker."""

    __slots__ = (
        "wins_with_oeilleres",
        "races_with_oeilleres",
        "wins_without_oeilleres",
        "races_without_oeilleres",
        "ever_had_oeilleres",
        "wins_by_deferre",
        "races_by_deferre",
        "prev_deferre",
        "recent_configs",  # list of last N (oeilleres, deferre) tuples
    )

    def __init__(self) -> None:
        self.wins_with_oeilleres: int = 0
        self.races_with_oeilleres: int = 0
        self.wins_without_oeilleres: int = 0
        self.races_without_oeilleres: int = 0
        self.ever_had_oeilleres: bool = False
        self.wins_by_deferre: dict[str, int] = defaultdict(int)
        self.races_by_deferre: dict[str, int] = defaultdict(int)
        self.prev_deferre: Optional[str] = None
        self.recent_configs: list[tuple[str, str]] = []


# ===========================================================================
# HELPERS
# ===========================================================================


def _normalise_oeilleres(raw: Any) -> str:
    """Normalise oeilleres to 'WITH' or 'WITHOUT'."""
    if not raw or str(raw).upper() in ("SANS", "", "NONE", "0"):
        return "WITHOUT"
    return "WITH"


def _normalise_deferre(raw: Any) -> str:
    """Normalise deferre to a canonical string."""
    if not raw or str(raw).upper() in ("SANS", "", "NONE", "0"):
        return "SANS"
    return str(raw).upper().strip()


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
# MAIN BUILD
# ===========================================================================


def build_equipment_impact_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build equipment impact features from partants_master.jsonl."""
    logger.info("=== Equipment Impact Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "oeilleres": _normalise_oeilleres(rec.get("oeilleres")),
            "deferre": _normalise_deferre(rec.get("deferre")),
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process date by date --
    t2 = time.time()
    horse_state: dict[str, _EquipState] = defaultdict(_EquipState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        while i < total and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # -- Emit features (pre-update snapshot) --
        for rec in date_group:
            cheval = rec["cheval"]

            if not cheval:
                results.append({
                    "partant_uid": rec["uid"],
                    "oeilleres_change_impact": None,
                    "first_time_oeilleres_boost": None,
                    "deferre_change_impact": None,
                    "equipment_stability_score": None,
                })
                continue

            state = horse_state.get(cheval)
            total_races = 0 if state is None else (
                state.races_with_oeilleres + state.races_without_oeilleres
            )

            if state is None or total_races == 0:
                # No history
                results.append({
                    "partant_uid": rec["uid"],
                    "oeilleres_change_impact": None,
                    "first_time_oeilleres_boost": None,
                    "deferre_change_impact": None,
                    "equipment_stability_score": None,
                })
                continue

            # -- oeilleres_change_impact --
            wr_with = (
                state.wins_with_oeilleres / state.races_with_oeilleres
                if state.races_with_oeilleres > 0
                else None
            )
            wr_without = (
                state.wins_without_oeilleres / state.races_without_oeilleres
                if state.races_without_oeilleres > 0
                else None
            )
            if wr_with is not None and wr_without is not None:
                oeilleres_impact = round(wr_with - wr_without, 6)
            else:
                oeilleres_impact = None

            # -- first_time_oeilleres_boost --
            current_oeilleres = rec["oeilleres"]
            if (
                current_oeilleres == "WITH"
                and not state.ever_had_oeilleres
            ):
                first_time_boost = 1
            else:
                first_time_boost = 0

            # -- deferre_change_impact --
            current_deferre = rec["deferre"]
            prev_deferre = state.prev_deferre
            if prev_deferre is not None and prev_deferre != current_deferre:
                wr_current = (
                    state.wins_by_deferre.get(current_deferre, 0)
                    / state.races_by_deferre[current_deferre]
                    if state.races_by_deferre.get(current_deferre, 0) > 0
                    else None
                )
                wr_prev = (
                    state.wins_by_deferre.get(prev_deferre, 0)
                    / state.races_by_deferre[prev_deferre]
                    if state.races_by_deferre.get(prev_deferre, 0) > 0
                    else None
                )
                if wr_current is not None and wr_prev is not None:
                    deferre_impact = round(wr_current - wr_prev, 6)
                else:
                    deferre_impact = None
            else:
                deferre_impact = 0.0 if prev_deferre is not None else None

            # -- equipment_stability_score --
            today_config = (current_oeilleres, current_deferre)
            recent = state.recent_configs
            if len(recent) > 0:
                matches = sum(1 for cfg in recent if cfg == today_config)
                stability = round(matches / len(recent), 4)
            else:
                stability = None

            results.append({
                "partant_uid": rec["uid"],
                "oeilleres_change_impact": oeilleres_impact,
                "first_time_oeilleres_boost": first_time_boost,
                "deferre_change_impact": deferre_impact,
                "equipment_stability_score": stability,
            })

        # -- Update state with this date's outcomes --
        for rec in date_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            state = horse_state[cheval]
            oeilleres = rec["oeilleres"]
            deferre = rec["deferre"]
            gagnant = rec["gagnant"]

            if oeilleres == "WITH":
                state.races_with_oeilleres += 1
                if gagnant:
                    state.wins_with_oeilleres += 1
                state.ever_had_oeilleres = True
            else:
                state.races_without_oeilleres += 1
                if gagnant:
                    state.wins_without_oeilleres += 1

            state.races_by_deferre[deferre] += 1
            if gagnant:
                state.wins_by_deferre[deferre] += 1
            state.prev_deferre = deferre

            # Track recent configs (sliding window)
            state.recent_configs.append((oeilleres, deferre))
            if len(state.recent_configs) > _STABILITY_WINDOW:
                state.recent_configs.pop(0)

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Equipment impact build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_state),
    )

    return results


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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features equipment impact a partir de partants_master"
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
        help="Repertoire de sortie (defaut: output/equipment_impact/)",
    )
    args = parser.parse_args()

    logger = setup_logging("equipment_impact_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_equipment_impact_features(input_path, logger)

    # Save
    out_path = output_dir / "equipment_impact_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
