#!/usr/bin/env python3
"""
feature_builders.ground_going_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Computes detailed ground/going condition features and horse preferences.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant going-condition features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the going statistics -- no future leakage.

Terrain scale (turf):
  "bon"              -> 1  (firm/good)
  "bon souple"       -> 2  (good to soft)
  "souple"           -> 3  (soft)
  "tres souple/lourd"-> 4  (very soft / heavy)
  "collant"          -> 5  (sticky/holding)

All-weather/PSF:
  "standard"         -> 10
  "psf"              -> 10
  "leger"            -> 11
  "lourd"            -> 12

Produces:
  - ground_going.jsonl   in output/ground_going/

Features per partant (8):
  - gg_terrain_code          : numeric terrain code for current race (1-5 or 10-12)
  - gg_horse_terrain_win_rate: horse's win rate on this terrain type historically
  - gg_horse_preferred_terrain: terrain code where horse has best win rate
  - gg_terrain_match         : 1 if current terrain matches horse's preferred, else 0
  - gg_terrain_versatility   : number of different terrain types horse has won on
  - gg_horse_soft_vs_firm    : win rate on soft (code>=3) minus win rate on firm (code<=2)
  - gg_terrain_recent_form   : horse's avg position on this terrain in last 5 appearances
  - gg_field_terrain_advantage: proportion of field horses who prefer this terrain

Usage:
    python feature_builders/ground_going_builder.py
    python feature_builders/ground_going_builder.py --input /path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/ground_going")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
_OUTPUT_DIR_FALLBACK = _PROJECT_ROOT / "output" / "ground_going"

_LOG_EVERY = 500_000

# Terrain scale (1-5 for turf, 10-12 for all-weather)
TERRAIN_FIRM = 1
TERRAIN_GOOD_TO_SOFT = 2
TERRAIN_SOFT = 3
TERRAIN_HEAVY = 4
TERRAIN_STICKY = 5
TERRAIN_PSF_STANDARD = 10
TERRAIN_PSF_LIGHT = 11
TERRAIN_PSF_HEAVY = 12

# Threshold: code >= this is considered "soft"
_SOFT_THRESHOLD = 3
# Threshold: code <= this is considered "firm"
_FIRM_THRESHOLD = 2

# Recent form window (last N appearances on this terrain)
_RECENT_FORM_WINDOW = 5

# ===========================================================================
# TERRAIN NORMALISATION
# ===========================================================================

# Maps normalised label -> terrain code
_TERRAIN_LABEL_TO_CODE: dict[str, int] = {
    # Firm/good
    "bon": TERRAIN_FIRM,
    "good": TERRAIN_FIRM,
    "ferme": TERRAIN_FIRM,
    "b": TERRAIN_FIRM,
    # Good to soft
    "bon souple": TERRAIN_GOOD_TO_SOFT,
    "good to soft": TERRAIN_GOOD_TO_SOFT,
    "assez souple": TERRAIN_GOOD_TO_SOFT,
    "bon_souple": TERRAIN_GOOD_TO_SOFT,
    # Soft
    "souple": TERRAIN_SOFT,
    "soft": TERRAIN_SOFT,
    "s": TERRAIN_SOFT,
    # Very soft / heavy (turf)
    "tres souple": TERRAIN_HEAVY,
    "tres souple lourd": TERRAIN_HEAVY,
    "tres_souple": TERRAIN_HEAVY,
    "lourd": TERRAIN_HEAVY,
    "heavy": TERRAIN_HEAVY,
    "very soft": TERRAIN_HEAVY,
    "h": TERRAIN_HEAVY,
    # Sticky/collant
    "collant": TERRAIN_STICKY,
    "sticky": TERRAIN_STICKY,
    "holding": TERRAIN_STICKY,
    # All-weather / PSF standard
    "standard": TERRAIN_PSF_STANDARD,
    "psf": TERRAIN_PSF_STANDARD,
    "all weather": TERRAIN_PSF_STANDARD,
    "all-weather": TERRAIN_PSF_STANDARD,
    "polytrack": TERRAIN_PSF_STANDARD,
    "fibresand": TERRAIN_PSF_STANDARD,
    "tapeta": TERRAIN_PSF_STANDARD,
    "synthétique": TERRAIN_PSF_STANDARD,
    "synthetique": TERRAIN_PSF_STANDARD,
    # All-weather light
    "leger": TERRAIN_PSF_LIGHT,
    "light": TERRAIN_PSF_LIGHT,
    # All-weather heavy / lourd (PSF context)
    # Note: "lourd" is also terrain 4 for turf; we default turf interpretation.
    # PSF-specific labels resolved here if encountered with PSF prefix.
    "psf lourd": TERRAIN_PSF_HEAVY,
    "psf standard": TERRAIN_PSF_STANDARD,
    "psf leger": TERRAIN_PSF_LIGHT,
}


def _normalise_terrain_code(raw: Any) -> Optional[int]:
    """Normalise a raw terrain string to a numeric code, or None if unknown."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = int(raw)
        if v in (1, 2, 3, 4, 5, 10, 11, 12):
            return v
        return None
    if not isinstance(raw, str):
        return None
    key = raw.strip().lower()
    # Direct lookup
    code = _TERRAIN_LABEL_TO_CODE.get(key)
    if code is not None:
        return code
    # Substring matching: longest matching key wins
    best_match: Optional[int] = None
    best_len = 0
    for label, c in _TERRAIN_LABEL_TO_CODE.items():
        if label in key and len(label) > best_len:
            best_match = c
            best_len = len(label)
    return best_match


def _extract_terrain_code(rec: dict) -> Optional[int]:
    """Extract terrain code from a partant record, trying multiple fields."""
    for field in ("etat_terrain", "terrain", "cnd_cond_type_terrain",
                  "met_terrain_predit", "nature_terrain"):
        val = rec.get(field)
        if val is not None:
            code = _normalise_terrain_code(val)
            if code is not None:
                return code
    return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseGroundState:
    """Tracks per-horse statistics across terrain codes."""

    __slots__ = ("terrain_wins", "terrain_runs", "terrain_positions", "total_runs")

    def __init__(self) -> None:
        # terrain_code -> win count
        self.terrain_wins: dict[int, int] = defaultdict(int)
        # terrain_code -> total run count
        self.terrain_runs: dict[int, int] = defaultdict(int)
        # terrain_code -> deque of last N positions (int, low=better)
        self.terrain_positions: dict[int, deque] = defaultdict(
            lambda: deque(maxlen=_RECENT_FORM_WINDOW)
        )
        self.total_runs: int = 0

    def win_rate_for(self, code: int) -> Optional[float]:
        """Win rate at a specific terrain code, or None if no runs."""
        runs = self.terrain_runs.get(code, 0)
        if runs == 0:
            return None
        return self.terrain_wins.get(code, 0) / runs

    def preferred_terrain(self) -> Optional[int]:
        """Terrain code with highest win rate (min 1 run). Ties by most runs."""
        best_code: Optional[int] = None
        best_rate = -1.0
        best_runs = 0
        for code, runs in self.terrain_runs.items():
            if runs == 0:
                continue
            rate = self.terrain_wins.get(code, 0) / runs
            if rate > best_rate or (rate == best_rate and runs > best_runs):
                best_rate = rate
                best_code = code
                best_runs = runs
        return best_code

    def versatility(self) -> int:
        """Number of distinct terrain types on which the horse has won at least once."""
        return sum(1 for code, wins in self.terrain_wins.items() if wins > 0)

    def soft_vs_firm(self) -> Optional[float]:
        """Win rate on soft terrain (code >= 3) minus win rate on firm (code <= 2).

        Returns None if the horse has no runs on either category.
        """
        soft_wins = 0
        soft_runs = 0
        firm_wins = 0
        firm_runs = 0
        for code, runs in self.terrain_runs.items():
            # Only turf codes (1-5); skip all-weather (10-12) for this metric
            if code > 9:
                continue
            wins = self.terrain_wins.get(code, 0)
            if code >= _SOFT_THRESHOLD:
                soft_wins += wins
                soft_runs += runs
            elif code <= _FIRM_THRESHOLD:
                firm_wins += wins
                firm_runs += runs

        wr_soft = (soft_wins / soft_runs) if soft_runs > 0 else None
        wr_firm = (firm_wins / firm_runs) if firm_runs > 0 else None

        if wr_soft is None and wr_firm is None:
            return None
        # If only one side is known, report difference relative to zero
        sv = wr_soft if wr_soft is not None else 0.0
        fv = wr_firm if wr_firm is not None else 0.0
        return sv - fv

    def recent_form_on_terrain(self, code: int) -> Optional[float]:
        """Average finishing position in last _RECENT_FORM_WINDOW runs on this terrain.

        Lower is better (1 = winner). Returns None if no history on this terrain.
        """
        positions = self.terrain_positions.get(code)
        if not positions:
            return None
        valid = [p for p in positions if p is not None]
        if not valid:
            return None
        return sum(valid) / len(valid)

    def update(self, code: int, is_winner: bool, position: Optional[int]) -> None:
        """Record a race result (called AFTER snapshotting pre-race features)."""
        self.terrain_runs[code] += 1
        self.total_runs += 1
        if is_winner:
            self.terrain_wins[code] += 1
        # Track position in rolling window for recent form
        self.terrain_positions[code].append(position)


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
# NULL FEATURE HELPER
# ===========================================================================


def _null_features(uid) -> dict[str, Any]:
    """Return a feature dict with all null values."""
    return {
        "partant_uid": uid,
        "gg_terrain_code": None,
        "gg_horse_terrain_win_rate": None,
        "gg_horse_preferred_terrain": None,
        "gg_terrain_match": None,
        "gg_terrain_versatility": None,
        "gg_horse_soft_vs_firm": None,
        "gg_terrain_recent_form": None,
        "gg_field_terrain_advantage": None,
    }


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_ground_going_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build ground/going features from partants_master.jsonl.

    Algorithm (strict temporal integrity):
      1. Read minimal fields into memory (slim records).
      2. Sort chronologically by (date, course_uid, num_pmu).
      3. Group records by course_uid.
      4. For each course group:
         a. Snapshot pre-race features for all horses (using PAST data only).
         b. Compute gg_field_terrain_advantage using the pre-race preferred terrain
            of each horse in the field.
         c. Update horse state with race results.
    """
    logger.info("=== Ground Going Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        # Parse position_arrivee
        pos_raw = rec.get("position_arrivee")
        try:
            position = int(pos_raw) if pos_raw is not None else None
        except (ValueError, TypeError):
            position = None

        # Resolve horse identity (prefer partant_uid compound or nom_cheval)
        horse_id = (rec.get("horse_id") or rec.get("cheval_id")
                    or rec.get("nom_cheval") or "").strip()

        slim_records.append({
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": int(rec.get("num_pmu") or 0),
            "horse": horse_id,
            "terrain_code": _extract_terrain_code(rec),
            "is_gagnant": bool(rec.get("is_gagnant")),
            "position": position,
            "nombre_partants": rec.get("nombre_partants"),
        })

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course --
    t2 = time.time()
    horse_states: dict[str, _HorseGroundState] = defaultdict(_HorseGroundState)
    results: list[dict[str, Any]] = []
    n_processed = 0
    total = len(slim_records)
    i = 0

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        # Collect all records belonging to this course
        while (
            i < total
            and slim_records[i]["course"] == course_uid
            and slim_records[i]["date"] == course_date
        ):
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        # --- Step A: Compute pre-race preferred terrain for each horse in field ---
        # This is used for gg_field_terrain_advantage.
        # We gather preferred terrain BEFORE updating any states.
        field_size = len(course_group)
        pre_race_preferred: list[Optional[int]] = []
        for rec in course_group:
            horse = rec["horse"]
            if horse:
                pref = horse_states[horse].preferred_terrain()
            else:
                pref = None
            pre_race_preferred.append(pref)

        # --- Step B: Snapshot pre-race features for each partant ---
        pre_race_features: list[dict[str, Any]] = []

        for idx, rec in enumerate(course_group):
            horse = rec["horse"]
            terrain_code = rec["terrain_code"]

            if not horse:
                pre_race_features.append(_null_features(rec["uid"]))
                continue

            state = horse_states[horse]
            pref_terrain = pre_race_preferred[idx]

            # gg_terrain_code
            feat_terrain_code = terrain_code  # may be None

            # gg_horse_terrain_win_rate
            if terrain_code is not None:
                wr = state.win_rate_for(terrain_code)
                feat_wr = round(wr, 4) if wr is not None else None
            else:
                feat_wr = None

            # gg_horse_preferred_terrain
            feat_pref = pref_terrain  # int or None

            # gg_terrain_match
            if terrain_code is not None and pref_terrain is not None:
                feat_match = 1 if terrain_code == pref_terrain else 0
            else:
                feat_match = None

            # gg_terrain_versatility
            feat_versatility = state.versatility() if state.total_runs > 0 else 0

            # gg_horse_soft_vs_firm
            svf = state.soft_vs_firm()
            feat_svf = round(svf, 4) if svf is not None else None

            # gg_terrain_recent_form
            if terrain_code is not None:
                rf = state.recent_form_on_terrain(terrain_code)
                feat_recent_form = round(rf, 2) if rf is not None else None
            else:
                feat_recent_form = None

            # gg_field_terrain_advantage (computed after collecting all pre-race preferreds)
            # Will be filled in step C below; placeholder for now
            feat_field_adv = None  # filled below

            pre_race_features.append({
                "partant_uid": rec["uid"],
                "gg_terrain_code": feat_terrain_code,
                "gg_horse_terrain_win_rate": feat_wr,
                "gg_horse_preferred_terrain": feat_pref,
                "gg_terrain_match": feat_match,
                "gg_terrain_versatility": feat_versatility,
                "gg_horse_soft_vs_firm": feat_svf,
                "gg_terrain_recent_form": feat_recent_form,
                "gg_field_terrain_advantage": feat_field_adv,  # placeholder
            })

        # --- Step C: Compute gg_field_terrain_advantage for each horse ---
        # For each horse in the field, count how many other horses' preferred terrain
        # matches the current race terrain. Divide by field size for a proportion.
        # This gives a proxy for how favourable the terrain is for the current field.
        for idx, rec in enumerate(course_group):
            terrain_code = rec["terrain_code"]
            if terrain_code is None or field_size == 0:
                pre_race_features[idx]["gg_field_terrain_advantage"] = None
                continue
            # Count horses in the field whose preferred terrain == current terrain_code
            count_prefer = sum(
                1 for pref in pre_race_preferred if pref == terrain_code
            )
            pre_race_features[idx]["gg_field_terrain_advantage"] = round(
                count_prefer / field_size, 4
            )

        # Emit all pre-race features for this course
        results.extend(pre_race_features)

        # --- Step D: Update horse states AFTER snapshotting (temporal integrity) ---
        for rec in course_group:
            horse = rec["horse"]
            terrain_code = rec["terrain_code"]
            if horse and terrain_code is not None:
                horse_states[horse].update(
                    terrain_code,
                    rec["is_gagnant"],
                    rec["position"],
                )

        n_processed += len(course_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Ground going build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results), elapsed, len(horse_states),
    )

    # Free memory
    del slim_records
    del horse_states
    gc.collect()

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
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in _INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features ground/going a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: D:/turf-data-pipeline/...)",
    )
    args = parser.parse_args()

    logger = setup_logging("ground_going_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else (
        OUTPUT_DIR if OUTPUT_DIR.parent.exists() else _OUTPUT_DIR_FALLBACK
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_ground_going_features(input_path, logger)

    # Save
    out_path = output_dir / "ground_going.jsonl"
    save_jsonl(results, out_path, logger)

    # Fill-rate summary
    if results:
        keys = [k for k in results[0] if k != "partant_uid"]
        filled = {k: 0 for k in keys}
        for r in results:
            for k in keys:
                if r.get(k) is not None:
                    filled[k] += 1
        total_r = len(results)
        logger.info("=== Fill rates ===")
        for k in keys:
            v = filled[k]
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total_r, 100 * v / total_r)


if __name__ == "__main__":
    main()
