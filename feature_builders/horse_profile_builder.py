"""
feature_builders.horse_profile_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep horse profile features derived from career history.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.

Produces:
  - horse_profile_features.jsonl  in output/horse_profile/

Features per partant (6):
  - is_front_runner_by_history  : float, % of past races where horse finished top-3 from low corde (<=4)
  - preferred_distance_match    : 1.0 if today's distance category == horse's best win-rate category, else 0
  - preferred_terrain_match     : 1.0 if today's type_piste == horse's best win-rate terrain, else 0
  - career_roi                  : (total gains - total mise) / total mise  (mise = nb_courses * 1)
  - career_avg_beaten_length    : average beaten-length across career
  - versatility_score           : count of unique (distance_cat, type_piste, hippodrome) combos raced

Usage:
    python feature_builders/horse_profile_builder.py
    python feature_builders/horse_profile_builder.py --input data_master/partants_master.jsonl
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "horse_profile"

_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================

def _distance_category(distance: Optional[int]) -> Optional[str]:
    if distance is None:
        return None
    if distance <= 1200:
        return "sprint"
    elif distance <= 1600:
        return "mile"
    elif distance <= 2200:
        return "intermediaire"
    elif distance <= 3000:
        return "long"
    else:
        return "marathon"


def _safe_div(num: float, den: float) -> Optional[float]:
    return round(num / den, 4) if den > 0 else None


# ===========================================================================
# CAREER STATE TRACKER
# ===========================================================================

class _HorseProfile:
    """Lightweight per-horse career accumulator for profile features."""

    __slots__ = (
        "nb_courses", "gains_total", "wins",
        "beaten_lengths", "beaten_length_sum",
        "dist_cat_wins", "dist_cat_total",
        "terrain_wins", "terrain_total",
        "front_run_count", "front_run_eligible",
        "unique_combos",
    )

    def __init__(self) -> None:
        self.nb_courses: int = 0
        self.gains_total: float = 0.0
        self.wins: int = 0
        self.beaten_lengths: int = 0
        self.beaten_length_sum: float = 0.0
        self.dist_cat_wins: dict[str, int] = defaultdict(int)
        self.dist_cat_total: dict[str, int] = defaultdict(int)
        self.terrain_wins: dict[str, int] = defaultdict(int)
        self.terrain_total: dict[str, int] = defaultdict(int)
        self.front_run_count: int = 0
        self.front_run_eligible: int = 0
        self.unique_combos: set[tuple] = set()


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


def _sort_key(rec: dict) -> tuple:
    return (
        rec.get("date", ""),
        rec.get("course", ""),
        rec.get("num", 0) or 0,
    )


# ===========================================================================
# MAIN BUILD
# ===========================================================================

def build_horse_profile_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build horse profile features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory, sort chronologically.
      2. Process in order, accumulating per-horse profile.
         Features are emitted BEFORE updating state (strict temporal integrity).
    """
    logger.info("=== Horse Profile Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        pos_raw = rec.get("position_arrivee")
        pos = None
        if pos_raw is not None:
            try:
                pos = int(pos_raw)
            except (ValueError, TypeError):
                pos = None

        gains_raw = rec.get("gains")
        gains = 0.0
        if gains_raw is not None:
            try:
                gains = float(gains_raw)
            except (ValueError, TypeError):
                gains = 0.0

        dist_raw = rec.get("distance")
        dist = None
        if dist_raw is not None:
            try:
                dist = int(dist_raw)
            except (ValueError, TypeError):
                dist = None

        place_corde_raw = rec.get("place_corde")
        place_corde = None
        if place_corde_raw is not None:
            try:
                place_corde = int(place_corde_raw)
            except (ValueError, TypeError):
                place_corde = None

        ecart_raw = rec.get("ecart_premier") or rec.get("ecart_longueur")
        ecart = None
        if ecart_raw is not None:
            try:
                ecart = float(ecart_raw)
            except (ValueError, TypeError):
                ecart = None

        slim_records.append({
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "position": pos,
            "gains": gains,
            "distance": dist,
            "dist_cat": _distance_category(dist),
            "type_piste": rec.get("type_piste"),
            "hippodrome": rec.get("hippodrome_normalise", ""),
            "place_corde": place_corde,
            "ecart": ecart,
        })

    logger.info("Phase 1 terminee: %d records slim en memoire", len(slim_records))

    # -- Phase 1b: Sort chronologically --
    slim_records.sort(key=_sort_key)
    logger.info("Tri chronologique termine")

    # -- Phase 2: Compute features --
    horse_state: dict[str, _HorseProfile] = defaultdict(_HorseProfile)
    results: list[dict[str, Any]] = []

    for i, rec in enumerate(slim_records):
        if i > 0 and i % _LOG_EVERY == 0:
            logger.info("  Traitement %d / %d...", i, len(slim_records))

        cheval = rec["cheval"]
        if not cheval:
            continue

        st = horse_state[cheval]

        # ---- EMIT features BEFORE update (temporal integrity) ----
        feat: dict[str, Any] = {"partant_uid": rec["uid"]}

        # 1. is_front_runner_by_history
        if st.front_run_eligible > 0:
            feat["is_front_runner_by_history"] = round(
                st.front_run_count / st.front_run_eligible, 4
            )
        else:
            feat["is_front_runner_by_history"] = None

        # 2. preferred_distance_match
        if st.nb_courses >= 3 and rec["dist_cat"]:
            best_dist = max(
                st.dist_cat_wins.keys(),
                key=lambda k: st.dist_cat_wins[k] / max(st.dist_cat_total[k], 1),
                default=None,
            ) if st.dist_cat_wins else None
            feat["preferred_distance_match"] = (
                1.0 if best_dist is not None and best_dist == rec["dist_cat"] else 0.0
            )
        else:
            feat["preferred_distance_match"] = None

        # 3. preferred_terrain_match
        if st.nb_courses >= 3 and rec["type_piste"]:
            best_terrain = max(
                st.terrain_wins.keys(),
                key=lambda k: st.terrain_wins[k] / max(st.terrain_total[k], 1),
                default=None,
            ) if st.terrain_wins else None
            feat["preferred_terrain_match"] = (
                1.0 if best_terrain is not None and best_terrain == rec["type_piste"] else 0.0
            )
        else:
            feat["preferred_terrain_match"] = None

        # 4. career_roi (mise = 1 per race)
        if st.nb_courses >= 1:
            feat["career_roi"] = round(
                (st.gains_total - st.nb_courses) / st.nb_courses, 4
            )
        else:
            feat["career_roi"] = None

        # 5. career_avg_beaten_length
        if st.beaten_lengths > 0:
            feat["career_avg_beaten_length"] = round(
                st.beaten_length_sum / st.beaten_lengths, 4
            )
        else:
            feat["career_avg_beaten_length"] = None

        # 6. versatility_score
        feat["versatility_score"] = len(st.unique_combos) if st.nb_courses > 0 else None

        results.append(feat)

        # ---- UPDATE state ----
        st.nb_courses += 1
        st.gains_total += rec["gains"]
        if rec["gagnant"]:
            st.wins += 1

        # beaten length
        if rec["ecart"] is not None:
            st.beaten_lengths += 1
            st.beaten_length_sum += rec["ecart"]

        # distance category stats
        dc = rec["dist_cat"]
        if dc:
            st.dist_cat_total[dc] += 1
            if rec["gagnant"]:
                st.dist_cat_wins[dc] += 1

        # terrain stats
        tp = rec["type_piste"]
        if tp:
            st.terrain_total[tp] += 1
            if rec["gagnant"]:
                st.terrain_wins[tp] += 1

        # front runner: horse had low corde (<=4) and finished top-3
        pc = rec["place_corde"]
        if pc is not None:
            st.front_run_eligible += 1
            if pc <= 4 and rec["position"] is not None and rec["position"] <= 3:
                st.front_run_count += 1

        # versatility: unique combo of (dist_cat, terrain, hippodrome)
        combo = (dc, tp, rec["hippodrome"])
        if any(x is not None for x in combo[:2]):
            st.unique_combos.add(combo)

    elapsed = time.time() - t0
    logger.info(
        "Horse Profile Builder termine: %d features en %.1f s",
        len(results), elapsed,
    )
    return results


# ===========================================================================
# CLI
# ===========================================================================

def _resolve_input(cli_input: Optional[str]) -> Path:
    if cli_input:
        p = Path(cli_input)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for cand in INPUT_CANDIDATES:
        if cand.exists():
            return cand
    raise FileNotFoundError(
        "Aucun fichier partants_master trouve. Candidats testes:\n"
        + "\n".join(f"  - {c}" for c in INPUT_CANDIDATES)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Horse Profile Feature Builder")
    parser.add_argument("--input", type=str, default=None, help="Path to partants_master.jsonl")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("horse_profile_builder")

    input_path = _resolve_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    features = build_horse_profile_features(input_path, logger)

    out_file = output_dir / "horse_profile_features.jsonl"
    save_jsonl(features, out_file, logger)
    logger.info("Sauvegarde: %s (%d records)", out_file, len(features))


if __name__ == "__main__":
    main()
