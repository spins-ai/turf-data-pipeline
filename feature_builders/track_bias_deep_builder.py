"""
feature_builders.track_bias_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep track-level bias features using rolling 365-day lookback per hippodrome.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.

Produces:
  - track_bias_deep_features.jsonl  in output/track_bias_deep/

Features per partant (5):
  - corde_advantage_today        : win-rate of this corde position at this hippo vs expected (obs - exp)
  - inside_vs_outside_winrate    : win-rate(inner cordes 1-4) - win-rate(outer cordes 9+) at this hippo
  - rail_position_bias           : correlation proxy -- % of inner-corde winners at this hippo, past 365d
  - track_speed_vs_average       : hippo avg speed / global avg speed (ratio), past 365d
  - hippodrome_unpredictability  : 1 - (favourite win-rate at this hippo), past 365d

Usage:
    python feature_builders/track_bias_deep_builder.py
    python feature_builders/track_bias_deep_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
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
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "track_bias_deep"

LOOKBACK_DAYS = 365
_LOG_EVERY = 500_000


# ===========================================================================
# HELPERS
# ===========================================================================

def _parse_date(iso: str) -> Optional[datetime]:
    try:
        return datetime.strptime(iso, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _safe_div(num: float, den: float) -> Optional[float]:
    return round(num / den, 4) if den > 0 else None


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
# HIPPO HISTORY RECORD (compact)
# ===========================================================================

class _HippoRecord:
    """Compact race record stored per hippodrome for lookback."""

    __slots__ = (
        "date", "date_dt", "gagnant", "place_corde",
        "nb_partants", "speed", "is_favori",
    )

    def __init__(
        self, date: str, date_dt: Optional[datetime],
        gagnant: bool, place_corde: Optional[int],
        nb_partants: Optional[int], speed: Optional[float],
        is_favori: bool,
    ):
        self.date = date
        self.date_dt = date_dt
        self.gagnant = gagnant
        self.place_corde = place_corde
        self.nb_partants = nb_partants
        self.speed = speed
        self.is_favori = is_favori


# ===========================================================================
# MAIN BUILD
# ===========================================================================

def build_track_bias_deep_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build deep track bias features from partants_master.jsonl.

    Two-phase approach:
      1. Read minimal fields into memory, sort chronologically.
      2. Process in order with rolling hippo history.
         Features are emitted BEFORE updating state (strict temporal integrity).
    """
    logger.info("=== Track Bias Deep Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        place_corde = None
        pc_raw = rec.get("place_corde")
        if pc_raw is not None:
            try:
                place_corde = int(pc_raw)
            except (ValueError, TypeError):
                pass

        nb_partants = None
        np_raw = rec.get("nombre_partants")
        if np_raw is not None:
            try:
                nb_partants = int(np_raw)
            except (ValueError, TypeError):
                pass

        distance = None
        d_raw = rec.get("distance")
        if d_raw is not None:
            try:
                distance = int(d_raw)
            except (ValueError, TypeError):
                pass

        temps = None
        t_raw = rec.get("temps_obtenu") or rec.get("temps_course")
        if t_raw is not None:
            try:
                temps = float(t_raw)
            except (ValueError, TypeError):
                pass

        speed = None
        if distance is not None and temps is not None and temps > 0:
            speed = round(distance / temps, 4)

        # Determine if favourite: lowest cote among starters in same course
        cote = None
        c_raw = rec.get("cote_finale") or rec.get("cote_probable")
        if c_raw is not None:
            try:
                cote = float(c_raw)
            except (ValueError, TypeError):
                pass

        slim_records.append({
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "hippodrome": rec.get("hippodrome_normalise", ""),
            "gagnant": bool(rec.get("is_gagnant")),
            "place_corde": place_corde,
            "nb_partants": nb_partants,
            "speed": speed,
            "cote": cote,
        })

    logger.info("Phase 1 terminee: %d records slim en memoire", len(slim_records))

    # -- Phase 1b: Sort chronologically --
    slim_records.sort(key=_sort_key)

    # -- Phase 1c: Mark favourites per course --
    # Group by course, assign is_favori to lowest-cote runner
    course_groups: dict[str, list[int]] = defaultdict(list)
    for idx, rec in enumerate(slim_records):
        if rec["course"]:
            course_groups[rec["course"]].append(idx)

    is_favori_flags: list[bool] = [False] * len(slim_records)
    for course_uid, indices in course_groups.items():
        best_idx = None
        best_cote = float("inf")
        for idx in indices:
            c = slim_records[idx]["cote"]
            if c is not None and c < best_cote:
                best_cote = c
                best_idx = idx
        if best_idx is not None:
            is_favori_flags[best_idx] = True

    logger.info("Tri + favoris termines")

    # -- Phase 2: Compute features with rolling hippo history --
    hippo_history: dict[str, list[_HippoRecord]] = defaultdict(list)
    global_speed_sum: float = 0.0
    global_speed_count: int = 0

    results: list[dict[str, Any]] = []

    for i, rec in enumerate(slim_records):
        if i > 0 and i % _LOG_EVERY == 0:
            logger.info("  Traitement %d / %d...", i, len(slim_records))

        uid = rec["uid"]
        hippo = rec["hippodrome"]
        date_iso = rec["date"]
        date_dt = _parse_date(date_iso)
        place_corde = rec["place_corde"]

        cutoff_dt = None
        if date_dt is not None:
            cutoff_dt = date_dt - timedelta(days=LOOKBACK_DAYS)

        # Get lookback window for this hippodrome
        past: list[_HippoRecord] = []
        if hippo and date_iso:
            for h in hippo_history.get(hippo, []):
                if h.date >= date_iso:
                    continue
                if cutoff_dt is not None and h.date_dt is not None and h.date_dt < cutoff_dt:
                    continue
                past.append(h)

        feat: dict[str, Any] = {"partant_uid": uid}

        # ---- 1. corde_advantage_today ----
        # obs win-rate of this corde vs expected (1/nb_partants)
        feat["corde_advantage_today"] = None
        if place_corde is not None and past:
            same_corde = [h for h in past if h.place_corde == place_corde]
            if len(same_corde) >= 5:
                obs_rate = sum(1 for h in same_corde if h.gagnant) / len(same_corde)
                exp_rates = [
                    1.0 / h.nb_partants
                    for h in same_corde
                    if h.nb_partants is not None and h.nb_partants > 0
                ]
                exp_rate = sum(exp_rates) / len(exp_rates) if exp_rates else None
                if exp_rate is not None:
                    feat["corde_advantage_today"] = round(obs_rate - exp_rate, 4)

        # ---- 2. inside_vs_outside_winrate ----
        # win-rate(corde 1-4) - win-rate(corde 9+)
        feat["inside_vs_outside_winrate"] = None
        if past:
            inner = [h for h in past if h.place_corde is not None and h.place_corde <= 4]
            outer = [h for h in past if h.place_corde is not None and h.place_corde >= 9]
            if len(inner) >= 5 and len(outer) >= 5:
                inner_wr = sum(1 for h in inner if h.gagnant) / len(inner)
                outer_wr = sum(1 for h in outer if h.gagnant) / len(outer)
                feat["inside_vs_outside_winrate"] = round(inner_wr - outer_wr, 4)

        # ---- 3. rail_position_bias ----
        # % of winners that had corde <= 4
        feat["rail_position_bias"] = None
        winners = [h for h in past if h.gagnant and h.place_corde is not None]
        if len(winners) >= 3:
            rail_winners = sum(1 for h in winners if h.place_corde <= 4)
            feat["rail_position_bias"] = round(rail_winners / len(winners), 4)

        # ---- 4. track_speed_vs_average ----
        # hippo avg speed / global avg speed
        feat["track_speed_vs_average"] = None
        hippo_speeds = [h.speed for h in past if h.speed is not None]
        if hippo_speeds and global_speed_count >= 10:
            hippo_avg = sum(hippo_speeds) / len(hippo_speeds)
            global_avg = global_speed_sum / global_speed_count
            if global_avg > 0:
                feat["track_speed_vs_average"] = round(hippo_avg / global_avg, 4)

        # ---- 5. hippodrome_unpredictability ----
        # 1 - (favourite win-rate at this hippo)
        feat["hippodrome_unpredictability"] = None
        fav_past = [h for h in past if h.is_favori]
        if len(fav_past) >= 5:
            fav_wr = sum(1 for h in fav_past if h.gagnant) / len(fav_past)
            feat["hippodrome_unpredictability"] = round(1.0 - fav_wr, 4)

        results.append(feat)

        # ---- UPDATE state ----
        hr = _HippoRecord(
            date=date_iso,
            date_dt=date_dt,
            gagnant=rec["gagnant"],
            place_corde=place_corde,
            nb_partants=rec["nb_partants"],
            speed=rec["speed"],
            is_favori=is_favori_flags[i],
        )
        if hippo:
            hippo_history[hippo].append(hr)

        if rec["speed"] is not None:
            global_speed_sum += rec["speed"]
            global_speed_count += 1

    elapsed = time.time() - t0
    logger.info(
        "Track Bias Deep Builder termine: %d features en %.1f s",
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
    parser = argparse.ArgumentParser(description="Track Bias Deep Feature Builder")
    parser.add_argument("--input", type=str, default=None, help="Path to partants_master.jsonl")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory")
    args = parser.parse_args()

    logger = setup_logging("track_bias_deep_builder")

    input_path = _resolve_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    features = build_track_bias_deep_features(input_path, logger)

    out_file = output_dir / "track_bias_deep_features.jsonl"
    save_jsonl(features, out_file, logger)
    logger.info("Sauvegarde: %s (%d records)", out_file, len(features))


if __name__ == "__main__":
    main()
