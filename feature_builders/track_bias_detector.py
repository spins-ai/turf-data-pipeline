"""
feature_builders.track_bias_detector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Detects track biases per hippodrome using a rolling 365-day lookback.

Temporal integrity: for any partant at date D, only races with date < D are used.

Biases detected:
  - Stall/corde bias (galop stall number, trot inner/middle/outer)
  - Front-runner bias per hippodrome
  - Terrain bias (type_piste x penetrometre)
  - Favourite-distance bias

Produces:
  - track_bias_features.json / .parquet / .csv

Usage:
    python3 -m feature_builders.track_bias_detector
    python3 -m feature_builders.track_bias_detector --partants path --courses path --output dir
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Optional parquet support
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logging_setup import setup_logging


# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = _PROJECT_ROOT / "output" / "02_liste_courses" / "partants_normalises.json"
INPUT_COURSES = _PROJECT_ROOT / "output" / "02_liste_courses" / "courses_normalisees.json"
OUTPUT_DIR = _PROJECT_ROOT / "output" / "track_bias"

LOOKBACK_DAYS = 365


# ===========================================================================
# SAUVEGARDE
# ===========================================================================

def sauver_json(data: list[dict], path: Path, logger: logging.Logger):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)
    size_mb = path.stat().st_size / 1_048_576
    logger.info("JSON saved: %s (%.1f MB)", path, size_mb)


def sauver_csv(data: list[dict], path: Path, logger: logging.Logger):
    if not data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    all_keys: list[str] = list(data[0].keys())
    seen = set(all_keys)
    for r in data:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    size_mb = path.stat().st_size / 1_048_576
    logger.info("CSV saved: %s (%.1f MB)", path, size_mb)


def sauver_parquet(data: list[dict], path: Path, logger: logging.Logger) -> bool:
    if not HAS_PARQUET:
        logger.warning("pyarrow not installed, skipping .parquet output.")
        return False
    if not data:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    all_keys: list[str] = []
    seen: set[str] = set()
    for r in data:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    columns = {k: [r.get(k) for r in data] for k in all_keys}
    table = pa.table(columns)
    pq.write_table(table, str(path))
    size_mb = path.stat().st_size / 1_048_576
    logger.info("Parquet saved: %s (%.1f MB)", path, size_mb)
    return True


# ===========================================================================
# HELPERS
# ===========================================================================

def _safe_rate(count: int, total: int) -> Optional[float]:
    if total == 0:
        return None
    return count / total


def _distance_category(distance: Optional[int]) -> Optional[str]:
    """Classify race distance into a category."""
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


def _corde_bin(place_corde: Optional[int]) -> Optional[str]:
    """Bin corde position into inner / middle / outer (for trot)."""
    if place_corde is None:
        return None
    if place_corde <= 4:
        return "inner"
    elif place_corde <= 8:
        return "middle"
    else:
        return "outer"


def _parse_date(iso: str) -> Optional[datetime]:
    try:
        return datetime.strptime(iso, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ===========================================================================
# CORE: BUILD TRACK BIAS FEATURES
# ===========================================================================

def build_track_bias_features(
    partants: list[dict],
    courses: list[dict],
) -> list[dict]:
    """Compute per-partant track bias features.

    Parameters
    ----------
    partants : list[dict]
        All partant records from partants_normalises.json.
    courses : list[dict]
        All course records from courses_normalisees.json.

    Returns
    -------
    list[dict]
        One dict per partant_uid with bias features.
    """
    # ---- Build course lookup ----
    course_map: dict[str, dict] = {}
    for c in courses:
        course_map[c.get("course_uid", "")] = c

    # ---- Enrich partants with course-level fields ----
    enriched: list[dict] = []
    for p in partants:
        rec = dict(p)
        cuid = p.get("course_uid", "")
        course = course_map.get(cuid, {})
        # Inherit course-level fields if not already on partant
        for field in ("corde", "type_piste", "penetrometre", "nombre_partants", "discipline"):
            if field not in rec or rec[field] is None:
                rec[field] = course.get(field)
        if rec.get("distance") is None:
            rec["distance"] = course.get("distance")
        enriched.append(rec)

    # ---- Sort chronologically for rolling lookback ----
    enriched.sort(key=lambda r: (
        r.get("date_reunion_iso", ""),
        r.get("course_uid", ""),
        r.get("num_pmu", 0),
    ))

    # ---- Build history index per hippodrome ----
    # For each hippodrome we accumulate race records as we iterate.
    # Key: hippodrome_normalise -> list of past records (sorted by date)
    hippo_history: dict[str, list[dict]] = defaultdict(list)

    # ---- Global accumulators (for comparing hippo vs global) ----
    global_terrain_wins: dict[str, int] = defaultdict(int)  # type_piste -> wins
    global_terrain_total: dict[str, int] = defaultdict(int)  # type_piste -> total
    global_favori_dist_wins: dict[str, int] = defaultdict(int)  # dist_cat -> fav wins
    global_favori_dist_total: dict[str, int] = defaultdict(int)  # dist_cat -> fav races

    results: list[dict] = []

    for p in enriched:
        uid = p.get("partant_uid")
        hippo = p.get("hippodrome_normalise", "")
        date_iso = p.get("date_reunion_iso", "")
        date_dt = _parse_date(date_iso)
        discipline = p.get("discipline", "")
        place_corde = p.get("place_corde")
        nb_partants = p.get("nombre_partants")
        distance = p.get("distance")
        type_piste = p.get("type_piste")
        penetrometre = p.get("penetrometre")
        cote = p.get("cote_finale")
        is_gagnant = bool(p.get("is_gagnant"))
        is_place = bool(p.get("is_place"))
        position = p.get("position_arrivee")
        dist_cat = _distance_category(distance)

        # Try to parse place_corde as int
        if place_corde is not None:
            try:
                place_corde = int(place_corde)
            except (ValueError, TypeError):
                place_corde = None

        if nb_partants is not None:
            try:
                nb_partants = int(nb_partants)
            except (ValueError, TypeError):
                nb_partants = None

        # ---- Determine lookback window ----
        cutoff_dt = None
        if date_dt is not None:
            cutoff_dt = date_dt - timedelta(days=LOOKBACK_DAYS)

        # Get past history at this hippodrome (strictly < current date, within 365 days)
        past_hippo = []
        if hippo and date_iso:
            for h in hippo_history.get(hippo, []):
                if h["date"] >= date_iso:
                    continue  # no future leakage
                if cutoff_dt is not None:
                    h_dt = _parse_date(h["date"])
                    if h_dt is not None and h_dt < cutoff_dt:
                        continue  # outside lookback window
                past_hippo.append(h)

        # ==== 1. Biais stalle (galop) ====
        biais_stalle: Optional[float] = None
        if discipline == "galop" and place_corde is not None and 1 <= place_corde <= 20:
            stall_wins = sum(1 for h in past_hippo if h.get("place_corde") == place_corde and h["gagnant"])
            stall_total = sum(1 for h in past_hippo if h.get("place_corde") == place_corde)
            if stall_total >= 5:
                observed_rate = stall_wins / stall_total
                # Expected rate: average 1/nb_partants across races at this stall
                expected_rates = [
                    1.0 / h["nb_partants"]
                    for h in past_hippo
                    if h.get("place_corde") == place_corde and h.get("nb_partants") and h["nb_partants"] > 0
                ]
                expected_rate = sum(expected_rates) / len(expected_rates) if expected_rates else None
                if expected_rate is not None:
                    biais_stalle = observed_rate - expected_rate

        # ==== 2. Biais corde interieur/exterieur (trot) ====
        biais_corde_position: Optional[str] = None
        biais_corde_winrate: Optional[float] = None
        if discipline in ("trot_attele", "trot_monte", "trot") and place_corde is not None:
            corde_pos = _corde_bin(place_corde)
            biais_corde_position = corde_pos
            if corde_pos is not None:
                bin_wins = sum(
                    1 for h in past_hippo
                    if _corde_bin(h.get("place_corde")) == corde_pos and h["gagnant"]
                )
                bin_total = sum(
                    1 for h in past_hippo
                    if _corde_bin(h.get("place_corde")) == corde_pos
                )
                if bin_total >= 5:
                    observed_rate = bin_wins / bin_total
                    expected_rates = [
                        1.0 / h["nb_partants"]
                        for h in past_hippo
                        if _corde_bin(h.get("place_corde")) == corde_pos
                        and h.get("nb_partants") and h["nb_partants"] > 0
                    ]
                    expected_rate = sum(expected_rates) / len(expected_rates) if expected_rates else None
                    if expected_rate is not None:
                        biais_corde_winrate = observed_rate - expected_rate

        # ==== 3. Biais leaders vs attentistes ====
        # "Front-runner bias": % of winners at this hippo that came from low
        # stall/corde positions (1-4).  We compare this horse's position category.
        biais_frontrunner: Optional[float] = None
        winners_hippo = [h for h in past_hippo if h["gagnant"]]
        if winners_hippo and place_corde is not None:
            front_winners = sum(
                1 for h in winners_hippo
                if h.get("place_corde") is not None and h["place_corde"] <= 4
            )
            total_winners_with_corde = sum(
                1 for h in winners_hippo
                if h.get("place_corde") is not None
            )
            if total_winners_with_corde >= 5:
                front_ratio = front_winners / total_winners_with_corde
                # Positive means track favours front positions; negative means it
                # doesn't.  We adjust for this partant's position:
                # if partant is in front group -> feature = front_ratio
                # if partant is in back group  -> feature = -(1 - front_ratio)
                if place_corde <= 4:
                    biais_frontrunner = front_ratio
                else:
                    biais_frontrunner = -(1.0 - front_ratio)

        # ==== 4. Biais terrain ====
        biais_terrain_hippodrome: Optional[float] = None
        if type_piste:
            # Hippo-level win rate on this surface
            hippo_surface_wins = sum(
                1 for h in past_hippo if h.get("type_piste") == type_piste and h["gagnant"]
            )
            hippo_surface_total = sum(
                1 for h in past_hippo if h.get("type_piste") == type_piste
            )
            hippo_rate = _safe_rate(hippo_surface_wins, hippo_surface_total)

            # Global win rate on this surface (use accumulated global stats)
            global_rate = _safe_rate(
                global_terrain_wins.get(type_piste, 0),
                global_terrain_total.get(type_piste, 0),
            )
            if hippo_rate is not None and global_rate is not None:
                biais_terrain_hippodrome = hippo_rate - global_rate

        # ==== 5. Biais distance favori ====
        biais_favori_distance: Optional[float] = None
        if dist_cat and cote is not None:
            # Favourite = lowest cote in the race -> we check if cote < 5
            is_favori = cote < 5.0
            if is_favori:
                # Hippo-level favourite win rate at this distance category
                hippo_fav_wins = sum(
                    1 for h in past_hippo
                    if h.get("dist_cat") == dist_cat and h.get("is_favori") and h["gagnant"]
                )
                hippo_fav_total = sum(
                    1 for h in past_hippo
                    if h.get("dist_cat") == dist_cat and h.get("is_favori")
                )
                hippo_fav_rate = _safe_rate(hippo_fav_wins, hippo_fav_total)

                global_fav_rate = _safe_rate(
                    global_favori_dist_wins.get(dist_cat, 0),
                    global_favori_dist_total.get(dist_cat, 0),
                )
                if hippo_fav_rate is not None and global_fav_rate is not None:
                    biais_favori_distance = hippo_fav_rate - global_fav_rate
                elif hippo_fav_rate is not None:
                    biais_favori_distance = hippo_fav_rate

        # ---- Build feature dict ----
        feat = {
            "partant_uid": uid,
            "biais_stalle": biais_stalle,
            "biais_corde_position": biais_corde_position,
            "biais_corde_winrate": biais_corde_winrate,
            "biais_frontrunner": biais_frontrunner,
            "biais_terrain_hippodrome": biais_terrain_hippodrome,
            "biais_favori_distance": biais_favori_distance,
        }
        results.append(feat)

        # ---- Append current race to history (for future partants) ----
        is_favori_flag = (cote is not None and cote < 5.0)

        hippo_history[hippo].append({
            "date": date_iso,
            "place_corde": place_corde,
            "nb_partants": nb_partants,
            "gagnant": is_gagnant,
            "place": is_place,
            "position": position,
            "type_piste": type_piste,
            "penetrometre": penetrometre,
            "dist_cat": dist_cat,
            "is_favori": is_favori_flag,
            "discipline": discipline,
        })

        # Update global accumulators
        if type_piste:
            global_terrain_total[type_piste] += 1
            if is_gagnant:
                global_terrain_wins[type_piste] += 1
        if dist_cat and is_favori_flag:
            global_favori_dist_total[dist_cat] += 1
            if is_gagnant:
                global_favori_dist_wins[dist_cat] += 1

    return results


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Detect track biases per hippodrome and build per-partant bias features.",
    )
    default_base = os.path.join(
        os.path.dirname(__file__), "..", "output", "02_liste_courses",
    )
    parser.add_argument(
        "--partants",
        default=os.path.join(default_base, "partants_normalises.json"),
        help="Path to partants_normalises.json",
    )
    parser.add_argument(
        "--courses",
        default=os.path.join(default_base, "courses_normalisees.json"),
        help="Path to courses_normalisees.json",
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR),
        help="Output directory (default: output/track_bias)",
    )
    args = parser.parse_args()

    logger = setup_logging("track_bias_detector")
    t0 = time.time()

    logger.info("=" * 70)
    logger.info("TRACK BIAS DETECTOR")
    logger.info("=" * 70)

    # 1. Load data
    logger.info("Loading partants from %s", args.partants)
    with open(args.partants, encoding="utf-8") as f:
        partants = json.load(f)
    logger.info("Loading courses from %s", args.courses)
    with open(args.courses, encoding="utf-8") as f:
        courses = json.load(f)
    logger.info("Loaded %d partants, %d courses", len(partants), len(courses))

    # 2. Build features
    logger.info("Building track bias features (lookback=%d days)...", LOOKBACK_DAYS)
    t1 = time.time()
    feats = build_track_bias_features(partants, courses)
    logger.info("Built %d records in %.1fs", len(feats), time.time() - t1)

    # 3. Save outputs
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    sauver_json(feats, out / "track_bias_features.json", logger)
    sauver_csv(feats, out / "track_bias_features.csv", logger)
    sauver_parquet(feats, out / "track_bias_features.parquet", logger)

    # 4. Summary
    feature_keys = [k for k in feats[0] if k != "partant_uid"] if feats else []
    logger.info("-" * 50)
    logger.info("SUMMARY: %d partants, %d features", len(feats), len(feature_keys))
    for k in feature_keys:
        filled = sum(1 for r in feats if r.get(k) is not None)
        pct = 100 * filled / len(feats) if feats else 0
        logger.info("  %-30s %d/%d (%.1f%%)", k, filled, len(feats), pct)

    elapsed = time.time() - t0
    logger.info("Total time: %.1fs", elapsed)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
