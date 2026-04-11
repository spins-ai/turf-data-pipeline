#!/usr/bin/env python3
"""
feature_builders.ml_features_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ML-ready features computed from existing partants_master data.

Creates features needed by CatBoost/XGBoost/LightGBM, meta-selectors,
outsider detectors, and other model phases.

Temporal integrity: for any partant at date D, only races with date < D
contribute -- no future leakage.

Produces:
  - ml_features.jsonl   in output/ml_features/

Features per partant (21):
  - win_probability_implied   : 1/cote normalised by field sum
  - trainer_jockey_combo_roi  : historical ROI of the trainer-jockey pair
  - trainer_jockey_combo_wins : historical win count of the pair
  - trainer_jockey_combo_runs : historical run count of the pair
  - horse_improvement_rate    : linear Elo slope over last 5 races
  - distance_change_impact    : current distance - last race distance (metres)
  - weight_change_impact      : current weight - last race weight (kg)
  - days_since_win            : calendar days since last win
  - surface_switch_flag       : 1 if surface differs from last race
  - race_type_encoding_plat   : 1 if galop plat
  - race_type_encoding_obstacle: 1 if obstacle/steeple/haies
  - race_type_encoding_trot   : 1 if trot (attele or monte)
  - field_size_bucket         : 0=small(<=8), 1=medium(9-14), 2=large(>=15)
  - upset_frequency_cond      : historical upset rate for this hippodrome+discipline
  - variance_historical       : variance of finishing positions (last 10)
  - entropy_field             : Shannon entropy of implied probabilities in field
  - frequency_enc_hippodrome  : nb past races at this hippodrome (horse)
  - frequency_enc_jockey      : nb past races for this jockey (career)
  - frequency_enc_trainer     : nb past races for this trainer (career)
  - discipline_is_trot        : 1 if trot discipline
  - discipline_is_galop       : 1 if galop discipline

Usage:
    python feature_builders/ml_features_builder.py
    python feature_builders/ml_features_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/ml_features")

_LOG_EVERY = 500_000

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
# TRACKERS
# ===========================================================================


def _safe_float(v, default=None):
    if v is None:
        return default
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (ValueError, TypeError):
        return default


def _safe_int(v, default=0):
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_ml_features(input_path: Path, logger) -> list[dict[str, Any]]:
    """Build ML features from partants_master.jsonl."""
    logger.info("=== ML Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # Phase 1: Read minimal fields
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        discipline = (rec.get("discipline") or "").lower()
        type_piste = (rec.get("type_piste") or "").lower()

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "cheval": rec.get("nom_cheval"),
            "jockey": rec.get("jockey") or rec.get("driver"),
            "entraineur": rec.get("entraineur"),
            "hippodrome": rec.get("hippodrome_normalise") or rec.get("hippodrome"),
            "discipline": discipline,
            "cote_finale": _safe_float(rec.get("cote_finale") or rec.get("rapport_final")),
            "position": _safe_int(rec.get("position_finale") or rec.get("place"), 0),
            "is_gagnant": bool(rec.get("is_gagnant")),
            "distance": _safe_float(rec.get("distance")),
            "poids": _safe_float(rec.get("poids_porte_kg") or rec.get("poids_monte")),
            "type_piste": type_piste,
            "nombre_partants": _safe_int(rec.get("nombre_partants"), 0),
        }
        slim_records.append(slim)

    logger.info("Phase 1 terminee: %d records en %.1fs", len(slim_records), time.time() - t0)

    # Phase 2: Sort chronologically
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # Phase 3: Process course by course
    t2 = time.time()

    # Trackers
    horse_elo: dict[str, list[float]] = defaultdict(list)  # Elo snapshots
    horse_last_win_date: dict[str, str] = {}
    horse_last_distance: dict[str, float] = {}
    horse_last_poids: dict[str, float] = {}
    horse_last_surface: dict[str, str] = {}
    horse_positions: dict[str, list[int]] = defaultdict(list)
    horse_hippo_count: dict[tuple, int] = defaultdict(int)

    # trainer_jockey combo: (trainer, jockey) -> {wins, runs, gain}
    tj_combo: dict[tuple, dict] = defaultdict(lambda: {"wins": 0, "runs": 0, "gain": 0.0})

    jockey_count: dict[str, int] = defaultdict(int)
    trainer_count: dict[str, int] = defaultdict(int)

    # upset tracking: (hippo, discipline) -> {total, upsets}
    upset_tracker: dict[tuple, dict] = defaultdict(lambda: {"total": 0, "upsets": 0})

    # Simple Elo
    horse_elo_val: dict[str, float] = defaultdict(lambda: 1500.0)

    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        course_uid = slim_records[i]["course"]
        course_date = slim_records[i]["date"]
        course_group: list[dict] = []

        while i < total and slim_records[i]["course"] == course_uid:
            course_group.append(slim_records[i])
            i += 1

        if not course_group:
            continue

        n_runners = len(course_group)

        # Compute field-level implied probabilities for entropy
        implied_probs = []
        raw_probs = []
        for rec in course_group:
            c = rec["cote_finale"]
            if c and c > 1.0:
                raw_probs.append(1.0 / c)
            else:
                raw_probs.append(None)

        prob_sum = sum(p for p in raw_probs if p is not None) or 1.0

        for p in raw_probs:
            if p is not None:
                implied_probs.append(p / prob_sum)
            else:
                implied_probs.append(1.0 / max(n_runners, 1))

        # Shannon entropy
        entropy = 0.0
        for p in implied_probs:
            if p > 0:
                entropy -= p * math.log2(p)

        # field size bucket
        if n_runners <= 8:
            field_bucket = 0
        elif n_runners <= 14:
            field_bucket = 1
        else:
            field_bucket = 2

        # Snapshot pre-race features per runner
        for idx, rec in enumerate(course_group):
            h = rec["cheval"]
            j = rec["jockey"]
            e = rec["entraineur"]
            hippo = rec["hippodrome"]
            disc = rec["discipline"]
            date_str = rec["date"]

            # win_probability_implied
            win_prob = implied_probs[idx] if idx < len(implied_probs) else None

            # trainer_jockey_combo
            tj_key = (e, j) if e and j else None
            tj_roi = None
            tj_wins = None
            tj_runs = None
            if tj_key and tj_combo[tj_key]["runs"] > 0:
                d = tj_combo[tj_key]
                tj_roi = d["gain"] / d["runs"]
                tj_wins = d["wins"]
                tj_runs = d["runs"]

            # horse_improvement_rate (Elo slope over last 5)
            improvement = None
            elo_hist = horse_elo.get(h, []) if h else []
            if len(elo_hist) >= 3:
                last5 = elo_hist[-5:]
                n = len(last5)
                x_mean = (n - 1) / 2
                y_mean = sum(last5) / n
                num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in enumerate(last5))
                den = sum((xi - x_mean) ** 2 for xi in range(n))
                improvement = num / den if den > 0 else 0.0

            # distance_change_impact
            dist_change = None
            if h and rec["distance"] and h in horse_last_distance:
                dist_change = rec["distance"] - horse_last_distance[h]

            # weight_change_impact
            weight_change = None
            if h and rec["poids"] and h in horse_last_poids:
                weight_change = rec["poids"] - horse_last_poids[h]

            # days_since_win
            dsw = None
            if h and h in horse_last_win_date and date_str:
                try:
                    from datetime import datetime
                    d1 = datetime.fromisoformat(horse_last_win_date[h][:10])
                    d2 = datetime.fromisoformat(date_str[:10])
                    dsw = (d2 - d1).days
                    if dsw < 0:
                        dsw = None
                except Exception:
                    pass

            # surface_switch_flag
            surf_switch = None
            if h and rec["type_piste"] and h in horse_last_surface:
                surf_switch = 1 if rec["type_piste"] != horse_last_surface[h] else 0

            # race_type_encoding
            is_plat = 1 if "plat" in disc else 0
            is_obstacle = 1 if any(x in disc for x in ("obstacle", "steeple", "haies")) else 0
            is_trot = 1 if "trot" in disc else 0

            # variance_historical
            positions_hist = horse_positions.get(h, []) if h else []
            variance = None
            if len(positions_hist) >= 3:
                last10 = positions_hist[-10:]
                m = sum(last10) / len(last10)
                variance = sum((x - m) ** 2 for x in last10) / len(last10)

            # upset_frequency for this condition
            cond_key = (hippo, disc)
            ut = upset_tracker.get(cond_key)
            upset_freq = None
            if ut and ut["total"] > 10:
                upset_freq = ut["upsets"] / ut["total"]

            # frequency encodings
            freq_hippo = horse_hippo_count.get((h, hippo), 0) if h else 0
            freq_jockey = jockey_count.get(j, 0) if j else 0
            freq_trainer = trainer_count.get(e, 0) if e else 0

            results.append({
                "partant_uid": rec["uid"],
                "win_probability_implied": round(win_prob, 6) if win_prob is not None else None,
                "trainer_jockey_combo_roi": round(tj_roi, 4) if tj_roi is not None else None,
                "trainer_jockey_combo_wins": tj_wins,
                "trainer_jockey_combo_runs": tj_runs,
                "horse_improvement_rate": round(improvement, 4) if improvement is not None else None,
                "distance_change_impact": round(dist_change, 1) if dist_change is not None else None,
                "weight_change_impact": round(weight_change, 2) if weight_change is not None else None,
                "days_since_win": dsw,
                "surface_switch_flag": surf_switch,
                "race_type_encoding_plat": is_plat,
                "race_type_encoding_obstacle": is_obstacle,
                "race_type_encoding_trot": is_trot,
                "field_size_bucket": field_bucket,
                "upset_frequency_cond": round(upset_freq, 4) if upset_freq is not None else None,
                "variance_historical": round(variance, 4) if variance is not None else None,
                "entropy_field": round(entropy, 4),
                "frequency_enc_hippodrome": freq_hippo,
                "frequency_enc_jockey": freq_jockey,
                "frequency_enc_trainer": freq_trainer,
                "discipline_is_trot": is_trot,
                "discipline_is_galop": is_plat or is_obstacle,
            })

            n_processed += 1
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Traite %d records...", n_processed)

        # Post-race updates
        # Find the winner (lowest position > 0 or is_gagnant)
        winner_cote = None
        for rec in course_group:
            if rec["is_gagnant"] and rec["cote_finale"]:
                winner_cote = rec["cote_finale"]
                break

        # Check for upset (favourite didn't win, winner cote > 5)
        fav_cote = min((r["cote_finale"] for r in course_group if r["cote_finale"] and r["cote_finale"] > 1.0), default=None)
        is_upset = False
        if winner_cote and fav_cote and winner_cote > 5.0 and fav_cote < winner_cote:
            is_upset = True

        hippo0 = course_group[0]["hippodrome"]
        disc0 = course_group[0]["discipline"]
        cond_key0 = (hippo0, disc0)
        upset_tracker[cond_key0]["total"] += 1
        if is_upset:
            upset_tracker[cond_key0]["upsets"] += 1

        for rec in course_group:
            h = rec["cheval"]
            j = rec["jockey"]
            e = rec["entraineur"]

            if not h:
                continue

            # Update horse positions
            if rec["position"] and rec["position"] > 0:
                horse_positions[h].append(rec["position"])

            # Update last win date
            if rec["is_gagnant"]:
                horse_last_win_date[h] = rec["date"]

            # Update last distance/weight/surface
            if rec["distance"]:
                horse_last_distance[h] = rec["distance"]
            if rec["poids"]:
                horse_last_poids[h] = rec["poids"]
            if rec["type_piste"]:
                horse_last_surface[h] = rec["type_piste"]

            # Update Elo
            n_r = len(course_group)
            if n_r > 1 and rec["position"] and rec["position"] > 0:
                avg_opp = sum(horse_elo_val[r["cheval"]] for r in course_group if r["cheval"] and r["cheval"] != h) / max(n_r - 1, 1)
                expected = 1.0 / (1.0 + 10.0 ** ((avg_opp - horse_elo_val[h]) / 400.0))
                actual = max(0.0, 1.0 - (rec["position"] - 1) / max(n_r - 1, 1))
                horse_elo_val[h] += 24 * (actual - expected)
                horse_elo[h].append(horse_elo_val[h])

            # Update hippo frequency
            if rec["hippodrome"]:
                horse_hippo_count[(h, rec["hippodrome"])] += 1

            # Update trainer-jockey combo
            tj_key = (e, j) if e and j else None
            if tj_key:
                tj_combo[tj_key]["runs"] += 1
                if rec["is_gagnant"]:
                    tj_combo[tj_key]["wins"] += 1
                cote = rec["cote_finale"]
                if rec["is_gagnant"] and cote:
                    tj_combo[tj_key]["gain"] += cote - 1.0
                else:
                    tj_combo[tj_key]["gain"] -= 1.0

            # Update jockey/trainer counts
            if j:
                jockey_count[j] += 1
            if e:
                trainer_count[e] += 1

    dt = time.time() - t2
    logger.info("Phase 3 terminee: %d features en %.1fs", len(results), dt)
    return results


# ===========================================================================
# ENTRY POINT
# ===========================================================================


def main():
    logger = setup_logging("ml_features_builder")
    parser = argparse.ArgumentParser(description="ML Features Builder")
    parser.add_argument("--input", type=str, help="Path to partants_master.jsonl")
    args = parser.parse_args()

    input_path = None
    if args.input:
        input_path = Path(args.input)
    else:
        for c in INPUT_CANDIDATES:
            if c.exists():
                input_path = c
                break

    if not input_path or not input_path.exists():
        logger.error("Aucun fichier partants_master.jsonl trouve.")
        sys.exit(1)

    results = build_ml_features(input_path, logger)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "ml_features.jsonl"
    save_jsonl(results, out_path, logger)
    logger.info("Sauvegarde: %s (%d records)", out_path, len(results))


if __name__ == "__main__":
    main()
