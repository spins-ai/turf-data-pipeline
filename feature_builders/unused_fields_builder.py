#!/usr/bin/env python3
"""
feature_builders.unused_fields_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Exploits 10 high-value fields from partants_master that were previously
unused by any feature builder.  Each feature combines or transforms raw
unused columns into predictive signals.

Reads partants_master.jsonl in streaming mode, sorts chronologically,
then processes course-by-course to preserve temporal integrity.

Temporal integrity: every feature is computed from data available at
race time (career stats, pre-race fields, course conditions) -- no
future leakage.

Produces:
  - unused_fields_features.jsonl   in output/unused_fields/

Features per partant (10):
  - network_strength          : gnn_cheval_degree * gnn_jockey_nb_chevaux
                                 (social-network connectivity signal)
  - history_coverage_rate     : seq_nb_courses_historique / nb_courses_carriere
                                 (how much of the career is in our sequence data)
  - minor_place_rate          : (nb_places_2eme + nb_places_3eme) / nb_courses_carriere
                                 (rate of 2nd/3rd finishes -- consistency without winning)
  - genetic_fitness           : ped_inbreeding_score * ped_stamina_index
                                 (combined pedigree quality signal)
  - field_compression         : spd_field_strength_avg / spd_field_strength_max
                                 (tight vs spread field -- close to 1 = compressed)
  - track_inside_bias         : spd_bias_interieur (direct passthrough, never used)
  - weight_advantage          : poids_base_kg - poids_monte_kg (positive = lighter ride)
                                 Falls back to poids_porte_kg when poids_monte_kg absent.
  - implied_market_prob       : 1 / (rap_rapport_simple_gagnant / 100)
                                 (market implied probability from actual winning dividend)
  - terrain_weather_combo     : cnd_cond_type_terrain * met_impact_meteo_score
                                 (interaction between ground type and weather impact)
  - age_distance_interaction  : pgr_age_ans * distance / 1000
                                 (age-distance interaction, older horses may fade at distance)

Usage:
    python feature_builders/unused_fields_builder.py
    python feature_builders/unused_fields_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
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
OUTPUT_DIR = _PROJECT_ROOT / "output" / "unused_fields"

# Progress log every N records
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
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    """Convert value to float, returning None on failure."""
    if val is None:
        return None
    try:
        v = float(val)
        return v
    except (ValueError, TypeError):
        return None


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Safe division returning None when inputs are missing or b == 0."""
    if a is None or b is None or b == 0:
        return None
    return a / b


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_unused_fields_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build 10 features from previously unused partants_master fields."""
    logger.info("=== Unused Fields Features Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read needed fields (slim) --
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
            # Network fields
            "gnn_cheval_degree": _safe_float(rec.get("gnn_cheval_degree")),
            "gnn_jockey_nb_chevaux": _safe_float(rec.get("gnn_jockey_nb_chevaux")),
            # History coverage
            "seq_nb_courses_historique": _safe_float(rec.get("seq_nb_courses_historique")),
            "nb_courses_carriere": _safe_float(rec.get("nb_courses_carriere")),
            # Minor place
            "nb_places_2eme": _safe_float(rec.get("nb_places_2eme")),
            "nb_places_3eme": _safe_float(rec.get("nb_places_3eme")),
            # Pedigree
            "ped_inbreeding_score": _safe_float(rec.get("ped_inbreeding_score")),
            "ped_stamina_index": _safe_float(rec.get("ped_stamina_index")),
            # Field compression
            "spd_field_strength_avg": _safe_float(rec.get("spd_field_strength_avg")),
            "spd_field_strength_max": _safe_float(rec.get("spd_field_strength_max")),
            # Track bias
            "spd_bias_interieur": _safe_float(rec.get("spd_bias_interieur")),
            # Weight
            "poids_base_kg": _safe_float(rec.get("poids_base_kg")),
            "poids_monte_kg": _safe_float(rec.get("poids_monte_kg")),
            "poids_porte_kg": _safe_float(rec.get("poids_porte_kg")),
            # Dividend (rap_rapport_simple_gagnant is in centimes)
            "rap_rapport_simple_gagnant": _safe_float(rec.get("rap_rapport_simple_gagnant")),
            # Terrain x weather
            "cnd_cond_type_terrain": _safe_float(rec.get("cnd_cond_type_terrain")),
            "met_impact_meteo_score": _safe_float(rec.get("met_impact_meteo_score")),
            # Age x distance
            "pgr_age_ans": _safe_float(rec.get("pgr_age_ans")),
            "distance": _safe_float(rec.get("distance")),
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

    # -- Phase 3: Compute features per record --
    t2 = time.time()
    results: list[dict[str, Any]] = []
    n_processed = 0

    for rec in slim_records:
        n_processed += 1
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, len(slim_records))

        # 1. network_strength = gnn_cheval_degree * gnn_jockey_nb_chevaux
        network_strength: Optional[float] = None
        gd = rec["gnn_cheval_degree"]
        jn = rec["gnn_jockey_nb_chevaux"]
        if gd is not None and jn is not None:
            network_strength = round(gd * jn, 4)

        # 2. history_coverage_rate = seq_nb_courses_historique / nb_courses_carriere
        history_coverage_rate = _safe_div(
            rec["seq_nb_courses_historique"],
            rec["nb_courses_carriere"],
        )
        if history_coverage_rate is not None:
            history_coverage_rate = round(min(history_coverage_rate, 1.0), 4)

        # 3. minor_place_rate = (nb_places_2eme + nb_places_3eme) / nb_courses_carriere
        minor_place_rate: Optional[float] = None
        p2 = rec["nb_places_2eme"]
        p3 = rec["nb_places_3eme"]
        nbc = rec["nb_courses_carriere"]
        if p2 is not None and p3 is not None and nbc is not None and nbc > 0:
            minor_place_rate = round((p2 + p3) / nbc, 4)

        # 4. genetic_fitness = ped_inbreeding_score * ped_stamina_index
        genetic_fitness: Optional[float] = None
        ibs = rec["ped_inbreeding_score"]
        sti = rec["ped_stamina_index"]
        if ibs is not None and sti is not None:
            genetic_fitness = round(ibs * sti, 4)

        # 5. field_compression = spd_field_strength_avg / spd_field_strength_max
        field_compression = _safe_div(
            rec["spd_field_strength_avg"],
            rec["spd_field_strength_max"],
        )
        if field_compression is not None:
            field_compression = round(field_compression, 4)

        # 6. track_inside_bias = spd_bias_interieur (direct passthrough)
        track_inside_bias = rec["spd_bias_interieur"]
        if track_inside_bias is not None:
            track_inside_bias = round(track_inside_bias, 4)

        # 7. weight_advantage = poids_base_kg - poids_monte_kg
        #    Falls back to poids_porte_kg when poids_monte_kg is absent.
        weight_advantage: Optional[float] = None
        base = rec["poids_base_kg"]
        monte = rec["poids_monte_kg"]
        porte = rec["poids_porte_kg"]
        actual_weight = monte if monte is not None else porte
        if base is not None and actual_weight is not None:
            weight_advantage = round(base - actual_weight, 2)

        # 8. implied_market_prob = 1 / (rap_rapport_simple_gagnant / 100)
        #    rap_rapport_simple_gagnant is stored in centimes: 600 = 6.00 EUR
        #    For a 1 EUR bet paying 6.00, implied prob = 1/6.00
        implied_market_prob: Optional[float] = None
        rsg = rec["rap_rapport_simple_gagnant"]
        if rsg is not None and rsg > 0:
            dividend_eur = rsg / 100.0
            if dividend_eur > 0:
                implied_market_prob = round(1.0 / dividend_eur, 6)

        # 9. terrain_weather_combo = cnd_cond_type_terrain * met_impact_meteo_score
        terrain_weather_combo: Optional[float] = None
        ctt = rec["cnd_cond_type_terrain"]
        mis = rec["met_impact_meteo_score"]
        if ctt is not None and mis is not None:
            terrain_weather_combo = round(ctt * mis, 4)

        # 10. age_distance_interaction = pgr_age_ans * distance / 1000
        age_distance_interaction: Optional[float] = None
        age = rec["pgr_age_ans"]
        dist = rec["distance"]
        if age is not None and dist is not None and dist > 0:
            age_distance_interaction = round(age * dist / 1000.0, 4)

        results.append({
            "partant_uid": rec["uid"],
            "network_strength": network_strength,
            "history_coverage_rate": history_coverage_rate,
            "minor_place_rate": minor_place_rate,
            "genetic_fitness": genetic_fitness,
            "field_compression": field_compression,
            "track_inside_bias": track_inside_bias,
            "weight_advantage": weight_advantage,
            "implied_market_prob": implied_market_prob,
            "terrain_weather_combo": terrain_weather_combo,
            "age_distance_interaction": age_distance_interaction,
        })

    elapsed = time.time() - t0
    logger.info(
        "Unused fields build termine: %d features en %.1fs",
        len(results),
        elapsed,
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
        description="Construction de 10 features a partir de champs inutilises de partants_master"
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
        help="Repertoire de sortie (defaut: output/unused_fields/)",
    )
    args = parser.parse_args()

    logger = setup_logging("unused_fields_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_unused_fields_features(input_path, logger)

    # Save
    out_path = output_dir / "unused_fields_features.jsonl"
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
