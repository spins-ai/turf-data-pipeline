#!/usr/bin/env python3
"""
feature_builders.field_strength_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Comprehensive field-strength features computed per course and attached to
each partant.  Goes beyond the basic force_champ / dispersion_champ in
course_features.py by adding market concentration, competitive density,
experience metrics and per-horse relative ranks within the field.

Temporal integrity: for each partant at date D, only career counters
already available (nb_courses_carriere, nb_victoires_carriere, etc.) are
used — no future leakage.

Produit :
  - field_strength.json / .parquet / .csv   dans output/field_strength/

Usage :
    python3 feature_builders/field_strength_builder.py
    python3 feature_builders/field_strength_builder.py --input output/02_liste_courses/partants_normalises.json
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.math import safe_mean as _safe_mean, safe_stdev as _safe_stdev
from utils.output import sauver_json, sauver_csv, sauver_parquet

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PARTANTS = _PROJECT_ROOT / "output" / "02_liste_courses" / "partants_normalises.json"
OUTPUT_DIR = _PROJECT_ROOT / "output" / "field_strength"

# ===========================================================================
# SAUVEGARDE
# ===========================================================================





def charger_json(path: Path, logger: logging.Logger) -> list[dict]:
    logger.info("Chargement: %s", path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info("  %d entrees chargees", len(data))
    return data

# ===========================================================================
# HELPERS
# ===========================================================================

def _win_rate(p: dict) -> float:
    """Historical win rate from career counters already available."""
    nb_c = p.get("nb_courses_carriere")
    nb_v = p.get("nb_victoires_carriere", 0) or 0
    if nb_c is not None and nb_c > 0:
        return nb_v / nb_c
    return 0.0


def _rank_values(values: list[float], ascending: bool = True) -> list[int]:
    """Return dense ranks (1-based). ascending=True means smallest value gets rank 1."""
    indexed = sorted(enumerate(values), key=lambda x: x[1], reverse=not ascending)
    ranks = [0] * len(values)
    for rank, (orig_idx, _) in enumerate(indexed, start=1):
        ranks[orig_idx] = rank
    return ranks

# ===========================================================================
# TRAITEMENT
# ===========================================================================

def build_field_strength_features(
    partants: list[dict],
    logger: logging.Logger,
) -> list[dict]:
    """Build comprehensive field-strength features for every partant.

    Parameters
    ----------
    partants : list[dict]
        All partant records from partants_normalises.json.

    Returns
    -------
    list[dict]
        One dict per partant_uid with field-strength features.
    """

    # --- Group partants by course_uid ---
    course_partants: dict[str, list[dict]] = defaultdict(list)
    for p in partants:
        cuid = p.get("course_uid", "")
        if cuid:
            course_partants[cuid].append(p)

    logger.info("Courses uniques: %d", len(course_partants))

    # --- Pre-compute field features per course ---
    # course_uid -> dict of field-level features
    course_feats: dict[str, dict] = {}
    # course_uid -> list of per-runner relative features (same order as course_partants[cuid])
    runner_feats: dict[str, list[dict]] = {}

    for cuid, runners in course_partants.items():
        nb_partants = len(runners)

        # ====================================================================
        # 1. Niveau moyen du lot
        # ====================================================================
        win_rates = [_win_rate(r) for r in runners]

        gains_list = [
            r.get("gains_carriere_euros") for r in runners
        ]
        gains_clean = [g for g in gains_list if g is not None]

        handicaps = [
            r.get("handicap_valeur") for r in runners
        ]
        handicaps_clean = [h for h in handicaps if h is not None]

        rating_moyen = _safe_mean(win_rates)
        gains_moyen = _safe_mean(gains_clean)
        handicap_moyen = _safe_mean(handicaps_clean) if handicaps_clean else None

        # ====================================================================
        # 2. Dispersion du niveau
        # ====================================================================
        rating_std = _safe_stdev(win_rates)
        gains_std = _safe_stdev(gains_clean) if gains_clean else None
        rating_range: Optional[float] = None
        if win_rates:
            rating_range = max(win_rates) - min(win_rates)

        # ====================================================================
        # 3. Concentration des probabilites (marche)
        # ====================================================================
        probas = []
        for r in runners:
            pi = r.get("proba_implicite")
            if pi is not None and pi > 0:
                probas.append(pi)

        hhi_marche: Optional[float] = None
        proba_top1: Optional[float] = None
        proba_top3_sum: Optional[float] = None

        if probas:
            hhi_marche = sum(p ** 2 for p in probas)
            sorted_probas = sorted(probas, reverse=True)
            proba_top1 = sorted_probas[0]
            proba_top3_sum = sum(sorted_probas[:3])

        # ====================================================================
        # 4. Nb de chevaux competitifs
        # ====================================================================
        nb_competitifs: Optional[int] = None
        ratio_competitifs: Optional[float] = None

        if probas and nb_partants > 0:
            seuil = 1.0 / (2 * nb_partants)
            nb_competitifs = sum(1 for pi in probas if pi > seuil)
            ratio_competitifs = nb_competitifs / nb_partants

        # ====================================================================
        # 5. Densite du champ
        # ====================================================================
        ecart_favori_2eme: Optional[float] = None
        ecart_1er_dernier: Optional[float] = None
        is_open_race: Optional[bool] = None

        if probas and len(probas) >= 2:
            sorted_probas = sorted(probas, reverse=True)
            ecart_favori_2eme = sorted_probas[0] - sorted_probas[1]
            ecart_1er_dernier = sorted_probas[0] - sorted_probas[-1]

        if proba_top1 is not None:
            is_open_race = proba_top1 < 0.20

        # ====================================================================
        # 6. Experience du champ
        # ====================================================================
        experiences = []
        nb_inedits = 0
        for r in runners:
            nb_c = r.get("nb_courses_carriere")
            is_inedit = r.get("is_inedit", False)
            if nb_c is not None:
                experiences.append(nb_c)
            if nb_c == 0 or nb_c is None or is_inedit:
                nb_inedits += 1

        experience_moyenne = _safe_mean(experiences) if experiences else None
        pct_inedits = nb_inedits / nb_partants if nb_partants > 0 else None

        # Store course-level features
        course_feats[cuid] = {
            # 1. Niveau moyen
            "rating_moyen": rating_moyen,
            "gains_moyen": gains_moyen,
            "handicap_moyen": handicap_moyen,
            # 2. Dispersion
            "rating_std": rating_std,
            "gains_std": gains_std,
            "rating_range": rating_range,
            # 3. Concentration marche
            "hhi_marche": hhi_marche,
            "proba_top1": proba_top1,
            "proba_top3_sum": proba_top3_sum,
            # 4. Competitifs
            "nb_competitifs": nb_competitifs,
            "ratio_competitifs": ratio_competitifs,
            # 5. Densite
            "ecart_favori_2eme": ecart_favori_2eme,
            "ecart_1er_dernier": ecart_1er_dernier,
            "is_open_race": is_open_race,
            # 6. Experience
            "experience_moyenne": experience_moyenne,
            "nb_inedits": nb_inedits,
            "pct_inedits": pct_inedits,
            # context
            "nb_partants": nb_partants,
        }

        # ====================================================================
        # 7. Relative position of each horse within the field
        # ====================================================================
        # -- Rank by proba_implicite (highest proba = rank 1 = favorite) --
        proba_vals = [r.get("proba_implicite") or 0.0 for r in runners]
        rang_proba = _rank_values(proba_vals, ascending=False)

        # -- Rank by gains_carriere_euros (highest = rank 1) --
        gains_vals = [r.get("gains_carriere_euros") or 0.0 for r in runners]
        rang_gains = _rank_values(gains_vals, ascending=False)

        # -- Rank by nb_courses_carriere (most experienced = rank 1) --
        exp_vals = [r.get("nb_courses_carriere") or 0 for r in runners]
        rang_experience = _rank_values(exp_vals, ascending=False)

        rf = []
        for i in range(len(runners)):
            rf.append({
                "rang_proba": rang_proba[i],
                "rang_gains": rang_gains[i],
                "rang_experience": rang_experience[i],
            })
        runner_feats[cuid] = rf

    # --- Assemble output: one record per partant ---
    results = []
    for cuid, runners in course_partants.items():
        cf = course_feats.get(cuid, {})
        rfs = runner_feats.get(cuid, [])
        for i, p in enumerate(runners):
            feat: dict = {"partant_uid": p.get("partant_uid")}
            feat.update(cf)
            if i < len(rfs):
                feat.update(rfs[i])
            results.append(feat)

    logger.info(
        "Features construites: %d partants, %d courses",
        len(results),
        len(course_feats),
    )

    return results

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de force du champ (field strength)"
    )
    parser.add_argument(
        "--input", type=str, default=str(INPUT_PARTANTS),
        help="Chemin vers partants_normalises.json"
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Repertoire de sortie"
    )
    args = parser.parse_args()

    logger = setup_logging("field_strength_builder")
    logger.info("=" * 70)
    logger.info("field_strength_builder.py — Features de force du champ")
    logger.info("=" * 70)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        logger.error("Fichier introuvable: %s", input_path)
        sys.exit(1)

    partants = charger_json(input_path, logger)
    resultats = build_field_strength_features(partants, logger)

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    sauver_json(resultats, output_dir / "field_strength.json", logger)
    sauver_parquet(resultats, output_dir / "field_strength.parquet", logger)
    sauver_csv(resultats, output_dir / "field_strength.csv", logger)

    # Stats recap
    if resultats:
        keys = [k for k in resultats[0] if k != "partant_uid"]
        logger.info("Features (%d): %s", len(keys), ", ".join(keys))
        for k in keys:
            filled = sum(1 for r in resultats if r.get(k) is not None)
            logger.info("  %s: %d/%d (%.1f%%)", k, filled, len(resultats), 100 * filled / len(resultats))

    logger.info("Termine — %d partants traites", len(resultats))


if __name__ == "__main__":
    main()
