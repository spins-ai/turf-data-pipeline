#!/usr/bin/env python3
"""
post_course/feedback_learning_builder.py
=========================================
Construit les donnees de feedback pour le re-entrainement des modeles.

Fonctionnalites :
  - Extraction des erreurs de prediction pour recalibration
  - Identification des biais systematiques (ex. surestimation des favoris)
  - Construction du dataset de recalibration : (predicted_proba, actual_outcome)
  - Segmentation par contexte : discipline, terrain, taille du champ, tranche de cotes
  - Sortie : donnees pretes pour calibration.py

Aucun appel API : traitement 100 % local.

Usage :
    python3 post_course/feedback_learning_builder.py \\
        --reconciliation output/reconciliation/2025-03-01.json \\
        --output output/feedback/recalibration_data.json
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import numpy as np

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path("logs")
OUTPUT_DIR = Path("output/feedback")

# Tranches de cotes pour segmentation
ODDS_RANGES = [
    (0.0, 3.0, "favori"),
    (3.0, 6.0, "deuxieme_plan"),
    (6.0, 15.0, "outsider"),
    (15.0, float("inf"), "longshot"),
]

# Tranches de taille de champ
FIELD_SIZE_RANGES = [
    (0, 8, "petit"),
    (8, 13, "moyen"),
    (13, 99, "grand"),
]

CALIBRATION_BINS = 10


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("feedback_learning_builder")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(LOG_DIR / "feedback_learning_builder.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# ===========================================================================
# DATACLASSES
# ===========================================================================

@dataclass
class CalibrationPair:
    """Paire (prediction, resultat reel) pour recalibration."""
    predicted_proba: float
    actual_outcome: int             # 0 ou 1
    model_name: str
    course_uid: str
    partant_uid: str
    date_course: str
    discipline: Optional[str] = None
    terrain: Optional[str] = None
    field_size: Optional[int] = None
    odds_range: Optional[str] = None
    field_size_category: Optional[str] = None


@dataclass
class BiasReport:
    """Rapport de biais systematique detecte."""
    segment: str
    segment_value: str
    n_samples: int
    mean_predicted: float
    mean_actual: float
    bias: float                     # mean_predicted - mean_actual
    direction: str                  # "surestimation" ou "sous-estimation"


@dataclass
class FeedbackDataset:
    """Dataset complet de feedback pour recalibration."""
    calibration_pairs: list[dict]
    biases: list[dict]
    calibration_curve: dict          # bin -> {mean_pred, mean_actual, count}
    summary: dict


# ===========================================================================
# UTILITAIRES
# ===========================================================================

def _classify_odds(cote: Optional[float]) -> Optional[str]:
    if cote is None:
        return None
    for lo, hi, label in ODDS_RANGES:
        if lo <= cote < hi:
            return label
    return None


def _classify_field_size(n: Optional[int]) -> Optional[str]:
    if n is None:
        return None
    for lo, hi, label in FIELD_SIZE_RANGES:
        if lo <= n < hi:
            return label
    return None


# ===========================================================================
# CONSTRUCTION
# ===========================================================================

def build_calibration_pairs(
    reconciliation_records: list[dict],
    metadata: Optional[list[dict]] = None,
    logger: Optional[logging.Logger] = None,
) -> list[CalibrationPair]:
    """
    Extrait les paires (predicted_proba, actual_outcome) depuis les records
    de reconciliation.

    Args:
        reconciliation_records: details de reconciliation (chaque dict contient
            predicted_proba, correct_win, course_uid, partant_uid, etc.)
        metadata: donnees supplementaires par course (discipline, terrain, nb_partants)

    Returns:
        Liste de CalibrationPair
    """
    if logger is None:
        logger = setup_logging()

    # Indexer les metadata par course_uid si disponible
    meta_idx: dict[str, dict] = {}
    if metadata:
        for m in metadata:
            cuid = m.get("course_uid", "")
            if cuid:
                meta_idx[cuid] = m

    pairs: list[CalibrationPair] = []

    for rec in reconciliation_records:
        pred_proba = rec.get("predicted_proba")
        actual_pos = rec.get("actual_position")

        if pred_proba is None or actual_pos is None:
            continue

        actual_outcome = 1 if actual_pos == 1 else 0
        cote = rec.get("cote_marche")
        course_uid = rec.get("course_uid", "")

        meta = meta_idx.get(course_uid, {})
        discipline = meta.get("discipline")
        terrain = meta.get("terrain") or meta.get("etat_terrain")
        field_size = meta.get("nb_partants")

        pair = CalibrationPair(
            predicted_proba=float(pred_proba),
            actual_outcome=actual_outcome,
            model_name=rec.get("model_name", ""),
            course_uid=course_uid,
            partant_uid=rec.get("partant_uid", ""),
            date_course=rec.get("date_course", ""),
            discipline=discipline,
            terrain=terrain,
            field_size=int(field_size) if field_size is not None else None,
            odds_range=_classify_odds(cote),
            field_size_category=_classify_field_size(
                int(field_size) if field_size is not None else None
            ),
        )
        pairs.append(pair)

    logger.info("Paires de calibration construites: %d", len(pairs))
    return pairs


def detect_biases(
    pairs: list[CalibrationPair],
    min_samples: int = 30,
    logger: Optional[logging.Logger] = None,
) -> list[BiasReport]:
    """
    Identifie les biais systematiques par segment.

    Segments analyses :
      - odds_range (favori, outsider, etc.)
      - discipline
      - terrain
      - field_size_category
      - model_name

    Un biais est detecte quand mean_predicted != mean_actual de maniere significative.
    """
    if logger is None:
        logger = setup_logging()

    segments = ["odds_range", "discipline", "terrain", "field_size_category", "model_name"]
    biases: list[BiasReport] = []

    for seg in segments:
        groups: dict[str, list[CalibrationPair]] = {}
        for p in pairs:
            val = getattr(p, seg, None)
            if val is not None:
                groups.setdefault(str(val), []).append(p)

        for val, group in groups.items():
            if len(group) < min_samples:
                continue

            preds = np.array([p.predicted_proba for p in group])
            actuals = np.array([p.actual_outcome for p in group])

            mean_pred = float(np.mean(preds))
            mean_actual = float(np.mean(actuals))
            bias_val = mean_pred - mean_actual

            # Seuil de significativite : |bias| > 2 * stderr
            stderr = float(np.std(actuals) / np.sqrt(len(group)))
            if abs(bias_val) > 2 * stderr and stderr > 0:
                direction = "surestimation" if bias_val > 0 else "sous-estimation"
                br = BiasReport(
                    segment=seg,
                    segment_value=val,
                    n_samples=len(group),
                    mean_predicted=round(mean_pred, 4),
                    mean_actual=round(mean_actual, 4),
                    bias=round(bias_val, 4),
                    direction=direction,
                )
                biases.append(br)
                logger.warning(
                    "Biais detecte: %s=%s | pred=%.4f actual=%.4f | %s (n=%d)",
                    seg, val, mean_pred, mean_actual, direction, len(group),
                )

    logger.info("Biais detectes: %d", len(biases))
    return biases


def build_calibration_curve(
    pairs: list[CalibrationPair],
    n_bins: int = CALIBRATION_BINS,
) -> dict[int, dict]:
    """
    Construit la courbe de calibration : pour chaque bin de proba,
    moyenne des predictions vs moyenne des resultats reels.
    """
    bins: dict[int, dict] = {}
    for i in range(n_bins):
        bins[i] = {"sum_pred": 0.0, "sum_actual": 0.0, "count": 0}

    for p in pairs:
        bin_idx = min(int(p.predicted_proba * n_bins), n_bins - 1)
        bins[bin_idx]["sum_pred"] += p.predicted_proba
        bins[bin_idx]["sum_actual"] += p.actual_outcome
        bins[bin_idx]["count"] += 1

    curve: dict[int, dict] = {}
    for i, b in bins.items():
        if b["count"] > 0:
            curve[i] = {
                "mean_predicted": round(b["sum_pred"] / b["count"], 4),
                "mean_actual": round(b["sum_actual"] / b["count"], 4),
                "count": b["count"],
                "bin_range": f"[{i / n_bins:.1f}, {(i + 1) / n_bins:.1f})",
            }

    return curve


def build_feedback_dataset(
    reconciliation_records: list[dict],
    metadata: Optional[list[dict]] = None,
    logger: Optional[logging.Logger] = None,
) -> FeedbackDataset:
    """
    Pipeline complet : paires + biais + courbe de calibration.

    Args:
        reconciliation_records: sortie de reconcilier() (champ 'details')
        metadata: infos par course (discipline, terrain, nb_partants)

    Returns:
        FeedbackDataset pret pour calibration.py
    """
    if logger is None:
        logger = setup_logging()

    pairs = build_calibration_pairs(reconciliation_records, metadata, logger)
    biases = detect_biases(pairs, logger=logger)
    curve = build_calibration_curve(pairs)

    # Resume
    if pairs:
        all_preds = np.array([p.predicted_proba for p in pairs])
        all_actuals = np.array([p.actual_outcome for p in pairs])
        summary = {
            "n_pairs": len(pairs),
            "mean_predicted": round(float(np.mean(all_preds)), 4),
            "mean_actual": round(float(np.mean(all_actuals)), 4),
            "global_bias": round(float(np.mean(all_preds) - np.mean(all_actuals)), 4),
            "n_biases_detected": len(biases),
            "models": list(set(p.model_name for p in pairs)),
        }
    else:
        summary = {"n_pairs": 0}

    return FeedbackDataset(
        calibration_pairs=[asdict(p) for p in pairs],
        biases=[asdict(b) for b in biases],
        calibration_curve={str(k): v for k, v in curve.items()},
        summary=summary,
    )


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Construction du dataset de feedback / recalibration")
    parser.add_argument("--reconciliation", type=str, required=True,
                        help="Fichier JSON de reconciliation (sortie de post_race_reconciliation)")
    parser.add_argument("--metadata", type=str, default=None,
                        help="Fichier JSON metadata par course (optionnel)")
    parser.add_argument("--output", type=str, default=str(OUTPUT_DIR / "recalibration_data.json"))
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("feedback_learning_builder.py")
    logger.info("=" * 70)

    with open(args.reconciliation, "r", encoding="utf-8") as f:
        recon = json.load(f)

    # Le rapport contient un champ 'details' avec les records individuels
    records = recon.get("details", recon if isinstance(recon, list) else [])

    metadata = None
    if args.metadata:
        with open(args.metadata, "r", encoding="utf-8") as f:
            metadata = json.load(f)

    dataset = build_feedback_dataset(records, metadata, logger)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(asdict(dataset), f, ensure_ascii=False, indent=2, default=str)
    logger.info("Dataset de feedback sauve: %s (%d paires)", out_path, len(dataset.calibration_pairs))

    # Afficher resume
    print(json.dumps(dataset.summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
