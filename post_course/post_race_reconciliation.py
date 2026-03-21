#!/usr/bin/env python3
"""
post_course/post_race_reconciliation.py
========================================
Compare les predictions aux resultats reels apres une course.

Pour chaque prediction :
  - correct_win (bool), correct_place (bool)
  - rank_error (predit - reel)
  - roi_realise (gain reel base sur la mise)

Agregats : accuracy, log_loss realise, erreur de calibration, ROI.
Signale les pires predictions (forte confiance + erreur) pour analyse.

Aucun appel API : traitement 100 % local.

Usage :
    python3 post_course/post_race_reconciliation.py \\
        --predictions output/predictions/2025-03-01.json \\
        --results output/04_resultats/resultats_2025-03-01.json
"""

from __future__ import annotations

import json
import logging
import math
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
OUTPUT_DIR = _PROJECT_ROOT / "output" / "reconciliation"


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# DATACLASSES
# ===========================================================================

@dataclass
class ReconciliationRecord:
    """Resultat de la reconciliation d'une prediction."""
    date_course: str
    course_uid: str
    partant_uid: str
    model_name: str
    predicted_proba: float
    predicted_rank: int
    actual_position: Optional[int]
    cote_marche: Optional[float]
    mise: Optional[float]
    correct_win: bool
    correct_place: bool           # top 3
    rank_error: Optional[int]     # predicted_rank - actual_position
    roi_realise: Optional[float]  # gain/perte reel


@dataclass
class ReconciliationReport:
    """Rapport agrege de reconciliation."""
    date_course: str
    total_predictions: int
    matched: int
    accuracy_win: float
    accuracy_place: float
    mean_rank_error: Optional[float]
    log_loss: Optional[float]
    calibration_error: Optional[float]
    roi_global: Optional[float]
    total_mise: float
    total_gains: float
    worst_predictions: list[dict]
    details: list[dict]


# ===========================================================================
# CHARGEMENT
# ===========================================================================

def charger_json(path: Path, logger: logging.Logger) -> list[dict]:
    logger.info("Chargement: %s", path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info("  %d entrees chargees", len(data))
    return data


# ===========================================================================
# RECONCILIATION
# ===========================================================================

def _safe_log(p: float) -> float:
    """Log securise pour eviter log(0)."""
    eps = 1e-15
    return math.log(max(p, eps))


def reconcilier(
    predictions: list[dict],
    resultats: list[dict],
    logger: Optional[logging.Logger] = None,
) -> ReconciliationReport:
    """
    Reconcilie predictions et resultats reels.

    Args:
        predictions: liste de dicts (PredictionRecord-like)
        resultats: liste de dicts avec partant_uid, course_uid,
                   position_arrivee, is_gagnant, is_place, cote_finale

    Returns:
        ReconciliationReport
    """
    if logger is None:
        logger = setup_logging("post_race_reconciliation")

    # Indexer les resultats par (course_uid, partant_uid)
    results_idx: dict[tuple[str, str], dict] = {}
    for r in resultats:
        key = (r.get("course_uid", ""), r.get("partant_uid", ""))
        results_idx[key] = r

    records: list[ReconciliationRecord] = []
    total_mise = 0.0
    total_gains = 0.0
    log_loss_sum = 0.0
    log_loss_count = 0
    rank_errors: list[int] = []

    # Bins de calibration : (sum predicted_proba, sum actual_outcome, count)
    cal_bins: dict[int, list[float]] = {}
    for i in range(10):
        cal_bins[i] = [0.0, 0.0, 0.0]  # [sum_pred, sum_actual, count]

    for pred in predictions:
        key = (pred.get("course_uid", ""), pred.get("partant_uid", ""))
        actual = results_idx.get(key)

        if actual is None:
            continue

        actual_pos = actual.get("position_arrivee")
        is_gagnant = actual.get("is_gagnant", False) or (actual_pos == 1)
        is_place = actual.get("is_place", False) or (
            actual_pos is not None and actual_pos <= 3
        )

        predicted_proba = float(pred.get("predicted_proba", 0.0))
        predicted_rank = int(pred.get("predicted_rank", 99))
        cote = pred.get("cote_marche") or actual.get("cote_finale")
        mise = pred.get("mise")

        correct_win = (predicted_rank == 1) and is_gagnant
        correct_place = (predicted_rank <= 3) and is_place

        # Rank error
        rank_error: Optional[int] = None
        if actual_pos is not None:
            rank_error = predicted_rank - actual_pos
            rank_errors.append(abs(rank_error))

        # ROI realise
        roi_realise: Optional[float] = None
        if mise is not None and mise > 0 and cote is not None:
            total_mise += mise
            if is_gagnant and pred.get("ticket_propose", "simple_gagnant") == "simple_gagnant":
                gain = mise * cote
                total_gains += gain
                roi_realise = round((gain - mise) / mise, 4)
            elif is_place and pred.get("ticket_propose") == "simple_place":
                gain = mise * (cote / 3.0)
                total_gains += gain
                roi_realise = round((gain - mise) / mise, 4)
            else:
                roi_realise = -1.0  # mise perdue

        # Log-loss
        y = 1.0 if is_gagnant else 0.0
        ll = -(y * _safe_log(predicted_proba) + (1 - y) * _safe_log(1 - predicted_proba))
        log_loss_sum += ll
        log_loss_count += 1

        # Calibration bin
        bin_idx = min(int(predicted_proba * 10), 9)
        cal_bins[bin_idx][0] += predicted_proba
        cal_bins[bin_idx][1] += y
        cal_bins[bin_idx][2] += 1

        rec = ReconciliationRecord(
            date_course=pred.get("date_course", ""),
            course_uid=pred.get("course_uid", ""),
            partant_uid=pred.get("partant_uid", ""),
            model_name=pred.get("model_name", ""),
            predicted_proba=predicted_proba,
            predicted_rank=predicted_rank,
            actual_position=actual_pos,
            cote_marche=cote,
            mise=mise,
            correct_win=correct_win,
            correct_place=correct_place,
            rank_error=rank_error,
            roi_realise=roi_realise,
        )
        records.append(rec)

    matched = len(records)
    wins = sum(1 for r in records if r.correct_win)
    places = sum(1 for r in records if r.correct_place)

    accuracy_win = round(wins / matched, 4) if matched else 0.0
    accuracy_place = round(places / matched, 4) if matched else 0.0
    mean_rank_err = round(sum(rank_errors) / len(rank_errors), 2) if rank_errors else None
    log_loss_val = round(log_loss_sum / log_loss_count, 4) if log_loss_count else None
    roi_global = round((total_gains - total_mise) / total_mise, 4) if total_mise > 0 else None

    # Calibration error (ECE - Expected Calibration Error)
    ece = 0.0
    total_cal = sum(b[2] for b in cal_bins.values())
    for b in cal_bins.values():
        if b[2] > 0:
            avg_pred = b[0] / b[2]
            avg_actual = b[1] / b[2]
            ece += (b[2] / total_cal) * abs(avg_pred - avg_actual) if total_cal > 0 else 0.0
    calibration_error = round(ece, 4) if total_cal > 0 else None

    # Pires predictions : haute confiance et erreur
    details_list = [asdict(r) for r in records]
    worst = sorted(
        [r for r in records if not r.correct_win and r.predicted_proba > 0.3],
        key=lambda r: -r.predicted_proba,
    )[:20]

    report = ReconciliationReport(
        date_course=predictions[0].get("date_course", "") if predictions else "",
        total_predictions=len(predictions),
        matched=matched,
        accuracy_win=accuracy_win,
        accuracy_place=accuracy_place,
        mean_rank_error=mean_rank_err,
        log_loss=log_loss_val,
        calibration_error=calibration_error,
        roi_global=roi_global,
        total_mise=round(total_mise, 2),
        total_gains=round(total_gains, 2),
        worst_predictions=[asdict(w) for w in worst],
        details=details_list,
    )

    logger.info(
        "Reconciliation: %d/%d matched | acc_win=%.3f | acc_place=%.3f | ROI=%s",
        matched, len(predictions), accuracy_win, accuracy_place,
        f"{roi_global:.3f}" if roi_global is not None else "N/A",
    )

    return report


def format_report(report: ReconciliationReport) -> str:
    """Formate le rapport de reconciliation en texte lisible."""
    lines = [
        "=" * 70,
        "RAPPORT DE RECONCILIATION POST-COURSE",
        "=" * 70,
        f"Date         : {report.date_course}",
        f"Predictions  : {report.total_predictions}",
        f"Matchees     : {report.matched}",
        "",
        f"Accuracy win   : {report.accuracy_win:.4f}",
        f"Accuracy place : {report.accuracy_place:.4f}",
        f"Rank error moy : {report.mean_rank_error}",
        f"Log-loss       : {report.log_loss}",
        f"Calibration err: {report.calibration_error}",
        "",
        f"Mise totale    : {report.total_mise:.2f} EUR",
        f"Gains totaux   : {report.total_gains:.2f} EUR",
        f"ROI global     : {report.roi_global}",
        "",
    ]

    if report.worst_predictions:
        lines.append(f"--- Pires predictions (haute confiance + erreur) : {len(report.worst_predictions)} ---")
        for w in report.worst_predictions[:10]:
            lines.append(
                f"  {w['partant_uid']} | proba={w['predicted_proba']:.3f} "
                f"rank_predit={w['predicted_rank']} | position_reelle={w['actual_position']}"
            )

    lines.append("=" * 70)
    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Reconciliation predictions vs resultats")
    parser.add_argument("--predictions", type=str, required=True)
    parser.add_argument("--results", type=str, required=True)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    logger = setup_logging("post_race_reconciliation")
    logger.info("=" * 70)
    logger.info("post_race_reconciliation.py")
    logger.info("=" * 70)

    predictions = charger_json(Path(args.predictions), logger)
    resultats = charger_json(Path(args.results), logger)

    report = reconcilier(predictions, resultats, logger)
    txt = format_report(report)
    print(txt)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, ensure_ascii=False, indent=2, default=str)
        logger.info("Rapport sauve: %s", out_path)


if __name__ == "__main__":
    main()
