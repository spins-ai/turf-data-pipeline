#!/usr/bin/env python3
"""
post_course/model_decay_detector.py
====================================
Detecte quand un modele necessite un re-entrainement.

Verifications :
  - Performance recente (30 derniers jours) vs performance a l'entrainement
  - Degradation de la calibration dans le temps
  - Detection de glissement d'importance des features
  - Seuil : si la performance chute de > 10 % par rapport au baseline, signaler
  - Recommandation de frequence de re-entrainement basee sur le taux de degradation

Aucun appel API : traitement 100 % local.

Usage :
    python3 post_course/model_decay_detector.py \\
        --reconciliation-dir output/reconciliation/ \\
        --baseline output/models/baseline_metrics.json \\
        --output output/decay/decay_report.json
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
OUTPUT_DIR = _PROJECT_ROOT / "output" / "decay"

RECENT_WINDOW_DAYS = 30
DECAY_THRESHOLD_PCT = 10.0          # seuil de degradation (%)
CALIBRATION_ECE_BINS = 10
FEATURE_IMPORTANCE_SHIFT_THRESHOLD = 0.15  # cosine distance seuil


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# DATACLASSES
# ===========================================================================

@dataclass
class BaselineMetrics:
    """Metriques de reference d'un modele a l'entrainement."""
    model_name: str
    training_date: str
    accuracy_win: float
    accuracy_place: float
    log_loss: float
    calibration_ece: float
    roi: Optional[float] = None
    feature_importances: Optional[dict[str, float]] = None


@dataclass
class DecayAlert:
    """Alerte de degradation de modele."""
    model_name: str
    metric: str
    baseline_value: float
    current_value: float
    decay_pct: float             # pourcentage de degradation
    severity: str                # "warning", "retrain_needed"
    detail: str


@dataclass
class DecayReport:
    """Rapport complet de detection de degradation."""
    alerts: list[dict]
    model_metrics: dict          # model_name -> {metric -> {baseline, current, decay_pct}}
    calibration_evolution: dict  # model_name -> [ece par periode]
    retrain_recommendations: dict  # model_name -> {recommended, frequency_days, reason}
    summary: dict


# ===========================================================================
# METRIQUES
# ===========================================================================

def compute_ece(
    predicted_probas: list[float],
    actual_outcomes: list[int],
    n_bins: int = CALIBRATION_ECE_BINS,
) -> float:
    """Calcule l'Expected Calibration Error."""
    if not predicted_probas:
        return 0.0

    bins: dict[int, list] = {i: [[], []] for i in range(n_bins)}
    for p, y in zip(predicted_probas, actual_outcomes):
        b = min(int(p * n_bins), n_bins - 1)
        bins[b][0].append(p)
        bins[b][1].append(y)

    total = len(predicted_probas)
    ece = 0.0
    for preds, actuals in bins.values():
        if preds:
            avg_pred = np.mean(preds)
            avg_actual = np.mean(actuals)
            ece += (len(preds) / total) * abs(avg_pred - avg_actual)

    return round(float(ece), 4)


def compute_metrics_from_records(
    records: list[dict],
) -> dict:
    """Calcule accuracy, log_loss, ECE, ROI depuis les records."""
    if not records:
        return {}

    n = len(records)
    wins = sum(1 for r in records if r.get("correct_win"))
    places = sum(1 for r in records if r.get("correct_place"))

    preds = []
    actuals = []
    log_losses = []
    rois = []

    for r in records:
        pp = r.get("predicted_proba")
        pos = r.get("actual_position")
        if pp is not None and pos is not None:
            preds.append(float(pp))
            y = 1 if pos == 1 else 0
            actuals.append(y)
            eps = 1e-15
            ll = -(y * np.log(max(pp, eps)) + (1 - y) * np.log(max(1 - pp, eps)))
            log_losses.append(float(ll))

        roi = r.get("roi_realise")
        if roi is not None:
            rois.append(float(roi))

    return {
        "accuracy_win": round(wins / n, 4) if n else 0.0,
        "accuracy_place": round(places / n, 4) if n else 0.0,
        "log_loss": round(float(np.mean(log_losses)), 4) if log_losses else None,
        "calibration_ece": compute_ece(preds, actuals) if preds else None,
        "roi": round(float(np.mean(rois)), 4) if rois else None,
        "n_records": n,
    }


def cosine_distance(a: dict[str, float], b: dict[str, float]) -> float:
    """Distance cosinus entre deux vecteurs d'importance de features."""
    keys = sorted(set(a.keys()) | set(b.keys()))
    va = np.array([a.get(k, 0.0) for k in keys])
    vb = np.array([b.get(k, 0.0) for k in keys])

    norm_a = np.linalg.norm(va)
    norm_b = np.linalg.norm(vb)

    if norm_a < 1e-10 or norm_b < 1e-10:
        return 1.0

    cos_sim = float(np.dot(va, vb) / (norm_a * norm_b))
    return round(1.0 - cos_sim, 4)


# ===========================================================================
# DETECTION
# ===========================================================================

def detect_decay(
    reconciliation_records: list[dict],
    baselines: Optional[dict[str, BaselineMetrics]] = None,
    current_feature_importances: Optional[dict[str, dict[str, float]]] = None,
    logger: Optional[logging.Logger] = None,
) -> DecayReport:
    """
    Detecte la degradation de chaque modele.

    Args:
        reconciliation_records: records de reconciliation tries par date
        baselines: metriques de baseline par modele (optionnel)
        current_feature_importances: importances actuelles par modele (optionnel)

    Returns:
        DecayReport
    """
    if logger is None:
        logger = setup_logging("model_decay_detector")

    # Grouper par modele
    by_model: dict[str, list[dict]] = {}
    for r in reconciliation_records:
        model = r.get("model_name", "inconnu")
        by_model.setdefault(model, []).append(r)

    all_alerts: list[DecayAlert] = []
    model_metrics: dict = {}
    calibration_evolution: dict = {}
    retrain_recs: dict = {}

    # Date seuil pour "recent"
    cutoff_str = None
    dates = [r.get("date_course", "") for r in reconciliation_records if r.get("date_course")]
    if dates:
        try:
            max_date = max(dates)
            cutoff_dt = datetime.strptime(max_date, "%Y-%m-%d") - timedelta(days=RECENT_WINDOW_DAYS)
            cutoff_str = cutoff_dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    for model_name, records in by_model.items():
        logger.info("Analyse modele: %s (%d records)", model_name, len(records))

        # Separer recent vs historique
        if cutoff_str:
            recent = [r for r in records if r.get("date_course", "") >= cutoff_str]
            historical = [r for r in records if r.get("date_course", "") < cutoff_str]
        else:
            mid = len(records) // 2
            recent = records[mid:]
            historical = records[:mid]

        metrics_recent = compute_metrics_from_records(recent)
        metrics_historical = compute_metrics_from_records(historical)

        # Baseline : priorite au baseline fourni, sinon utiliser l'historique
        baseline = baselines.get(model_name) if baselines else None

        ref_metrics: dict = {}
        if baseline:
            ref_metrics = {
                "accuracy_win": baseline.accuracy_win,
                "accuracy_place": baseline.accuracy_place,
                "log_loss": baseline.log_loss,
                "calibration_ece": baseline.calibration_ece,
                "roi": baseline.roi,
            }
        else:
            ref_metrics = metrics_historical

        # Comparer recentes vs baseline
        model_metrics[model_name] = {}
        metrics_to_check = [
            ("accuracy_win", True),    # higher is better
            ("accuracy_place", True),
            ("log_loss", False),       # lower is better
            ("calibration_ece", False),
            ("roi", True),
        ]

        for metric_name, higher_is_better in metrics_to_check:
            ref_val = ref_metrics.get(metric_name)
            cur_val = metrics_recent.get(metric_name)

            if ref_val is None or cur_val is None or ref_val == 0:
                continue

            if higher_is_better:
                decay_pct = ((ref_val - cur_val) / abs(ref_val)) * 100
            else:
                decay_pct = ((cur_val - ref_val) / abs(ref_val)) * 100

            model_metrics[model_name][metric_name] = {
                "baseline": ref_val,
                "current": cur_val,
                "decay_pct": round(decay_pct, 2),
            }

            if decay_pct > DECAY_THRESHOLD_PCT:
                severity = "retrain_needed" if decay_pct > 2 * DECAY_THRESHOLD_PCT else "warning"
                alert = DecayAlert(
                    model_name=model_name,
                    metric=metric_name,
                    baseline_value=ref_val,
                    current_value=cur_val,
                    decay_pct=round(decay_pct, 2),
                    severity=severity,
                    detail=(
                        f"{model_name}: {metric_name} degrade de {decay_pct:.1f}% "
                        f"(baseline={ref_val:.4f}, recent={cur_val:.4f})"
                    ),
                )
                all_alerts.append(alert)
                logger.warning("Degradation: %s", alert.detail)

        # Calibration evolution : decouper en periodes et calculer ECE
        if len(records) >= 50:
            n_periods = min(5, len(records) // 50)
            chunk_size = len(records) // n_periods
            ece_evolution = []
            for i in range(n_periods):
                chunk = records[i * chunk_size:(i + 1) * chunk_size]
                preds = [float(r["predicted_proba"]) for r in chunk
                         if r.get("predicted_proba") is not None and r.get("actual_position") is not None]
                acts = [1 if r.get("actual_position") == 1 else 0 for r in chunk
                        if r.get("predicted_proba") is not None and r.get("actual_position") is not None]
                if preds:
                    ece_evolution.append(compute_ece(preds, acts))
            calibration_evolution[model_name] = ece_evolution

        # Feature importance shift
        if current_feature_importances and baseline and baseline.feature_importances:
            current_fi = current_feature_importances.get(model_name, {})
            if current_fi:
                dist = cosine_distance(baseline.feature_importances, current_fi)
                if dist > FEATURE_IMPORTANCE_SHIFT_THRESHOLD:
                    alert = DecayAlert(
                        model_name=model_name,
                        metric="feature_importance_shift",
                        baseline_value=0.0,
                        current_value=dist,
                        decay_pct=round(dist * 100, 2),
                        severity="warning",
                        detail=f"{model_name}: distance cosinus feature importance = {dist:.4f}",
                    )
                    all_alerts.append(alert)

        # Recommandation de re-entrainement
        model_alerts = [a for a in all_alerts if a.model_name == model_name]
        if model_alerts:
            max_decay = max(a.decay_pct for a in model_alerts)
            # Estimer frequence : plus la degradation est rapide, plus souvent re-entrainer
            if max_decay > 2 * DECAY_THRESHOLD_PCT:
                freq = 7
                reason = "Degradation severe (>{:.0f}%), re-entrainement hebdomadaire recommande".format(
                    2 * DECAY_THRESHOLD_PCT
                )
            elif max_decay > DECAY_THRESHOLD_PCT:
                freq = 14
                reason = "Degradation moderee (>{:.0f}%), re-entrainement bi-mensuel recommande".format(
                    DECAY_THRESHOLD_PCT
                )
            else:
                freq = 30
                reason = "Degradation legere, re-entrainement mensuel suffisant"

            retrain_recs[model_name] = {
                "recommended": True,
                "frequency_days": freq,
                "max_decay_pct": round(max_decay, 2),
                "reason": reason,
            }
        else:
            retrain_recs[model_name] = {
                "recommended": False,
                "frequency_days": 60,
                "max_decay_pct": 0.0,
                "reason": "Performance stable, re-entrainement bimestriel suffisant",
            }

    # Resume
    models_needing_retrain = [m for m, r in retrain_recs.items() if r["recommended"]]
    summary = {
        "total_models": len(by_model),
        "total_alerts": len(all_alerts),
        "models_needing_retrain": models_needing_retrain,
        "models_stable": [m for m in by_model if m not in models_needing_retrain],
    }

    logger.info(
        "Decay detection: %d modeles analyses, %d alertes, %d a re-entrainer",
        len(by_model), len(all_alerts), len(models_needing_retrain),
    )

    return DecayReport(
        alerts=[asdict(a) for a in all_alerts],
        model_metrics=model_metrics,
        calibration_evolution=calibration_evolution,
        retrain_recommendations=retrain_recs,
        summary=summary,
    )


def format_report(report: DecayReport) -> str:
    """Formate le rapport de degradation en texte lisible."""
    lines = [
        "=" * 70,
        "RAPPORT DE DEGRADATION DES MODELES",
        "=" * 70,
        f"Modeles analyses     : {report.summary.get('total_models', 0)}",
        f"Alertes totales      : {report.summary.get('total_alerts', 0)}",
        f"A re-entrainer       : {report.summary.get('models_needing_retrain', [])}",
        "",
    ]

    for model, metrics in report.model_metrics.items():
        lines.append(f"--- {model} ---")
        for metric, vals in metrics.items():
            marker = " !" if vals["decay_pct"] > DECAY_THRESHOLD_PCT else "  "
            lines.append(
                f" {marker} {metric}: baseline={vals['baseline']:.4f} "
                f"current={vals['current']:.4f} decay={vals['decay_pct']:.1f}%"
            )

        rec = report.retrain_recommendations.get(model, {})
        if rec.get("recommended"):
            lines.append(f"  => RETRAIN recommande (tous les {rec['frequency_days']}j) : {rec['reason']}")
        lines.append("")

    lines.append("=" * 70)
    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Detection de degradation de modele")
    parser.add_argument("--reconciliation-dir", type=str, required=True)
    parser.add_argument("--baseline", type=str, default=None,
                        help="Fichier JSON des metriques baseline par modele")
    parser.add_argument("--output", type=str, default=str(OUTPUT_DIR / "decay_report.json"))
    args = parser.parse_args()

    logger = setup_logging("model_decay_detector")
    logger.info("=" * 70)
    logger.info("model_decay_detector.py")
    logger.info("=" * 70)

    # Charger reconciliations
    recon_dir = Path(args.reconciliation_dir)
    all_records: list[dict] = []
    for fpath in sorted(recon_dir.glob("*.json")):
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        details = data.get("details", data if isinstance(data, list) else [])
        all_records.extend(details)

    logger.info("Records charges: %d", len(all_records))

    # Charger baseline si fourni
    baselines: Optional[dict[str, BaselineMetrics]] = None
    if args.baseline:
        bp = Path(args.baseline)
        if bp.exists():
            with open(bp, "r", encoding="utf-8") as f:
                raw = json.load(f)
            baselines = {}
            for model_name, vals in raw.items():
                baselines[model_name] = BaselineMetrics(
                    model_name=model_name,
                    training_date=vals.get("training_date", ""),
                    accuracy_win=vals.get("accuracy_win", 0.0),
                    accuracy_place=vals.get("accuracy_place", 0.0),
                    log_loss=vals.get("log_loss", 1.0),
                    calibration_ece=vals.get("calibration_ece", 0.1),
                    roi=vals.get("roi"),
                    feature_importances=vals.get("feature_importances"),
                )

    if not all_records:
        logger.warning("Aucun record trouve, abandon.")
        return

    report = detect_decay(all_records, baselines, logger=logger)
    txt = format_report(report)
    print(txt)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(asdict(report), f, ensure_ascii=False, indent=2, default=str)
    logger.info("Rapport sauve: %s", out_path)


if __name__ == "__main__":
    main()
