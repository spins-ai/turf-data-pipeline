#!/usr/bin/env python3
"""
post_course/concept_drift_detector.py
======================================
Detecte quand l'environnement de prediction change (concept drift).

Methodes :
  - Monitoring de l'accuracy / log_loss / ROI sur fenetres glissantes
  - Algorithme CUSUM (Cumulative Sum) pour detection de derive
  - Test de Page-Hinkley comme alternative
  - Alerte si la metrique glissante passe sous historique - 2*std
  - Suivi par modele, discipline, hippodrome

Aucun appel API : traitement 100 % local.

Usage :
    python3 post_course/concept_drift_detector.py \\
        --reconciliation-dir output/reconciliation/ \\
        --output output/drift/drift_report.json
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
OUTPUT_DIR = _PROJECT_ROOT / "output" / "drift"

# Fenetres glissantes (en nombre d'observations)
ROLLING_WINDOW_SHORT = 50
ROLLING_WINDOW_LONG = 200

# CUSUM
CUSUM_THRESHOLD = 5.0       # seuil de detection
CUSUM_DRIFT_MARGIN = 0.02   # tolerance autour de la moyenne historique

# Page-Hinkley
PH_THRESHOLD = 10.0
PH_ALPHA = 0.005             # facteur d'oubli

# Alerte
ALERT_SIGMA = 2.0             # nombre d'ecarts-types pour declenchement


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# DATACLASSES
# ===========================================================================

@dataclass
class DriftAlert:
    """Alerte de derive detectee."""
    method: str            # "cusum", "page_hinkley", "rolling_threshold"
    metric: str            # "accuracy", "log_loss", "roi"
    segment_type: str      # "global", "model", "discipline", "hippodrome"
    segment_value: str
    current_value: float
    reference_value: float
    threshold: float
    severity: str          # "warning", "critical"
    observation_index: int
    detail: str


@dataclass
class DriftReport:
    """Rapport complet de detection de derive."""
    alerts: list[dict]
    rolling_metrics: dict       # segment -> {metric -> [values]}
    cusum_states: dict          # segment -> {metric -> cusum_value}
    summary: dict


# ===========================================================================
# ALGORITHMES DE DETECTION
# ===========================================================================

def cusum_detect(
    values: list[float],
    target_mean: float,
    threshold: float = CUSUM_THRESHOLD,
    drift_margin: float = CUSUM_DRIFT_MARGIN,
) -> list[int]:
    """
    Algorithme CUSUM (Cumulative Sum) pour detection de derive.

    Detecte un changement de moyenne dans une serie temporelle.

    Args:
        values: serie de valeurs (ex. accuracy par course)
        target_mean: moyenne de reference (historique)
        threshold: seuil de declenchement
        drift_margin: marge de tolerance

    Returns:
        Liste des indices ou une derive est detectee.
    """
    g_pos = 0.0  # cumul positif (detection hausse)
    g_neg = 0.0  # cumul negatif (detection baisse)
    drift_points: list[int] = []

    for i, x in enumerate(values):
        s = x - target_mean
        g_pos = max(0.0, g_pos + s - drift_margin)
        g_neg = max(0.0, g_neg - s - drift_margin)

        if g_pos > threshold or g_neg > threshold:
            drift_points.append(i)
            # Reset apres detection
            g_pos = 0.0
            g_neg = 0.0

    return drift_points


def page_hinkley_detect(
    values: list[float],
    threshold: float = PH_THRESHOLD,
    alpha: float = PH_ALPHA,
) -> list[int]:
    """
    Test de Page-Hinkley pour detection de derive.

    Alternative au CUSUM avec facteur d'oubli.

    Args:
        values: serie de valeurs
        threshold: seuil de declenchement
        alpha: facteur de tolerance

    Returns:
        Liste des indices ou une derive est detectee.
    """
    if len(values) < 2:
        return []

    m_t = 0.0       # cumul
    M_t = 0.0       # minimum du cumul
    x_mean = 0.0    # moyenne courante
    drift_points: list[int] = []

    for i, x in enumerate(values):
        x_mean = (x_mean * i + x) / (i + 1)
        m_t += (x - x_mean - alpha)
        M_t = min(M_t, m_t)

        if (m_t - M_t) > threshold:
            drift_points.append(i)
            # Reset
            m_t = 0.0
            M_t = 0.0
            x_mean = x

    return drift_points


def rolling_threshold_detect(
    values: list[float],
    window: int = ROLLING_WINDOW_SHORT,
    n_sigma: float = ALERT_SIGMA,
    higher_is_better: bool = True,
) -> list[DriftAlert]:
    """
    Detecte quand la metrique glissante passe sous historique - n_sigma * std.

    Args:
        values: serie de valeurs
        window: taille de la fenetre glissante
        n_sigma: nombre d'ecarts-types pour l'alerte
        higher_is_better: si True, alerte quand ca baisse ; si False, quand ca monte

    Returns:
        Liste d'alertes
    """
    if len(values) < window + 10:
        return []

    arr = np.array(values)
    alerts: list[DriftAlert] = []

    # Calculer la moyenne et std sur la premiere moitie (reference)
    ref_end = len(values) // 2
    ref_mean = float(np.mean(arr[:ref_end]))
    ref_std = float(np.std(arr[:ref_end]))

    if ref_std < 1e-10:
        return []

    # Verifier les fenetres glissantes sur la deuxieme moitie
    for i in range(ref_end, len(values) - window + 1):
        window_mean = float(np.mean(arr[i:i + window]))

        if higher_is_better:
            threshold_val = ref_mean - n_sigma * ref_std
            if window_mean < threshold_val:
                severity = "critical" if window_mean < ref_mean - 3 * ref_std else "warning"
                alerts.append(DriftAlert(
                    method="rolling_threshold",
                    metric="",  # sera rempli par l'appelant
                    segment_type="",
                    segment_value="",
                    current_value=round(window_mean, 4),
                    reference_value=round(ref_mean, 4),
                    threshold=round(threshold_val, 4),
                    severity=severity,
                    observation_index=i,
                    detail=f"Fenetre [{i}:{i + window}] : {window_mean:.4f} < seuil {threshold_val:.4f}",
                ))
        else:
            threshold_val = ref_mean + n_sigma * ref_std
            if window_mean > threshold_val:
                severity = "critical" if window_mean > ref_mean + 3 * ref_std else "warning"
                alerts.append(DriftAlert(
                    method="rolling_threshold",
                    metric="",
                    segment_type="",
                    segment_value="",
                    current_value=round(window_mean, 4),
                    reference_value=round(ref_mean, 4),
                    threshold=round(threshold_val, 4),
                    severity=severity,
                    observation_index=i,
                    detail=f"Fenetre [{i}:{i + window}] : {window_mean:.4f} > seuil {threshold_val:.4f}",
                ))

    return alerts


# ===========================================================================
# EXTRACTION DES METRIQUES
# ===========================================================================

def _extract_metrics_from_records(
    records: list[dict],
) -> dict[str, list[float]]:
    """
    Extrait les series temporelles des metriques depuis les records de reconciliation.

    Returns:
        Dict avec cles 'accuracy' (0/1 par prediction), 'log_loss', 'roi'.
    """
    accuracy: list[float] = []
    log_losses: list[float] = []
    rois: list[float] = []

    for r in records:
        # Accuracy : correct_win (0 ou 1)
        cw = r.get("correct_win")
        if cw is not None:
            accuracy.append(1.0 if cw else 0.0)

        # Log-loss individuelle
        pp = r.get("predicted_proba")
        pos = r.get("actual_position")
        if pp is not None and pos is not None:
            y = 1.0 if pos == 1 else 0.0
            eps = 1e-15
            ll = -(y * np.log(max(pp, eps)) + (1 - y) * np.log(max(1 - pp, eps)))
            log_losses.append(float(ll))

        # ROI
        roi = r.get("roi_realise")
        if roi is not None:
            rois.append(float(roi))

    return {"accuracy": accuracy, "log_loss": log_losses, "roi": rois}


def _segment_records(
    records: list[dict],
    segment_field: str,
) -> dict[str, list[dict]]:
    """Segmente les records par un champ donne."""
    groups: dict[str, list[dict]] = {}
    for r in records:
        val = r.get(segment_field)
        if val is not None:
            groups.setdefault(str(val), []).append(r)
    return groups


# ===========================================================================
# DETECTION PRINCIPALE
# ===========================================================================

def detect_drift(
    reconciliation_records: list[dict],
    logger: Optional[logging.Logger] = None,
) -> DriftReport:
    """
    Lance la detection de derive sur toutes les metriques et segments.

    Args:
        reconciliation_records: liste de records de reconciliation (triee par date)

    Returns:
        DriftReport
    """
    if logger is None:
        logger = setup_logging("concept_drift_detector")

    all_alerts: list[DriftAlert] = []
    rolling_metrics: dict[str, dict] = {}
    cusum_states: dict[str, dict] = {}

    # --- Global ---
    metrics = _extract_metrics_from_records(reconciliation_records)
    rolling_metrics["global"] = {}

    metric_configs = [
        ("accuracy", True),   # higher is better
        ("log_loss", False),   # lower is better
        ("roi", True),
    ]

    for metric_name, higher_is_better in metric_configs:
        values = metrics.get(metric_name, [])
        if len(values) < ROLLING_WINDOW_SHORT + 10:
            continue

        arr = np.array(values)
        rolling_metrics["global"][metric_name] = {
            "mean": round(float(np.mean(arr)), 4),
            "std": round(float(np.std(arr)), 4),
            "recent_mean": round(float(np.mean(arr[-ROLLING_WINDOW_SHORT:])), 4),
            "n_values": len(values),
        }

        # CUSUM
        target = float(np.mean(arr))
        cusum_points = cusum_detect(values, target)
        if cusum_points:
            last_drift = cusum_points[-1]
            alert = DriftAlert(
                method="cusum",
                metric=metric_name,
                segment_type="global",
                segment_value="global",
                current_value=round(float(np.mean(arr[last_drift:])), 4),
                reference_value=round(target, 4),
                threshold=CUSUM_THRESHOLD,
                severity="warning",
                observation_index=last_drift,
                detail=f"CUSUM detecte {len(cusum_points)} points de derive",
            )
            all_alerts.append(alert)
            cusum_states.setdefault("global", {})[metric_name] = {
                "drift_points": cusum_points[-5:],
                "last_drift_index": last_drift,
            }

        # Page-Hinkley
        ph_points = page_hinkley_detect(values)
        if ph_points:
            last_ph = ph_points[-1]
            alert = DriftAlert(
                method="page_hinkley",
                metric=metric_name,
                segment_type="global",
                segment_value="global",
                current_value=round(float(np.mean(arr[last_ph:])), 4),
                reference_value=round(float(np.mean(arr)), 4),
                threshold=PH_THRESHOLD,
                severity="warning",
                observation_index=last_ph,
                detail=f"Page-Hinkley detecte {len(ph_points)} points de derive",
            )
            all_alerts.append(alert)

        # Rolling threshold
        rt_alerts = rolling_threshold_detect(values, ROLLING_WINDOW_SHORT, ALERT_SIGMA, higher_is_better)
        for rta in rt_alerts:
            rta.metric = metric_name
            rta.segment_type = "global"
            rta.segment_value = "global"
        # Garder seulement le dernier pour eviter le bruit
        if rt_alerts:
            all_alerts.append(rt_alerts[-1])

    # --- Par segment (model_name, discipline, hippodrome) ---
    segment_fields = {
        "model": "model_name",
        "discipline": "discipline",
        "hippodrome": "hippodrome",
    }

    for seg_type, seg_field in segment_fields.items():
        groups = _segment_records(reconciliation_records, seg_field)
        for seg_val, group_records in groups.items():
            group_metrics = _extract_metrics_from_records(group_records)

            for metric_name, higher_is_better in metric_configs:
                values = group_metrics.get(metric_name, [])
                if len(values) < ROLLING_WINDOW_SHORT:
                    continue

                arr = np.array(values)
                key = f"{seg_type}:{seg_val}"
                rolling_metrics.setdefault(key, {})[metric_name] = {
                    "mean": round(float(np.mean(arr)), 4),
                    "std": round(float(np.std(arr)), 4),
                    "recent_mean": round(float(np.mean(arr[-min(ROLLING_WINDOW_SHORT, len(arr)):])), 4),
                    "n_values": len(values),
                }

                # CUSUM par segment
                target = float(np.mean(arr))
                cusum_pts = cusum_detect(values, target)
                if cusum_pts:
                    alert = DriftAlert(
                        method="cusum",
                        metric=metric_name,
                        segment_type=seg_type,
                        segment_value=seg_val,
                        current_value=round(float(np.mean(arr[cusum_pts[-1]:])), 4),
                        reference_value=round(target, 4),
                        threshold=CUSUM_THRESHOLD,
                        severity="warning",
                        observation_index=cusum_pts[-1],
                        detail=f"CUSUM [{seg_type}={seg_val}] : {len(cusum_pts)} derives",
                    )
                    all_alerts.append(alert)

    # --- Resume ---
    n_critical = sum(1 for a in all_alerts if a.severity == "critical")
    n_warning = sum(1 for a in all_alerts if a.severity == "warning")

    summary = {
        "total_alerts": len(all_alerts),
        "critical": n_critical,
        "warning": n_warning,
        "segments_analysed": list(rolling_metrics.keys()),
        "drift_detected": len(all_alerts) > 0,
    }

    logger.info(
        "Drift detection: %d alertes (%d critiques, %d warnings)",
        len(all_alerts), n_critical, n_warning,
    )

    return DriftReport(
        alerts=[asdict(a) for a in all_alerts],
        rolling_metrics=rolling_metrics,
        cusum_states=cusum_states,
        summary=summary,
    )


def format_report(report: DriftReport) -> str:
    """Formate le rapport de derive en texte lisible."""
    lines = [
        "=" * 70,
        "RAPPORT DE DETECTION DE DERIVE (CONCEPT DRIFT)",
        "=" * 70,
        f"Alertes totales  : {report.summary.get('total_alerts', 0)}",
        f"  Critiques      : {report.summary.get('critical', 0)}",
        f"  Warnings       : {report.summary.get('warning', 0)}",
        f"Derive detectee  : {report.summary.get('drift_detected', False)}",
        "",
    ]

    if report.alerts:
        lines.append("--- Alertes ---")
        for a in report.alerts:
            lines.append(
                f"  [{a['severity'].upper()}] {a['method']} | {a['metric']} | "
                f"{a['segment_type']}={a['segment_value']} | "
                f"actuel={a['current_value']} vs ref={a['reference_value']}"
            )
            lines.append(f"    {a['detail']}")

    lines.append("=" * 70)
    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Detection de concept drift")
    parser.add_argument("--reconciliation-dir", type=str, required=True,
                        help="Repertoire contenant les fichiers de reconciliation JSON")
    parser.add_argument("--output", type=str, default=str(OUTPUT_DIR / "drift_report.json"))
    args = parser.parse_args()

    logger = setup_logging("concept_drift_detector")
    logger.info("=" * 70)
    logger.info("concept_drift_detector.py")
    logger.info("=" * 70)

    # Charger tous les fichiers de reconciliation
    recon_dir = Path(args.reconciliation_dir)
    all_records: list[dict] = []

    for fpath in sorted(recon_dir.glob("*.json")):
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        details = data.get("details", data if isinstance(data, list) else [])
        all_records.extend(details)

    logger.info("Records charges: %d depuis %s", len(all_records), recon_dir)

    if not all_records:
        logger.warning("Aucun record trouve, abandon.")
        return

    report = detect_drift(all_records, logger)
    txt = format_report(report)
    print(txt)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(asdict(report), f, ensure_ascii=False, indent=2, default=str)
    logger.info("Rapport sauve: %s", out_path)


if __name__ == "__main__":
    main()
