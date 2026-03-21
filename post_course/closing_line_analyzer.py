#!/usr/bin/env python3
"""
post_course/closing_line_analyzer.py
=====================================
Analyse de la valeur de cloture (Closing Line Value — CLV).

Fonctionnalites :
  - Comparaison cote au moment du pari vs cote de cloture (derniere cote avant depart)
  - CLV = (closing_odds / bet_odds) - 1
  - CLV positif = on a battu la closing line (indicateur fort de rentabilite long terme)
  - Suivi du CLV dans le temps, par strategie
  - Significativite statistique du CLV

Aucun appel API : traitement 100 % local.

Usage :
    python3 post_course/closing_line_analyzer.py \\
        --bets output/predictions/2025-03-01.json \\
        --closing-odds output/07_cotes_marche/closing_odds.json \\
        --output output/clv/clv_report.json
"""

from __future__ import annotations

import json
import logging
import math
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import numpy as np

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output" / "clv"


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("closing_line_analyzer")
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
        fh = logging.FileHandler(LOG_DIR / "closing_line_analyzer.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# ===========================================================================
# DATACLASSES
# ===========================================================================

@dataclass
class CLVRecord:
    """Enregistrement CLV pour un pari individuel."""
    date_course: str
    course_uid: str
    partant_uid: str
    model_name: str
    strategy: Optional[str]      # nom de la strategie (ex. "value_bet", "kelly")
    bet_odds: float              # cote au moment du pari
    closing_odds: float          # derniere cote avant depart
    clv: float                   # (closing_odds / bet_odds) - 1
    clv_pct: float               # CLV en pourcentage
    beat_closing_line: bool      # True si CLV > 0
    mise: Optional[float] = None
    actual_position: Optional[int] = None


@dataclass
class CLVReport:
    """Rapport complet d'analyse CLV."""
    records: list[dict]
    summary: dict
    by_strategy: dict            # strategie -> stats
    by_model: dict               # modele -> stats
    time_series: dict            # date -> avg_clv
    statistical_significance: dict


# ===========================================================================
# CALCULS
# ===========================================================================

def compute_clv(bet_odds: float, closing_odds: float) -> float:
    """
    Calcule le Closing Line Value.

    CLV = (closing_odds / bet_odds) - 1

    > 0 : on a obtenu une meilleure cote que le marche final
    < 0 : on a parie a une cote inferieure au marche final
    """
    if bet_odds <= 0:
        return 0.0
    return (closing_odds / bet_odds) - 1.0


def clv_statistical_significance(
    clv_values: list[float],
    null_mean: float = 0.0,
) -> dict:
    """
    Teste la significativite statistique du CLV moyen.

    H0 : CLV moyen = 0 (pas de valeur ajoutee)
    H1 : CLV moyen != 0

    Utilise un t-test unilateral.
    """
    n = len(clv_values)
    if n < 10:
        return {
            "n": n,
            "mean_clv": None,
            "t_statistic": None,
            "p_value_approx": None,
            "significant": False,
            "detail": "Pas assez de donnees (n < 10)",
        }

    arr = np.array(clv_values)
    mean_clv = float(np.mean(arr))
    std_clv = float(np.std(arr, ddof=1))
    se = std_clv / math.sqrt(n)

    if se < 1e-10:
        return {
            "n": n,
            "mean_clv": round(mean_clv, 4),
            "t_statistic": None,
            "p_value_approx": None,
            "significant": False,
            "detail": "Variance nulle",
        }

    t_stat = (mean_clv - null_mean) / se

    # Approximation p-value via la distribution normale (n grand)
    # P(Z > |t|) pour un test bilateral
    z = abs(t_stat)
    # Approximation de la CDF normale via formule de Abramowitz & Stegun
    p_approx = _normal_sf(z) * 2  # bilateral

    significant = p_approx < 0.05

    return {
        "n": n,
        "mean_clv": round(mean_clv, 4),
        "std_clv": round(std_clv, 4),
        "t_statistic": round(t_stat, 4),
        "p_value_approx": round(p_approx, 6),
        "significant": significant,
        "confidence_interval_95": [
            round(mean_clv - 1.96 * se, 4),
            round(mean_clv + 1.96 * se, 4),
        ],
        "detail": (
            f"CLV moyen = {mean_clv:.4f}, t={t_stat:.3f}, p~{p_approx:.4f} "
            f"({'significatif' if significant else 'non significatif'} a 5%)"
        ),
    }


def _normal_sf(z: float) -> float:
    """Approximation de la survival function de la loi normale standard."""
    # Approximation Abramowitz & Stegun 26.2.17
    p = 0.2316419
    b1 = 0.319381530
    b2 = -0.356563782
    b3 = 1.781477937
    b4 = -1.821255978
    b5 = 1.330274429

    t = 1.0 / (1.0 + p * abs(z))
    pdf = math.exp(-0.5 * z * z) / math.sqrt(2 * math.pi)
    poly = ((((b5 * t + b4) * t + b3) * t + b2) * t + b1) * t
    sf = pdf * poly

    if z < 0:
        return 1.0 - sf
    return sf


# ===========================================================================
# ANALYSE
# ===========================================================================

def analyze_clv(
    bets: list[dict],
    closing_odds_data: list[dict],
    logger: Optional[logging.Logger] = None,
) -> CLVReport:
    """
    Analyse complete du CLV.

    Args:
        bets: liste de paris (dicts avec course_uid, partant_uid, cote_marche/bet_odds,
              model_name, mise, ticket_propose/strategy)
        closing_odds_data: cotes de cloture (dicts avec course_uid, partant_uid, closing_odds)

    Returns:
        CLVReport
    """
    if logger is None:
        logger = setup_logging()

    # Indexer les closing odds par (course_uid, partant_uid)
    closing_idx: dict[tuple[str, str], float] = {}
    for co in closing_odds_data:
        key = (co.get("course_uid", ""), co.get("partant_uid", ""))
        odds = co.get("closing_odds") or co.get("cote_finale") or co.get("cote_cloture")
        if odds is not None:
            closing_idx[key] = float(odds)

    records: list[CLVRecord] = []

    for bet in bets:
        course_uid = bet.get("course_uid", "")
        partant_uid = bet.get("partant_uid", "")
        bet_odds = bet.get("cote_marche") or bet.get("bet_odds")

        if bet_odds is None or bet_odds <= 1.0:
            continue

        key = (course_uid, partant_uid)
        closing = closing_idx.get(key)

        if closing is None or closing <= 1.0:
            continue

        clv_val = compute_clv(float(bet_odds), closing)

        rec = CLVRecord(
            date_course=bet.get("date_course", ""),
            course_uid=course_uid,
            partant_uid=partant_uid,
            model_name=bet.get("model_name", ""),
            strategy=bet.get("ticket_propose") or bet.get("strategy"),
            bet_odds=float(bet_odds),
            closing_odds=closing,
            clv=round(clv_val, 4),
            clv_pct=round(clv_val * 100, 2),
            beat_closing_line=clv_val > 0,
            mise=bet.get("mise"),
            actual_position=bet.get("actual_position"),
        )
        records.append(rec)

    logger.info("CLV calcule pour %d paris (sur %d)", len(records), len(bets))

    # Stats globales
    if records:
        clv_values = [r.clv for r in records]
        arr = np.array(clv_values)
        beats = sum(1 for r in records if r.beat_closing_line)

        summary = {
            "total_bets": len(records),
            "mean_clv": round(float(np.mean(arr)), 4),
            "median_clv": round(float(np.median(arr)), 4),
            "std_clv": round(float(np.std(arr)), 4),
            "beat_rate": round(beats / len(records), 4),
            "beats": beats,
            "losses": len(records) - beats,
            "mean_clv_pct": round(float(np.mean(arr)) * 100, 2),
        }
    else:
        summary = {"total_bets": 0}
        clv_values = []

    # Par strategie
    by_strategy: dict = {}
    strats: dict[str, list[CLVRecord]] = {}
    for r in records:
        s = r.strategy or "default"
        strats.setdefault(s, []).append(r)

    for s, recs in strats.items():
        vals = [r.clv for r in recs]
        beats = sum(1 for r in recs if r.beat_closing_line)
        by_strategy[s] = {
            "n": len(recs),
            "mean_clv": round(float(np.mean(vals)), 4),
            "beat_rate": round(beats / len(recs), 4),
            "std_clv": round(float(np.std(vals)), 4),
        }

    # Par modele
    by_model: dict = {}
    models: dict[str, list[CLVRecord]] = {}
    for r in records:
        models.setdefault(r.model_name, []).append(r)

    for m, recs in models.items():
        vals = [r.clv for r in recs]
        beats = sum(1 for r in recs if r.beat_closing_line)
        by_model[m] = {
            "n": len(recs),
            "mean_clv": round(float(np.mean(vals)), 4),
            "beat_rate": round(beats / len(recs), 4),
        }

    # Serie temporelle
    by_date: dict[str, list[float]] = {}
    for r in records:
        by_date.setdefault(r.date_course, []).append(r.clv)

    time_series = {
        d: round(float(np.mean(vals)), 4)
        for d, vals in sorted(by_date.items())
    }

    # Significativite statistique
    stat_sig = clv_statistical_significance(clv_values)

    report = CLVReport(
        records=[asdict(r) for r in records],
        summary=summary,
        by_strategy=by_strategy,
        by_model=by_model,
        time_series=time_series,
        statistical_significance=stat_sig,
    )

    logger.info(
        "CLV analyse: mean=%.4f, beat_rate=%.3f, significatif=%s",
        summary.get("mean_clv", 0), summary.get("beat_rate", 0),
        stat_sig.get("significant", False),
    )

    return report


def format_report(report: CLVReport) -> str:
    """Formate le rapport CLV en texte lisible."""
    s = report.summary
    lines = [
        "=" * 70,
        "RAPPORT CLOSING LINE VALUE (CLV)",
        "=" * 70,
        f"Paris analyses       : {s.get('total_bets', 0)}",
        f"CLV moyen            : {s.get('mean_clv', 0):.4f} ({s.get('mean_clv_pct', 0):.2f}%)",
        f"CLV median           : {s.get('median_clv', 0):.4f}",
        f"Beat rate            : {s.get('beat_rate', 0):.4f} ({s.get('beats', 0)}/{s.get('total_bets', 0)})",
        "",
    ]

    sig = report.statistical_significance
    lines.append("--- Significativite statistique ---")
    lines.append(f"  {sig.get('detail', 'N/A')}")
    if sig.get("confidence_interval_95"):
        ci = sig["confidence_interval_95"]
        lines.append(f"  IC 95% : [{ci[0]:.4f}, {ci[1]:.4f}]")
    lines.append("")

    if report.by_strategy:
        lines.append("--- Par strategie ---")
        for strat, vals in report.by_strategy.items():
            lines.append(
                f"  {strat}: n={vals['n']} | CLV={vals['mean_clv']:.4f} | "
                f"beat={vals['beat_rate']:.3f}"
            )
        lines.append("")

    if report.by_model:
        lines.append("--- Par modele ---")
        for model, vals in report.by_model.items():
            lines.append(
                f"  {model}: n={vals['n']} | CLV={vals['mean_clv']:.4f} | "
                f"beat={vals['beat_rate']:.3f}"
            )

    lines.append("=" * 70)
    return "\n".join(lines)


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Analyse Closing Line Value (CLV)")
    parser.add_argument("--bets", type=str, required=True,
                        help="Fichier JSON des paris (predictions archivees)")
    parser.add_argument("--closing-odds", type=str, required=True,
                        help="Fichier JSON des cotes de cloture")
    parser.add_argument("--output", type=str, default=str(OUTPUT_DIR / "clv_report.json"))
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("closing_line_analyzer.py")
    logger.info("=" * 70)

    with open(args.bets, "r", encoding="utf-8") as f:
        bets = json.load(f)
    with open(args.closing_odds, "r", encoding="utf-8") as f:
        closing_odds = json.load(f)

    report = analyze_clv(bets, closing_odds, logger)
    txt = format_report(report)
    print(txt)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(asdict(report), f, ensure_ascii=False, indent=2, default=str)
    logger.info("Rapport CLV sauve: %s", out_path)


if __name__ == "__main__":
    main()
