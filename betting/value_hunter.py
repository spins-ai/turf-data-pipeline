"""
betting.value_hunter
~~~~~~~~~~~~~~~~~~~~
Detect value bets by comparing calibrated model probabilities
against market-implied probabilities derived from final odds.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ValueBet:
    """A single value bet opportunity."""

    partant_uid: str
    runner_name: str
    model_proba: float
    market_proba: float
    cote_finale: float
    value: float          # model_proba * cote - 1
    edge: float           # model_proba - market_proba
    kelly_fraction: float # raw Kelly fraction (before fractional scaling)
    suggested_stake: float  # stake as fraction of bankroll


@dataclass
class ValueHunterConfig:
    """Configurable thresholds for value detection."""

    min_edge: float = 0.05          # minimum edge (model - market) to qualify
    min_odds: float = 1.5           # ignore very short prices
    max_odds: float = 50.0          # ignore extreme longshots
    kelly_fraction: float = 0.25    # fractional Kelly multiplier
    max_stake_pct: float = 0.05     # max stake as % of bankroll
    min_model_proba: float = 0.02   # ignore runners with near-zero model prob


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_market_probas(cotes: list[float]) -> list[float]:
    """Convert raw odds to normalized probabilities (remove overround).

    Parameters
    ----------
    cotes : list[float]
        Decimal odds for all runners in a race.

    Returns
    -------
    list[float]
        Normalized implied probabilities summing to 1.0.
    """
    raw_probas = np.array([1.0 / c for c in cotes])
    overround = raw_probas.sum()
    if overround > 0:
        return (raw_probas / overround).tolist()
    return raw_probas.tolist()


def _kelly_raw(p: float, b: float) -> float:
    """Full Kelly fraction: f* = (p*b - q) / b.

    Parameters
    ----------
    p : float
        Estimated true probability of winning.
    b : float
        Net decimal odds (decimal_odds - 1).

    Returns
    -------
    float
        Kelly fraction (can be negative = no bet).
    """
    q = 1.0 - p
    if b <= 0:
        return 0.0
    return (p * b - q) / b


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def find_value_bets(
    runners: list[dict],
    config: Optional[ValueHunterConfig] = None,
    bankroll: float = 1000.0,
) -> list[ValueBet]:
    """Scan a race field and return ranked value bets.

    Parameters
    ----------
    runners : list[dict]
        Each dict must contain:
        - partant_uid : str
        - nom (or runner_name) : str
        - model_proba : float  (calibrated probability of winning)
        - cote_finale : float  (decimal market odds)
    config : ValueHunterConfig, optional
        Detection thresholds. Uses defaults if not provided.
    bankroll : float
        Current bankroll for stake computation.

    Returns
    -------
    list[ValueBet]
        Value bets sorted by descending edge.
    """
    if config is None:
        config = ValueHunterConfig()

    # Filter runners with valid odds and model proba
    valid = []
    for r in runners:
        cote = r.get("cote_finale")
        mp = r.get("model_proba")
        if cote is not None and mp is not None and cote > 0 and mp > 0:
            valid.append(r)

    if not valid:
        return []

    # Compute normalized market probabilities
    cotes = [r["cote_finale"] for r in valid]
    market_probas = _normalize_market_probas(cotes)

    value_bets: list[ValueBet] = []

    for r, mkt_p in zip(valid, market_probas):
        model_p: float = r["model_proba"]
        cote: float = r["cote_finale"]
        name: str = r.get("nom", r.get("runner_name", r.get("partant_uid", "")))

        # Skip based on config filters
        if model_p < config.min_model_proba:
            continue
        if cote < config.min_odds or cote > config.max_odds:
            continue

        # Core value metrics
        value = model_p * cote - 1.0
        edge = model_p - mkt_p

        if edge < config.min_edge:
            continue
        if value <= 0:
            continue

        # Kelly sizing
        b = cote - 1.0
        raw_kelly = _kelly_raw(model_p, b)
        if raw_kelly <= 0:
            continue

        fractional_kelly = raw_kelly * config.kelly_fraction
        stake_pct = min(fractional_kelly, config.max_stake_pct)
        suggested_stake = round(stake_pct * bankroll, 2)

        value_bets.append(ValueBet(
            partant_uid=r.get("partant_uid", ""),
            runner_name=name,
            model_proba=round(model_p, 4),
            market_proba=round(mkt_p, 4),
            cote_finale=cote,
            value=round(value, 4),
            edge=round(edge, 4),
            kelly_fraction=round(raw_kelly, 4),
            suggested_stake=suggested_stake,
        ))

    # Sort by edge descending
    value_bets.sort(key=lambda vb: vb.edge, reverse=True)
    return value_bets


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Quick demo with synthetic data
    demo_runners = [
        {"partant_uid": "A", "nom": "Cheval Alpha", "model_proba": 0.35, "cote_finale": 3.5},
        {"partant_uid": "B", "nom": "Cheval Beta",  "model_proba": 0.25, "cote_finale": 4.0},
        {"partant_uid": "C", "nom": "Cheval Gamma", "model_proba": 0.15, "cote_finale": 8.0},
        {"partant_uid": "D", "nom": "Cheval Delta", "model_proba": 0.10, "cote_finale": 12.0},
        {"partant_uid": "E", "nom": "Cheval Epsilon", "model_proba": 0.08, "cote_finale": 15.0},
        {"partant_uid": "F", "nom": "Cheval Zeta",  "model_proba": 0.07, "cote_finale": 18.0},
    ]
    bets = find_value_bets(demo_runners, bankroll=2000.0)
    print(f"Found {len(bets)} value bet(s):")
    for vb in bets:
        print(f"  {vb.runner_name}: edge={vb.edge:.2%}, value={vb.value:.2f}, "
              f"stake={vb.suggested_stake:.2f}EUR (model={vb.model_proba:.1%} vs market={vb.market_proba:.1%})")
