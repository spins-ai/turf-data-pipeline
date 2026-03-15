"""
betting.kelly_optimizer
~~~~~~~~~~~~~~~~~~~~~~~
Kelly criterion bet sizing with fractional variants and
portfolio optimization for simultaneous bets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------

FULL_KELLY = 1.0
HALF_KELLY = 0.5
QUARTER_KELLY = 0.25


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class KellyResult:
    """Result of Kelly calculation for a single bet."""

    partant_uid: str
    model_proba: float
    decimal_odds: float
    full_kelly: float       # raw f*
    fractional_kelly: float # f* * fraction
    stake_pct: float        # capped at max_stake
    stake_amount: float     # actual stake in currency
    edge: float             # expected edge (p*b - q) / b simplified
    is_bet: bool            # True if positive Kelly


@dataclass
class KellyConfig:
    """Configuration for Kelly sizing."""

    fraction: float = QUARTER_KELLY  # fractional Kelly multiplier
    max_stake_pct: float = 0.05      # max stake as % of bankroll
    min_stake: float = 1.0           # minimum bet in currency
    min_proba: float = 0.01          # ignore near-zero probabilities


# ---------------------------------------------------------------------------
# Core Kelly
# ---------------------------------------------------------------------------

def _full_kelly(p: float, b: float) -> float:
    """Full Kelly fraction: f* = (p*b - q) / b.

    Parameters
    ----------
    p : float
        Model probability of winning.
    b : float
        Net odds (decimal_odds - 1).

    Returns
    -------
    float
        Kelly fraction. Negative means no bet.
    """
    if b <= 0 or p <= 0 or p >= 1:
        return 0.0
    q = 1.0 - p
    return (p * b - q) / b


def kelly_stake(
    model_proba: float,
    decimal_odds: float,
    bankroll: float,
    config: Optional[KellyConfig] = None,
    partant_uid: str = "",
) -> KellyResult:
    """Compute Kelly-optimal stake for a single bet.

    Parameters
    ----------
    model_proba : float
        Calibrated probability of winning.
    decimal_odds : float
        Decimal odds (e.g. 3.5 means pays 3.5x for 1 unit staked).
    bankroll : float
        Current bankroll.
    config : KellyConfig, optional
        Sizing parameters.
    partant_uid : str
        Runner identifier.

    Returns
    -------
    KellyResult
    """
    if config is None:
        config = KellyConfig()

    b = decimal_odds - 1.0
    fk = _full_kelly(model_proba, b)
    is_bet = fk > 0 and model_proba >= config.min_proba

    fractional = fk * config.fraction if is_bet else 0.0
    stake_pct = min(fractional, config.max_stake_pct) if is_bet else 0.0
    stake_amount = round(max(stake_pct * bankroll, 0.0), 2)

    # Enforce minimum stake (set to 0 if below threshold)
    if 0 < stake_amount < config.min_stake:
        stake_amount = 0.0
        stake_pct = 0.0
        is_bet = False

    edge = (model_proba * decimal_odds - 1.0) if decimal_odds > 0 else 0.0

    return KellyResult(
        partant_uid=partant_uid,
        model_proba=round(model_proba, 4),
        decimal_odds=decimal_odds,
        full_kelly=round(fk, 6),
        fractional_kelly=round(fractional, 6),
        stake_pct=round(stake_pct, 6),
        stake_amount=stake_amount,
        edge=round(edge, 4),
        is_bet=is_bet,
    )


# ---------------------------------------------------------------------------
# Portfolio Kelly
# ---------------------------------------------------------------------------

def portfolio_kelly(
    bets: list[dict],
    bankroll: float,
    config: Optional[KellyConfig] = None,
    max_total_exposure: float = 0.30,
) -> list[KellyResult]:
    """Optimize stake allocation across multiple simultaneous bets.

    Uses independent Kelly fractions with a total exposure cap.
    When the sum of individual Kelly fractions exceeds the cap,
    stakes are proportionally scaled down.

    Parameters
    ----------
    bets : list[dict]
        Each dict must contain:
        - partant_uid : str
        - model_proba : float
        - decimal_odds : float (or cote_finale)
    bankroll : float
        Current bankroll.
    config : KellyConfig, optional
        Sizing parameters.
    max_total_exposure : float
        Maximum fraction of bankroll to risk across all bets.

    Returns
    -------
    list[KellyResult]
        Sized bets, sorted by stake descending.
    """
    if config is None:
        config = KellyConfig()

    results: list[KellyResult] = []
    for bet in bets:
        odds = bet.get("decimal_odds", bet.get("cote_finale", 0.0))
        proba = bet.get("model_proba", 0.0)
        uid = bet.get("partant_uid", "")

        kr = kelly_stake(proba, odds, bankroll, config, partant_uid=uid)
        if kr.is_bet:
            results.append(kr)

    if not results:
        return results

    # Check total exposure
    total_pct = sum(r.stake_pct for r in results)

    if total_pct > max_total_exposure:
        # Scale down proportionally
        scale = max_total_exposure / total_pct
        scaled: list[KellyResult] = []
        for r in results:
            new_pct = r.stake_pct * scale
            new_amount = round(max(new_pct * bankroll, 0.0), 2)
            is_bet = new_amount >= config.min_stake

            scaled.append(KellyResult(
                partant_uid=r.partant_uid,
                model_proba=r.model_proba,
                decimal_odds=r.decimal_odds,
                full_kelly=r.full_kelly,
                fractional_kelly=round(r.fractional_kelly * scale, 6),
                stake_pct=round(new_pct, 6),
                stake_amount=new_amount if is_bet else 0.0,
                edge=r.edge,
                is_bet=is_bet,
            ))
        results = [r for r in scaled if r.is_bet]

    # Sort by stake descending
    results.sort(key=lambda r: r.stake_amount, reverse=True)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    bankroll = 2000.0
    print(f"Bankroll: {bankroll:.0f}EUR\n")

    # Single bet
    res = kelly_stake(0.35, 3.5, bankroll)
    print(f"Single bet: p={res.model_proba}, odds={res.decimal_odds}")
    print(f"  Full Kelly: {res.full_kelly:.4f}, Stake: {res.stake_amount:.2f}EUR "
          f"({res.stake_pct:.2%})\n")

    # Portfolio
    portfolio = [
        {"partant_uid": "A", "model_proba": 0.35, "decimal_odds": 3.5},
        {"partant_uid": "B", "model_proba": 0.20, "decimal_odds": 6.0},
        {"partant_uid": "C", "model_proba": 0.12, "decimal_odds": 10.0},
    ]
    sized = portfolio_kelly(portfolio, bankroll)
    print("Portfolio Kelly:")
    total = 0.0
    for r in sized:
        print(f"  {r.partant_uid}: stake={r.stake_amount:.2f}EUR ({r.stake_pct:.2%}), "
              f"edge={r.edge:.2%}")
        total += r.stake_amount
    print(f"  Total exposure: {total:.2f}EUR ({total/bankroll:.1%})")
