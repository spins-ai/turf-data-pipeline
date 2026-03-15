"""
betting.ticket_optimizer
~~~~~~~~~~~~~~~~~~~~~~~~
Optimize multi-bet tickets for tierce, quarte, and quinte
using model-based probabilities to select the best combinations
within a budget constraint.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from itertools import permutations, combinations
from typing import Optional

import numpy as np


# ---------------------------------------------------------------------------
# Enums and data structures
# ---------------------------------------------------------------------------

class BetType(Enum):
    TIERCE = 3   # top 3 in order
    QUARTE = 4   # top 4 in order
    QUINTE = 5   # top 5 in order


@dataclass
class Ticket:
    """A single multi-bet ticket (combination of runners in finishing order)."""

    runners: tuple[str, ...]       # partant_uids in predicted order
    runner_names: tuple[str, ...]  # human-readable names
    joint_proba: float             # model-based joint probability
    expected_value: float          # EV = joint_proba * estimated_payout - cost
    cost: float                    # cost of this ticket


@dataclass
class TicketOptimizerConfig:
    """Configuration for ticket optimization."""

    bet_type: BetType = BetType.TIERCE
    max_tickets: int = 100           # budget constraint (max combinations)
    unit_cost: float = 1.0           # cost per ticket in EUR
    partial_order: bool = False      # True = any order (desordre), False = exact order (ordre)
    top_n_candidates: int = 8        # only consider top N runners by model proba
    payout_estimates: Optional[dict[BetType, float]] = None  # average payout per type


# Default average payouts (rough estimates for PMU tierce/quarte/quinte)
DEFAULT_PAYOUTS: dict[BetType, dict[str, float]] = {
    BetType.TIERCE: {"ordre": 500.0, "desordre": 50.0},
    BetType.QUARTE: {"ordre": 5000.0, "desordre": 300.0},
    BetType.QUINTE: {"ordre": 50000.0, "desordre": 1000.0},
}


# ---------------------------------------------------------------------------
# Probability computation
# ---------------------------------------------------------------------------

def _joint_ordered_proba(
    runner_indices: tuple[int, ...],
    probas: np.ndarray,
) -> float:
    """Compute joint probability of runners finishing in exact order.

    Uses conditional probability: P(A 1st) * P(B 2nd | A 1st) * ...
    Approximation: P(B 2nd | A 1st) = P(B) / (1 - P(A))

    Parameters
    ----------
    runner_indices : tuple[int, ...]
        Indices of runners in predicted finishing order.
    probas : np.ndarray
        Normalized model probabilities for all runners.

    Returns
    -------
    float
        Joint probability of this exact finishing order.
    """
    p = 1.0
    remaining_mass = 1.0

    for idx in runner_indices:
        if remaining_mass <= 0:
            return 0.0
        conditional_p = probas[idx] / remaining_mass
        p *= conditional_p
        remaining_mass -= probas[idx]

    return p


def _joint_unordered_proba(
    runner_indices: tuple[int, ...],
    probas: np.ndarray,
) -> float:
    """Compute probability of runners finishing in top-N in any order.

    Sums joint_ordered_proba over all permutations of the given runners.

    Parameters
    ----------
    runner_indices : tuple[int, ...]
        Indices of runners (order does not matter).
    probas : np.ndarray
        Normalized model probabilities.

    Returns
    -------
    float
        Probability that these runners occupy the top-N positions.
    """
    total = 0.0
    for perm in permutations(runner_indices):
        total += _joint_ordered_proba(perm, probas)
    return total


# ---------------------------------------------------------------------------
# Main optimizer
# ---------------------------------------------------------------------------

def optimize_tickets(
    runners: list[dict],
    config: Optional[TicketOptimizerConfig] = None,
) -> list[Ticket]:
    """Generate and rank optimal tickets for a race.

    Parameters
    ----------
    runners : list[dict]
        Each dict must contain:
        - partant_uid : str
        - nom (or runner_name) : str
        - model_proba : float  (calibrated probability of winning)
    config : TicketOptimizerConfig, optional
        Optimization parameters.

    Returns
    -------
    list[Ticket]
        Ranked tickets sorted by expected value descending, limited to
        config.max_tickets entries.
    """
    if config is None:
        config = TicketOptimizerConfig()

    n_positions = config.bet_type.value  # 3, 4, or 5

    # Extract and normalize probabilities
    valid_runners = [
        r for r in runners
        if r.get("model_proba") is not None and r["model_proba"] > 0
    ]
    if len(valid_runners) < n_positions:
        return []

    # Sort by model_proba descending, take top candidates
    valid_runners.sort(key=lambda r: r["model_proba"], reverse=True)
    candidates = valid_runners[: config.top_n_candidates]

    probas_raw = np.array([r["model_proba"] for r in candidates])
    probas = probas_raw / probas_raw.sum()  # renormalize among candidates

    uids = [r.get("partant_uid", "") for r in candidates]
    names = [r.get("nom", r.get("runner_name", r.get("partant_uid", "")))
             for r in candidates]

    # Determine payout estimate
    payouts = DEFAULT_PAYOUTS[config.bet_type]
    mode = "desordre" if config.partial_order else "ordre"
    payout_estimate = payouts[mode]

    # Generate all combinations / permutations
    n_candidates = len(candidates)
    tickets: list[Ticket] = []

    if config.partial_order:
        # Unordered: enumerate combinations of n_positions from candidates
        for combo in combinations(range(n_candidates), n_positions):
            jp = _joint_unordered_proba(combo, probas)
            ev = jp * payout_estimate - config.unit_cost
            ticket_names = tuple(names[i] for i in combo)
            ticket_uids = tuple(uids[i] for i in combo)
            tickets.append(Ticket(
                runners=ticket_uids,
                runner_names=ticket_names,
                joint_proba=jp,
                expected_value=round(ev, 4),
                cost=config.unit_cost,
            ))
    else:
        # Ordered: enumerate permutations of n_positions from candidates
        for perm in permutations(range(n_candidates), n_positions):
            jp = _joint_ordered_proba(perm, probas)
            ev = jp * payout_estimate - config.unit_cost
            ticket_names = tuple(names[i] for i in perm)
            ticket_uids = tuple(uids[i] for i in perm)
            tickets.append(Ticket(
                runners=ticket_uids,
                runner_names=ticket_names,
                joint_proba=jp,
                expected_value=round(ev, 4),
                cost=config.unit_cost,
            ))

    # Sort by expected value descending and trim
    tickets.sort(key=lambda t: t.expected_value, reverse=True)
    tickets = tickets[: config.max_tickets]

    return tickets


def ticket_summary(tickets: list[Ticket]) -> dict:
    """Compute summary statistics for a set of tickets.

    Parameters
    ----------
    tickets : list[Ticket]

    Returns
    -------
    dict
        Summary with total_cost, total_ev, best_ticket, coverage.
    """
    if not tickets:
        return {"total_cost": 0, "total_ev": 0, "n_tickets": 0}

    total_cost = sum(t.cost for t in tickets)
    total_ev = sum(t.expected_value for t in tickets)
    total_proba = sum(t.joint_proba for t in tickets)

    return {
        "n_tickets": len(tickets),
        "total_cost": round(total_cost, 2),
        "total_ev": round(total_ev, 2),
        "coverage_proba": round(total_proba, 4),
        "best_ticket": tickets[0].runner_names if tickets else None,
        "best_ev": tickets[0].expected_value if tickets else None,
        "roi_estimate": round(total_ev / total_cost, 4) if total_cost > 0 else 0.0,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    demo_runners = [
        {"partant_uid": "A", "nom": "Alpha",   "model_proba": 0.25},
        {"partant_uid": "B", "nom": "Beta",    "model_proba": 0.20},
        {"partant_uid": "C", "nom": "Gamma",   "model_proba": 0.15},
        {"partant_uid": "D", "nom": "Delta",   "model_proba": 0.12},
        {"partant_uid": "E", "nom": "Epsilon", "model_proba": 0.10},
        {"partant_uid": "F", "nom": "Zeta",    "model_proba": 0.08},
        {"partant_uid": "G", "nom": "Eta",     "model_proba": 0.06},
        {"partant_uid": "H", "nom": "Theta",   "model_proba": 0.04},
    ]

    for bt in [BetType.TIERCE, BetType.QUARTE, BetType.QUINTE]:
        print(f"\n{'='*60}")
        print(f" {bt.name} (ordre) - top 10 tickets")
        print(f"{'='*60}")
        cfg = TicketOptimizerConfig(bet_type=bt, max_tickets=10, top_n_candidates=6)
        tkts = optimize_tickets(demo_runners, cfg)
        summary = ticket_summary(tkts)
        for i, t in enumerate(tkts, 1):
            print(f"  {i:2d}. {' > '.join(t.runner_names):30s}  "
                  f"P={t.joint_proba:.6f}  EV={t.expected_value:+.2f}EUR")
        print(f"  Total cost: {summary['total_cost']:.2f}EUR, "
              f"Total EV: {summary['total_ev']:+.2f}EUR, "
              f"Coverage: {summary['coverage_proba']:.2%}")
