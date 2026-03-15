"""
betting -- modules de paris et optimisation pour la prediction hippique.

Outils de detection de value bets, dimensionnement Kelly, optimisation
de tickets multi-paris (tierce, quarte, quinte) et suivi de ROI.
"""

from .value_hunter import find_value_bets, ValueBet
from .kelly_optimizer import kelly_stake, portfolio_kelly
from .ticket_optimizer import optimize_tickets, Ticket
from .roi_tracker import ROITracker, BetRecord

__all__ = [
    "find_value_bets",
    "ValueBet",
    "kelly_stake",
    "portfolio_kelly",
    "optimize_tickets",
    "Ticket",
    "ROITracker",
    "BetRecord",
]
