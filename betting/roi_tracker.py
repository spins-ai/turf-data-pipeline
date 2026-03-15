"""
betting.roi_tracker
~~~~~~~~~~~~~~~~~~~
Track betting performance: ROI, bankroll evolution, drawdown,
and statistical significance of results.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class BetRecord:
    """A single bet record."""

    date: str                 # ISO date (YYYY-MM-DD)
    race_id: str              # course_uid
    partant_uid: str
    runner_name: str
    bet_type: str             # "simple_gagnant", "simple_place", "tierce", etc.
    strategy: str             # e.g. "value_hunter", "kelly", "manual"
    stake: float
    odds: float               # decimal odds at time of bet
    result: str               # "win", "loss", "place", "void"
    payout: float             # amount returned (0 if loss)
    timestamp: str = ""       # ISO datetime of bet placement


@dataclass
class PerformanceStats:
    """Aggregated performance statistics."""

    n_bets: int
    n_wins: int
    win_rate: float
    total_staked: float
    total_payout: float
    net_pnl: float
    roi: float                # (total_payout - total_staked) / total_staked
    avg_odds_winners: float
    avg_stake: float
    max_drawdown: float
    current_drawdown: float
    sharpe_ratio: Optional[float]
    z_score: Optional[float]  # statistical significance
    p_value: Optional[float]


# ---------------------------------------------------------------------------
# ROI Tracker
# ---------------------------------------------------------------------------

class ROITracker:
    """Track and analyse betting performance over time.

    Usage
    -----
    >>> tracker = ROITracker(initial_bankroll=2000.0)
    >>> tracker.add_bet(BetRecord(...))
    >>> stats = tracker.compute_stats()
    >>> tracker.save("betting_history.json")
    """

    def __init__(self, initial_bankroll: float = 1000.0):
        self.initial_bankroll = initial_bankroll
        self.bets: list[BetRecord] = []

    # ------------------------------------------------------------------
    # Bet management
    # ------------------------------------------------------------------

    def add_bet(self, bet: BetRecord) -> None:
        """Add a single bet record."""
        if not bet.timestamp:
            bet.timestamp = datetime.now().isoformat()
        self.bets.append(bet)

    def add_bets(self, bets: list[BetRecord]) -> None:
        """Add multiple bet records."""
        for b in bets:
            self.add_bet(b)

    # ------------------------------------------------------------------
    # Core computations
    # ------------------------------------------------------------------

    @property
    def current_bankroll(self) -> float:
        """Current bankroll after all bets."""
        pnl = sum(b.payout - b.stake for b in self.bets)
        return self.initial_bankroll + pnl

    def _pnl_series(self) -> list[float]:
        """Cumulative P&L series (one entry per bet)."""
        cumul = 0.0
        series = []
        for b in self.bets:
            cumul += b.payout - b.stake
            series.append(cumul)
        return series

    def _bankroll_series(self) -> list[float]:
        """Bankroll evolution (one entry per bet)."""
        br = self.initial_bankroll
        series = [br]
        for b in self.bets:
            br += b.payout - b.stake
            series.append(br)
        return series

    def _drawdown_analysis(self) -> tuple[float, float]:
        """Compute max drawdown and current drawdown.

        Returns
        -------
        tuple[float, float]
            (max_drawdown, current_drawdown) as fractions of peak.
        """
        bankroll_series = self._bankroll_series()
        if len(bankroll_series) < 2:
            return 0.0, 0.0

        peak = bankroll_series[0]
        max_dd = 0.0

        for br in bankroll_series[1:]:
            if br > peak:
                peak = br
            dd = (peak - br) / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)

        current_peak = max(bankroll_series)
        current_br = bankroll_series[-1]
        current_dd = (current_peak - current_br) / current_peak if current_peak > 0 else 0.0

        return max_dd, current_dd

    def _statistical_significance(self) -> tuple[Optional[float], Optional[float]]:
        """Test if ROI is significantly different from 0.

        Uses a z-test on per-bet returns.

        Returns
        -------
        tuple[Optional[float], Optional[float]]
            (z_score, p_value). None if insufficient data.
        """
        if len(self.bets) < 30:
            return None, None

        returns = []
        for b in self.bets:
            if b.stake > 0:
                ret = (b.payout - b.stake) / b.stake
                returns.append(ret)

        if len(returns) < 30:
            return None, None

        arr = np.array(returns)
        mean_ret = arr.mean()
        std_ret = arr.std(ddof=1)

        if std_ret == 0:
            return None, None

        z = mean_ret / (std_ret / math.sqrt(len(arr)))

        # Two-tailed p-value using normal approximation
        from scipy import stats as sp_stats
        p_value = 2.0 * (1.0 - sp_stats.norm.cdf(abs(z)))

        return round(z, 4), round(p_value, 6)

    def _sharpe_ratio(self) -> Optional[float]:
        """Compute Sharpe ratio of bet returns (risk-free rate = 0)."""
        if len(self.bets) < 2:
            return None

        returns = []
        for b in self.bets:
            if b.stake > 0:
                returns.append((b.payout - b.stake) / b.stake)

        if len(returns) < 2:
            return None

        arr = np.array(returns)
        std = arr.std(ddof=1)
        if std == 0:
            return None

        return round(float(arr.mean() / std), 4)

    # ------------------------------------------------------------------
    # Aggregated stats
    # ------------------------------------------------------------------

    def compute_stats(
        self,
        strategy: Optional[str] = None,
    ) -> PerformanceStats:
        """Compute overall performance statistics.

        Parameters
        ----------
        strategy : str, optional
            Filter bets by strategy name.

        Returns
        -------
        PerformanceStats
        """
        bets = self.bets
        if strategy:
            bets = [b for b in bets if b.strategy == strategy]

        if not bets:
            return PerformanceStats(
                n_bets=0, n_wins=0, win_rate=0.0, total_staked=0.0,
                total_payout=0.0, net_pnl=0.0, roi=0.0,
                avg_odds_winners=0.0, avg_stake=0.0,
                max_drawdown=0.0, current_drawdown=0.0,
                sharpe_ratio=None, z_score=None, p_value=None,
            )

        winners = [b for b in bets if b.result in ("win", "place")]
        n_wins = len(winners)
        total_staked = sum(b.stake for b in bets)
        total_payout = sum(b.payout for b in bets)
        net_pnl = total_payout - total_staked

        roi = net_pnl / total_staked if total_staked > 0 else 0.0
        win_rate = n_wins / len(bets) if bets else 0.0

        avg_odds_win = 0.0
        if winners:
            avg_odds_win = sum(b.odds for b in winners) / len(winners)

        avg_stake = total_staked / len(bets) if bets else 0.0

        max_dd, current_dd = self._drawdown_analysis()
        sharpe = self._sharpe_ratio()

        try:
            z_score, p_value = self._statistical_significance()
        except ImportError:
            z_score, p_value = None, None

        return PerformanceStats(
            n_bets=len(bets),
            n_wins=n_wins,
            win_rate=round(win_rate, 4),
            total_staked=round(total_staked, 2),
            total_payout=round(total_payout, 2),
            net_pnl=round(net_pnl, 2),
            roi=round(roi, 4),
            avg_odds_winners=round(avg_odds_win, 2),
            avg_stake=round(avg_stake, 2),
            max_drawdown=round(max_dd, 4),
            current_drawdown=round(current_dd, 4),
            sharpe_ratio=sharpe,
            z_score=z_score,
            p_value=p_value,
        )

    def roi_by_strategy(self) -> dict[str, float]:
        """Compute ROI grouped by strategy.

        Returns
        -------
        dict[str, float]
            Strategy name -> ROI.
        """
        by_strat: dict[str, list[BetRecord]] = defaultdict(list)
        for b in self.bets:
            by_strat[b.strategy].append(b)

        result = {}
        for strat, bets in by_strat.items():
            staked = sum(b.stake for b in bets)
            payout = sum(b.payout for b in bets)
            result[strat] = round((payout - staked) / staked, 4) if staked > 0 else 0.0

        return result

    def roi_by_period(
        self,
        period: str = "monthly",
    ) -> dict[str, float]:
        """Compute ROI grouped by time period.

        Parameters
        ----------
        period : str
            One of "daily", "weekly", "monthly".

        Returns
        -------
        dict[str, float]
            Period key -> ROI.
        """
        by_period: dict[str, list[BetRecord]] = defaultdict(list)

        for b in self.bets:
            dt = b.date[:10]  # YYYY-MM-DD
            if period == "daily":
                key = dt
            elif period == "weekly":
                d = date.fromisoformat(dt)
                iso = d.isocalendar()
                key = f"{iso.year}-W{iso.week:02d}"
            elif period == "monthly":
                key = dt[:7]  # YYYY-MM
            else:
                key = dt
            by_period[key].append(b)

        result = {}
        for key in sorted(by_period):
            bets = by_period[key]
            staked = sum(b.stake for b in bets)
            payout = sum(b.payout for b in bets)
            result[key] = round((payout - staked) / staked, 4) if staked > 0 else 0.0

        return result

    def pnl_chart_data(self) -> dict[str, list]:
        """Generate data for a cumulative P&L chart.

        Returns
        -------
        dict
            Keys: "dates", "cumulative_pnl", "bankroll".
        """
        dates: list[str] = []
        cum_pnl: list[float] = []
        bankroll: list[float] = []

        running_pnl = 0.0
        running_br = self.initial_bankroll

        for b in self.bets:
            running_pnl += b.payout - b.stake
            running_br += b.payout - b.stake
            dates.append(b.date)
            cum_pnl.append(round(running_pnl, 2))
            bankroll.append(round(running_br, 2))

        return {"dates": dates, "cumulative_pnl": cum_pnl, "bankroll": bankroll}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str | Path) -> None:
        """Save betting history and config to JSON.

        Parameters
        ----------
        path : str or Path
            Output file path.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "initial_bankroll": self.initial_bankroll,
            "bets": [asdict(b) for b in self.bets],
        }

        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        tmp.replace(path)

    @classmethod
    def load(cls, path: str | Path) -> ROITracker:
        """Load betting history from JSON.

        Parameters
        ----------
        path : str or Path

        Returns
        -------
        ROITracker
        """
        path = Path(path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        tracker = cls(initial_bankroll=data.get("initial_bankroll", 1000.0))
        for b_dict in data.get("bets", []):
            tracker.bets.append(BetRecord(**b_dict))

        return tracker


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tracker = ROITracker(initial_bankroll=2000.0)

    # Simulate a few bets
    demo_bets = [
        BetRecord("2025-01-15", "R1C2", "A", "Alpha", "simple_gagnant", "value_hunter",
                  20.0, 3.5, "win", 70.0),
        BetRecord("2025-01-15", "R1C3", "B", "Beta", "simple_gagnant", "value_hunter",
                  15.0, 5.0, "loss", 0.0),
        BetRecord("2025-01-16", "R2C1", "C", "Gamma", "simple_gagnant", "kelly",
                  25.0, 4.0, "win", 100.0),
        BetRecord("2025-01-16", "R2C2", "D", "Delta", "simple_gagnant", "kelly",
                  10.0, 8.0, "loss", 0.0),
        BetRecord("2025-01-17", "R3C1", "E", "Epsilon", "simple_place", "value_hunter",
                  30.0, 2.5, "place", 45.0),
        BetRecord("2025-01-17", "R3C2", "F", "Zeta", "tierce", "manual",
                  5.0, 50.0, "loss", 0.0),
    ]
    tracker.add_bets(demo_bets)

    stats = tracker.compute_stats()
    print(f"Performance ({stats.n_bets} bets):")
    print(f"  Win rate:    {stats.win_rate:.1%}")
    print(f"  ROI:         {stats.roi:+.1%}")
    print(f"  Net P&L:     {stats.net_pnl:+.2f}EUR")
    print(f"  Bankroll:    {tracker.current_bankroll:.2f}EUR")
    print(f"  Max DD:      {stats.max_drawdown:.1%}")
    print(f"  Avg odds W:  {stats.avg_odds_winners:.2f}")

    print("\nROI by strategy:")
    for strat, roi in tracker.roi_by_strategy().items():
        print(f"  {strat}: {roi:+.1%}")

    print("\nROI by day:")
    for period, roi in tracker.roi_by_period("daily").items():
        print(f"  {period}: {roi:+.1%}")

    # Save / reload
    tracker.save("/tmp/betting_demo.json")
    reloaded = ROITracker.load("/tmp/betting_demo.json")
    print(f"\nReloaded {len(reloaded.bets)} bets, "
          f"bankroll={reloaded.current_bankroll:.2f}EUR")
