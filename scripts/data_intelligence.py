#!/usr/bin/env python3
"""
data_intelligence.py — Pilier 7 : Intelligence

Detect patterns in the data:
  - Seasonal trends
  - Hippodrome biases
  - Jockey-trainer winning patterns
  - Correlation matrix between key numeric features (sample 10K records)
  - Top 20 most predictive features (highest correlation with is_winner)

Outputs quality/intelligence_report.md

Streams line-by-line with reservoir sampling to keep RAM under 2 GB.

Usage:
    python scripts/data_intelligence.py
"""

from __future__ import annotations

import json
import math
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PARTANTS_MASTER, QUALITY_DIR  # noqa: E402

OUTPUT_REPORT = QUALITY_DIR / "intelligence_report.md"
SAMPLE_SIZE = 10_000
RESERVOIR_SEED = 42


# ---------------------------------------------------------------------------
# Numeric feature keys we want to correlate with is_winner
# ---------------------------------------------------------------------------
NUMERIC_FEATURES = [
    "cote_finale",
    "cote_reference",
    "proba_implicite",
    "age",
    "nb_courses_carriere",
    "nb_victoires_carriere",
    "nb_places_carriere",
    "gains_carriere_euros",
    "gains_annee_euros",
    "poids_porte_kg",
    "handicap_valeur",
    "nombre_partants",
    "distance",
    "ecart_precedent",
    "seq_nb_courses_historique",
    "seq_serie_victoires",
    "seq_serie_places",
    "seq_serie_non_places",
    "seq_nb_victoires_recent_5",
    "seq_nb_places_recent_5",
    "met_impact_meteo_score",
    "ped_inbreeding_count",
    "ped_inbreeding_score",
    "ped_stamina_index",
    "ped_speed_index",
    "gnn_jockey_nb_chevaux",
    "gnn_entraineur_nb_chevaux",
    "gnn_cheval_degree",
    "spd_bias_corde_gagnant_moy",
    "spd_speed_figure",
    "spd_class_rating",
    "spd_field_strength_avg",
    "spd_field_strength_max",
    "spd_class_vs_field",
    "temps_ms",
    "reduction_km_ms",
    "place_corde",
    "poids_base_kg",
    "surcharge_decharge_kg",
]


# ---------------------------------------------------------------------------
# Streaming accumulators for pattern detection
# ---------------------------------------------------------------------------

class IntelligenceAccumulator:
    """Collects pattern data in a single streaming pass."""

    def __init__(self) -> None:
        self.total = 0

        # Seasonal (month)
        self.month_wins: Counter = Counter()
        self.month_total: Counter = Counter()

        # Hippodrome bias (win rate deviation)
        self.hippo_wins: Counter = Counter()
        self.hippo_total: Counter = Counter()

        # Jockey-trainer combos
        self.jt_wins: Counter = Counter()
        self.jt_total: Counter = Counter()

        # Day of week
        self.dow_wins: Counter = Counter()
        self.dow_total: Counter = Counter()

        # Distance bins
        self.dist_wins: Counter = Counter()
        self.dist_total: Counter = Counter()

    def feed(self, rec: dict) -> None:
        self.total += 1
        won = _is_winner(rec)

        # Month
        date_str = rec.get("date_reunion_iso", "")
        if len(date_str) >= 7:
            month = date_str[5:7]
            self.month_total[month] += 1
            if won:
                self.month_wins[month] += 1

            # Day of week (approximate from date)
            try:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                dow = dt.strftime("%A")
                self.dow_total[dow] += 1
                if won:
                    self.dow_wins[dow] += 1
            except (ValueError, TypeError):
                pass

        # Hippodrome
        hippo = rec.get("hippodrome_normalise", "")
        if hippo:
            self.hippo_total[hippo] += 1
            if won:
                self.hippo_wins[hippo] += 1

        # Jockey-trainer
        jockey = rec.get("jockey_driver", "")
        trainer = rec.get("entraineur", "")
        if jockey and trainer:
            key = f"{jockey} / {trainer}"
            self.jt_total[key] += 1
            if won:
                self.jt_wins[key] += 1

        # Distance bin
        dist = rec.get("distance")
        if isinstance(dist, (int, float)) and dist > 0:
            dbin = _distance_bin(int(dist))
            self.dist_total[dbin] += 1
            if won:
                self.dist_wins[dbin] += 1


def _is_winner(rec: dict) -> bool:
    w = rec.get("is_gagnant")
    if w is True or w == 1 or w == "1":
        return True
    pos = rec.get("position_arrivee")
    return pos == 1 or pos == "1"


def _distance_bin(d: int) -> str:
    if d < 1200:
        return "<1200m"
    elif d < 1600:
        return "1200-1599m"
    elif d < 2000:
        return "1600-1999m"
    elif d < 2400:
        return "2000-2399m"
    elif d < 3000:
        return "2400-2999m"
    else:
        return "3000m+"


def _safe_float(v) -> float | None:
    """Convert to float or return None."""
    if v is None or v == "" or v == "N/A":
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Correlation computation (pure Python, no numpy)
# ---------------------------------------------------------------------------

def compute_correlations(
    samples: list[dict],
    features: list[str],
    target: str = "is_winner_num",
) -> dict[str, float]:
    """
    Pearson correlation of each feature with the binary target.
    We add is_winner_num = 1/0 to each sample dict.
    Returns {feature: correlation}.
    """
    # Prepare target
    for s in samples:
        s[target] = 1.0 if _is_winner(s) else 0.0

    results: dict[str, float] = {}
    n = len(samples)
    if n < 10:
        return results

    # Pre-compute target stats
    ty = [s[target] for s in samples]
    mean_y = sum(ty) / n
    var_y = sum((y - mean_y) ** 2 for y in ty)
    if var_y == 0:
        return results

    for feat in features:
        # Extract valid (x, y) pairs
        xs: list[float] = []
        ys: list[float] = []
        for s in samples:
            xv = _safe_float(s.get(feat))
            if xv is not None:
                xs.append(xv)
                ys.append(s[target])

        if len(xs) < 30:
            continue

        nf = len(xs)
        mean_x = sum(xs) / nf
        mean_yf = sum(ys) / nf

        cov = sum((xs[i] - mean_x) * (ys[i] - mean_yf) for i in range(nf))
        var_x = sum((x - mean_x) ** 2 for x in xs)
        var_yf = sum((y - mean_yf) ** 2 for y in ys)

        denom = math.sqrt(var_x * var_yf) if var_x > 0 and var_yf > 0 else 0
        if denom > 0:
            results[feat] = cov / denom
        else:
            results[feat] = 0.0

    return results


def reservoir_sample(filepath: Path, k: int, seed: int) -> tuple[list[dict], int]:
    """Reservoir sampling of k records. Returns (samples, total_count)."""
    reservoir: list[dict] = []
    rng = random.Random(seed)
    n = 0
    with open(filepath, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            n += 1
            if n <= k:
                reservoir.append(rec)
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    reservoir[j] = rec
            if n % 500_000 == 0:
                print(f"  ... {n:,} records scanned")
    print(f"  Total: {n:,} — sampled {len(reservoir):,}")
    return reservoir, n


def stream_patterns(filepath: Path) -> IntelligenceAccumulator:
    """Stream the full file for pattern detection (counters only, low RAM)."""
    acc = IntelligenceAccumulator()
    with open(filepath, "r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            acc.feed(rec)
            if (i + 1) % 500_000 == 0:
                print(f"  ... {i + 1:,} lines processed")
    return acc


def write_report(
    acc: IntelligenceAccumulator,
    correlations: dict[str, float],
    sample_size: int,
    elapsed: float,
) -> None:
    """Write the intelligence report."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = []
    lines.append("# Data Intelligence Report (Pilier 7 — Intelligence)")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"Total partants: {acc.total:,}")
    lines.append(f"Correlation sample: {sample_size:,} records")
    lines.append(f"Elapsed: {elapsed:.1f}s")
    lines.append("")

    # -----------------------------------------------------------------------
    # 1. Seasonal trends
    # -----------------------------------------------------------------------
    lines.append("## 1. Seasonal Trends (Win Rate by Month)")
    lines.append("")
    lines.append("| Month | Partants | Winners | Win Rate |")
    lines.append("|-------|----------|---------|----------|")
    global_wr = sum(acc.month_wins.values()) / acc.total * 100 if acc.total else 0
    for m in [f"{i:02d}" for i in range(1, 13)]:
        tot = acc.month_total.get(m, 0)
        wins = acc.month_wins.get(m, 0)
        wr = wins / tot * 100 if tot else 0
        marker = " *" if abs(wr - global_wr) > 1.0 else ""
        lines.append(f"| {m} | {tot:,} | {wins:,} | {wr:.2f}%{marker} |")
    lines.append("")
    lines.append(f"Global win rate: {global_wr:.2f}% (* = >1pp deviation)")
    lines.append("")

    # -----------------------------------------------------------------------
    # 2. Day of week
    # -----------------------------------------------------------------------
    lines.append("## 2. Day of Week Win Rate")
    lines.append("")
    lines.append("| Day | Partants | Win Rate |")
    lines.append("|-----|----------|----------|")
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
        tot = acc.dow_total.get(day, 0)
        wins = acc.dow_wins.get(day, 0)
        wr = wins / tot * 100 if tot else 0
        lines.append(f"| {day} | {tot:,} | {wr:.2f}% |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 3. Hippodrome biases (most extreme deviations from global win rate)
    # -----------------------------------------------------------------------
    lines.append("## 3. Hippodrome Biases (min 500 partants)")
    lines.append("")
    lines.append("Top 20 highest win rate / bottom 20 lowest win rate deviations.")
    lines.append("")

    hippo_rates: list[tuple[str, float, int]] = []
    for h in acc.hippo_total:
        tot = acc.hippo_total[h]
        if tot < 500:
            continue
        wins = acc.hippo_wins.get(h, 0)
        wr = wins / tot * 100
        hippo_rates.append((h, wr, tot))
    hippo_rates.sort(key=lambda x: -x[1])

    lines.append("### Highest Win Rate Hippodromes")
    lines.append("")
    lines.append("| Hippodrome | Partants | Win Rate | Delta vs Global |")
    lines.append("|------------|----------|----------|----------------|")
    for h, wr, tot in hippo_rates[:20]:
        delta = wr - global_wr
        lines.append(f"| {h} | {tot:,} | {wr:.2f}% | {delta:+.2f}pp |")
    lines.append("")

    lines.append("### Lowest Win Rate Hippodromes")
    lines.append("")
    lines.append("| Hippodrome | Partants | Win Rate | Delta vs Global |")
    lines.append("|------------|----------|----------|----------------|")
    for h, wr, tot in hippo_rates[-20:]:
        delta = wr - global_wr
        lines.append(f"| {h} | {tot:,} | {wr:.2f}% | {delta:+.2f}pp |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 4. Jockey-trainer patterns (top combos by volume with high win rate)
    # -----------------------------------------------------------------------
    lines.append("## 4. Jockey-Trainer Winning Combos (min 100 runs)")
    lines.append("")
    lines.append("| Jockey / Trainer | Runs | Wins | Win Rate |")
    lines.append("|------------------|------|------|----------|")

    jt_rates: list[tuple[str, float, int, int]] = []
    for key in acc.jt_total:
        tot = acc.jt_total[key]
        if tot < 100:
            continue
        wins = acc.jt_wins.get(key, 0)
        wr = wins / tot * 100
        jt_rates.append((key, wr, wins, tot))
    jt_rates.sort(key=lambda x: -x[1])

    for key, wr, wins, tot in jt_rates[:30]:
        lines.append(f"| {key} | {tot:,} | {wins:,} | {wr:.1f}% |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 5. Distance patterns
    # -----------------------------------------------------------------------
    lines.append("## 5. Distance Patterns")
    lines.append("")
    lines.append("| Distance | Partants | Win Rate |")
    lines.append("|----------|----------|----------|")
    for dbin in ["<1200m", "1200-1599m", "1600-1999m", "2000-2399m", "2400-2999m", "3000m+"]:
        tot = acc.dist_total.get(dbin, 0)
        wins = acc.dist_wins.get(dbin, 0)
        wr = wins / tot * 100 if tot else 0
        lines.append(f"| {dbin} | {tot:,} | {wr:.2f}% |")
    lines.append("")

    # -----------------------------------------------------------------------
    # 6. Correlation with is_winner — top 20 most predictive features
    # -----------------------------------------------------------------------
    lines.append("## 6. Top 20 Most Predictive Features (Pearson r vs is_winner)")
    lines.append("")
    lines.append("| Rank | Feature | Correlation | Abs Correlation |")
    lines.append("|------|---------|-------------|-----------------|")

    sorted_corr = sorted(correlations.items(), key=lambda x: -abs(x[1]))
    for rank, (feat, corr) in enumerate(sorted_corr[:20], 1):
        lines.append(f"| {rank} | `{feat}` | {corr:+.4f} | {abs(corr):.4f} |")
    lines.append("")

    # Full correlation table
    lines.append("### Full Correlation Table")
    lines.append("")
    lines.append("| Feature | Correlation | N valid |")
    lines.append("|---------|-------------|---------|")
    for feat, corr in sorted_corr:
        lines.append(f"| `{feat}` | {corr:+.4f} | - |")
    lines.append("")

    OUTPUT_REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to {OUTPUT_REPORT}")


def main() -> None:
    print("=" * 60)
    print("Pilier 7 — Data Intelligence")
    print("=" * 60)
    t0 = time.time()

    if not PARTANTS_MASTER.exists():
        print(f"ERROR: {PARTANTS_MASTER} not found.")
        sys.exit(1)

    print(f"\n[1/3] Reservoir sampling {SAMPLE_SIZE:,} records for correlations ...")
    samples, total_count = reservoir_sample(PARTANTS_MASTER, SAMPLE_SIZE, RESERVOIR_SEED)

    print(f"\n[2/3] Computing correlations on {len(samples):,} samples ...")
    correlations = compute_correlations(samples, NUMERIC_FEATURES)
    del samples  # free memory before second pass

    print(f"\n[3/3] Streaming full file for pattern detection ...")
    acc = stream_patterns(PARTANTS_MASTER)

    elapsed = time.time() - t0
    write_report(acc, correlations, SAMPLE_SIZE, elapsed)
    print(f"\nDone in {elapsed:.1f}s — {acc.total:,} records analyzed, "
          f"{len(correlations)} features correlated.")


if __name__ == "__main__":
    main()
