# -*- coding: utf-8 -*-
"""
Rolling Stats Generator
=======================
Compute rolling means, stds, min, max over the last 3/5/10/20 races
per horse. Point-in-time correct (only uses past data).
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("rolling_stats_generator")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"

WINDOWS = [3, 5, 10, 20]

# Numeric columns to compute rolling stats on
ROLL_COLS = [
    "place_arrivee",
    "nb_courses_carriere",
    "nb_victoires_carriere",
    "gains_carriere_euros",
    "cote_prob",
    "cote_direct",
    "poids_porte_kg",
    "distance",
]


class RollingStatsGenerator:
    """Compute per-horse rolling statistics over recent races."""

    def __init__(
        self,
        windows: Optional[List[int]] = None,
        roll_cols: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
    ):
        self.windows = windows or WINDOWS
        self.roll_cols = roll_cols or ROLL_COLS
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add rolling features to DataFrame (point-in-time safe)."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        # Sort chronologically per horse
        sort_cols = ["horse_id", "date_reunion_iso"]
        sort_cols = [c for c in sort_cols if c in df.columns]
        if not sort_cols:
            logger.warning("  Cannot sort; missing horse_id or date_reunion_iso")
            return df
        df = df.sort_values(sort_cols).reset_index(drop=True)

        available = [c for c in self.roll_cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
        logger.info("Computing rolling stats for %d cols x %d windows ...", len(available), len(self.windows))

        new_features = {}
        for col in available:
            grouped = df.groupby("horse_id")[col]
            for w in self.windows:
                # shift(1) ensures we do not include the current race
                rolled = grouped.shift(1).rolling(w, min_periods=1)
                new_features[f"roll_{col}_mean_{w}"] = rolled.mean().values
                new_features[f"roll_{col}_std_{w}"] = rolled.std().values
                new_features[f"roll_{col}_min_{w}"] = rolled.min().values
                new_features[f"roll_{col}_max_{w}"] = rolled.max().values

        feats_df = pd.DataFrame(new_features, index=df.index)
        df = pd.concat([df, feats_df], axis=1)

        # Win rate and place rate rolling
        if "place_arrivee" in df.columns:
            for w in self.windows:
                shifted = df.groupby("horse_id")["place_arrivee"].shift(1)
                win_flag = (shifted == 1).astype(float)
                place_flag = (shifted.between(1, 3)).astype(float)
                df[f"roll_win_rate_{w}"] = win_flag.rolling(w, min_periods=1).mean().values
                df[f"roll_place_rate_{w}"] = place_flag.rolling(w, min_periods=1).mean().values

        # Race count (how many past races the horse has)
        df["horse_race_count"] = df.groupby("horse_id").cumcount()

        n_new = len([c for c in df.columns if c.startswith("roll_") or c == "horse_race_count"])
        logger.info("  -> %d rolling features generated", n_new)
        return df

    def save(self, df: pd.DataFrame, name: str = "rolling_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Rolling Stats Generator")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    gen = RollingStatsGenerator(output_dir=args.output_dir)
    df = gen.generate(df)
    gen.save(df)
    print("[OK] Rolling stats generated: %d features." % len([c for c in df.columns if c.startswith("roll_")]))


if __name__ == "__main__":
    main()
