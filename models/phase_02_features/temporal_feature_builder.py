# -*- coding: utf-8 -*-
"""
Temporal Feature Builder
========================
Days since last race, seasonal patterns, day of week, time-of-day effects.
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("temporal_feature_builder")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class TemporalFeatureBuilder:
    """Build time-based features for horse racing data."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add temporal features to DataFrame."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("Building temporal features ...")

        df = self._days_since_last_race(df)
        df = self._calendar_features(df)
        df = self._seasonal_features(df)
        df = self._layoff_categories(df)
        df = self._race_frequency(df)

        n_new = len([c for c in df.columns if c.startswith("tmp_")])
        logger.info("  -> %d temporal features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _days_since_last_race(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute days since previous race for each horse."""
        if "horse_id" not in df.columns or "date_reunion_iso" not in df.columns:
            return df
        df = df.sort_values(["horse_id", "date_reunion_iso"]).reset_index(drop=True)
        df["tmp_prev_race_date"] = df.groupby("horse_id")["date_reunion_iso"].shift(1)
        df["tmp_days_since_last"] = (df["date_reunion_iso"] - df["tmp_prev_race_date"]).dt.days
        df["tmp_days_since_last_log"] = np.log1p(df["tmp_days_since_last"].fillna(365))
        df.drop(columns=["tmp_prev_race_date"], inplace=True, errors="ignore")
        return df

    def _calendar_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Day of week, month, quarter from race date."""
        if "date_reunion_iso" not in df.columns:
            return df
        dt = df["date_reunion_iso"]
        df["tmp_day_of_week"] = dt.dt.dayofweek  # 0=Mon
        df["tmp_is_weekend"] = (dt.dt.dayofweek >= 5).astype(int)
        df["tmp_month"] = dt.dt.month
        df["tmp_quarter"] = dt.dt.quarter
        df["tmp_day_of_year"] = dt.dt.dayofyear
        df["tmp_year"] = dt.dt.year

        # Cyclical encoding
        df["tmp_month_sin"] = np.sin(2 * np.pi * df["tmp_month"] / 12)
        df["tmp_month_cos"] = np.cos(2 * np.pi * df["tmp_month"] / 12)
        df["tmp_dow_sin"] = np.sin(2 * np.pi * df["tmp_day_of_week"] / 7)
        df["tmp_dow_cos"] = np.cos(2 * np.pi * df["tmp_day_of_week"] / 7)
        return df

    def _seasonal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Season classification and seasonal performance indicators."""
        if "tmp_month" not in df.columns:
            return df
        season_map = {
            12: "winter", 1: "winter", 2: "winter",
            3: "spring", 4: "spring", 5: "spring",
            6: "summer", 7: "summer", 8: "summer",
            9: "autumn", 10: "autumn", 11: "autumn",
        }
        df["tmp_season"] = df["tmp_month"].map(season_map)
        # One-hot
        for s in ["winter", "spring", "summer", "autumn"]:
            df[f"tmp_season_{s}"] = (df["tmp_season"] == s).astype(int)
        df.drop(columns=["tmp_season"], inplace=True)
        return df

    def _layoff_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize rest period length."""
        if "tmp_days_since_last" not in df.columns:
            return df
        days = df["tmp_days_since_last"]
        df["tmp_layoff_fresh"] = ((days >= 14) & (days <= 35)).astype(int)
        df["tmp_layoff_short"] = (days < 14).astype(int)
        df["tmp_layoff_long"] = ((days > 35) & (days <= 90)).astype(int)
        df["tmp_layoff_very_long"] = (days > 90).astype(int)
        df["tmp_is_debut"] = df["tmp_days_since_last"].isna().astype(int)
        return df

    def _race_frequency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Number of races in the last 30/60/90 days per horse."""
        if "horse_id" not in df.columns or "date_reunion_iso" not in df.columns:
            return df
        df = df.sort_values(["horse_id", "date_reunion_iso"]).reset_index(drop=True)

        for window_days in [30, 60, 90]:
            col_name = f"tmp_races_last_{window_days}d"
            counts = []
            grouped = df.groupby("horse_id")
            for _, grp in grouped:
                dates = grp["date_reunion_iso"].values
                cnt = []
                for i, d in enumerate(dates):
                    if pd.isna(d):
                        cnt.append(0)
                        continue
                    cutoff = d - np.timedelta64(window_days, "D")
                    # Count previous races (not including current)
                    n = int(np.sum((dates[:i] >= cutoff) & (dates[:i] <= d)))
                    cnt.append(n)
                counts.extend(cnt)
            df[col_name] = counts
        return df

    def save(self, df: pd.DataFrame, name: str = "temporal_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Temporal Feature Builder")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = TemporalFeatureBuilder(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Temporal features generated.")


if __name__ == "__main__":
    main()
