# -*- coding: utf-8 -*-
"""
Sectional Feature Builder
=========================
Sectional times analysis, finishing speed, acceleration patterns.
Uses temps_features and raw timing data when available.
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

logger = setup_logging("sectional_feature_builder")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
FEATURES_DIR = PROJECT_ROOT / "output" / "features"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class SectionalFeatureBuilder:
    """Build features from sectional timing data."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add sectional timing features."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("Building sectional features ...")

        df = self._load_temps_features(df)
        df = self._speed_metrics(df)
        df = self._finishing_speed(df)
        df = self._acceleration_pattern(df)
        df = self._time_comparison(df)
        df = self._distance_speed_rating(df)

        n_new = len([c for c in df.columns if c.startswith("sec_")])
        logger.info("  -> %d sectional features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _load_temps_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Merge pre-computed temps features if available."""
        temps_file = FEATURES_DIR / "temps_features.jsonl"
        if not temps_file.exists():
            logger.info("  temps_features.jsonl not found; skipping merge")
            return df

        try:
            tf = pd.read_json(temps_file, lines=True, encoding="utf-8")
            if "partant_uid" in tf.columns and "partant_uid" in df.columns:
                # Only add columns not already present
                new_cols = [c for c in tf.columns if c not in df.columns]
                if new_cols:
                    tf_sub = tf[["partant_uid"] + new_cols]
                    df = df.merge(tf_sub, on="partant_uid", how="left")
                    logger.info("  Merged %d cols from temps_features", len(new_cols))
        except Exception as e:
            logger.warning("  Could not load temps_features: %s", e)
        return df

    def _speed_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute speed = distance / time if timing data available."""
        # Look for timing columns
        time_cols = [c for c in df.columns if "temps" in c.lower() or "time" in c.lower() or "duree" in c.lower()]
        if not time_cols or "distance" not in df.columns:
            # Create synthetic speed from available data
            if "distance" in df.columns and "place_arrivee" in df.columns:
                # Approximate speed rating from distance + place
                df["sec_distance_norm"] = df["distance"] / df["distance"].median()
            return df

        # If we have actual timing
        for tc in time_cols:
            if pd.api.types.is_numeric_dtype(df[tc]):
                time_s = df[tc].clip(lower=1)
                df[f"sec_speed_{tc}"] = df["distance"] / time_s  # m/s
                df[f"sec_speed_kmh_{tc}"] = (df["distance"] / time_s) * 3.6
        return df

    def _finishing_speed(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estimate finishing speed from position changes.
        Uses place_arrivee relative to num_pmu as a proxy when sectionals unavailable.
        """
        if "place_arrivee" not in df.columns or "num_pmu" not in df.columns:
            return df

        # Position gain = draw - finish (positive = gained places)
        df["sec_position_gain"] = df["num_pmu"] - df["place_arrivee"].fillna(df["num_pmu"])
        df["sec_position_gain_pct"] = df["sec_position_gain"] / df["num_pmu"].clip(lower=1)

        # Strong finisher indicator
        df["sec_strong_finish"] = (df["sec_position_gain"] >= 3).astype(int)
        df["sec_weak_finish"] = (df["sec_position_gain"] <= -3).astype(int)
        return df

    def _acceleration_pattern(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build acceleration features from historical position gains.
        """
        if "sec_position_gain" not in df.columns or "horse_id" not in df.columns:
            return df

        df = df.sort_values(["horse_id", "date_reunion_iso"]).reset_index(drop=True)
        grp = df.groupby("horse_id")

        # Rolling average position gain (finishing ability)
        shifted = grp["sec_position_gain"].shift(1)
        for w in [3, 5, 10]:
            df[f"sec_avg_pos_gain_{w}"] = shifted.rolling(w, min_periods=1).mean().values
            df[f"sec_max_pos_gain_{w}"] = shifted.rolling(w, min_periods=1).max().values

        return df

    def _time_comparison(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compare horse time vs race average (when timing available)."""
        speed_cols = [c for c in df.columns if c.startswith("sec_speed_") and "kmh" not in c]
        if not speed_cols or "course_uid" not in df.columns:
            return df

        for sc in speed_cols:
            race_avg = df.groupby("course_uid")[sc].transform("mean")
            race_std = df.groupby("course_uid")[sc].transform("std").clip(lower=0.01)
            df[f"{sc}_vs_avg"] = df[sc] - race_avg
            df[f"{sc}_zscore"] = (df[sc] - race_avg) / race_std
        return df

    def _distance_speed_rating(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Speed rating adjusted for distance.
        Horses running longer distances will be slower in absolute terms.
        """
        if "distance" not in df.columns or "place_arrivee" not in df.columns:
            return df

        # Distance factor (expected slowdown per meter)
        med_dist = df["distance"].median()
        df["sec_dist_factor"] = df["distance"] / med_dist

        # Performance rating: lower finish at longer distance = better stamina
        df["sec_stamina_rating"] = df["place_arrivee"].fillna(10) / df["sec_dist_factor"].clip(lower=0.5)

        # Rolling stamina rating per horse
        if "horse_id" in df.columns:
            df = df.sort_values(["horse_id", "date_reunion_iso"]).reset_index(drop=True)
            grp = df.groupby("horse_id")
            shifted = grp["sec_stamina_rating"].shift(1)
            df["sec_avg_stamina_5"] = shifted.rolling(5, min_periods=1).mean().values

        return df

    def save(self, df: pd.DataFrame, name: str = "sectional_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Sectional Feature Builder")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = SectionalFeatureBuilder(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Sectional features generated.")


if __name__ == "__main__":
    main()
