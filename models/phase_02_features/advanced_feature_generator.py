# -*- coding: utf-8 -*-
"""
Advanced Feature Generator
===========================
Wrapper around all feature builders. Orchestrates generation of 100+ features
from raw data by calling each sub-builder in sequence.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

from .rolling_stats_generator import RollingStatsGenerator
from .temporal_feature_builder import TemporalFeatureBuilder
from .odds_feature_builder import OddsFeatureBuilder
from .jockey_trainer_synergy_builder import JockeyTrainerSynergyBuilder
from .pedigree_feature_builder import PedigreeFeatureBuilder
from .track_bias_detector import TrackBiasDetector
from .pace_profile_builder import PaceProfileBuilder
from .sectional_feature_builder import SectionalFeatureBuilder
from .field_strength_builder import FieldStrengthBuilder

logger = setup_logging("advanced_feature_generator")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
FEATURES_DIR = PROJECT_ROOT / "output" / "features"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class AdvancedFeatureGenerator:
    """
    Orchestrate all feature builders to produce a comprehensive feature matrix.

    Usage
    -----
    >>> gen = AdvancedFeatureGenerator()
    >>> df = gen.load_source()
    >>> df = gen.generate_all(df)
    >>> gen.save(df)
    """

    BUILDERS = [
        ("rolling_stats", RollingStatsGenerator),
        ("temporal", TemporalFeatureBuilder),
        ("odds", OddsFeatureBuilder),
        ("jockey_trainer", JockeyTrainerSynergyBuilder),
        ("pedigree", PedigreeFeatureBuilder),
        ("track_bias", TrackBiasDetector),
        ("pace_profile", PaceProfileBuilder),
        ("sectional", SectionalFeatureBuilder),
        ("field_strength", FieldStrengthBuilder),
    ]

    def __init__(
        self,
        output_dir: Optional[str] = None,
        skip_builders: Optional[List[str]] = None,
    ):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.skip_builders = set(skip_builders or [])
        self.timing: dict = {}

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------
    def load_source(
        self,
        partants_path: Optional[str] = None,
        features_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """Load and optionally merge partants + features."""
        pp = Path(partants_path) if partants_path else DATA_MASTER / "partants_master.jsonl"
        fp = Path(features_path) if features_path else FEATURES_DIR / "features_matrix.jsonl"

        logger.info("Loading source data ...")
        if pp.suffix == ".parquet":
            df = pd.read_parquet(pp)
        else:
            df = pd.read_json(pp, lines=True, encoding="utf-8")

        if fp.exists():
            logger.info("  Merging features from %s ...", fp)
            if fp.suffix == ".parquet":
                feats = pd.read_parquet(fp)
            else:
                feats = pd.read_json(fp, lines=True, encoding="utf-8")
            new_cols = [c for c in feats.columns if c not in df.columns]
            if new_cols and "partant_uid" in feats.columns:
                df = df.merge(feats[["partant_uid"] + new_cols], on="partant_uid", how="left")
                logger.info("  -> merged %d extra columns from features_matrix", len(new_cols))

        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("  Source: %d rows, %d columns", len(df), len(df.columns))
        return df

    # ------------------------------------------------------------------
    # Run all builders
    # ------------------------------------------------------------------
    def generate_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run every feature builder in sequence."""
        initial_cols = set(df.columns)
        total_t0 = time.time()

        for name, BuilderCls in self.BUILDERS:
            if name in self.skip_builders:
                logger.info("[SKIP] %s (user-excluded)", name)
                continue

            t0 = time.time()
            logger.info("=" * 60)
            logger.info("Running builder: %s", name)
            try:
                builder = BuilderCls(output_dir=str(self.output_dir))
                df = builder.generate(df)
            except Exception as e:
                logger.error("  FAILED: %s -> %s", name, e)
                continue
            elapsed = time.time() - t0
            self.timing[name] = round(elapsed, 2)
            new_cols = set(df.columns) - initial_cols
            logger.info("  %s done in %.1fs (+%d features)", name, elapsed, len(new_cols))
            initial_cols = set(df.columns)

        total = time.time() - total_t0
        logger.info("=" * 60)
        logger.info("ALL BUILDERS COMPLETE: %d total columns in %.1fs", len(df.columns), total)
        return df

    # ------------------------------------------------------------------
    # Additional cross-feature interactions
    # ------------------------------------------------------------------
    def add_interactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add selected feature interactions and ratios."""
        logger.info("Adding feature interactions ...")
        n_before = len(df.columns)

        # Win rate x odds value
        if "roll_win_rate_5" in df.columns and "odds_fair_prob" in df.columns:
            df["ix_winrate_x_fairprob"] = df["roll_win_rate_5"] * df["odds_fair_prob"]

        # Jockey form x trainer form
        if "jk_form_5" in df.columns and "tr_form_5" in df.columns:
            df["ix_jk_tr_form"] = df["jk_form_5"] * df["tr_form_5"]

        # Days rest x consistency
        if "tmp_days_since_last" in df.columns and "pace_consistency" in df.columns:
            df["ix_rest_x_consistency"] = df["tmp_days_since_last"].fillna(365) * df["pace_consistency"].fillna(5)

        # Sire aptitude x current distance
        if "ped_sire_dist_win_rate" in df.columns:
            df["ix_sire_dist_match"] = df["ped_sire_dist_win_rate"].fillna(0)

        # Field strength x horse strength
        if "fld_rel_strength" in df.columns and "fld_hhi" in df.columns:
            df["ix_strength_x_hhi"] = df["fld_rel_strength"] * df["fld_hhi"]

        # Position gain trend x pace scenario
        if "sec_avg_pos_gain_5" in df.columns and "pace_front_density" in df.columns:
            df["ix_pos_gain_x_pace"] = df["sec_avg_pos_gain_5"].fillna(0) * df["pace_front_density"].fillna(0.5)

        # Draw bias x draw position
        if "bias_draw_win_rate" in df.columns and "odds_rank_norm" in df.columns:
            df["ix_draw_bias_x_rank"] = df["bias_draw_win_rate"].fillna(0) * df["odds_rank_norm"].fillna(0.5)

        n_new = len(df.columns) - n_before
        logger.info("  -> %d interaction features added", n_new)
        return df

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    def feature_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a summary of all generated features."""
        prefixes = {
            "roll_": "Rolling Stats",
            "tmp_": "Temporal",
            "odds_": "Odds",
            "jk_": "Jockey",
            "tr_": "Trainer",
            "jt_": "Jockey-Trainer",
            "ped_": "Pedigree",
            "bias_": "Track Bias",
            "pace_": "Pace Profile",
            "sec_": "Sectional",
            "fld_": "Field Strength",
            "ix_": "Interactions",
        }
        rows = []
        for col in df.columns:
            group = "Other"
            for pref, name in prefixes.items():
                if col.startswith(pref):
                    group = name
                    break
            rows.append({
                "feature": col,
                "group": group,
                "dtype": str(df[col].dtype),
                "null_pct": round(df[col].isna().mean() * 100, 1),
            })
        summary = pd.DataFrame(rows)
        group_counts = summary.groupby("group").size().reset_index(name="count")
        logger.info("Feature groups:\n%s", group_counts.to_string(index=False))
        return summary

    def save(self, df: pd.DataFrame, name: str = "advanced_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s (%d rows, %d cols)", out, len(df), len(df.columns))
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Advanced Feature Generator")
    parser.add_argument("--partants", default=None, help="Path to partants file")
    parser.add_argument("--features", default=None, help="Path to features_matrix file")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--skip", nargs="*", default=[], help="Builders to skip")
    parser.add_argument("--interactions", action="store_true", help="Add interaction features")
    parser.add_argument("--summary", action="store_true", help="Print feature summary")
    args = parser.parse_args()

    gen = AdvancedFeatureGenerator(output_dir=args.output_dir, skip_builders=args.skip)
    df = gen.load_source(partants_path=args.partants, features_path=args.features)
    df = gen.generate_all(df)

    if args.interactions:
        df = gen.add_interactions(df)

    if args.summary:
        summary = gen.feature_summary(df)
        out_csv = gen.output_dir / "feature_summary.csv"
        summary.to_csv(out_csv, index=False, encoding="utf-8")
        logger.info("  Summary saved -> %s", out_csv)

    gen.save(df)

    print("[OK] Advanced feature generation complete.")
    print("  Total features: %d" % len(df.columns))
    print("  Timing: %s" % gen.timing)


if __name__ == "__main__":
    main()
