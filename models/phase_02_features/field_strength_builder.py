# -*- coding: utf-8 -*-
"""
Field Strength Builder
======================
HHI (Herfindahl-Hirschman Index), entropy, relative strength,
class level estimation.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("field_strength_builder")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class FieldStrengthBuilder:
    """Measure competitive strength of each race field."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add field strength features."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("Building field strength features ...")

        df = self._field_size(df)
        df = self._hhi_concentration(df)
        df = self._field_entropy(df)
        df = self._relative_strength(df)
        df = self._class_level(df)
        df = self._competitive_balance(df)

        n_new = len([c for c in df.columns if c.startswith("fld_")])
        logger.info("  -> %d field strength features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _field_size(self, df: pd.DataFrame) -> pd.DataFrame:
        """Count runners per race."""
        if "course_uid" not in df.columns:
            return df
        df["fld_size"] = df.groupby("course_uid")["course_uid"].transform("count")
        df["fld_size_log"] = np.log1p(df["fld_size"])
        return df

    def _hhi_concentration(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Herfindahl-Hirschman Index from implied probabilities.
        High HHI = one horse dominates; Low HHI = competitive race.
        """
        if "course_uid" not in df.columns:
            return df

        # Use odds-based probability if available, else uniform
        prob_col = None
        for c in ["odds_fair_prob", "odds_prob_implied", "cote_prob"]:
            if c in df.columns:
                prob_col = c
                break

        if prob_col is None or not pd.api.types.is_numeric_dtype(df.get(prob_col, pd.Series(dtype=float))):
            # Uniform probability
            df["fld_hhi"] = 1.0 / df["fld_size"].clip(lower=1)
            return df

        if prob_col == "cote_prob":
            # Convert odds to prob
            p = (1.0 / df[prob_col].clip(lower=1.01))
            race_sum = p.groupby(df["course_uid"]).transform("sum").clip(lower=0.01)
            p = p / race_sum
        else:
            p = df[prob_col].clip(lower=1e-9)

        # HHI = sum of squared market shares
        p_sq = p ** 2
        df["fld_hhi"] = p_sq.groupby(df["course_uid"]).transform("sum")

        # Normalised HHI: (HHI - 1/N) / (1 - 1/N)
        n = df["fld_size"].clip(lower=2)
        df["fld_hhi_norm"] = (df["fld_hhi"] - 1.0 / n) / (1.0 - 1.0 / n).clip(lower=0.01)

        return df

    def _field_entropy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Shannon entropy of the probability distribution."""
        if "course_uid" not in df.columns:
            return df

        prob_col = None
        for c in ["odds_fair_prob", "odds_prob_implied"]:
            if c in df.columns:
                prob_col = c
                break

        if prob_col is None:
            # Use 1/odds if available
            if "cote_prob" in df.columns:
                p = 1.0 / df["cote_prob"].clip(lower=1.01)
                race_sum = p.groupby(df["course_uid"]).transform("sum").clip(lower=0.01)
                p = p / race_sum
            else:
                p = 1.0 / df["fld_size"].clip(lower=1) if "fld_size" in df.columns else pd.Series(0.1, index=df.index)
        else:
            p = df[prob_col].clip(lower=1e-9)

        log_p = np.log2(p.clip(lower=1e-9))
        neg_p_logp = -p * log_p
        df["fld_entropy"] = neg_p_logp.groupby(df["course_uid"]).transform("sum")

        # Max entropy = log2(N)
        if "fld_size" in df.columns:
            max_ent = np.log2(df["fld_size"].clip(lower=2))
            df["fld_entropy_norm"] = df["fld_entropy"] / max_ent.clip(lower=0.01)
        return df

    def _relative_strength(self, df: pd.DataFrame) -> pd.DataFrame:
        """How strong is this horse relative to the field?"""
        if "course_uid" not in df.columns:
            return df

        # Use win rate if available
        wr_col = None
        for c in ["roll_win_rate_10", "roll_win_rate_5", "jk_win_rate"]:
            if c in df.columns:
                wr_col = c
                break

        if wr_col is None:
            # Use nb_victoires / nb_courses as fallback
            if "nb_victoires_carriere" in df.columns and "nb_courses_carriere" in df.columns:
                df["_wr_proxy"] = df["nb_victoires_carriere"] / df["nb_courses_carriere"].clip(lower=1)
                wr_col = "_wr_proxy"
            else:
                return df

        field_avg = df.groupby("course_uid")[wr_col].transform("mean")
        field_max = df.groupby("course_uid")[wr_col].transform("max")
        field_std = df.groupby("course_uid")[wr_col].transform("std").clip(lower=1e-6)

        df["fld_rel_strength"] = (df[wr_col] - field_avg) / field_std
        df["fld_strength_rank"] = df.groupby("course_uid")[wr_col].rank(ascending=False, method="min")
        df["fld_is_strongest"] = (df["fld_strength_rank"] == 1).astype(int)
        df["fld_gap_to_best"] = field_max - df[wr_col]

        if "_wr_proxy" in df.columns:
            df.drop(columns=["_wr_proxy"], inplace=True, errors="ignore")
        return df

    def _class_level(self, df: pd.DataFrame) -> pd.DataFrame:
        """Estimate race class level from average earnings and career wins."""
        if "course_uid" not in df.columns:
            return df

        # Average career earnings of runners = class proxy
        if "gains_carriere_euros" in df.columns:
            df["fld_avg_earnings"] = df.groupby("course_uid")["gains_carriere_euros"].transform("mean")
            df["fld_earnings_rank"] = df["gains_carriere_euros"].fillna(0) / df["fld_avg_earnings"].clip(lower=1)

        # Average career wins
        if "nb_victoires_carriere" in df.columns:
            df["fld_avg_wins"] = df.groupby("course_uid")["nb_victoires_carriere"].transform("mean")

        # Average career races (experience)
        if "nb_courses_carriere" in df.columns:
            df["fld_avg_experience"] = df.groupby("course_uid")["nb_courses_carriere"].transform("mean")
            df["fld_experience_ratio"] = df["nb_courses_carriere"] / df["fld_avg_experience"].clip(lower=1)

        return df

    def _competitive_balance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Overall competitiveness: std of relevant metrics within the field.
        Low std = balanced, high std = one-sided.
        """
        if "course_uid" not in df.columns:
            return df

        for metric in ["nb_victoires_carriere", "nb_courses_carriere", "gains_carriere_euros"]:
            if metric in df.columns:
                race_std = df.groupby("course_uid")[metric].transform("std")
                race_mean = df.groupby("course_uid")[metric].transform("mean").clip(lower=1)
                # Coefficient of variation
                df[f"fld_cv_{metric}"] = race_std / race_mean
        return df

    def save(self, df: pd.DataFrame, name: str = "field_strength_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Field Strength Builder")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = FieldStrengthBuilder(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Field strength features generated.")


if __name__ == "__main__":
    main()
