# -*- coding: utf-8 -*-
"""
Odds Feature Builder
====================
Odds movement, market efficiency, implied probability, overround.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class OddsFeatureBuilder:
    """Build features from odds / cote data."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add odds-based features."""
        df = df.copy()
        logger.info("Building odds features ...")

        df = self._implied_probability(df)
        df = self._overround(df)
        df = self._odds_rank(df)
        df = self._odds_movement(df)
        df = self._market_consensus(df)
        df = self._value_indicators(df)

        n_new = len([c for c in df.columns if c.startswith("odds_")])
        logger.info("  -> %d odds features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _implied_probability(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert decimal odds to implied probability."""
        for odds_col in ["cote_prob", "cote_direct"]:
            if odds_col not in df.columns:
                continue
            odds = df[odds_col].clip(lower=1.01)
            prefix = "odds_prob" if odds_col == "cote_prob" else "odds_direct"
            df[f"{prefix}_implied"] = 1.0 / odds
            df[f"{prefix}_log"] = np.log(odds)
            df[f"{prefix}_inv_sqrt"] = 1.0 / np.sqrt(odds)
        return df

    def _overround(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute race-level overround (sum of implied probs > 1 = bookmaker margin)."""
        odds_col = "cote_prob" if "cote_prob" in df.columns else "cote_direct"
        if odds_col not in df.columns or "course_uid" not in df.columns:
            return df
        odds = df[odds_col].clip(lower=1.01)
        implied = 1.0 / odds
        race_overround = implied.groupby(df["course_uid"]).transform("sum")
        df["odds_overround"] = race_overround
        # Normalised probability (fair odds)
        df["odds_fair_prob"] = implied / race_overround.clip(lower=0.01)
        return df

    def _odds_rank(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rank horses by odds within each race (1 = favourite)."""
        odds_col = "cote_prob" if "cote_prob" in df.columns else "cote_direct"
        if odds_col not in df.columns or "course_uid" not in df.columns:
            return df
        df["odds_rank"] = df.groupby("course_uid")[odds_col].rank(method="min")
        df["odds_is_favourite"] = (df["odds_rank"] == 1).astype(int)

        # Normalised rank within field
        field_size = df.groupby("course_uid")[odds_col].transform("count")
        df["odds_rank_norm"] = df["odds_rank"] / field_size.clip(lower=1)
        return df

    def _odds_movement(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute odds drift between cote_prob and cote_direct if both exist."""
        if "cote_prob" not in df.columns or "cote_direct" not in df.columns:
            return df
        prob = df["cote_prob"].clip(lower=1.01)
        direct = df["cote_direct"].clip(lower=1.01)

        df["odds_drift_abs"] = direct - prob
        df["odds_drift_pct"] = (direct - prob) / prob
        df["odds_shortened"] = (direct < prob).astype(int)  # money came in
        df["odds_drifted"] = (direct > prob).astype(int)
        return df

    def _market_consensus(self, df: pd.DataFrame) -> pd.DataFrame:
        """How much the market agrees: entropy of implied probabilities."""
        if "odds_fair_prob" not in df.columns or "course_uid" not in df.columns:
            return df
        p = df["odds_fair_prob"].clip(lower=1e-9)

        def entropy(probs):
            return -np.sum(probs * np.log2(probs))

        race_entropy = p.groupby(df["course_uid"]).transform(entropy)
        df["odds_market_entropy"] = race_entropy

        # Max entropy for a uniform field
        field_size = df.groupby("course_uid")["odds_fair_prob"].transform("count")
        max_ent = np.log2(field_size.clip(lower=2))
        df["odds_market_certainty"] = 1.0 - (race_entropy / max_ent.clip(lower=0.01))
        return df

    def _value_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify potential value: implied prob vs historical win rate."""
        if "odds_fair_prob" not in df.columns:
            return df
        # If we have rolling win rate from rolling_stats_generator
        for wrate_col in ["roll_win_rate_5", "roll_win_rate_10", "roll_win_rate_20"]:
            if wrate_col in df.columns:
                df[f"odds_value_{wrate_col}"] = df[wrate_col] - df["odds_fair_prob"]
        return df

    def save(self, df: pd.DataFrame, name: str = "odds_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Odds Feature Builder")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = OddsFeatureBuilder(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Odds features generated.")


if __name__ == "__main__":
    main()
