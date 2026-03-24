# -*- coding: utf-8 -*-
"""
Jockey-Trainer Synergy Builder
==============================
Duo win rate, synergy score, combo form, individual and pair statistics.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("jockey_trainer_synergy_builder")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class JockeyTrainerSynergyBuilder:
    """Build jockey, trainer, and jockey-trainer combo features."""

    MIN_SAMPLE = 5  # minimum races for a stat to be meaningful

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add jockey-trainer synergy features."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("Building jockey-trainer synergy features ...")

        df = self._individual_stats(df, "jockey_driver", "jk")
        df = self._individual_stats(df, "entraineur", "tr")
        df = self._combo_stats(df)
        df = self._synergy_score(df)
        df = self._recent_form(df)

        n_new = len([c for c in df.columns if c.startswith(("jk_", "tr_", "jt_"))])
        logger.info("  -> %d jockey/trainer features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _individual_stats(self, df: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:
        """Compute expanding win/place rate per entity (point-in-time)."""
        if col not in df.columns or "place_arrivee" not in df.columns:
            return df

        df = df.sort_values(["date_reunion_iso"]).reset_index(drop=True)

        # Use shift(1) + expanding to avoid leakage
        grp = df.groupby(col)
        win = (df["place_arrivee"] == 1).astype(float)
        place = (df["place_arrivee"].between(1, 3)).astype(float)

        # Expanding mean with shift
        df[f"{prefix}_win_rate"] = grp.apply(
            lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df[f"{prefix}_place_rate"] = grp.apply(
            lambda g: g.assign(_p=place.loc[g.index].shift(1))["_p"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        # Race count
        df[f"{prefix}_race_count"] = grp.cumcount()

        # Average finish position
        df[f"{prefix}_avg_finish"] = grp.apply(
            lambda g: g["place_arrivee"].shift(1).expanding().mean()
        ).reset_index(level=0, drop=True).values

        # Has enough history
        df[f"{prefix}_reliable"] = (df[f"{prefix}_race_count"] >= self.MIN_SAMPLE).astype(int)

        return df

    def _combo_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Stats for the jockey-trainer pair."""
        if "jockey_driver" not in df.columns or "entraineur" not in df.columns:
            return df
        if "place_arrivee" not in df.columns:
            return df

        df["_jt_combo"] = df["jockey_driver"].astype(str) + "||" + df["entraineur"].astype(str)
        df = df.sort_values("date_reunion_iso").reset_index(drop=True)

        grp = df.groupby("_jt_combo")
        win = (df["place_arrivee"] == 1).astype(float)

        df["jt_combo_win_rate"] = grp.apply(
            lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df["jt_combo_count"] = grp.cumcount()
        df["jt_combo_reliable"] = (df["jt_combo_count"] >= self.MIN_SAMPLE).astype(int)

        df.drop(columns=["_jt_combo"], inplace=True, errors="ignore")
        return df

    def _synergy_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Synergy = combo win rate - expected if independent.
        expected = jockey_win_rate * trainer_win_rate * field_size (approx).
        Positive synergy = they perform better together than expected.
        """
        for col in ["jk_win_rate", "tr_win_rate", "jt_combo_win_rate"]:
            if col not in df.columns:
                return df

        expected = df["jk_win_rate"] * df["tr_win_rate"]
        df["jt_synergy"] = df["jt_combo_win_rate"] - expected
        df["jt_synergy_ratio"] = df["jt_combo_win_rate"] / expected.clip(lower=1e-6)
        return df

    def _recent_form(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recent form for jockey and trainer over last 30 days."""
        if "date_reunion_iso" not in df.columns or "place_arrivee" not in df.columns:
            return df

        for col, prefix in [("jockey_driver", "jk"), ("entraineur", "tr")]:
            if col not in df.columns:
                continue
            # Last-14-days win rate (simplified: rolling by race count)
            grp = df.groupby(col)
            win = (df["place_arrivee"] == 1).astype(float)
            df[f"{prefix}_form_5"] = grp.apply(
                lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].rolling(5, min_periods=1).mean()
            ).reset_index(level=0, drop=True).values

            df[f"{prefix}_form_10"] = grp.apply(
                lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].rolling(10, min_periods=1).mean()
            ).reset_index(level=0, drop=True).values

        return df

    def save(self, df: pd.DataFrame, name: str = "jockey_trainer_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Jockey-Trainer Synergy Builder")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = JockeyTrainerSynergyBuilder(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Jockey-trainer synergy features generated.")


if __name__ == "__main__":
    main()
