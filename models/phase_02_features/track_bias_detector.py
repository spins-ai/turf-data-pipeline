# -*- coding: utf-8 -*-
"""
Track Bias Detector
===================
Rail bias, draw bias, pace bias per track per going (terrain condition).
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


class TrackBiasDetector:
    """Detect and encode track biases as features."""

    MIN_SAMPLE = 20  # minimum races to compute bias

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add track bias features."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("Building track bias features ...")

        df = self._draw_bias(df)
        df = self._rail_position_bias(df)
        df = self._track_win_distribution(df)
        df = self._track_distance_profile(df)
        df = self._pace_bias(df)

        n_new = len([c for c in df.columns if c.startswith("bias_")])
        logger.info("  -> %d track bias features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _draw_bias(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Does a certain draw (stall/post position) have an advantage at this track?
        Compute win rate by draw bracket at each track.
        """
        track_col = "hippodrome_normalise"
        draw_col = "num_pmu"  # post position
        if track_col not in df.columns or draw_col not in df.columns or "place_arrivee" not in df.columns:
            return df

        df = df.sort_values("date_reunion_iso").reset_index(drop=True)

        # Bracket draws: inside (1-4), middle (5-10), outside (11+)
        df["_draw_bracket"] = pd.cut(
            df[draw_col], bins=[0, 4, 10, 30], labels=["inside", "middle", "outside"], right=True
        ).astype(str)

        # Win rate per track x draw_bracket (expanding, point-in-time)
        df["_track_draw"] = df[track_col].astype(str) + "||" + df["_draw_bracket"]
        grp = df.groupby("_track_draw")
        win = (df["place_arrivee"] == 1).astype(float)

        df["bias_draw_win_rate"] = grp.apply(
            lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df["bias_draw_sample"] = grp.cumcount()

        # Overall track draw advantage: inside wr - outside wr
        inside_wr = df.loc[df["_draw_bracket"] == "inside"].groupby(track_col).apply(
            lambda g: (g["place_arrivee"] == 1).mean()
        )
        outside_wr = df.loc[df["_draw_bracket"] == "outside"].groupby(track_col).apply(
            lambda g: (g["place_arrivee"] == 1).mean()
        )
        draw_adv = (inside_wr - outside_wr).to_dict()
        df["bias_inside_advantage"] = df[track_col].map(draw_adv).fillna(0)

        df.drop(columns=["_draw_bracket", "_track_draw"], inplace=True, errors="ignore")
        return df

    def _rail_position_bias(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rail (corde) position bias at each track."""
        track_col = "hippodrome_normalise"
        if track_col not in df.columns or "place_corde" not in df.columns or "place_arrivee" not in df.columns:
            return df

        # Low corde = close to rail
        df["bias_near_rail"] = (df["place_corde"].fillna(99) <= 3).astype(int)

        df["_track_rail"] = df[track_col].astype(str) + "||" + df["bias_near_rail"].astype(str)
        grp = df.groupby("_track_rail")
        win = (df["place_arrivee"] == 1).astype(float)

        df["bias_rail_win_rate"] = grp.apply(
            lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df.drop(columns=["_track_rail"], inplace=True, errors="ignore")
        return df

    def _track_win_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """Track-level metrics: field sizes, favourite strike rate."""
        track_col = "hippodrome_normalise"
        if track_col not in df.columns or "course_uid" not in df.columns:
            return df

        # Average field size per track
        field_sizes = df.groupby([track_col, "course_uid"])["partant_uid"].count().groupby(level=0).mean()
        df["bias_avg_field_size"] = df[track_col].map(field_sizes).fillna(field_sizes.mean() if len(field_sizes) else 10)

        return df

    def _track_distance_profile(self, df: pd.DataFrame) -> pd.DataFrame:
        """How common is this distance at this track."""
        track_col = "hippodrome_normalise"
        if track_col not in df.columns or "distance" not in df.columns:
            return df

        # Proportion of races at each distance bucket per track
        dist_buckets = pd.cut(df["distance"], bins=[0, 1300, 1800, 2400, 3200, 10000],
                              labels=["sprint", "mile", "mid", "stay", "marathon"], right=True).astype(str)
        df["_dist_bucket"] = dist_buckets
        df["_td_key"] = df[track_col].astype(str) + "||" + df["_dist_bucket"]

        # How many times this track+distance combo has been run
        combo_counts = df["_td_key"].value_counts().to_dict()
        track_counts = df[track_col].value_counts().to_dict()
        df["bias_dist_frequency"] = df["_td_key"].map(combo_counts).fillna(0) / df[track_col].map(track_counts).clip(lower=1)

        df.drop(columns=["_dist_bucket", "_td_key"], inplace=True, errors="ignore")
        return df

    def _pace_bias(self, df: pd.DataFrame) -> pd.DataFrame:
        """Does this track favour front-runners or closers?
        Proxy: win rate of low draw numbers (front) vs high (back).
        """
        track_col = "hippodrome_normalise"
        if track_col not in df.columns or "num_pmu" not in df.columns or "place_arrivee" not in df.columns:
            return df

        # Front-runner proxy: low num_pmu tends to break faster (simplification)
        df["_is_front"] = (df["num_pmu"] <= 5).astype(float)
        front_wr = df[df["place_arrivee"] == 1].groupby(track_col)["_is_front"].mean()
        df["bias_front_runner_advantage"] = df[track_col].map(front_wr).fillna(0.5)

        df.drop(columns=["_is_front"], inplace=True, errors="ignore")
        return df

    def save(self, df: pd.DataFrame, name: str = "track_bias_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Track Bias Detector")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = TrackBiasDetector(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Track bias features generated.")


if __name__ == "__main__":
    main()
