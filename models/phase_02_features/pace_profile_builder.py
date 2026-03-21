# -*- coding: utf-8 -*-
"""
Pace Profile Builder
====================
Early/mid/late pace estimation, front-runner vs closer classification.
Uses musique (race history string) and positional data.
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from utils.logging_setup import setup_logging

logger = setup_logging("pace_profile_builder")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class PaceProfileBuilder:
    """Build running-style and pace features."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add pace profile features."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("Building pace profile features ...")

        df = self._parse_musique(df)
        df = self._running_style(df)
        df = self._pace_consistency(df)
        df = self._race_pace_scenario(df)
        df = self._improvement_pattern(df)

        n_new = len([c for c in df.columns if c.startswith("pace_")])
        logger.info("  -> %d pace features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _parse_musique(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse the 'musique' field (e.g., '1p3p2p0p0aDM') into numeric finishes.
        Letters: D=disqualified, A=arrete, T=tombe, 0=unplaced.
        """
        if "musique" not in df.columns:
            return df

        def parse_one(mus: str) -> List[Optional[int]]:
            if not isinstance(mus, str) or not mus:
                return []
            # Extract sequences of digits or single letters
            tokens = re.findall(r'(\d+|[A-Za-z])', mus)
            results = []
            for t in tokens:
                if t.isdigit():
                    results.append(int(t))
                elif t.upper() in ("D", "A", "T", "R"):
                    results.append(None)  # non-finish
                # Skip 'p', 'M', 'h', etc. (race type indicators)
            return results

        parsed = df["musique"].apply(parse_one)

        # Last N finishes
        for n in [3, 5, 10]:
            df[f"pace_last{n}_finishes"] = parsed.apply(
                lambda x: [v for v in x[:n] if v is not None]
            )
            df[f"pace_avg_finish_{n}"] = df[f"pace_last{n}_finishes"].apply(
                lambda x: np.mean(x) if x else np.nan
            )
            df[f"pace_best_finish_{n}"] = df[f"pace_last{n}_finishes"].apply(
                lambda x: min(x) if x else np.nan
            )
            df[f"pace_worst_finish_{n}"] = df[f"pace_last{n}_finishes"].apply(
                lambda x: max(x) if x else np.nan
            )
            df.drop(columns=[f"pace_last{n}_finishes"], inplace=True)

        # DNF rate
        df["pace_dnf_rate"] = parsed.apply(
            lambda x: sum(1 for v in x if v is None) / max(len(x), 1)
        )
        # Win rate from musique
        df["pace_mus_win_rate"] = parsed.apply(
            lambda x: sum(1 for v in x if v == 1) / max(len(x), 1)
        )
        # Place rate from musique
        df["pace_mus_place_rate"] = parsed.apply(
            lambda x: sum(1 for v in x if v is not None and 1 <= v <= 3) / max(len(x), 1)
        )
        return df

    def _running_style(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify running style based on average finish position and patterns.
        Front-runner: consistently finishes in top positions early.
        Closer: tends to finish strongly (improving positions).
        """
        if "pace_avg_finish_5" not in df.columns:
            return df

        avg = df["pace_avg_finish_5"].fillna(5)
        # Simple classification thresholds
        df["pace_style_front"] = (avg <= 3).astype(int)
        df["pace_style_closer"] = (avg > 5).astype(int)
        df["pace_style_stalker"] = ((avg > 3) & (avg <= 5)).astype(int)

        # Numeric running style score (lower = more front-running)
        df["pace_style_score"] = avg.clip(upper=15) / 15.0
        return df

    def _pace_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Variance in recent finishes = consistency measure."""
        if "musique" not in df.columns:
            return df

        def finish_std(mus):
            if not isinstance(mus, str):
                return np.nan
            nums = [int(t) for t in re.findall(r'\d+', mus)[:10] if t.isdigit()]
            return np.std(nums) if len(nums) >= 2 else np.nan

        df["pace_consistency"] = df["musique"].apply(finish_std)
        df["pace_is_consistent"] = (df["pace_consistency"].fillna(99) < 2.0).astype(int)
        return df

    def _race_pace_scenario(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estimate the likely pace of the race based on how many
        front-runners are in the field.
        """
        if "pace_style_front" not in df.columns or "course_uid" not in df.columns:
            return df

        front_count = df.groupby("course_uid")["pace_style_front"].transform("sum")
        field_size = df.groupby("course_uid")["pace_style_front"].transform("count")
        df["pace_front_density"] = front_count / field_size.clip(lower=1)

        # Fast pace expected when many front-runners
        df["pace_scenario_fast"] = (df["pace_front_density"] > 0.3).astype(int)
        df["pace_scenario_slow"] = (df["pace_front_density"] < 0.15).astype(int)
        return df

    def _improvement_pattern(self, df: pd.DataFrame) -> pd.DataFrame:
        """Is the horse improving or declining based on recent finishes?"""
        if "pace_avg_finish_3" not in df.columns or "pace_avg_finish_10" not in df.columns:
            return df

        recent = df["pace_avg_finish_3"].fillna(5)
        longer = df["pace_avg_finish_10"].fillna(5)
        df["pace_trend"] = longer - recent  # positive = improving
        df["pace_improving"] = (df["pace_trend"] > 0.5).astype(int)
        df["pace_declining"] = (df["pace_trend"] < -0.5).astype(int)
        return df

    def save(self, df: pd.DataFrame, name: str = "pace_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Pace Profile Builder")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = PaceProfileBuilder(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Pace profile features generated.")


if __name__ == "__main__":
    main()
