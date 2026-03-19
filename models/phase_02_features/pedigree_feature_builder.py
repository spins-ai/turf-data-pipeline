# -*- coding: utf-8 -*-
"""
Pedigree Feature Builder
========================
Sire/dam stats by distance/terrain, stamina/speed index, inbreeding coefficient.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_MASTER = PROJECT_ROOT / "data_master"
OUTPUT_DIR = PROJECT_ROOT / "models" / "data"


class PedigreeFeatureBuilder:
    """Build pedigree-based features from sire/dam performance data."""

    # Distance categories
    DIST_BINS = [0, 1300, 1800, 2400, 3200, 10000]
    DIST_LABELS = ["sprint", "mile", "intermediate", "staying", "marathon"]

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sire_stats_: Dict[str, Dict] = {}

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add pedigree features to DataFrame."""
        df = df.copy()
        if "date_reunion_iso" in df.columns:
            df["date_reunion_iso"] = pd.to_datetime(df["date_reunion_iso"], errors="coerce")

        logger.info("Building pedigree features ...")

        df = self._add_distance_category(df)
        df = self._sire_stats(df)
        df = self._dam_family(df)
        df = self._stamina_speed_index(df)
        df = self._sire_distance_aptitude(df)
        df = self._inbreeding_proxy(df)
        df = self._pedigree_quality(df)

        n_new = len([c for c in df.columns if c.startswith("ped_")])
        logger.info("  -> %d pedigree features generated", n_new)
        return df

    # ------------------------------------------------------------------
    def _add_distance_category(self, df: pd.DataFrame) -> pd.DataFrame:
        if "distance" not in df.columns:
            return df
        df["ped_dist_cat"] = pd.cut(
            df["distance"], bins=self.DIST_BINS, labels=self.DIST_LABELS, right=True
        )
        return df

    def _sire_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute sire win/place rate from historical data (point-in-time)."""
        sire_col = "pere" if "pere" in df.columns else "pgr_pere"
        if sire_col not in df.columns or "place_arrivee" not in df.columns:
            return df

        df = df.sort_values("date_reunion_iso").reset_index(drop=True)
        grp = df.groupby(sire_col)

        win = (df["place_arrivee"] == 1).astype(float)
        place = (df["place_arrivee"].between(1, 3)).astype(float)

        df["ped_sire_win_rate"] = grp.apply(
            lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df["ped_sire_place_rate"] = grp.apply(
            lambda g: g.assign(_p=place.loc[g.index].shift(1))["_p"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df["ped_sire_runners"] = grp.cumcount()

        # Average finish
        df["ped_sire_avg_finish"] = grp.apply(
            lambda g: g["place_arrivee"].shift(1).expanding().mean()
        ).reset_index(level=0, drop=True).values

        return df

    def _dam_family(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dam-level aggregate stats."""
        dam_col = "mere" if "mere" in df.columns else "pgr_mere"
        if dam_col not in df.columns or "place_arrivee" not in df.columns:
            return df

        df = df.sort_values("date_reunion_iso").reset_index(drop=True)
        grp = df.groupby(dam_col)
        win = (df["place_arrivee"] == 1).astype(float)

        df["ped_dam_win_rate"] = grp.apply(
            lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df["ped_dam_runners"] = grp.cumcount()
        return df

    def _stamina_speed_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sire stamina index: fraction of progeny wins at 2400m+.
        Sire speed index: fraction of progeny wins at <1400m.
        """
        sire_col = "pere" if "pere" in df.columns else "pgr_pere"
        if sire_col not in df.columns or "distance" not in df.columns or "place_arrivee" not in df.columns:
            return df

        df = df.sort_values("date_reunion_iso").reset_index(drop=True)
        won = df["place_arrivee"] == 1

        # Stamina: wins at 2400+
        df["_stam_win"] = (won & (df["distance"] >= 2400)).astype(float)
        df["_speed_win"] = (won & (df["distance"] < 1400)).astype(float)
        df["_any_win"] = won.astype(float)

        grp = df.groupby(sire_col)
        for metric, new_col in [("_stam_win", "ped_sire_stamina_idx"), ("_speed_win", "ped_sire_speed_idx")]:
            cumwin = grp[metric].apply(lambda s: s.shift(1).expanding().sum()).reset_index(level=0, drop=True)
            cumtot = grp["_any_win"].apply(lambda s: s.shift(1).expanding().sum()).reset_index(level=0, drop=True)
            df[new_col] = cumwin / cumtot.clip(lower=1)

        df.drop(columns=["_stam_win", "_speed_win", "_any_win"], inplace=True, errors="ignore")
        return df

    def _sire_distance_aptitude(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        How well the sire's progeny perform at the current race distance category.
        """
        sire_col = "pere" if "pere" in df.columns else "pgr_pere"
        if sire_col not in df.columns or "ped_dist_cat" not in df.columns:
            return df
        if "place_arrivee" not in df.columns:
            return df

        df = df.sort_values("date_reunion_iso").reset_index(drop=True)
        df["_sire_dist_key"] = df[sire_col].astype(str) + "||" + df["ped_dist_cat"].astype(str)

        grp = df.groupby("_sire_dist_key")
        win = (df["place_arrivee"] == 1).astype(float)

        df["ped_sire_dist_win_rate"] = grp.apply(
            lambda g: g.assign(_w=win.loc[g.index].shift(1))["_w"].expanding().mean()
        ).reset_index(level=0, drop=True).values

        df["ped_sire_dist_count"] = grp.cumcount()
        df.drop(columns=["_sire_dist_key"], inplace=True, errors="ignore")
        return df

    def _inbreeding_proxy(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Simple inbreeding proxy: is sire name found in dam name or vice versa.
        A real implementation would use full pedigree trees.
        """
        sire_col = "pere" if "pere" in df.columns else "pgr_pere"
        dam_col = "mere" if "mere" in df.columns else "pgr_mere"
        if sire_col not in df.columns or dam_col not in df.columns:
            return df

        # Placeholder: 0 for all (would need full pedigree DB for real computation)
        df["ped_inbreeding_coeff"] = 0.0
        return df

    def _pedigree_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Combine sire + dam stats into a quality score."""
        sire_wr = df.get("ped_sire_win_rate", pd.Series(dtype=float))
        dam_wr = df.get("ped_dam_win_rate", pd.Series(dtype=float))
        df["ped_parent_avg_wr"] = (sire_wr.fillna(0) + dam_wr.fillna(0)) / 2
        return df

    def save(self, df: pd.DataFrame, name: str = "pedigree_features") -> Path:
        out = self.output_dir / f"{name}.parquet"
        # Drop temp cat column for parquet compatibility
        drop_cols = [c for c in df.columns if df[c].dtype.name == "category"]
        if drop_cols:
            df = df.copy()
            for c in drop_cols:
                df[c] = df[c].astype(str)
        df.to_parquet(out, index=False)
        logger.info("  Saved -> %s", out)
        return out


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Pedigree Feature Builder")
    parser.add_argument("--input", default=None, help="Parquet or JSONL input")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.input:
        inp = Path(args.input)
        df = pd.read_parquet(inp) if inp.suffix == ".parquet" else pd.read_json(inp, lines=True, encoding="utf-8")
    else:
        df = pd.read_json(DATA_MASTER / "partants_master.jsonl", lines=True, encoding="utf-8")

    builder = PedigreeFeatureBuilder(output_dir=args.output_dir)
    df = builder.generate(df)
    builder.save(df)
    print("[OK] Pedigree features generated.")


if __name__ == "__main__":
    main()
