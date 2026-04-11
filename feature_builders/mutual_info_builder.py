#!/usr/bin/env python3
"""
feature_builders.mutual_info_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Mutual information proxies and advanced statistical features for ML.

Computes monotonic and non-linear transforms of key numeric fields so that
tree-based models (CatBoost, XGBoost, LightGBM) and linear models can both
exploit the signal without needing to learn the transform themselves.

Single-pass streaming over partants_master.jsonl.  All features are
point-in-time safe (no future leakage).

Features (10):
  - mi_log_odds          : log(cote_finale)                       – linearises odds scale
  - mi_log_gains         : log(1 + gains_carriere_euros)          – linearises career earnings
  - mi_log_nb_courses    : log(1 + nb_courses_carriere)           – linearises experience
  - mi_sqrt_allocation   : sqrt(allocation)                       – compresses prize outliers
  - mi_odds_squared      : cote_finale^2                          – captures non-linear odds signal
  - mi_wins_times_gains  : nb_victoires_carriere * log(1 + gains) – quality interaction
  - mi_age_squared       : age^2                                  – non-linear age effect
  - mi_inverse_odds      : 1 / cote_finale                        – probability-like transform
  - mi_position_moy_cubed: position_moy_5^3                       – amplifies form extremes
  - mi_nb_courses_binned : bin(nb_courses_carriere, edges=[5,15,40,100]) -> 0..4
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/mutual_info")
_LOG_EVERY = 500_000


# ---------------------------------------------------------------------------
# Safe helpers
# ---------------------------------------------------------------------------

def _sf(val) -> Optional[float]:
    """Safe float conversion; returns None for None, non-numeric, or NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if f != f else f  # NaN guard
    except (TypeError, ValueError):
        return None


def _si(val) -> Optional[int]:
    """Safe int conversion."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Feature computation helpers
# ---------------------------------------------------------------------------

def _log_odds(cote: Optional[float]) -> Optional[float]:
    """log(cote_finale).  Requires cote > 0."""
    if cote is None or cote <= 0:
        return None
    return math.log(cote)


def _log1p_val(val: Optional[float]) -> Optional[float]:
    """log(1 + val).  Requires val >= 0."""
    if val is None or val < 0:
        return None
    return math.log1p(val)


def _sqrt_val(val: Optional[float]) -> Optional[float]:
    """sqrt(val).  Requires val >= 0."""
    if val is None or val < 0:
        return None
    return math.sqrt(val)


def _bin_nb_courses(nb: Optional[int]) -> Optional[int]:
    """Bin nb_courses_carriere into 5 ordinal categories.

    Edges (inclusive lower, exclusive upper):
        0 – 5   -> 0  (very inexperienced)
        6 – 15  -> 1  (novice)
        16 – 40 -> 2  (intermediate)
        41 – 100-> 3  (experienced)
        101+    -> 4  (veteran)
    """
    if nb is None or nb < 0:
        return None
    if nb <= 5:
        return 0
    if nb <= 15:
        return 1
    if nb <= 40:
        return 2
    if nb <= 100:
        return 3
    return 4


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build(logger) -> None:
    t0 = time.time()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "mutual_info_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    feat_names = [
        "mi_log_odds",
        "mi_log_gains",
        "mi_log_nb_courses",
        "mi_sqrt_allocation",
        "mi_odds_squared",
        "mi_wins_times_gains",
        "mi_age_squared",
        "mi_inverse_odds",
        "mi_position_moy_cubed",
        "mi_nb_courses_binned",
    ]
    fill = {k: 0 for k in feat_names}

    logger.info("Single-pass streaming: calcul de 10 features mutual-info proxies...")

    n_written = 0
    n_skipped = 0

    with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        for raw_line in fin:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                rec = json.loads(raw_line)
            except json.JSONDecodeError:
                n_skipped += 1
                continue

            n_written += 1
            if n_written % _LOG_EVERY == 0:
                logger.info("  %d records traites...", n_written)
                gc.collect()

            # --- Extract raw fields ---
            partant_uid  = rec.get("partant_uid", "")
            course_uid   = rec.get("course_uid", "")
            date_str     = rec.get("date_reunion_iso", "")

            cote          = _sf(rec.get("cote_finale")) or _sf(rec.get("cote_reference"))
            gains_carriere = _sf(rec.get("gains_carriere_euros"))
            nb_courses    = _si(rec.get("nb_courses_carriere"))
            nb_victoires  = _si(rec.get("nb_victoires_carriere"))
            allocation    = _sf(rec.get("allocation"))
            age           = _si(rec.get("age"))
            # position_moy_5: average finishing position over last 5 races
            pos_moy       = _sf(rec.get("position_moy_5")) or _sf(rec.get("seq_position_moy_5"))

            out = {
                "partant_uid":     partant_uid,
                "course_uid":      course_uid,
                "date_reunion_iso": date_str,
            }

            # 1. mi_log_odds = log(cote_finale)
            v = _log_odds(cote)
            out["mi_log_odds"] = round(v, 6) if v is not None else None
            if v is not None:
                fill["mi_log_odds"] += 1

            # 2. mi_log_gains = log(1 + gains_carriere_euros)
            v = _log1p_val(gains_carriere)
            out["mi_log_gains"] = round(v, 6) if v is not None else None
            if v is not None:
                fill["mi_log_gains"] += 1

            # 3. mi_log_nb_courses = log(1 + nb_courses_carriere)
            v = _log1p_val(float(nb_courses) if nb_courses is not None else None)
            out["mi_log_nb_courses"] = round(v, 6) if v is not None else None
            if v is not None:
                fill["mi_log_nb_courses"] += 1

            # 4. mi_sqrt_allocation = sqrt(allocation)
            v = _sqrt_val(allocation)
            out["mi_sqrt_allocation"] = round(v, 4) if v is not None else None
            if v is not None:
                fill["mi_sqrt_allocation"] += 1

            # 5. mi_odds_squared = cote_finale^2
            if cote is not None and cote > 0:
                v = round(cote * cote, 4)
                out["mi_odds_squared"] = v
                fill["mi_odds_squared"] += 1
            else:
                out["mi_odds_squared"] = None

            # 6. mi_wins_times_gains = nb_victoires_carriere * log(1 + gains_carriere)
            if nb_victoires is not None and gains_carriere is not None:
                lg = _log1p_val(gains_carriere)
                if lg is not None:
                    v = round(nb_victoires * lg, 6)
                    out["mi_wins_times_gains"] = v
                    fill["mi_wins_times_gains"] += 1
                else:
                    out["mi_wins_times_gains"] = None
            else:
                out["mi_wins_times_gains"] = None

            # 7. mi_age_squared = age^2
            if age is not None:
                out["mi_age_squared"] = age * age
                fill["mi_age_squared"] += 1
            else:
                out["mi_age_squared"] = None

            # 8. mi_inverse_odds = 1 / cote_finale
            if cote is not None and cote > 0:
                out["mi_inverse_odds"] = round(1.0 / cote, 6)
                fill["mi_inverse_odds"] += 1
            else:
                out["mi_inverse_odds"] = None

            # 9. mi_position_moy_cubed = position_moy_5^3
            if pos_moy is not None:
                out["mi_position_moy_cubed"] = round(pos_moy ** 3, 4)
                fill["mi_position_moy_cubed"] += 1
            else:
                out["mi_position_moy_cubed"] = None

            # 10. mi_nb_courses_binned
            v_bin = _bin_nb_courses(nb_courses)
            out["mi_nb_courses_binned"] = v_bin
            if v_bin is not None:
                fill["mi_nb_courses_binned"] += 1

            fout.write(json.dumps(out, ensure_ascii=False) + "\n")

    # Atomic rename
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info("Termine: %d records ecrits, %d ignores en %.1fs", n_written, n_skipped, elapsed)
    logger.info("Fill rates:")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0.0
        logger.info("  %-30s: %7d / %d (%.1f%%)", k, v, n_written, pct)
    logger.info("Output: %s", output_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Mutual information proxies + advanced statistical features builder"
    )
    parser.parse_args()
    logger = setup_logging("mutual_info_builder")
    build(logger)


if __name__ == "__main__":
    main()
