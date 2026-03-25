#!/usr/bin/env python3
"""
feature_builders.public_money_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Public money / market sentiment features capturing divergences between
betting odds, form, and rating systems.

Temporal integrity: for any partant at date D, only races with date < D
contribute to historical statistics -- no future leakage.

Produces:
  - public_money_features.jsonl   in output/public_money/

Features per partant (4):
  - is_public_favorite        : 1 if this horse has the lowest cote in the
                                 field (public's top pick), else 0.
  - favorite_vs_form_gap      : rank_by_cote minus rank_by_recent_winrate
                                 within the field. Positive = market likes
                                 the horse more than form suggests.
  - longshot_form_signal      : 1 if horse has above-median recent win rate
                                 but cote >= 10 (good form, high odds).
                                 A potential value bet signal.
  - market_vs_elo_divergence  : z-scored difference between implied
                                 probability from cote and normalised Elo
                                 score within the field. Positive = market
                                 underestimates this horse vs Elo.

Usage:
    python feature_builders/public_money_builder.py
    python feature_builders/public_money_builder.py --input data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging
from utils.output import save_jsonl

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = _PROJECT_ROOT / "output" / "public_money"

_LOG_EVERY = 500_000

# Longshot threshold
LONGSHOT_COTE = 10.0

# Rolling window for recent form
_FORM_WINDOW = 10


# ===========================================================================
# FORM STATE TRACKER
# ===========================================================================


class _FormState:
    """Per-horse recent form accumulator."""

    __slots__ = ("recent_results",)

    def __init__(self) -> None:
        self.recent_results: list[int] = []  # 1=win, 0=not-win, last N

    @property
    def win_rate(self) -> Optional[float]:
        if not self.recent_results:
            return None
        return sum(self.recent_results) / len(self.recent_results)


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (ValueError, TypeError):
        return None


def _implied_prob(cote: float) -> float:
    """Convert decimal cote to implied probability."""
    return 1.0 / cote if cote > 0 else 0.0


def _zscore(value: float, values: list[float]) -> float:
    """Compute z-score of value within the list. Returns 0 if std=0."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(var) if var > 0 else 0.0
    if std == 0.0:
        return 0.0
    return (value - mean) / std


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_public_money_features(
    input_path: Path, logger
) -> list[dict[str, Any]]:
    """Build public money features from partants_master.jsonl."""
    logger.info("=== Public Money Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields --
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)

        cote = _safe_float(rec.get("cote_finale") or rec.get("rapport_final"))
        elo = _safe_float(rec.get("elo_rating") or rec.get("elo"))

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cheval": rec.get("nom_cheval"),
            "gagnant": bool(rec.get("is_gagnant")),
            "cote": cote,
            "elo": elo,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records),
        time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process date by date, course by course --
    t2 = time.time()
    horse_form: dict[str, _FormState] = defaultdict(_FormState)
    results: list[dict[str, Any]] = []
    n_processed = 0

    i = 0
    total = len(slim_records)

    while i < total:
        current_date = slim_records[i]["date"]
        date_group: list[dict] = []

        while i < total and slim_records[i]["date"] == current_date:
            date_group.append(slim_records[i])
            i += 1

        # Group by course for field-level features
        courses: dict[str, list[dict]] = defaultdict(list)
        for rec in date_group:
            courses[rec["course"]].append(rec)

        for course_uid, field in courses.items():
            # Collect cote, form, elo for the field
            field_cotes: list[Optional[float]] = []
            field_forms: list[Optional[float]] = []
            field_elos: list[Optional[float]] = []

            for rec in field:
                cheval = rec["cheval"]
                form_state = horse_form.get(cheval) if cheval else None
                wr = form_state.win_rate if form_state else None
                field_cotes.append(rec["cote"])
                field_forms.append(wr)
                field_elos.append(rec["elo"])

            # -- Compute field-level rankings --
            # Rank by cote (lowest cote = rank 1 = market favorite)
            valid_cotes = [(c, idx) for idx, c in enumerate(field_cotes) if c is not None]
            valid_cotes.sort(key=lambda x: x[0])
            cote_rank: dict[int, int] = {}
            for rank, (_, idx) in enumerate(valid_cotes, 1):
                cote_rank[idx] = rank

            min_cote_idx = valid_cotes[0][1] if valid_cotes else None

            # Rank by form (highest win rate = rank 1)
            valid_forms = [(f, idx) for idx, f in enumerate(field_forms) if f is not None]
            valid_forms.sort(key=lambda x: -x[0])
            form_rank: dict[int, int] = {}
            for rank, (_, idx) in enumerate(valid_forms, 1):
                form_rank[idx] = rank

            # Median form for longshot signal
            form_values = [f for f, _ in valid_forms]
            median_form = (
                sorted(form_values)[len(form_values) // 2]
                if form_values
                else None
            )

            # Implied prob and elo for divergence
            implied_probs: list[Optional[float]] = []
            for c in field_cotes:
                implied_probs.append(_implied_prob(c) if c is not None else None)

            # Normalise elo within field for divergence
            valid_elos_vals = [e for e in field_elos if e is not None]

            for idx, rec in enumerate(field):
                cheval = rec["cheval"]
                cote = rec["cote"]

                if not cheval:
                    results.append({
                        "partant_uid": rec["uid"],
                        "is_public_favorite": None,
                        "favorite_vs_form_gap": None,
                        "longshot_form_signal": None,
                        "market_vs_elo_divergence": None,
                    })
                    continue

                # -- is_public_favorite --
                is_fav = 1 if (min_cote_idx is not None and idx == min_cote_idx) else 0

                # -- favorite_vs_form_gap --
                cr = cote_rank.get(idx)
                fr = form_rank.get(idx)
                if cr is not None and fr is not None:
                    fav_form_gap = fr - cr  # positive = market favors more than form
                else:
                    fav_form_gap = None

                # -- longshot_form_signal --
                wr = field_forms[idx]
                if (
                    cote is not None
                    and cote >= LONGSHOT_COTE
                    and wr is not None
                    and median_form is not None
                    and wr >= median_form
                    and median_form > 0
                ):
                    longshot_signal = 1
                else:
                    longshot_signal = 0

                # -- market_vs_elo_divergence --
                ip = implied_probs[idx]
                elo = field_elos[idx]
                if (
                    ip is not None
                    and elo is not None
                    and len(valid_elos_vals) >= 2
                ):
                    # Normalised elo probability (share of total elo)
                    elo_sum = sum(valid_elos_vals)
                    if elo_sum > 0:
                        elo_prob = elo / elo_sum
                        diff = elo_prob - ip  # positive = elo rates higher than market
                        # z-score the diff across field
                        all_diffs = []
                        for j in range(len(field)):
                            ip_j = implied_probs[j]
                            elo_j = field_elos[j]
                            if ip_j is not None and elo_j is not None:
                                all_diffs.append((elo_j / elo_sum) - ip_j)
                        divergence = round(_zscore(diff, all_diffs), 4) if all_diffs else None
                    else:
                        divergence = None
                else:
                    divergence = None

                results.append({
                    "partant_uid": rec["uid"],
                    "is_public_favorite": is_fav,
                    "favorite_vs_form_gap": fav_form_gap,
                    "longshot_form_signal": longshot_signal,
                    "market_vs_elo_divergence": divergence,
                })

        # -- Update form state with this date's outcomes --
        for rec in date_group:
            cheval = rec["cheval"]
            if not cheval:
                continue

            state = horse_form[cheval]
            state.recent_results.append(1 if rec["gagnant"] else 0)
            if len(state.recent_results) > _FORM_WINDOW:
                state.recent_results.pop(0)

        n_processed += len(date_group)
        if n_processed % _LOG_EVERY == 0:
            logger.info("  Traite %d / %d records...", n_processed, total)

    elapsed = time.time() - t0
    logger.info(
        "Public money build termine: %d features en %.1fs (chevaux uniques: %d)",
        len(results),
        elapsed,
        len(horse_form),
    )

    return results


# ===========================================================================
# SAUVEGARDE & CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features public money a partir de partants_master"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Repertoire de sortie (defaut: output/public_money/)",
    )
    args = parser.parse_args()

    logger = setup_logging("public_money_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    results = build_public_money_features(input_path, logger)

    # Save
    out_path = output_dir / "public_money_features.jsonl"
    save_jsonl(results, out_path, logger)

    # Summary stats
    if results:
        filled = {k: 0 for k in results[0] if k != "partant_uid"}
        for r in results:
            for k in filled:
                if r.get(k) is not None:
                    filled[k] += 1
        total = len(results)
        logger.info("=== Fill rates ===")
        for k, v in filled.items():
            logger.info("  %s: %d/%d (%.1f%%)", k, v, total, 100 * v / total)


if __name__ == "__main__":
    main()
