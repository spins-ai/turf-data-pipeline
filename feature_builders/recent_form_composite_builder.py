#!/usr/bin/env python3
"""
feature_builders.recent_form_composite_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Aggregate recent performance into unified composite signals.

Reads partants_master.jsonl in two phases:
  Phase 1: build an index sorted chronologically.
  Phase 2: seek-based streaming, snapshot state BEFORE update for each horse,
           then update the horse's rolling history.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the features -- no future leakage.

Produces:
  - recent_form_composite.jsonl   in builder_outputs/recent_form_composite/

Features per partant (10):
  - rfc_form_rating           : weighted sum of last 5 results
                                (5pts win, 3pts place, 1pt finish, 0 DNF) / max_possible
  - rfc_momentum_score        : sum of position improvements over last 3 races
                                (pos[i-1] - pos[i])
  - rfc_consistency_rating    : 1 - (std of last 5 positions / mean) if 3+ races
  - rfc_place_streak          : current consecutive place (top 3) finishes
  - rfc_last_3_avg_beaten_pct : average (nb_partants - position) / nb_partants
                                over last 3 races
  - rfc_improving_form        : 1 if each of last 3 positions is better than or
                                equal to the previous
  - rfc_best_recent_vs_career : best position in last 5 / best position ever
                                (1.0 = at career best)
  - rfc_recent_vs_expected    : actual avg position last 5 vs implied from
                                average odds last 5
  - rfc_form_cycle_position   : 0=no data, 1=rising, 2=peak (last win within
                                3 races), 3=declining, 4=rebuilding
  - rfc_composite_form_score  : (0.4*form_rating + 0.3*consistency +
                                0.3*momentum) normalised 0-1

Usage:
    python feature_builders/recent_form_composite_builder.py
    python feature_builders/recent_form_composite_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/recent_form_composite")

_LOG_EVERY = 500_000

# Form rating weights
_WIN_POINTS = 5
_PLACE_POINTS = 3
_FINISH_POINTS = 1
_DNF_POINTS = 0
_MAX_FORM_WINDOW = 5
_MAX_FORM_SCORE = _WIN_POINTS * _MAX_FORM_WINDOW  # 25


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
# HELPERS
# ===========================================================================


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        val = float(v)
        return val if val > 0 else None
    except (TypeError, ValueError):
        return None


# ===========================================================================
# PER-HORSE STATE
# ===========================================================================


class _HorseFormState:
    """Rolling history for a single horse.

    All deques are ordered oldest-to-newest (append right = most recent).
    """

    __slots__ = (
        "positions", "partants", "is_placed", "is_gagnant",
        "odds", "best_ever_pos", "last_win_races_ago",
    )

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=10)       # int positions (None = DNF)
        self.partants: deque = deque(maxlen=10)        # int field sizes
        self.is_placed: deque = deque(maxlen=10)       # bool top-3
        self.is_gagnant: deque = deque(maxlen=10)      # bool winner
        self.odds: deque = deque(maxlen=10)            # float cote_finale
        self.best_ever_pos: Optional[int] = None       # best (lowest) position ever
        self.last_win_races_ago: Optional[int] = None  # how many races since last win

    # ---------------------------------------------------------------
    # Snapshot BEFORE update
    # ---------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Compute all 10 features from current (past-only) state."""

        feats: dict[str, Any] = {
            "rfc_form_rating": None,
            "rfc_momentum_score": None,
            "rfc_consistency_rating": None,
            "rfc_place_streak": None,
            "rfc_last_3_avg_beaten_pct": None,
            "rfc_improving_form": None,
            "rfc_best_recent_vs_career": None,
            "rfc_recent_vs_expected": None,
            "rfc_form_cycle_position": 0,
            "rfc_composite_form_score": None,
        }

        positions = list(self.positions)   # oldest..newest
        placed = list(self.is_placed)
        gagnant = list(self.is_gagnant)
        partants = list(self.partants)
        odds = list(self.odds)

        n = len(positions)
        if n == 0:
            return feats

        # --- 1. rfc_form_rating ---
        last5_pos = positions[-5:]
        last5_placed = placed[-5:]
        last5_gagnant = gagnant[-5:]
        total_pts = 0
        for pos, pl, gag in zip(last5_pos, last5_placed, last5_gagnant):
            if pos is None:
                total_pts += _DNF_POINTS
            elif gag:
                total_pts += _WIN_POINTS
            elif pl:
                total_pts += _PLACE_POINTS
            else:
                total_pts += _FINISH_POINTS
        max_possible = _WIN_POINTS * len(last5_pos)
        form_rating = round(total_pts / max_possible, 4) if max_possible > 0 else None
        feats["rfc_form_rating"] = form_rating

        # --- 2. rfc_momentum_score ---
        # Sum of (pos[i-1] - pos[i]) over last 3 numeric positions
        numeric_last3 = [p for p in positions[-3:] if p is not None]
        if len(numeric_last3) >= 2:
            momentum = 0
            for j in range(1, len(numeric_last3)):
                momentum += numeric_last3[j - 1] - numeric_last3[j]
            feats["rfc_momentum_score"] = momentum
        else:
            feats["rfc_momentum_score"] = None

        # --- 3. rfc_consistency_rating ---
        numeric_last5 = [p for p in last5_pos if p is not None]
        consistency = None
        if len(numeric_last5) >= 3:
            mean_pos = sum(numeric_last5) / len(numeric_last5)
            if mean_pos > 0:
                variance = sum((p - mean_pos) ** 2 for p in numeric_last5) / len(numeric_last5)
                std_pos = math.sqrt(variance)
                cv = std_pos / mean_pos
                consistency = round(max(0.0, min(1.0, 1.0 - cv)), 4)
        feats["rfc_consistency_rating"] = consistency

        # --- 4. rfc_place_streak ---
        streak = 0
        for pl in reversed(placed):
            if pl:
                streak += 1
            else:
                break
        feats["rfc_place_streak"] = streak

        # --- 5. rfc_last_3_avg_beaten_pct ---
        last3_pos = positions[-3:]
        last3_part = partants[-3:]
        beaten_pcts: list[float] = []
        for pos, npart in zip(last3_pos, last3_part):
            if pos is not None and npart is not None and npart > 1:
                beaten_pcts.append((npart - pos) / npart)
        if beaten_pcts:
            feats["rfc_last_3_avg_beaten_pct"] = round(
                sum(beaten_pcts) / len(beaten_pcts), 4
            )

        # --- 6. rfc_improving_form ---
        last3_numeric = [p for p in positions[-3:] if p is not None]
        if len(last3_numeric) >= 3:
            improving = all(
                last3_numeric[j] <= last3_numeric[j - 1]
                for j in range(1, len(last3_numeric))
            )
            feats["rfc_improving_form"] = 1 if improving else 0

        # --- 7. rfc_best_recent_vs_career ---
        recent5_numeric = [p for p in last5_pos if p is not None]
        if recent5_numeric and self.best_ever_pos is not None and self.best_ever_pos > 0:
            best_recent = min(recent5_numeric)
            feats["rfc_best_recent_vs_career"] = round(
                self.best_ever_pos / best_recent, 4
            ) if best_recent > 0 else None

        # --- 8. rfc_recent_vs_expected ---
        last5_odds = [o for o in odds[-5:] if o is not None]
        if len(numeric_last5) >= 2 and len(last5_odds) >= 2:
            avg_pos = sum(numeric_last5) / len(numeric_last5)
            # Implied expected position from odds: higher odds = worse expected position
            # Simple model: expected_pos ~= 1 / (1/odds_sum * odds_i) scaled by field
            avg_odds = sum(last5_odds) / len(last5_odds)
            # Expected position approximation: avg_odds / 2 (heuristic)
            expected_pos = avg_odds / 2.0
            if expected_pos > 0:
                feats["rfc_recent_vs_expected"] = round(expected_pos - avg_pos, 4)

        # --- 9. rfc_form_cycle_position ---
        cycle = 0
        if n >= 2:
            if self.last_win_races_ago is not None and self.last_win_races_ago <= 3:
                cycle = 2  # peak
            elif feats["rfc_momentum_score"] is not None and feats["rfc_momentum_score"] > 0:
                cycle = 1  # rising
            elif feats["rfc_momentum_score"] is not None and feats["rfc_momentum_score"] < -2:
                cycle = 3  # declining
            elif (
                self.last_win_races_ago is not None
                and self.last_win_races_ago > 5
                and feats["rfc_momentum_score"] is not None
                and feats["rfc_momentum_score"] >= 0
            ):
                cycle = 4  # rebuilding
            else:
                cycle = 1 if feats["rfc_momentum_score"] is not None and feats["rfc_momentum_score"] >= 0 else 3
        feats["rfc_form_cycle_position"] = cycle

        # --- 10. rfc_composite_form_score ---
        # (0.4*form_rating + 0.3*consistency + 0.3*momentum_normalised) in [0,1]
        if form_rating is not None:
            c_val = consistency if consistency is not None else 0.5
            # Normalise momentum to [0,1]: momentum ranges roughly -10..+10
            m_raw = feats["rfc_momentum_score"]
            if m_raw is not None:
                m_norm = max(0.0, min(1.0, (m_raw + 10.0) / 20.0))
            else:
                m_norm = 0.5
            composite = 0.4 * form_rating + 0.3 * c_val + 0.3 * m_norm
            feats["rfc_composite_form_score"] = round(max(0.0, min(1.0, composite)), 4)

        return feats

    # ---------------------------------------------------------------
    # Update AFTER snapshot
    # ---------------------------------------------------------------

    def update(
        self,
        position: Optional[int],
        nb_partants: Optional[int],
        is_place: bool,
        is_win: bool,
        cote: Optional[float],
    ) -> None:
        """Append race result to history."""
        self.positions.append(position)
        self.partants.append(nb_partants)
        self.is_placed.append(is_place)
        self.is_gagnant.append(is_win)
        if cote is not None:
            self.odds.append(cote)

        # Update best ever position
        if position is not None and position > 0:
            if self.best_ever_pos is None or position < self.best_ever_pos:
                self.best_ever_pos = position

        # Update last_win_races_ago
        if is_win:
            self.last_win_races_ago = 0
        elif self.last_win_races_ago is not None:
            self.last_win_races_ago += 1


# ===========================================================================
# MAIN BUILD (two-phase: index+sort, then seek-based streaming output)
# ===========================================================================


def build_recent_form_composite(input_path: Path, output_path: Path, logger) -> int:
    """Build recent form composite features.

    Phase 1: read + sort chronologically (index pass).
    Phase 2: iterate course-by-course, snapshot BEFORE update, stream to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Recent Form Composite Builder ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Read minimal fields and sort --
    logger.info("Phase 1: chargement et tri chronologique...")
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Phase 1: lu %d records...", n_read)

        pos = _safe_int(rec.get("position_arrivee"))
        nb_part = _safe_int(rec.get("nombre_partants"))
        cote = _safe_float(rec.get("cote_finale")) or _safe_float(rec.get("rapport_final"))

        slim = {
            "uid": rec.get("partant_uid"),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "horse_id": rec.get("horse_id") or rec.get("nom_cheval"),
            "position": pos,
            "nb_partants": nb_part,
            "is_gagnant": bool(rec.get("is_gagnant")),
            "is_place": bool(rec.get("is_place")),
            "cote": cote,
        }
        slim_records.append(slim)

    logger.info(
        "Phase 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 2: process course-by-course, snapshot before update --
    logger.info("Phase 2: calcul des features par partant...")
    t2 = time.time()

    horse_states: dict[str, _HorseFormState] = defaultdict(_HorseFormState)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    feature_keys = [
        "rfc_form_rating",
        "rfc_momentum_score",
        "rfc_consistency_rating",
        "rfc_place_streak",
        "rfc_last_3_avg_beaten_pct",
        "rfc_improving_form",
        "rfc_best_recent_vs_career",
        "rfc_recent_vs_expected",
        "rfc_form_cycle_position",
        "rfc_composite_form_score",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    n_written = 0
    n_processed = 0
    i = 0
    total = len(slim_records)

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        while i < total:
            course_uid = slim_records[i]["course"]
            course_date = slim_records[i]["date"]
            course_group: list[dict] = []

            while (
                i < total
                and slim_records[i]["course"] == course_uid
                and slim_records[i]["date"] == course_date
            ):
                course_group.append(slim_records[i])
                i += 1

            # Snapshot BEFORE update for every runner in this course
            snapshots: list[tuple[dict, dict[str, Any]]] = []
            for rec in course_group:
                hid = rec["horse_id"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                if hid:
                    state = horse_states[hid]
                    feats = state.snapshot()
                    features.update(feats)
                else:
                    features.update({k: None for k in feature_keys})
                    features["rfc_form_cycle_position"] = 0

                snapshots.append((rec, features))

            # Write features and update state
            for rec, features in snapshots:
                fout.write(json.dumps(features, ensure_ascii=False, default=str) + "\n")
                n_written += 1

                # Count fills
                for k in feature_keys:
                    if features.get(k) is not None:
                        fill_counts[k] += 1

                # Update state post-snapshot
                hid = rec["horse_id"]
                if hid:
                    pos_val = rec["position"]
                    # Determine is_place: explicit field or position <= 3
                    is_place = rec["is_place"]
                    if not is_place and pos_val is not None and pos_val <= 3:
                        is_place = True
                    horse_states[hid].update(
                        position=pos_val,
                        nb_partants=rec["nb_partants"],
                        is_place=is_place,
                        is_win=rec["is_gagnant"],
                        cote=rec["cote"],
                    )

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY == 0:
                logger.info("  Phase 2: traite %d / %d records...", n_processed, total)
                gc.collect()

    # Free slim records
    del slim_records
    gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Recent form composite build termine: %d features en %.1fs (chevaux uniques: %d)",
        n_written, elapsed, len(horse_states),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100 * v / n_written if n_written else 0
        logger.info("  %s: %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _find_input(cli_path: Optional[str]) -> Path:
    """Resolve input file path."""
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Fichier introuvable: {p}")
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Fichier d'entree introuvable: {INPUT_PARTANTS}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features recent form composite a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/recent_form_composite/)",
    )
    args = parser.parse_args()

    logger = setup_logging("recent_form_composite_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "recent_form_composite.jsonl"
    build_recent_form_composite(input_path, out_path, logger)


if __name__ == "__main__":
    main()
