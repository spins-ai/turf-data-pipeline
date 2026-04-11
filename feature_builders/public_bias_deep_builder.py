#!/usr/bin/env python3
"""
feature_builders.public_bias_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep public betting bias features for detecting market inefficiencies.

Identifies systematic biases in public betting patterns: favourite-longshot
bias, overreaction to recent results, smart money signals, and corrects
for age/gains-based distortions.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the global statistics -- no future leakage.  Course-level
features (entropy, compression) are computed from the race's own odds
(observed at race time, not future data).

Architecture:
  Pass 1 -- Stream JSONL, keep slim records (uid, date, course, num,
            cote_finale, cote_reference, is_gagnant, horse_id, age,
            gains_carriere_euros, nb_victoires_carriere, nb_courses_carriere,
            nombre_partants).
  Pass 2 -- Sort chronologically (date, course, num), then process
            course by course with seek-based grouping.
            Snapshot global + per-horse state BEFORE update.
            Compute race-level features (entropy, compression) from the
            course group's own odds.

Produces:
  - public_bias_deep_features.jsonl
    in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/public_bias_deep/

Features per partant (10):
  - pbd_favorite_bias          : overbet ratio = expected_wr(odds) / actual_wr(past)
                                 for this odds bracket
  - pbd_longshot_bias          : same but for longshots (cote > 15)
  - pbd_public_overreaction    : 1 if horse just won and odds dropped >30%
                                 (bandwagon) or just lost and odds rose >30%
                                 (abandonment)
  - pbd_smart_money_signal     : cote_finale / cote_reference -- if <0.9, late
                                 money coming in (smart money)
  - pbd_odds_compression       : 1/cote_finale normalised within the race (how
                                 much of the market this horse has)
  - pbd_field_odds_entropy     : Shannon entropy of implied probabilities in the
                                 race (higher = more open race)
  - pbd_horse_odds_stability   : std deviation of horse's odds over last 5 races
                                 / mean odds
  - pbd_age_bias_correction    : historical win rate for horses of this age vs
                                 implied probability from odds
  - pbd_gains_bias_correction  : historical win rate for horses in this gains
                                 bracket vs implied probability from odds
  - pbd_is_false_favorite      : 1 if horse is race favorite but has <5%
                                 historical win rate (public trap)

Usage:
    python feature_builders/public_bias_deep_builder.py
    python feature_builders/public_bias_deep_builder.py --input path/to/partants_master.jsonl
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/public_bias_deep")
_LOG_EVERY = 500_000

# Odds bracket boundaries
_ODDS_BRACKETS = [
    (0, 3, "strong_fav"),
    (3, 6, "fav"),
    (6, 10, "mid"),
    (10, 15, "outsider"),
    (15, 999, "longshot"),
]

# Gains bracket boundaries (euros)
_GAINS_BRACKETS = [
    (0, 5_000, "low"),
    (5_000, 20_000, "mid_low"),
    (20_000, 50_000, "mid"),
    (50_000, 150_000, "mid_high"),
    (150_000, 999_999_999, "high"),
]

# Minimum observations for reliable statistics
_MIN_OBS = 30


# ===========================================================================
# HELPERS
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


def _sf(val: Any) -> Optional[float]:
    """Safe float conversion (returns None for invalid/non-positive)."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _si(val: Any) -> Optional[int]:
    """Safe int conversion."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _odds_bracket(cote: Optional[float]) -> Optional[str]:
    """Map cote to an odds bracket label."""
    if cote is None or cote <= 0:
        return None
    for lo, hi, label in _ODDS_BRACKETS:
        if lo <= cote < hi:
            return label
    return None


def _gains_bracket(gains: Optional[float]) -> Optional[str]:
    """Map gains_carriere_euros to a gains bracket label."""
    if gains is None or gains < 0:
        return None
    for lo, hi, label in _GAINS_BRACKETS:
        if lo <= gains < hi:
            return label
    return None


# ===========================================================================
# GLOBAL STATE TRACKERS
# ===========================================================================


class _BracketStats:
    """Track wins/total for a given bracket (odds, age, gains)."""

    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def win_rate(self) -> Optional[float]:
        if self.total < _MIN_OBS:
            return None
        return self.wins / self.total


class _HorseState:
    """Per-horse rolling state."""

    __slots__ = ("odds_history", "last_odds", "last_won")

    def __init__(self) -> None:
        self.odds_history: deque = deque(maxlen=5)
        self.last_odds: Optional[float] = None
        self.last_won: Optional[bool] = None


# ===========================================================================
# RACE-LEVEL FEATURES
# ===========================================================================


def _compute_race_level(course_group: list[dict]) -> dict[str, Any]:
    """Compute race-level features from all runners in a course.

    Returns:
        Dict with:
        - compressions: {num_pmu: compression_value}
        - entropy: Shannon entropy of implied probabilities
        - favorite_num: num_pmu of the favorite (lowest cote)
    """
    # Collect valid cotes
    cotes = []
    for rec in course_group:
        cote = rec.get("cote")
        if cote is not None and cote > 0:
            cotes.append((rec["num"], cote))

    result: dict[str, Any] = {
        "compressions": {},
        "entropy": None,
        "favorite_num": None,
    }

    if not cotes:
        return result

    # Implied probabilities
    implied = [(num, 1.0 / c) for num, c in cotes]
    total_implied = sum(ip for _, ip in implied)

    if total_implied <= 0:
        return result

    # Compression: normalised implied probability per horse
    result["compressions"] = {
        num: round(ip / total_implied, 6) for num, ip in implied
    }

    # Shannon entropy of normalised implied probabilities
    entropy = 0.0
    for _, ip in implied:
        p = ip / total_implied
        if p > 0:
            entropy -= p * math.log2(p)
    result["entropy"] = round(entropy, 4)

    # Favorite: lowest cote = highest implied probability
    fav = min(cotes, key=lambda x: x[1])
    result["favorite_num"] = fav[0]

    return result


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build(input_path: Path, logger) -> None:
    t0 = time.time()
    logger.info("=== Public Bias Deep Builder ===")
    logger.info("Input: %s", input_path)

    # -------------------------------------------------------------------
    # Pass 1: Stream JSONL, collect slim records
    # -------------------------------------------------------------------
    logger.info("Pass 1: Chargement et extraction des champs...")
    slim_records: list[dict] = []
    n_read = 0

    for rec in _iter_jsonl(input_path, logger):
        n_read += 1
        if n_read % _LOG_EVERY == 0:
            logger.info("  Lu %d records...", n_read)
            gc.collect()

        cote = _sf(rec.get("cote_finale"))
        cote_ref = _sf(rec.get("cote_reference"))
        gains = _sf(rec.get("gains_carriere_euros"))
        age = _si(rec.get("age"))
        horse_id = rec.get("horse_id") or rec.get("nom_cheval") or ""
        nb_vic = _si(rec.get("nb_victoires_carriere"))
        nb_courses = _si(rec.get("nb_courses_carriere"))

        slim = {
            "uid": rec.get("partant_uid", ""),
            "date": rec.get("date_reunion_iso", ""),
            "course": rec.get("course_uid", ""),
            "num": rec.get("num_pmu", 0) or 0,
            "cote": cote,
            "cote_ref": cote_ref,
            "is_gagnant": bool(rec.get("is_gagnant")),
            "horse_id": horse_id,
            "age": age,
            "gains": gains,
            "nb_vic": nb_vic,
            "nb_courses": nb_courses,
            "nombre_partants": _si(rec.get("nombre_partants")),
        }
        slim_records.append(slim)

    logger.info(
        "Pass 1 terminee: %d records en %.1fs",
        len(slim_records), time.time() - t0,
    )

    # -------------------------------------------------------------------
    # Pass 2: Sort chronologically, process course by course
    # -------------------------------------------------------------------
    t1 = time.time()
    slim_records.sort(key=lambda r: (r["date"], r["course"], r["num"]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # Global state
    odds_bracket_stats: dict[str, _BracketStats] = defaultdict(_BracketStats)
    age_stats: dict[int, _BracketStats] = defaultdict(_BracketStats)
    gains_bracket_stats: dict[str, _BracketStats] = defaultdict(_BracketStats)
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)

    # Output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "public_bias_deep_features.jsonl"
    tmp_out = output_path.with_suffix(".tmp")

    feat_names = [
        "pbd_favorite_bias",
        "pbd_longshot_bias",
        "pbd_public_overreaction",
        "pbd_smart_money_signal",
        "pbd_odds_compression",
        "pbd_field_odds_entropy",
        "pbd_horse_odds_stability",
        "pbd_age_bias_correction",
        "pbd_gains_bias_correction",
        "pbd_is_false_favorite",
    ]
    fill = {k: 0 for k in feat_names}
    n_written = 0

    i = 0
    total = len(slim_records)

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        while i < total:
            course_uid = slim_records[i]["course"]
            course_date = slim_records[i]["date"]
            course_group: list[dict] = []

            # Seek: collect all records for this course
            while (
                i < total
                and slim_records[i]["course"] == course_uid
                and slim_records[i]["date"] == course_date
            ):
                course_group.append(slim_records[i])
                i += 1

            if not course_group:
                continue

            # Compute race-level features from this course's odds
            race_level = _compute_race_level(course_group)
            compressions = race_level["compressions"]
            entropy = race_level["entropy"]
            favorite_num = race_level["favorite_num"]

            # --- Snapshot pre-race features, then defer updates ---
            post_updates: list[dict] = []

            for rec in course_group:
                horse_id = rec["horse_id"]
                cote = rec["cote"]
                cote_ref = rec["cote_ref"]
                num = rec["num"]
                age = rec["age"]
                gains = rec["gains"]
                nb_vic = rec["nb_vic"]
                nb_courses = rec["nb_courses"]

                out: dict[str, Any] = {
                    "partant_uid": rec["uid"],
                    "course_uid": course_uid,
                    "date_reunion_iso": course_date,
                }

                # ---- Feature 1: pbd_favorite_bias ----
                # overbet ratio for this odds bracket
                ob = _odds_bracket(cote)
                if ob is not None:
                    implied_wr = 1.0 / cote if cote and cote > 0 else None
                    actual_wr = odds_bracket_stats[ob].win_rate()
                    if implied_wr is not None and actual_wr is not None and actual_wr > 0:
                        out["pbd_favorite_bias"] = round(implied_wr / actual_wr, 4)
                        fill["pbd_favorite_bias"] += 1
                    else:
                        out["pbd_favorite_bias"] = None
                else:
                    out["pbd_favorite_bias"] = None

                # ---- Feature 2: pbd_longshot_bias ----
                # Same but only for longshots (cote > 15)
                if cote is not None and cote > 15:
                    longshot_bracket = "longshot"
                    implied_wr_ls = 1.0 / cote
                    actual_wr_ls = odds_bracket_stats[longshot_bracket].win_rate()
                    if actual_wr_ls is not None and actual_wr_ls > 0:
                        out["pbd_longshot_bias"] = round(implied_wr_ls / actual_wr_ls, 4)
                        fill["pbd_longshot_bias"] += 1
                    else:
                        out["pbd_longshot_bias"] = None
                else:
                    out["pbd_longshot_bias"] = None

                # ---- Feature 3: pbd_public_overreaction ----
                hs = horse_state.get(horse_id) if horse_id else None
                if hs is not None and hs.last_odds is not None and cote is not None and hs.last_won is not None:
                    odds_change_pct = (cote - hs.last_odds) / hs.last_odds if hs.last_odds > 0 else 0
                    if hs.last_won and odds_change_pct < -0.30:
                        # Just won and odds dropped >30% = bandwagon
                        out["pbd_public_overreaction"] = 1
                        fill["pbd_public_overreaction"] += 1
                    elif not hs.last_won and odds_change_pct > 0.30:
                        # Just lost and odds rose >30% = abandonment
                        out["pbd_public_overreaction"] = 1
                        fill["pbd_public_overreaction"] += 1
                    else:
                        out["pbd_public_overreaction"] = 0
                        fill["pbd_public_overreaction"] += 1
                else:
                    out["pbd_public_overreaction"] = None

                # ---- Feature 4: pbd_smart_money_signal ----
                if cote is not None and cote_ref is not None and cote_ref > 0:
                    out["pbd_smart_money_signal"] = round(cote / cote_ref, 4)
                    fill["pbd_smart_money_signal"] += 1
                else:
                    out["pbd_smart_money_signal"] = None

                # ---- Feature 5: pbd_odds_compression ----
                compression = compressions.get(num)
                if compression is not None:
                    out["pbd_odds_compression"] = compression
                    fill["pbd_odds_compression"] += 1
                else:
                    out["pbd_odds_compression"] = None

                # ---- Feature 6: pbd_field_odds_entropy ----
                out["pbd_field_odds_entropy"] = entropy
                if entropy is not None:
                    fill["pbd_field_odds_entropy"] += 1

                # ---- Feature 7: pbd_horse_odds_stability ----
                if hs is not None and len(hs.odds_history) >= 2:
                    odds_list = list(hs.odds_history)
                    mean_odds = sum(odds_list) / len(odds_list)
                    if mean_odds > 0:
                        variance = sum((o - mean_odds) ** 2 for o in odds_list) / len(odds_list)
                        std_odds = math.sqrt(variance)
                        out["pbd_horse_odds_stability"] = round(std_odds / mean_odds, 4)
                        fill["pbd_horse_odds_stability"] += 1
                    else:
                        out["pbd_horse_odds_stability"] = None
                else:
                    out["pbd_horse_odds_stability"] = None

                # ---- Feature 8: pbd_age_bias_correction ----
                if age is not None and cote is not None and cote > 0:
                    age_wr = age_stats[age].win_rate()
                    implied_prob = 1.0 / cote
                    if age_wr is not None:
                        out["pbd_age_bias_correction"] = round(age_wr - implied_prob, 4)
                        fill["pbd_age_bias_correction"] += 1
                    else:
                        out["pbd_age_bias_correction"] = None
                else:
                    out["pbd_age_bias_correction"] = None

                # ---- Feature 9: pbd_gains_bias_correction ----
                gb = _gains_bracket(gains)
                if gb is not None and cote is not None and cote > 0:
                    gains_wr = gains_bracket_stats[gb].win_rate()
                    implied_prob = 1.0 / cote
                    if gains_wr is not None:
                        out["pbd_gains_bias_correction"] = round(gains_wr - implied_prob, 4)
                        fill["pbd_gains_bias_correction"] += 1
                    else:
                        out["pbd_gains_bias_correction"] = None
                else:
                    out["pbd_gains_bias_correction"] = None

                # ---- Feature 10: pbd_is_false_favorite ----
                if favorite_num is not None and num == favorite_num and horse_id:
                    hs_check = horse_state.get(horse_id)
                    if hs_check is not None and len(hs_check.odds_history) >= 5:
                        # Use historical career win rate
                        if nb_courses is not None and nb_courses > 0:
                            career_wr = (nb_vic or 0) / nb_courses
                            out["pbd_is_false_favorite"] = 1 if career_wr < 0.05 else 0
                            fill["pbd_is_false_favorite"] += 1
                        else:
                            out["pbd_is_false_favorite"] = None
                    else:
                        out["pbd_is_false_favorite"] = None
                else:
                    out["pbd_is_false_favorite"] = 0 if favorite_num is not None else None
                    if out["pbd_is_false_favorite"] is not None:
                        fill["pbd_is_false_favorite"] += 1

                fout.write(json.dumps(out, ensure_ascii=False) + "\n")
                n_written += 1

                # Collect deferred update data
                post_updates.append({
                    "horse_id": horse_id,
                    "cote": cote,
                    "is_gagnant": rec["is_gagnant"],
                    "age": age,
                    "gains_bracket": gb,
                    "odds_bracket": ob,
                })

            # --- Update state post-race (no leakage) ---
            for upd in post_updates:
                h = upd["horse_id"]
                cote_u = upd["cote"]
                is_w = upd["is_gagnant"]

                # Update per-horse state
                if h:
                    hs_upd = horse_state[h]
                    if cote_u is not None:
                        hs_upd.odds_history.append(cote_u)
                    hs_upd.last_odds = cote_u
                    hs_upd.last_won = is_w

                # Update odds bracket stats
                ob_u = upd["odds_bracket"]
                if ob_u is not None:
                    odds_bracket_stats[ob_u].total += 1
                    if is_w:
                        odds_bracket_stats[ob_u].wins += 1

                # Update age stats
                age_u = upd["age"]
                if age_u is not None:
                    age_stats[age_u].total += 1
                    if is_w:
                        age_stats[age_u].wins += 1

                # Update gains bracket stats
                gb_u = upd["gains_bracket"]
                if gb_u is not None:
                    gains_bracket_stats[gb_u].total += 1
                    if is_w:
                        gains_bracket_stats[gb_u].wins += 1

            if n_written % _LOG_EVERY == 0:
                logger.info("  Traite %d / %d records...", n_written, total)
                gc.collect()

    # Rename tmp to final
    if tmp_out.exists():
        if output_path.exists():
            output_path.unlink()
        tmp_out.rename(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Termine: %d records en %.1fs (chevaux: %d, brackets odds: %d, ages: %d)",
        n_written, elapsed, len(horse_state),
        len(odds_bracket_stats), len(age_stats),
    )
    logger.info("=== Fill rates ===")
    for k, v in fill.items():
        pct = v / n_written * 100 if n_written > 0 else 0
        logger.info("  %-35s: %7d / %d (%.1f%%)", k, v, n_written, pct)


# ===========================================================================
# CLI
# ===========================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Public bias deep features a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: D:/turf-data-pipeline/...)",
    )
    args = parser.parse_args()

    logger = setup_logging("public_bias_deep_builder")
    logger.info("=" * 70)
    logger.info("public_bias_deep_builder.py")
    logger.info("=" * 70)

    input_path = INPUT_PARTANTS
    if args.input:
        p = Path(args.input)
        if p.exists():
            input_path = p
        else:
            logger.error("Fichier introuvable: %s", p)
            sys.exit(1)

    build(input_path, logger)


if __name__ == "__main__":
    main()
