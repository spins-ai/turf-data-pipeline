#!/usr/bin/env python3
"""
feature_builders.field_similarity_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Field similarity features: how similar is the current race field to fields
where this horse has previously performed well or poorly.

TWO-PASS builder:
  Pass 1 – Group records by course_uid to get full field composition.
  Pass 2 – Stream through chronologically, compute features using
            historical horse state (snapshot-before-update).

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Memory-optimised:
  - Phase 1 reads only sort-keys + byte offsets (not full dicts)
  - Phase 2 streams output to disk instead of accumulating in a list
  - defaultdict with deque(maxlen=50) for bounded state
  - gc.collect() every 500K records

Produces:
  - field_similarity.jsonl   in builder_outputs/field_similarity/

Features per partant (11):
  - fs_field_size_sim         : similarity of current field size to horse's best-perf sizes
  - fs_avg_odds_field         : mean cote_finale of all runners (field quality proxy)
  - fs_odds_spread_field      : std dev of cote_finale (field competitiveness)
  - fs_winrate_size_bucket    : horse's historical win rate in similar-sized fields
  - fs_winrate_quality_bucket : horse's historical perf in similar-quality fields
  - fs_nb_prev_opponents      : count of runners horse has raced against before
  - fs_record_vs_field        : win% in past races with overlapping runners
  - fs_fav_strength           : lowest cote_finale in field (strong fav = harder to win)
  - fs_odds_rank_norm         : horse's rank by odds within field (0=favorite, 1=outsider)
  - fs_field_concentration    : sum of top-3 implied probabilities (Herfindahl-like)
  - fs_pref_concentrated      : horse performs better in concentrated vs open fields

Usage:
    python feature_builders/field_similarity_builder.py
    python feature_builders/field_similarity_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
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
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_similarity")

_LOG_EVERY = 500_000

# Field size buckets: 4-8, 9-12, 13-16, 17+
_SIZE_BUCKETS = [(4, 8), (9, 12), (13, 16), (17, 999)]
# Avg odds quality buckets: low (<5), medium (5-15), high (15-30), very_high (30+)
_QUALITY_BUCKETS = [(0, 5), (5, 15), (15, 30), (30, 9999)]

# Bounded history per horse
_MAX_HISTORY = 50


# ===========================================================================
# HELPERS
# ===========================================================================


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _get_size_bucket(n: int) -> int:
    """Return bucket index (0..3) for field size n."""
    for i, (lo, hi) in enumerate(_SIZE_BUCKETS):
        if lo <= n <= hi:
            return i
    return 3  # 17+


def _get_quality_bucket(avg_odds: float) -> int:
    """Return bucket index (0..3) for avg odds."""
    for i, (lo, hi) in enumerate(_QUALITY_BUCKETS):
        if lo <= avg_odds < hi:
            return i
    return 3


def _safe_stdev(values: list[float]) -> Optional[float]:
    """Standard deviation, returns None if <2 values."""
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return round(variance ** 0.5, 4)


# ===========================================================================
# HORSE STATE TRACKER
# ===========================================================================


class _HorseFieldState:
    """Per-horse state tracking field-related performance history.

    Uses bounded deques (maxlen=50) to cap memory.
    """

    __slots__ = (
        "field_sizes_when_win",
        "field_sizes_when_place",
        "size_bucket_wins",
        "size_bucket_total",
        "quality_bucket_wins",
        "quality_bucket_total",
        "opponents_seen",
        "opponents_win_count",
        "opponents_race_count",
        "conc_wins",
        "conc_total",
        "open_wins",
        "open_total",
    )

    def __init__(self) -> None:
        # Field sizes where horse won (last N)
        self.field_sizes_when_win: deque = deque(maxlen=_MAX_HISTORY)
        self.field_sizes_when_place: deque = deque(maxlen=_MAX_HISTORY)

        # Win rate per size bucket: [wins, total] x 4 buckets
        self.size_bucket_wins = [0, 0, 0, 0]
        self.size_bucket_total = [0, 0, 0, 0]

        # Win rate per quality bucket: [wins, total] x 4 buckets
        self.quality_bucket_wins = [0, 0, 0, 0]
        self.quality_bucket_total = [0, 0, 0, 0]

        # Opponents tracking: set of horse names encountered
        self.opponents_seen: set = set()
        # Per-opponent: how many times horse finished ahead vs total encounters
        # opponent_name -> [wins_against, total_encounters]
        self.opponents_win_count: dict[str, int] = defaultdict(int)
        self.opponents_race_count: dict[str, int] = defaultdict(int)

        # Concentrated vs open field performance
        # Concentrated = top-3 proba sum > 0.45
        self.conc_wins = 0
        self.conc_total = 0
        self.open_wins = 0
        self.open_total = 0


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


def build_field_similarity_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build field similarity features from partants_master.jsonl.

    Two-pass approach:
      Pass 1 – Read sort-keys + byte offsets, sort chronologically.
      Pass 2 – Process course by course (snapshot-before-update),
               stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Field Similarity Builder (two-pass, memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -----------------------------------------------------------------------
    # Pass 1: Build lightweight index (date, course_uid, num_pmu, offset)
    # -----------------------------------------------------------------------
    index: list[tuple[str, str, int, int]] = []
    n_read = 0

    with open(input_path, "r", encoding="utf-8") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexe %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = _safe_int(rec.get("num_pmu")) or 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Pass 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # Sort chronologically
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -----------------------------------------------------------------------
    # Pass 2: Process course by course, snapshot-before-update
    # -----------------------------------------------------------------------
    t2 = time.time()
    horse_state: dict[str, _HorseFieldState] = defaultdict(_HorseFieldState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    # Feature names for fill rate tracking
    _FEATURE_KEYS = [
        "fs_field_size_sim",
        "fs_avg_odds_field",
        "fs_odds_spread_field",
        "fs_winrate_size_bucket",
        "fs_winrate_quality_bucket",
        "fs_nb_prev_opponents",
        "fs_record_vs_field",
        "fs_fav_strength",
        "fs_odds_rank_norm",
        "fs_field_concentration",
        "fs_pref_concentrated",
    ]
    fill_counts = {k: 0 for k in _FEATURE_KEYS}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(off: int) -> dict:
            fin.seek(off)
            return json.loads(fin.readline())

        def _extract_slim(rec: dict) -> dict:
            """Extract minimal fields from a full record."""
            nb_partants = _safe_int(rec.get("nombre_partants")) or 0
            cote = _safe_float(rec.get("cote_finale"))
            proba = _safe_float(rec.get("proba_implicite"))
            pos = _safe_int(rec.get("position_arrivee"))

            return {
                "uid": rec.get("partant_uid"),
                "date": rec.get("date_reunion_iso", ""),
                "course": rec.get("course_uid", ""),
                "cheval": rec.get("nom_cheval"),
                "gagnant": bool(rec.get("is_gagnant")),
                "position": pos,
                "nb_partants": nb_partants,
                "cote_finale": cote,
                "proba_implicite": proba,
                "discipline": (rec.get("discipline") or "").strip().upper(),
                "distance": _safe_int(rec.get("distance")),
                "type_piste": (rec.get("type_piste") or "").strip().upper(),
                "hippodrome": (rec.get("hippodrome_normalise") or "").strip().upper(),
            }

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date_str = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date_str
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read this course's records from disk
            course_group = [
                _extract_slim(_read_record_at(index[ci][3]))
                for ci in course_indices
            ]

            # ------ Compute course-level field metrics ------
            field_size = len(course_group)
            cotes = [r["cote_finale"] for r in course_group if r["cote_finale"] is not None and r["cote_finale"] > 0]
            probas = [r["proba_implicite"] for r in course_group if r["proba_implicite"] is not None and r["proba_implicite"] > 0]
            field_horses = [r["cheval"] for r in course_group if r["cheval"]]

            avg_odds_field = round(sum(cotes) / len(cotes), 4) if cotes else None
            odds_spread = _safe_stdev(cotes)
            fav_strength = round(min(cotes), 2) if cotes else None

            # Top-3 implied proba sum (concentration)
            field_concentration: Optional[float] = None
            if probas:
                sorted_probas = sorted(probas, reverse=True)
                field_concentration = round(sum(sorted_probas[:3]), 4)

            # Is this a concentrated field?
            is_concentrated = field_concentration is not None and field_concentration > 0.45

            # Rank runners by odds (ascending = lowest odds = favorite = rank 1)
            # Build odds_rank mapping: cheval -> rank (1-based)
            odds_rank_map: dict[str, int] = {}
            if cotes and len(cotes) >= 2:
                runners_with_odds = [
                    (r["cheval"], r["cote_finale"])
                    for r in course_group
                    if r["cheval"] and r["cote_finale"] is not None and r["cote_finale"] > 0
                ]
                runners_with_odds.sort(key=lambda x: x[1])
                for rank_idx, (name, _) in enumerate(runners_with_odds):
                    odds_rank_map[name] = rank_idx + 1
            n_with_odds = len(odds_rank_map)

            # Quality bucket for this field
            quality_bucket = _get_quality_bucket(avg_odds_field) if avg_odds_field is not None else None
            size_bucket = _get_size_bucket(field_size)

            # ------ Snapshot pre-race features for each partant ------
            for rec in course_group:
                cheval = rec["cheval"]
                features: dict[str, Any] = {"partant_uid": rec["uid"]}

                # Default all features to None
                for k in _FEATURE_KEYS:
                    features[k] = None

                # --- Course-level features (always available if data exists) ---
                features["fs_avg_odds_field"] = avg_odds_field
                if avg_odds_field is not None:
                    fill_counts["fs_avg_odds_field"] += 1

                features["fs_odds_spread_field"] = odds_spread
                if odds_spread is not None:
                    fill_counts["fs_odds_spread_field"] += 1

                features["fs_fav_strength"] = fav_strength
                if fav_strength is not None:
                    fill_counts["fs_fav_strength"] += 1

                features["fs_field_concentration"] = field_concentration
                if field_concentration is not None:
                    fill_counts["fs_field_concentration"] += 1

                # Odds rank normalized to 0-1
                if cheval and cheval in odds_rank_map and n_with_odds > 1:
                    rank = odds_rank_map[cheval]
                    features["fs_odds_rank_norm"] = round(
                        (rank - 1) / (n_with_odds - 1), 4
                    )
                    fill_counts["fs_odds_rank_norm"] += 1

                # --- Horse-historical features (snapshot before update) ---
                if cheval:
                    hs = horse_state[cheval]

                    # 1. Field size similarity: how close is current field size
                    #    to sizes where horse has won
                    if hs.field_sizes_when_win:
                        win_sizes = list(hs.field_sizes_when_win)
                        avg_win_size = sum(win_sizes) / len(win_sizes)
                        # Similarity: 1 - normalized distance
                        if avg_win_size > 0:
                            diff = abs(field_size - avg_win_size) / avg_win_size
                            features["fs_field_size_sim"] = round(
                                max(0.0, 1.0 - diff), 4
                            )
                            fill_counts["fs_field_size_sim"] += 1

                    # 4. Win rate in similar-sized fields
                    sb = size_bucket
                    if hs.size_bucket_total[sb] > 0:
                        features["fs_winrate_size_bucket"] = round(
                            hs.size_bucket_wins[sb] / hs.size_bucket_total[sb], 4
                        )
                        fill_counts["fs_winrate_size_bucket"] += 1

                    # 5. Win rate in similar-quality fields
                    if quality_bucket is not None and hs.quality_bucket_total[quality_bucket] > 0:
                        features["fs_winrate_quality_bucket"] = round(
                            hs.quality_bucket_wins[quality_bucket]
                            / hs.quality_bucket_total[quality_bucket],
                            4,
                        )
                        fill_counts["fs_winrate_quality_bucket"] += 1

                    # 6. Number of previous encounters with same opponents
                    current_opponents = set(
                        h for h in field_horses if h != cheval
                    )
                    prev_opponents = current_opponents & hs.opponents_seen
                    nb_prev = len(prev_opponents)
                    if nb_prev > 0:
                        features["fs_nb_prev_opponents"] = nb_prev
                        fill_counts["fs_nb_prev_opponents"] += 1

                        # 7. Win record against current field
                        total_enc = 0
                        total_wins = 0
                        for opp in prev_opponents:
                            enc = hs.opponents_race_count.get(opp, 0)
                            wins = hs.opponents_win_count.get(opp, 0)
                            total_enc += enc
                            total_wins += wins
                        if total_enc > 0:
                            features["fs_record_vs_field"] = round(
                                total_wins / total_enc, 4
                            )
                            fill_counts["fs_record_vs_field"] += 1

                    # 11. Preference for concentrated vs open fields
                    total_type = hs.conc_total + hs.open_total
                    if total_type >= 3:
                        conc_wr = (
                            hs.conc_wins / hs.conc_total
                            if hs.conc_total > 0
                            else 0.0
                        )
                        open_wr = (
                            hs.open_wins / hs.open_total
                            if hs.open_total > 0
                            else 0.0
                        )
                        # Positive = prefers concentrated, negative = prefers open
                        features["fs_pref_concentrated"] = round(
                            conc_wr - open_wr, 4
                        )
                        fill_counts["fs_pref_concentrated"] += 1

                # Write to output
                fout.write(
                    json.dumps(features, ensure_ascii=False, default=str) + "\n"
                )
                n_written += 1

            # ------ Update horse states after snapshotting (post-race) ------
            for rec in course_group:
                cheval = rec["cheval"]
                if not cheval:
                    continue

                hs = horse_state[cheval]
                is_winner = rec["gagnant"]
                position = rec["position"]

                # Update field size history
                if is_winner:
                    hs.field_sizes_when_win.append(field_size)
                if position is not None and 1 <= position <= 3:
                    hs.field_sizes_when_place.append(field_size)

                # Update size bucket stats
                sb = size_bucket
                hs.size_bucket_total[sb] += 1
                if is_winner:
                    hs.size_bucket_wins[sb] += 1

                # Update quality bucket stats
                if quality_bucket is not None:
                    hs.quality_bucket_total[quality_bucket] += 1
                    if is_winner:
                        hs.quality_bucket_wins[quality_bucket] += 1

                # Update opponents tracking
                current_opponents = set(h for h in field_horses if h != cheval)
                hs.opponents_seen.update(current_opponents)

                # Update per-opponent win/loss records
                if position is not None and position > 0:
                    for other_rec in course_group:
                        opp = other_rec["cheval"]
                        if opp is None or opp == cheval:
                            continue
                        opp_pos = other_rec["position"]
                        if opp_pos is None or opp_pos <= 0:
                            continue
                        hs.opponents_race_count[opp] += 1
                        if position < opp_pos:
                            hs.opponents_win_count[opp] += 1

                # Update concentrated vs open field performance
                if field_concentration is not None:
                    if is_concentrated:
                        hs.conc_total += 1
                        if is_winner:
                            hs.conc_wins += 1
                    else:
                        hs.open_total += 1
                        if is_winner:
                            hs.open_wins += 1

            n_processed += len(course_group)
            if n_processed % _LOG_EVERY < len(course_group):
                logger.info(
                    "  Traite %d / %d records (chevaux suivis: %d)...",
                    n_processed, total, len(horse_state),
                )
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Field similarity build termine: %d features en %.1fs (chevaux: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k in _FEATURE_KEYS:
        v = fill_counts[k]
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
    raise FileNotFoundError(f"Fichier introuvable: {INPUT_PARTANTS}")


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features de similarite de champ (field similarity)"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: %s)" % INPUT_PARTANTS,
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: %s)" % OUTPUT_DIR,
    )
    args = parser.parse_args()

    logger = setup_logging("field_similarity_builder")
    logger.info("=" * 70)
    logger.info("field_similarity_builder.py — Features de similarite de champ")
    logger.info("=" * 70)

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "field_similarity.jsonl"
    build_field_similarity_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
