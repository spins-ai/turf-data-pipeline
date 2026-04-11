#!/usr/bin/env python3
"""
feature_builders.relative_strength_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Relative strength index (RSI-like) features inspired by financial technical
analysis, applied to horse racing form.

Technical analysis indicators adapted from stock trading to horse racing form
analysis.  They capture momentum, mean reversion, and trend signals that
tree-based and NN models can exploit.

Temporal integrity: for any partant at date D, only races with date < D
contribute to indicators -- no future leakage.

Memory-optimised approach:
  1. Read only sort keys + file byte offsets into memory (not full records).
  2. Sort the lightweight index chronologically.
  3. Re-read records from disk using offsets, process course by course,
     and stream output directly to disk.

Produces:
  - relative_strength_features.jsonl  in builder_outputs/relative_strength/

Features per partant (10):
  - rsi_position_rsi_5        : RSI on finishing positions over last 5 races
  - rsi_position_rsi_10       : RSI on finishing positions over last 10 races
  - rsi_speed_rsi_5           : RSI on speed figures over last 5 races
  - rsi_cote_momentum         : rate of change of odds over last 3 races
  - rsi_exponential_form      : exponentially weighted average of last 10 positions
  - rsi_macd_form             : MACD -- avg_3 - avg_10 positions (cross signal)
  - rsi_bollinger_position    : current avg_5 position vs 2-std band of career avg
  - rsi_stochastic_form       : stochastic oscillator on last 5 positions
  - rsi_williams_r            : Williams %R on last 10 positions
  - rsi_accumulation_score    : accumulation/distribution line over last 10 positions

Usage:
    python feature_builders/relative_strength_builder.py
    python feature_builders/relative_strength_builder.py --input path/to/partants_master.jsonl
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_CANDIDATES = [
    Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl"),
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/relative_strength")

_LOG_EVERY = 500_000
_DEQUE_MAX = 15
_EXP_DECAY = 0.9


# ===========================================================================
# PER-HORSE STATE (memory-efficient with __slots__)
# ===========================================================================


class _HorseState:
    """Track rolling technical analysis state for one horse."""

    __slots__ = (
        "positions",          # deque(maxlen=15): normalized positions (pos/nb_partants)
        "speeds",             # deque(maxlen=15): speed_figures
        "cotes",              # deque(maxlen=15): log(cote)
        "career_pos_sum",     # running sum of all career positions
        "career_pos_sq_sum",  # running sum of squares for std calculation
        "career_pos_count",   # total number of career positions
    )

    def __init__(self) -> None:
        self.positions: deque = deque(maxlen=_DEQUE_MAX)
        self.speeds: deque = deque(maxlen=_DEQUE_MAX)
        self.cotes: deque = deque(maxlen=_DEQUE_MAX)
        self.career_pos_sum: float = 0.0
        self.career_pos_sq_sum: float = 0.0
        self.career_pos_count: int = 0

    def career_mean(self) -> Optional[float]:
        if self.career_pos_count < 1:
            return None
        return self.career_pos_sum / self.career_pos_count

    def career_std(self) -> Optional[float]:
        n = self.career_pos_count
        if n < 2:
            return None
        mean = self.career_pos_sum / n
        var = self.career_pos_sq_sum / n - mean * mean
        if var < 0:
            var = 0.0
        return math.sqrt(var)


# ===========================================================================
# RSI COMPUTATION HELPERS
# ===========================================================================


def _compute_rsi(values: list[float], window: int) -> Optional[float]:
    """Compute RSI-like indicator over the last `window` consecutive changes.

    RSI = 100 - 100 / (1 + avg_gains / avg_losses)
    For positions: a "gain" = position improved (value decreased),
    a "loss" = position worsened (value increased).
    We invert so that improvements are gains.
    """
    if len(values) < window + 1:
        return None

    recent = values[-(window + 1):]
    gains = 0.0
    losses = 0.0
    n_gains = 0
    n_losses = 0

    for j in range(1, len(recent)):
        delta = recent[j - 1] - recent[j]  # positive = improvement (pos decreased)
        if delta > 0:
            gains += delta
            n_gains += 1
        elif delta < 0:
            losses += abs(delta)
            n_losses += 1

    avg_gains = gains / window
    avg_losses = losses / window

    if avg_losses == 0:
        return 100.0 if avg_gains > 0 else 50.0
    rs = avg_gains / avg_losses
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return round(rsi, 2)


def _compute_speed_rsi(values: list[float], window: int) -> Optional[float]:
    """RSI on speed figures. Higher speed = gain (not inverted)."""
    if len(values) < window + 1:
        return None

    recent = values[-(window + 1):]
    gains = 0.0
    losses = 0.0

    for j in range(1, len(recent)):
        delta = recent[j] - recent[j - 1]  # positive = speed improved
        if delta > 0:
            gains += delta
        elif delta < 0:
            losses += abs(delta)

    avg_gains = gains / window
    avg_losses = losses / window

    if avg_losses == 0:
        return 100.0 if avg_gains > 0 else 50.0
    rs = avg_gains / avg_losses
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return round(rsi, 2)


# ===========================================================================
# FEATURE COMPUTATION (from snapshot before update)
# ===========================================================================


def _compute_features(hs: _HorseState) -> dict[str, Any]:
    """Compute all 10 RSI-like features from the horse's pre-race state."""
    feats: dict[str, Any] = {}

    pos_list = [v for v in hs.positions if v is not None]
    speed_list = [v for v in hs.speeds if v is not None]
    cote_list = [v for v in hs.cotes if v is not None]

    # 1. rsi_position_rsi_5: RSI on positions over last 5 races
    feats["rsi_position_rsi_5"] = _compute_rsi(pos_list, 5)

    # 2. rsi_position_rsi_10: RSI on positions over last 10 races
    feats["rsi_position_rsi_10"] = _compute_rsi(pos_list, 10)

    # 3. rsi_speed_rsi_5: RSI on speed figures over last 5 races
    feats["rsi_speed_rsi_5"] = _compute_speed_rsi(speed_list, 5)

    # 4. rsi_cote_momentum: rate of change of log(odds) over last 3 races
    if len(cote_list) >= 3:
        recent_3 = cote_list[-3:]
        # Rate of change: (latest - oldest) / oldest
        if recent_3[0] != 0:
            feats["rsi_cote_momentum"] = round(
                (recent_3[-1] - recent_3[0]) / abs(recent_3[0]), 4
            )
        else:
            feats["rsi_cote_momentum"] = None
    else:
        feats["rsi_cote_momentum"] = None

    # 5. rsi_exponential_form: exponentially weighted avg of last 10 positions
    if len(pos_list) >= 2:
        recent_10 = pos_list[-10:]
        n = len(recent_10)
        weight_sum = 0.0
        val_sum = 0.0
        for idx in range(n):
            # Most recent gets highest weight
            w = _EXP_DECAY ** (n - 1 - idx)
            val_sum += w * recent_10[idx]
            weight_sum += w
        feats["rsi_exponential_form"] = round(val_sum / weight_sum, 4) if weight_sum > 0 else None
    else:
        feats["rsi_exponential_form"] = None

    # 6. rsi_macd_form: avg_3 - avg_10 positions (MACD cross signal)
    if len(pos_list) >= 3:
        avg_3 = sum(pos_list[-3:]) / 3.0
        avg_10_vals = pos_list[-10:]
        avg_10 = sum(avg_10_vals) / len(avg_10_vals)
        feats["rsi_macd_form"] = round(avg_3 - avg_10, 4)
    else:
        feats["rsi_macd_form"] = None

    # 7. rsi_bollinger_position: is current avg_5 outside 2-std band of career avg?
    #    Returns z-score: (avg_5 - career_mean) / career_std
    if len(pos_list) >= 5 and hs.career_pos_count >= 10:
        avg_5 = sum(pos_list[-5:]) / 5.0
        c_mean = hs.career_mean()
        c_std = hs.career_std()
        if c_mean is not None and c_std is not None and c_std > 0:
            feats["rsi_bollinger_position"] = round((avg_5 - c_mean) / c_std, 4)
        else:
            feats["rsi_bollinger_position"] = None
    else:
        feats["rsi_bollinger_position"] = None

    # 8. rsi_stochastic_form: (current - worst_5) / (best_5 - worst_5) * 100
    #    For positions: best = lowest, worst = highest
    if len(pos_list) >= 5:
        recent_5 = pos_list[-5:]
        best_5 = min(recent_5)
        worst_5 = max(recent_5)
        current = recent_5[-1]
        denom = worst_5 - best_5
        if denom > 0:
            # Invert so that low position (good) gives high stochastic
            feats["rsi_stochastic_form"] = round(
                (worst_5 - current) / denom * 100.0, 2
            )
        else:
            feats["rsi_stochastic_form"] = 50.0  # all same position
    else:
        feats["rsi_stochastic_form"] = None

    # 9. rsi_williams_r: (best_10 - current_avg_3) / (best_10 - worst_10) * -100
    if len(pos_list) >= 3:
        recent_10 = pos_list[-10:]
        best_10 = min(recent_10)
        worst_10 = max(recent_10)
        avg_3 = sum(pos_list[-3:]) / 3.0
        denom = best_10 - worst_10
        if denom != 0:
            feats["rsi_williams_r"] = round(
                (best_10 - avg_3) / denom * -100.0, 2
            )
        else:
            feats["rsi_williams_r"] = 0.0  # all same
    else:
        feats["rsi_williams_r"] = None

    # 10. rsi_accumulation_score: sum of (+1 if improved, -1 if worsened) over last 10
    if len(pos_list) >= 2:
        recent_10 = pos_list[-10:]
        score = 0
        for j in range(1, len(recent_10)):
            if recent_10[j] < recent_10[j - 1]:
                score += 1   # improved (lower position)
            elif recent_10[j] > recent_10[j - 1]:
                score -= 1   # worsened
        feats["rsi_accumulation_score"] = score
    else:
        feats["rsi_accumulation_score"] = None

    return feats


# ===========================================================================
# UPDATE HORSE STATE (post-race)
# ===========================================================================


def _update_state(
    hs: _HorseState,
    norm_position: Optional[float],
    speed_figure: Optional[float],
    log_cote: Optional[float],
) -> None:
    """Update the horse's rolling state after a race."""
    hs.positions.append(norm_position)
    hs.speeds.append(speed_figure)
    hs.cotes.append(log_cote)

    if norm_position is not None:
        hs.career_pos_sum += norm_position
        hs.career_pos_sq_sum += norm_position * norm_position
        hs.career_pos_count += 1


# ===========================================================================
# MAIN BUILD (memory-optimised: index + sort + seek)
# ===========================================================================


def build_relative_strength_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build RSI-like technical analysis features from partants_master.jsonl.

    Memory-optimised approach:
      1. Read only sort keys + file byte offsets into memory.
      2. Sort the lightweight index chronologically.
      3. Re-read records from disk using offsets, process course by course,
         stream output directly to disk.

    Returns the total number of feature records written.
    """
    logger.info("=== Relative Strength Builder (memory-optimised) ===")
    logger.info("Lecture en streaming: %s", input_path)
    t0 = time.time()

    # -- Phase 1: Build lightweight index --
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
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 terminee: %d records indexes en %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Tri chronologique en %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()
    horse_state: dict[str, _HorseState] = defaultdict(_HorseState)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_processed = 0
    n_written = 0
    total = len(index)

    feature_keys = [
        "rsi_position_rsi_5",
        "rsi_position_rsi_10",
        "rsi_speed_rsi_5",
        "rsi_cote_momentum",
        "rsi_exponential_form",
        "rsi_macd_form",
        "rsi_bollinger_position",
        "rsi_stochastic_form",
        "rsi_williams_r",
        "rsi_accumulation_score",
    ]
    fill_counts = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_record_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

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
            course_records = [_read_record_at(index[ci][3]) for ci in course_indices]

            # Count runners in this race for normalization
            nb_partants = len(course_records)

            # -- Snapshot pre-race state and emit features --
            post_updates: list[tuple] = []

            for rec in course_records:
                cheval = rec.get("nom_cheval")
                partant_uid = rec.get("partant_uid")
                course_uid_val = rec.get("course_uid")
                date_val = rec.get("date_reunion_iso")

                if not cheval:
                    # Emit record with Nones
                    out_rec = {
                        "partant_uid": partant_uid,
                        "course_uid": course_uid_val,
                        "date_reunion_iso": date_val,
                    }
                    for k in feature_keys:
                        out_rec[k] = None
                    fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                    n_written += 1
                    continue

                hs = horse_state[cheval]

                # Compute features from pre-race state (BEFORE update)
                feats = _compute_features(hs)

                out_rec = {
                    "partant_uid": partant_uid,
                    "course_uid": course_uid_val,
                    "date_reunion_iso": date_val,
                }
                for k in feature_keys:
                    v = feats.get(k)
                    out_rec[k] = v
                    if v is not None:
                        fill_counts[k] += 1

                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                n_written += 1

                # -- Prepare deferred state update --
                # Normalized position: pos / nb_partants
                norm_pos = None
                pos_raw = rec.get("place_arrivee") or rec.get("position_arrivee")
                if pos_raw is not None:
                    try:
                        pos_int = int(pos_raw)
                        if pos_int > 0 and nb_partants > 0:
                            norm_pos = round(pos_int / nb_partants, 4)
                    except (ValueError, TypeError):
                        pass

                speed_fig = None
                sf_raw = rec.get("speed_figure") or rec.get("vitesse_moyenne")
                if sf_raw is not None:
                    try:
                        speed_fig = float(sf_raw)
                    except (ValueError, TypeError):
                        pass

                log_cote = None
                cote_raw = rec.get("cote_finale") or rec.get("rapport_final")
                if cote_raw is not None:
                    try:
                        cote_val = float(cote_raw)
                        if cote_val > 0:
                            log_cote = round(math.log(cote_val), 4)
                    except (ValueError, TypeError):
                        pass

                post_updates.append((cheval, norm_pos, speed_fig, log_cote))

            # -- Update horse states after race (no leakage) --
            for cheval, norm_pos, speed_fig, log_cote in post_updates:
                _update_state(horse_state[cheval], norm_pos, speed_fig, log_cote)

            n_processed += len(course_records)
            if n_processed % _LOG_EVERY < len(course_records):
                logger.info("  Traite %d / %d records...", n_processed, total)
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Relative strength build termine: %d features en %.1fs (chevaux suivis: %d)",
        n_written, elapsed, len(horse_state),
    )

    # Summary fill rates
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0
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
    for candidate in INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Aucun fichier d'entree trouve parmi: {[str(c) for c in INPUT_CANDIDATES]}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Construction des features RSI/technical analysis a partir de partants_master"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Chemin vers partants_master.jsonl (defaut: auto-detection)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Repertoire de sortie (defaut: builder_outputs/relative_strength/)",
    )
    args = parser.parse_args()

    logger = setup_logging("relative_strength_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "relative_strength_features.jsonl"
    build_relative_strength_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
