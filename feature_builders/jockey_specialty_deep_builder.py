#!/usr/bin/env python3
"""
feature_builders.jockey_specialty_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep jockey specialization features -- detailed analysis of what makes
this jockey perform in specific conditions.

Reads partants_master.jsonl using an index + seek architecture,
processes all records chronologically, and computes per-partant
deep jockey specialty features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the jockey stats -- no future leakage.

Produces:
  - jockey_specialty_deep_features.jsonl

Features per partant (10):
  - jsd_jockey_win_rate_global       : jockey's overall rolling win rate (last 200 rides)
  - jsd_jockey_favorites_conversion  : win rate when jockey rides favorites (cote < 5)
  - jsd_jockey_outsider_specialist   : win rate when jockey rides outsiders (cote > 10)
  - jsd_jockey_heavy_ground_wr       : win rate on heavy/soft ground
  - jsd_jockey_sprint_specialist     : win rate at distance < 1800m
  - jsd_jockey_stayer_specialist     : win rate at distance > 2400m
  - jsd_jockey_this_trainer_wr       : win rate when working with THIS trainer
  - jsd_jockey_experience_this_horse : number of times jockey has ridden THIS horse before
  - jsd_jockey_weight_efficiency     : jockey win rate when carrying above field avg weight
  - jsd_jockey_closing_style         : avg position improvement from mid-race to finish

Usage:
    python feature_builders/jockey_specialty_deep_builder.py
    python feature_builders/jockey_specialty_deep_builder.py --input path/to/partants_master.jsonl
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

INPUT_DEFAULT = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_specialty_deep")
OUTPUT_FILENAME = "jockey_specialty_deep_features.jsonl"

_LOG_EVERY = 500_000
_ROLLING_WINDOW = 200  # last N rides for global win rate

# Heavy / soft ground keywords
_HEAVY_GROUND = frozenset({
    "lourd", "tres lourd", "collant", "souple", "heavy",
    "soft", "very soft", "deep",
})


def _is_heavy_ground(terrain: str) -> bool:
    """Return True if terrain qualifies as heavy/soft ground."""
    if not terrain:
        return False
    t = terrain.strip().lower()
    return t in _HEAVY_GROUND


# ===========================================================================
# SAFE PARSERS
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


# ===========================================================================
# PER-JOCKEY STATE TRACKER
# ===========================================================================


class _JockeyGlobalState:
    """Tracks global rolling win/runs for a single jockey (last N rides)."""

    __slots__ = ("results",)

    def __init__(self) -> None:
        # deque of bools: True = win
        self.results: deque = deque(maxlen=_ROLLING_WINDOW)

    def win_rate(self) -> Optional[float]:
        if not self.results:
            return None
        return sum(self.results) / len(self.results)

    def update(self, won: bool) -> None:
        self.results.append(won)


class _WinRunTracker:
    """Simple wins/runs accumulator."""

    __slots__ = ("wins", "runs")

    def __init__(self) -> None:
        self.wins: int = 0
        self.runs: int = 0

    def win_rate(self) -> Optional[float]:
        if self.runs == 0:
            return None
        return round(self.wins / self.runs, 4)

    def update(self, won: bool) -> None:
        self.runs += 1
        if won:
            self.wins += 1


class _ClosingStyleTracker:
    """Tracks average position improvement (mid-race to finish) for a jockey."""

    __slots__ = ("improvements", "count")

    def __init__(self) -> None:
        self.improvements: float = 0.0
        self.count: int = 0

    def avg_improvement(self) -> Optional[float]:
        if self.count == 0:
            return None
        return round(self.improvements / self.count, 4)

    def update(self, improvement: float) -> None:
        self.improvements += improvement
        self.count += 1


# ===========================================================================
# MAIN BUILD (index + seek + streaming output)
# ===========================================================================


def build_jockey_specialty_deep_features(
    input_path: Path, output_path: Path, logger
) -> int:
    """Build deep jockey specialty features from partants_master.jsonl.

    Architecture:
      1. Build a lightweight index: (date, course_uid, num_pmu, byte_offset)
      2. Sort chronologically
      3. Seek to each record, snapshot features BEFORE update, write output

    Returns the total number of feature records written.
    """
    logger.info("=== Jockey Specialty Deep Builder (index + seek) ===")
    logger.info("Input: %s", input_path)
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
            line_s = line.strip()
            if not line_s:
                continue
            try:
                rec = json.loads(line_s)
            except json.JSONDecodeError:
                continue

            n_read += 1
            if n_read % _LOG_EVERY == 0:
                logger.info("  Indexing %d records...", n_read)

            date_str = rec.get("date_reunion_iso", "") or ""
            course_uid = rec.get("course_uid", "") or ""
            num_pmu = rec.get("num_pmu", 0) or 0
            try:
                num_pmu = int(num_pmu)
            except (ValueError, TypeError):
                num_pmu = 0

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1: %d records indexed in %.1fs",
        len(index), time.time() - t0,
    )

    # -- Phase 2: Sort chronologically --
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Phase 2: sorted in %.1fs", time.time() - t1)

    # -- Phase 3: Process course by course, streaming output --
    t2 = time.time()

    # Per jockey: global rolling win rate
    jockey_global: dict[str, _JockeyGlobalState] = defaultdict(_JockeyGlobalState)

    # Per jockey: favorites conversion (cote < 5)
    jockey_fav: dict[str, _WinRunTracker] = defaultdict(_WinRunTracker)

    # Per jockey: outsider specialist (cote > 10)
    jockey_outsider: dict[str, _WinRunTracker] = defaultdict(_WinRunTracker)

    # Per jockey: heavy ground win rate
    jockey_heavy: dict[str, _WinRunTracker] = defaultdict(_WinRunTracker)

    # Per jockey: sprint specialist (distance < 1800m)
    jockey_sprint: dict[str, _WinRunTracker] = defaultdict(_WinRunTracker)

    # Per jockey: stayer specialist (distance > 2400m)
    jockey_stayer: dict[str, _WinRunTracker] = defaultdict(_WinRunTracker)

    # Per (jockey, trainer): win rate
    jockey_trainer: dict[tuple[str, str], _WinRunTracker] = defaultdict(_WinRunTracker)

    # Per (jockey, horse): ride count
    jockey_horse_count: dict[tuple[str, str], int] = defaultdict(int)

    # Per jockey: weight efficiency (win rate when above field avg weight)
    jockey_heavy_weight: dict[str, _WinRunTracker] = defaultdict(_WinRunTracker)

    # Per jockey: closing style
    jockey_closing: dict[str, _ClosingStyleTracker] = defaultdict(_ClosingStyleTracker)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    n_written = 0
    total = len(index)

    feature_keys = [
        "jsd_jockey_win_rate_global",
        "jsd_jockey_favorites_conversion",
        "jsd_jockey_outsider_specialist",
        "jsd_jockey_heavy_ground_wr",
        "jsd_jockey_sprint_specialist",
        "jsd_jockey_stayer_specialist",
        "jsd_jockey_this_trainer_wr",
        "jsd_jockey_experience_this_horse",
        "jsd_jockey_weight_efficiency",
        "jsd_jockey_closing_style",
    ]
    fill_counts: dict[str, int] = {k: 0 for k in feature_keys}

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(offset: int) -> dict:
            fin.seek(offset)
            return json.loads(fin.readline())

        i = 0
        while i < total:
            # Collect all index entries for this course
            course_uid = index[i][1]
            course_date = index[i][0]
            course_indices: list[int] = []

            while (
                i < total
                and index[i][1] == course_uid
                and index[i][0] == course_date
            ):
                course_indices.append(i)
                i += 1

            if not course_indices:
                continue

            # Read records from disk for this course
            course_records: list[dict] = []
            for ci in course_indices:
                rec = _read_at(index[ci][3])
                course_records.append(rec)

            # Compute field average weight for weight_efficiency feature
            weights_in_field: list[float] = []
            for rec in course_records:
                w = _safe_float(rec.get("poids_monte") or rec.get("handicap_poids"))
                if w is not None and w > 0:
                    weights_in_field.append(w)
            field_avg_weight = (
                sum(weights_in_field) / len(weights_in_field)
                if weights_in_field
                else None
            )

            # -- Deferred updates to apply AFTER snapshotting all features --
            deferred_updates: list[dict[str, Any]] = []

            for rec in course_records:
                jockey = rec.get("nom_jockey") or ""
                trainer = rec.get("nom_entraineur") or ""
                cheval = rec.get("nom_cheval") or ""
                uid = rec.get("partant_uid")
                course_uid_val = rec.get("course_uid", "")
                date_iso = rec.get("date_reunion_iso", "")

                won = bool(rec.get("is_gagnant"))

                # Parse fields
                cote = _safe_float(
                    rec.get("cote_finale")
                    or rec.get("cote_reference")
                    or rec.get("rapport_final")
                    or rec.get("cote_probable")
                )
                if cote is not None and cote <= 0:
                    cote = None

                distance = _safe_int(rec.get("distance"))

                terrain = (
                    rec.get("cnd_cond_type_terrain")
                    or rec.get("met_terrain_predit")
                    or rec.get("terrain")
                    or rec.get("type_piste")
                    or ""
                )
                terrain = str(terrain).strip().lower() if terrain else ""

                weight = _safe_float(
                    rec.get("poids_monte") or rec.get("handicap_poids")
                )

                # Position improvement proxy: nb_partants - position_arrivee
                # (higher = better closing -- approximation of closing style)
                position = _safe_int(rec.get("position_arrivee"))
                nb_partants = _safe_int(rec.get("nombre_partants"))

                # === SNAPSHOT features BEFORE update ===
                feat: dict[str, Any] = {
                    "partant_uid": uid,
                    "course_uid": course_uid_val,
                    "date_reunion_iso": date_iso,
                }

                if jockey:
                    # 1. jsd_jockey_win_rate_global
                    wr_global = jockey_global[jockey].win_rate()
                    feat["jsd_jockey_win_rate_global"] = (
                        round(wr_global, 4) if wr_global is not None else None
                    )
                    if wr_global is not None:
                        fill_counts["jsd_jockey_win_rate_global"] += 1

                    # 2. jsd_jockey_favorites_conversion
                    if cote is not None and cote < 5:
                        fav_wr = jockey_fav[jockey].win_rate()
                        feat["jsd_jockey_favorites_conversion"] = fav_wr
                        if fav_wr is not None:
                            fill_counts["jsd_jockey_favorites_conversion"] += 1
                    else:
                        feat["jsd_jockey_favorites_conversion"] = None

                    # 3. jsd_jockey_outsider_specialist
                    if cote is not None and cote > 10:
                        out_wr = jockey_outsider[jockey].win_rate()
                        feat["jsd_jockey_outsider_specialist"] = out_wr
                        if out_wr is not None:
                            fill_counts["jsd_jockey_outsider_specialist"] += 1
                    else:
                        feat["jsd_jockey_outsider_specialist"] = None

                    # 4. jsd_jockey_heavy_ground_wr
                    if _is_heavy_ground(terrain):
                        hg_wr = jockey_heavy[jockey].win_rate()
                        feat["jsd_jockey_heavy_ground_wr"] = hg_wr
                        if hg_wr is not None:
                            fill_counts["jsd_jockey_heavy_ground_wr"] += 1
                    else:
                        feat["jsd_jockey_heavy_ground_wr"] = None

                    # 5. jsd_jockey_sprint_specialist
                    if distance is not None and distance < 1800:
                        sp_wr = jockey_sprint[jockey].win_rate()
                        feat["jsd_jockey_sprint_specialist"] = sp_wr
                        if sp_wr is not None:
                            fill_counts["jsd_jockey_sprint_specialist"] += 1
                    else:
                        feat["jsd_jockey_sprint_specialist"] = None

                    # 6. jsd_jockey_stayer_specialist
                    if distance is not None and distance > 2400:
                        st_wr = jockey_stayer[jockey].win_rate()
                        feat["jsd_jockey_stayer_specialist"] = st_wr
                        if st_wr is not None:
                            fill_counts["jsd_jockey_stayer_specialist"] += 1
                    else:
                        feat["jsd_jockey_stayer_specialist"] = None

                    # 7. jsd_jockey_this_trainer_wr
                    if trainer:
                        jt_key = (jockey, trainer)
                        jt_wr = jockey_trainer[jt_key].win_rate()
                        feat["jsd_jockey_this_trainer_wr"] = jt_wr
                        if jt_wr is not None:
                            fill_counts["jsd_jockey_this_trainer_wr"] += 1
                    else:
                        feat["jsd_jockey_this_trainer_wr"] = None

                    # 8. jsd_jockey_experience_this_horse
                    if cheval:
                        jh_key = (jockey, cheval)
                        exp_count = jockey_horse_count.get(jh_key, 0)
                        feat["jsd_jockey_experience_this_horse"] = exp_count if exp_count > 0 else None
                        if exp_count > 0:
                            fill_counts["jsd_jockey_experience_this_horse"] += 1
                    else:
                        feat["jsd_jockey_experience_this_horse"] = None

                    # 9. jsd_jockey_weight_efficiency
                    if (
                        weight is not None
                        and field_avg_weight is not None
                        and weight > field_avg_weight
                    ):
                        hw_wr = jockey_heavy_weight[jockey].win_rate()
                        feat["jsd_jockey_weight_efficiency"] = hw_wr
                        if hw_wr is not None:
                            fill_counts["jsd_jockey_weight_efficiency"] += 1
                    else:
                        feat["jsd_jockey_weight_efficiency"] = None

                    # 10. jsd_jockey_closing_style
                    cs_val = jockey_closing[jockey].avg_improvement()
                    feat["jsd_jockey_closing_style"] = cs_val
                    if cs_val is not None:
                        fill_counts["jsd_jockey_closing_style"] += 1

                else:
                    # No jockey -- all None
                    for k in feature_keys:
                        feat[k] = None

                fout.write(json.dumps(feat, ensure_ascii=False) + "\n")
                n_written += 1

                # Queue deferred update info
                deferred_updates.append({
                    "jockey": jockey,
                    "trainer": trainer,
                    "cheval": cheval,
                    "won": won,
                    "cote": cote,
                    "distance": distance,
                    "terrain": terrain,
                    "weight": weight,
                    "field_avg_weight": field_avg_weight,
                    "position": position,
                    "nb_partants": nb_partants,
                })

            # -- Apply updates AFTER all features for this course are snapshotted --
            for upd in deferred_updates:
                jockey = upd["jockey"]
                if not jockey:
                    continue

                won = upd["won"]
                cote = upd["cote"]
                distance = upd["distance"]
                terrain = upd["terrain"]
                trainer = upd["trainer"]
                cheval = upd["cheval"]
                weight = upd["weight"]
                field_avg_w = upd["field_avg_weight"]
                position = upd["position"]
                nb_partants = upd["nb_partants"]

                # 1. Global rolling
                jockey_global[jockey].update(won)

                # 2. Favorites conversion (only if cote < 5)
                if cote is not None and cote < 5:
                    jockey_fav[jockey].update(won)

                # 3. Outsider specialist (only if cote > 10)
                if cote is not None and cote > 10:
                    jockey_outsider[jockey].update(won)

                # 4. Heavy ground (only if heavy/soft)
                if _is_heavy_ground(terrain):
                    jockey_heavy[jockey].update(won)

                # 5. Sprint specialist (only if distance < 1800m)
                if distance is not None and distance < 1800:
                    jockey_sprint[jockey].update(won)

                # 6. Stayer specialist (only if distance > 2400m)
                if distance is not None and distance > 2400:
                    jockey_stayer[jockey].update(won)

                # 7. Jockey-trainer combo
                if trainer:
                    jockey_trainer[(jockey, trainer)].update(won)

                # 8. Jockey-horse experience count
                if cheval:
                    jockey_horse_count[(jockey, cheval)] += 1

                # 9. Weight efficiency (only if above field average)
                if (
                    weight is not None
                    and field_avg_w is not None
                    and weight > field_avg_w
                ):
                    jockey_heavy_weight[jockey].update(won)

                # 10. Closing style: improvement = (nb_partants - position) / nb_partants
                #     Normalized so 1.0 = first place, 0 = last
                if (
                    position is not None
                    and nb_partants is not None
                    and position > 0
                    and nb_partants > 1
                ):
                    improvement = (nb_partants - position) / (nb_partants - 1)
                    jockey_closing[jockey].update(improvement)

            # Periodic GC
            if n_written % _LOG_EVERY < len(course_records):
                logger.info("  Processed %d / %d records...", n_written, total)
                gc.collect()

    # Atomic rename
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Jockey Specialty Deep build done: %d features in %.1fs (jockeys: %d, combos j-t: %d, combos j-h: %d)",
        n_written, elapsed,
        len(jockey_global),
        len(jockey_trainer),
        len(jockey_horse_count),
    )

    # Fill rates
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
        raise FileNotFoundError(f"Input file not found: {p}")
    if INPUT_DEFAULT.exists():
        return INPUT_DEFAULT
    raise FileNotFoundError(f"Input file not found: {INPUT_DEFAULT}")


def main():
    parser = argparse.ArgumentParser(
        description="Jockey Specialty Deep features from partants_master.jsonl"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help=f"Path to partants_master.jsonl (default: {INPUT_DEFAULT})",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Output directory (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    logger = setup_logging("jockey_specialty_deep_builder")

    input_path = _find_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / OUTPUT_FILENAME
    build_jockey_specialty_deep_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
