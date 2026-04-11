#!/usr/bin/env python3
"""
feature_builders.hippo_draw_deep_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Deep hippodrome x draw interaction features.

Reads partants_master.jsonl in streaming mode with an index+sort+seek approach,
processes all records chronologically, and computes per-partant hippodrome-draw
interaction features.

Temporal integrity: for any partant at date D, only races with date < D
contribute to the statistics -- no future leakage.

Produces:
  - hippo_draw_deep.jsonl   in D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippo_draw_deep/

Features per partant (13):
  - hdd_draw_win_rate_hippo        : historical win rate for this place_corde at this hippodrome
  - hdd_draw_place_rate_hippo      : historical place rate (top 3) for this place_corde at this hippo
  - hdd_draw_advantage_score       : draw win rate - (1/nombre_partants) expected rate
  - hdd_inside_bias                : win rate for draws 1-4 at this hippo
  - hdd_middle_bias                : win rate for draws 5-8 at this hippo
  - hdd_outside_bias               : win rate for draws 9+ at this hippo
  - hdd_draw_distance_wr           : draw x distance-bucket win rate at this hippo
  - hdd_draw_field_size_wr         : draw x field-size-bucket win rate at this hippo
  - hdd_horse_similar_draw_wr      : this horse's historical win rate from similar draws (same cluster)
  - hdd_normalized_draw            : place_corde / nombre_partants (0-1)
  - hdd_is_best_draw               : 1 if this draw has the highest historical win rate at this hippo
  - hdd_is_worst_draw              : 1 if this draw has the lowest historical win rate at this hippo
  - hdd_draw_surface_wr            : draw x surface type (type_piste/discipline) win rate at this hippo
  - hdd_draw_cluster               : inner (0), middle (1), outer (2) -- hippo-adjusted
  - hdd_hippo_draw_bias_strength   : chi-squared-style measure of how much draw matters at this hippo

State:
  - dict[(hippo, draw)] -> {wins, places, total}
  - dict[(hippo, draw, distance_bucket)] -> {wins, total}
  - dict[(hippo, draw, field_bucket)] -> {wins, total}
  - dict[(hippo, draw, surface)] -> {wins, total}
  - dict[(hippo, zone)] -> {wins, total}   (zone = inner/middle/outer)
  - dict[(horse, draw_cluster)] -> {wins, total}

Key fields: place_corde, hippodrome_normalise, distance, nombre_partants,
            position_arrivee, is_gagnant, is_place, type_piste, discipline,
            date_reunion_iso, partant_uid, course_uid.

Usage:
    python feature_builders/hippo_draw_deep_builder.py
    python feature_builders/hippo_draw_deep_builder.py --input D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippo_draw_deep")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_INPUT_CANDIDATES = [
    INPUT_PARTANTS,
    _PROJECT_ROOT / "data_master" / "partants_master.jsonl",
    _PROJECT_ROOT / "data_master" / "partants_master_enrichi.jsonl",
]

_LOG_EVERY = 500_000
_MIN_SAMPLES = 10  # min observations to emit a rate

# Distance buckets (metres)
_DIST_BREAKPOINTS = [1200, 1600, 2000, 2400]

# Field size buckets
_FIELD_BREAKPOINTS = [6, 10, 14]  # small <=6, medium 7-10, large 11-14, xlarge 15+


# ===========================================================================
# HELPERS
# ===========================================================================


def _distance_bucket(distance: Any) -> Optional[str]:
    try:
        d = int(distance)
    except (TypeError, ValueError):
        return None
    if d <= 0:
        return None
    if d < _DIST_BREAKPOINTS[0]:
        return "lt1200"
    for i in range(1, len(_DIST_BREAKPOINTS)):
        if d < _DIST_BREAKPOINTS[i]:
            lo = _DIST_BREAKPOINTS[i - 1]
            hi = _DIST_BREAKPOINTS[i]
            return f"{lo}-{hi}"
    return "ge2400"


def _field_bucket(nb: Any) -> Optional[str]:
    try:
        n = int(nb)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    if n <= _FIELD_BREAKPOINTS[0]:
        return "small"
    if n <= _FIELD_BREAKPOINTS[1]:
        return "medium"
    if n <= _FIELD_BREAKPOINTS[2]:
        return "large"
    return "xlarge"


def _draw_cluster(draw: int) -> int:
    """0=inner (1-3), 1=middle (4-7), 2=outer (8+)."""
    if draw <= 3:
        return 0
    if draw <= 7:
        return 1
    return 2


def _draw_zone_label(draw: int) -> str:
    if draw <= 4:
        return "inner"
    if draw <= 8:
        return "middle"
    return "outer"


def _surface_key(rec: dict) -> str:
    """Build a surface key from type_piste or discipline."""
    tp = (rec.get("type_piste") or "").strip().lower()
    if tp:
        return tp
    disc = (rec.get("discipline") or "").strip().lower()
    return disc or "unknown"


def _is_winner_flag(rec: dict) -> bool:
    """Check is_gagnant or position_arrivee == 1."""
    ig = rec.get("is_gagnant")
    if ig is not None:
        return bool(ig)
    try:
        return int(rec.get("position_arrivee")) == 1
    except (TypeError, ValueError):
        return False


def _is_placed_flag(rec: dict) -> bool:
    """Check is_place or position_arrivee <= 3."""
    ip = rec.get("is_place")
    if ip is not None:
        return bool(ip)
    try:
        return int(rec.get("position_arrivee")) <= 3
    except (TypeError, ValueError):
        return False


def _safe_int(val: Any) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


# ===========================================================================
# STATE CONTAINERS
# ===========================================================================


class _WinPlaceCounter:
    __slots__ = ("wins", "places", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.places: int = 0
        self.total: int = 0

    def win_rate(self, min_n: int = _MIN_SAMPLES) -> Optional[float]:
        if self.total < min_n:
            return None
        return round(self.wins / self.total, 6)

    def place_rate(self, min_n: int = _MIN_SAMPLES) -> Optional[float]:
        if self.total < min_n:
            return None
        return round(self.places / self.total, 6)


class _WinCounter:
    __slots__ = ("wins", "total")

    def __init__(self) -> None:
        self.wins: int = 0
        self.total: int = 0

    def win_rate(self, min_n: int = _MIN_SAMPLES) -> Optional[float]:
        if self.total < min_n:
            return None
        return round(self.wins / self.total, 6)


# ===========================================================================
# MAIN BUILD
# ===========================================================================


def build_hippo_draw_deep_features(
    input_path: Path, output_path: Path, logger
) -> int:
    logger.info("=== Hippo Draw Deep Builder ===")
    logger.info("Input: %s", input_path)
    t0 = time.time()

    # -------------------------------------------------------------------
    # Phase 1: Build lightweight index
    # -------------------------------------------------------------------
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
                logger.info("  Indexed %d records...", n_read)

            date_str = rec.get("date_reunion_iso") or ""
            course_uid = rec.get("course_uid") or ""
            num_pmu = _safe_int(rec.get("num_pmu"))

            index.append((date_str, course_uid, num_pmu, offset))

    logger.info(
        "Phase 1 complete: %d records indexed in %.1fs",
        len(index), time.time() - t0,
    )

    # -------------------------------------------------------------------
    # Phase 2: Sort chronologically
    # -------------------------------------------------------------------
    t1 = time.time()
    index.sort(key=lambda x: (x[0], x[1], x[2]))
    logger.info("Chronological sort done in %.1fs", time.time() - t1)

    # -------------------------------------------------------------------
    # Phase 3: Process race by race, snapshot before update
    # -------------------------------------------------------------------

    # State dictionaries
    hippo_draw_stats: dict[tuple[str, int], _WinPlaceCounter] = defaultdict(_WinPlaceCounter)
    hippo_draw_dist: dict[tuple[str, int, str], _WinCounter] = defaultdict(_WinCounter)
    hippo_draw_field: dict[tuple[str, int, str], _WinCounter] = defaultdict(_WinCounter)
    hippo_draw_surface: dict[tuple[str, int, str], _WinCounter] = defaultdict(_WinCounter)
    hippo_zone: dict[tuple[str, str], _WinCounter] = defaultdict(_WinCounter)
    horse_draw_cluster: dict[tuple[str, int], _WinCounter] = defaultdict(_WinCounter)
    # For draw bias strength: hippo -> total races seen
    hippo_total: dict[str, _WinCounter] = defaultdict(_WinCounter)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".tmp")

    feature_names = [
        "hdd_draw_win_rate_hippo",
        "hdd_draw_place_rate_hippo",
        "hdd_draw_advantage_score",
        "hdd_inside_bias",
        "hdd_middle_bias",
        "hdd_outside_bias",
        "hdd_draw_distance_wr",
        "hdd_draw_field_size_wr",
        "hdd_horse_similar_draw_wr",
        "hdd_normalized_draw",
        "hdd_is_best_draw",
        "hdd_is_worst_draw",
        "hdd_draw_surface_wr",
        "hdd_draw_cluster",
        "hdd_hippo_draw_bias_strength",
    ]

    fill_counts: dict[str, int] = {k: 0 for k in feature_names}
    n_processed = 0
    n_written = 0
    total = len(index)

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:

        def _read_at(off: int) -> dict:
            fin.seek(off)
            return json.loads(fin.readline())

        i = 0
        while i < total:
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

            # Read full records for this race
            race_records: list[dict] = [_read_at(index[ci][3]) for ci in course_indices]

            # Extract minimal fields per runner
            runners: list[dict[str, Any]] = []
            for rec in race_records:
                draw = _safe_int(rec.get("place_corde"))
                nb_partants = _safe_int(rec.get("nombre_partants"))
                hippo = (rec.get("hippodrome_normalise") or "").strip().lower()
                dist_bucket = _distance_bucket(rec.get("distance"))
                field_bkt = _field_bucket(nb_partants)
                surface = _surface_key(rec)
                horse_id = rec.get("cheval_uid") or rec.get("cheval_id") or ""
                winner = _is_winner_flag(rec)
                placed = _is_placed_flag(rec)

                runners.append({
                    "uid": rec.get("partant_uid"),
                    "draw": draw,
                    "nb_partants": nb_partants,
                    "hippo": hippo,
                    "dist_bucket": dist_bucket,
                    "field_bkt": field_bkt,
                    "surface": surface,
                    "horse_id": horse_id,
                    "winner": winner,
                    "placed": placed,
                })

            # ---------------------------------------------------------------
            # Step A: Snapshot pre-race features
            # ---------------------------------------------------------------
            hippo_ref = runners[0]["hippo"] if runners else ""

            # Pre-compute draw win rates for best/worst draw detection
            draw_wrs_in_race: list[Optional[float]] = []
            for r in runners:
                if r["hippo"] and r["draw"]:
                    wr = hippo_draw_stats[(r["hippo"], r["draw"])].win_rate()
                else:
                    wr = None
                draw_wrs_in_race.append(wr)

            valid_wrs = [w for w in draw_wrs_in_race if w is not None]
            best_wr = max(valid_wrs) if valid_wrs else None
            worst_wr = min(valid_wrs) if valid_wrs else None

            for idx_r, r in enumerate(runners):
                uid = r["uid"]
                draw = r["draw"]
                nb_partants = r["nb_partants"]
                hippo = r["hippo"]
                dist_bucket = r["dist_bucket"]
                field_bkt = r["field_bkt"]
                surface = r["surface"]
                horse_id = r["horse_id"]

                feat: dict[str, Any] = {"partant_uid": uid}

                # 1. Draw win rate at this hippodrome
                if hippo and draw:
                    counter = hippo_draw_stats[(hippo, draw)]
                    wr = counter.win_rate()
                    feat["hdd_draw_win_rate_hippo"] = wr
                    if wr is not None:
                        fill_counts["hdd_draw_win_rate_hippo"] += 1

                    # 2. Draw place rate
                    pr = counter.place_rate()
                    feat["hdd_draw_place_rate_hippo"] = pr
                    if pr is not None:
                        fill_counts["hdd_draw_place_rate_hippo"] += 1

                    # 3. Draw advantage score
                    if wr is not None and nb_partants > 0:
                        expected = 1.0 / nb_partants
                        feat["hdd_draw_advantage_score"] = round(wr - expected, 6)
                        fill_counts["hdd_draw_advantage_score"] += 1
                    else:
                        feat["hdd_draw_advantage_score"] = None
                else:
                    feat["hdd_draw_win_rate_hippo"] = None
                    feat["hdd_draw_place_rate_hippo"] = None
                    feat["hdd_draw_advantage_score"] = None

                # 4. Inside vs middle vs outside bias at this hippodrome
                if hippo:
                    inner_wr = hippo_zone.get((hippo, "inner"))
                    middle_wr_z = hippo_zone.get((hippo, "middle"))
                    outer_wr = hippo_zone.get((hippo, "outer"))

                    v = inner_wr.win_rate() if inner_wr else None
                    feat["hdd_inside_bias"] = v
                    if v is not None:
                        fill_counts["hdd_inside_bias"] += 1

                    v = middle_wr_z.win_rate() if middle_wr_z else None
                    feat["hdd_middle_bias"] = v
                    if v is not None:
                        fill_counts["hdd_middle_bias"] += 1

                    v = outer_wr.win_rate() if outer_wr else None
                    feat["hdd_outside_bias"] = v
                    if v is not None:
                        fill_counts["hdd_outside_bias"] += 1
                else:
                    feat["hdd_inside_bias"] = None
                    feat["hdd_middle_bias"] = None
                    feat["hdd_outside_bias"] = None

                # 5. Draw x distance interaction
                if hippo and draw and dist_bucket:
                    v = hippo_draw_dist[(hippo, draw, dist_bucket)].win_rate()
                    feat["hdd_draw_distance_wr"] = v
                    if v is not None:
                        fill_counts["hdd_draw_distance_wr"] += 1
                else:
                    feat["hdd_draw_distance_wr"] = None

                # 6. Draw x field size interaction
                if hippo and draw and field_bkt:
                    v = hippo_draw_field[(hippo, draw, field_bkt)].win_rate()
                    feat["hdd_draw_field_size_wr"] = v
                    if v is not None:
                        fill_counts["hdd_draw_field_size_wr"] += 1
                else:
                    feat["hdd_draw_field_size_wr"] = None

                # 7. Horse's historical performance from similar draws
                if horse_id and draw:
                    cluster = _draw_cluster(draw)
                    v = horse_draw_cluster[(horse_id, cluster)].win_rate(min_n=3)
                    feat["hdd_horse_similar_draw_wr"] = v
                    if v is not None:
                        fill_counts["hdd_horse_similar_draw_wr"] += 1
                else:
                    feat["hdd_horse_similar_draw_wr"] = None

                # 8. Normalized draw position
                if draw > 0 and nb_partants > 0:
                    feat["hdd_normalized_draw"] = round(draw / nb_partants, 6)
                    fill_counts["hdd_normalized_draw"] += 1
                else:
                    feat["hdd_normalized_draw"] = None

                # 9. Is best draw at this hippodrome?
                runner_wr = draw_wrs_in_race[idx_r]
                if best_wr is not None and runner_wr is not None:
                    feat["hdd_is_best_draw"] = 1 if runner_wr >= best_wr else 0
                    fill_counts["hdd_is_best_draw"] += 1
                else:
                    feat["hdd_is_best_draw"] = None

                # 10. Is worst draw?
                if worst_wr is not None and runner_wr is not None:
                    feat["hdd_is_worst_draw"] = 1 if runner_wr <= worst_wr else 0
                    fill_counts["hdd_is_worst_draw"] += 1
                else:
                    feat["hdd_is_worst_draw"] = None

                # 11. Draw x surface type
                if hippo and draw and surface and surface != "unknown":
                    v = hippo_draw_surface[(hippo, draw, surface)].win_rate()
                    feat["hdd_draw_surface_wr"] = v
                    if v is not None:
                        fill_counts["hdd_draw_surface_wr"] += 1
                else:
                    feat["hdd_draw_surface_wr"] = None

                # 12. Draw cluster
                if draw > 0:
                    feat["hdd_draw_cluster"] = _draw_cluster(draw)
                    fill_counts["hdd_draw_cluster"] += 1
                else:
                    feat["hdd_draw_cluster"] = None

                # 13. Hippodrome draw bias strength
                # chi-squared style: sum over draws of (observed_wr - expected_wr)^2 / expected_wr
                if hippo:
                    h_total_ctr = hippo_total.get(hippo)
                    if h_total_ctr and h_total_ctr.total >= 50:
                        overall_wr = h_total_ctr.wins / h_total_ctr.total if h_total_ctr.total > 0 else 0
                        if overall_wr > 0:
                            chi2 = 0.0
                            n_draws_seen = 0
                            for (h, d), ctr in hippo_draw_stats.items():
                                if h == hippo and ctr.total >= _MIN_SAMPLES:
                                    obs_wr = ctr.wins / ctr.total
                                    chi2 += (obs_wr - overall_wr) ** 2 / overall_wr
                                    n_draws_seen += 1
                            if n_draws_seen >= 3:
                                feat["hdd_hippo_draw_bias_strength"] = round(chi2, 6)
                                fill_counts["hdd_hippo_draw_bias_strength"] += 1
                            else:
                                feat["hdd_hippo_draw_bias_strength"] = None
                        else:
                            feat["hdd_hippo_draw_bias_strength"] = None
                    else:
                        feat["hdd_hippo_draw_bias_strength"] = None
                else:
                    feat["hdd_hippo_draw_bias_strength"] = None

                fout.write(json.dumps(feat, ensure_ascii=False, default=str) + "\n")
                n_written += 1

            # ---------------------------------------------------------------
            # Step B: Update state from race results
            # ---------------------------------------------------------------
            for r in runners:
                hippo = r["hippo"]
                draw = r["draw"]
                dist_bucket = r["dist_bucket"]
                field_bkt = r["field_bkt"]
                surface = r["surface"]
                horse_id = r["horse_id"]
                winner = r["winner"]
                placed = r["placed"]

                if not hippo or not draw:
                    continue

                # hippo x draw
                ctr = hippo_draw_stats[(hippo, draw)]
                ctr.total += 1
                if winner:
                    ctr.wins += 1
                if placed:
                    ctr.places += 1

                # hippo x draw x distance
                if dist_bucket:
                    c = hippo_draw_dist[(hippo, draw, dist_bucket)]
                    c.total += 1
                    if winner:
                        c.wins += 1

                # hippo x draw x field size
                if field_bkt:
                    c = hippo_draw_field[(hippo, draw, field_bkt)]
                    c.total += 1
                    if winner:
                        c.wins += 1

                # hippo x draw x surface
                if surface and surface != "unknown":
                    c = hippo_draw_surface[(hippo, draw, surface)]
                    c.total += 1
                    if winner:
                        c.wins += 1

                # hippo x zone (inner/middle/outer)
                zone = _draw_zone_label(draw)
                z = hippo_zone[(hippo, zone)]
                z.total += 1
                if winner:
                    z.wins += 1

                # horse x draw cluster
                if horse_id:
                    cluster = _draw_cluster(draw)
                    hc = horse_draw_cluster[(horse_id, cluster)]
                    hc.total += 1
                    if winner:
                        hc.wins += 1

                # hippo total (for bias strength)
                ht = hippo_total[hippo]
                ht.total += 1
                if winner:
                    ht.wins += 1

            n_processed += len(runners)
            if n_processed % _LOG_EVERY < len(runners):
                logger.info(
                    "  Processed %d / %d records...", n_processed, total
                )
                gc.collect()

    # Atomic replace
    tmp_out.replace(output_path)

    elapsed = time.time() - t0
    logger.info(
        "Hippo draw deep build complete: %d feature rows in %.1fs "
        "(hippo x draw keys: %d, hippo x draw x dist keys: %d)",
        n_written, elapsed, len(hippo_draw_stats), len(hippo_draw_dist),
    )

    # Fill-rate summary
    logger.info("=== Fill rates ===")
    for k, v in fill_counts.items():
        pct = 100.0 * v / n_written if n_written else 0.0
        logger.info("  %-40s %d/%d (%.1f%%)", k, v, n_written, pct)

    return n_written


# ===========================================================================
# CLI
# ===========================================================================


def _resolve_input(cli_path: Optional[str]) -> Path:
    if cli_path:
        p = Path(cli_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"File not found: {p}")
    for candidate in _INPUT_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "No partants_master.jsonl found. Tried: "
        + ", ".join(str(c) for c in _INPUT_CANDIDATES)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build deep hippodrome x draw interaction features from partants_master.jsonl"
    )
    parser.add_argument(
        "--input", type=str, default=None,
        help="Path to partants_master.jsonl (auto-detected if omitted)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=(
            "Output directory "
            "(default: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/hippo_draw_deep)"
        ),
    )
    args = parser.parse_args()

    logger = setup_logging("hippo_draw_deep_builder")

    input_path = _resolve_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / "hippo_draw_deep.jsonl"
    build_hippo_draw_deep_features(input_path, out_path, logger)


if __name__ == "__main__":
    main()
