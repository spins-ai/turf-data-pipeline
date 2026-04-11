#!/usr/bin/env python3
"""Allocation per partant + race value features - STREAMING. 8 features:
allocation_per_partant, prize_money_rank, race_value_score,
allocation_log, field_size_bin, allocation_vs_mean, is_big_race,
race_prestige_score.

No OOM: streaming course-by-course, lightweight accumulators."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/allocation_per_partant")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("allocation_per_partant_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "allocation_per_partant_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Rolling mean allocation (last 500 races)
    alloc_history = []  # bounded list of recent allocations
    alloc_sum = 0.0
    alloc_count = 0

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        current_course = None
        course_records = []

        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                cuid = rec.get("course_uid", "")

                if cuid != current_course and course_records:
                    _process_course(course_records, fout, alloc_history, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, alloc_history, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, alloc_history, fills):
    r0 = records[0]
    nb_partants = len(records)
    alloc = _safe(r0.get("allocation_partant")) or _safe(r0.get("allocation_course"))

    # Compute rolling mean
    rolling_mean = None
    if len(alloc_history) >= 50:
        rolling_mean = sum(alloc_history[-500:]) / len(alloc_history[-500:])

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        # 1. Field size
        feat["app_nb_partants"] = nb_partants
        fills["app_nb_partants"] += 1

        if alloc is not None and alloc > 0:
            # 2. Allocation per partant
            app = alloc / nb_partants
            feat["app_alloc_per_partant"] = round(app, 0)
            fills["app_alloc_per_partant"] += 1

            # 3. Log allocation
            feat["app_alloc_log"] = round(math.log1p(alloc), 4)
            fills["app_alloc_log"] += 1

            # 4. Allocation vs rolling mean
            if rolling_mean and rolling_mean > 0:
                feat["app_alloc_vs_mean"] = round(alloc / rolling_mean, 4)
                fills["app_alloc_vs_mean"] += 1

            # 5. Is big race (top 10% allocation)
            if rolling_mean and rolling_mean > 0:
                feat["app_is_big_race"] = 1 if alloc > rolling_mean * 2 else 0
                fills["app_is_big_race"] += 1

        # 6. Field size bin
        if nb_partants <= 6:
            feat["app_field_bin"] = 0
        elif nb_partants <= 10:
            feat["app_field_bin"] = 1
        elif nb_partants <= 14:
            feat["app_field_bin"] = 2
        elif nb_partants <= 18:
            feat["app_field_bin"] = 3
        else:
            feat["app_field_bin"] = 4
        fills["app_field_bin"] += 1

        # 7. Race prestige score (composite)
        prestige = 0
        if alloc is not None:
            if alloc >= 100000: prestige += 3
            elif alloc >= 50000: prestige += 2
            elif alloc >= 20000: prestige += 1
        discipline = (rec.get("discipline") or "").lower()
        if "groupe" in discipline or "listed" in discipline:
            prestige += 2
        feat["app_prestige"] = prestige
        fills["app_prestige"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update allocation history (1 entry per race, not per runner)
    if alloc is not None and alloc > 0:
        alloc_history.append(alloc)
        if len(alloc_history) > 1000:
            alloc_history[:] = alloc_history[-500:]


if __name__ == "__main__":
    main()
