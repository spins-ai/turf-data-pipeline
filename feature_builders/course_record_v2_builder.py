#!/usr/bin/env python3
"""Course record comparison V2 - STREAMING. 6 features:
reduction_vs_track_best, reduction_vs_track_avg, speed_percentile,
horse_vs_personal_best, is_personal_best, horse_avg_speed.

Compares horse times to hippodrome track records.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/course_record_v2")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _dist_cat(distance):
    d = _safe(distance)
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "long"
    return "marathon"


def main():
    logger = setup_logging("course_record_v2_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "course_record_v2_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    track_speeds = defaultdict(lambda: deque(maxlen=200))
    track_best = {}
    horse_speeds = defaultdict(lambda: deque(maxlen=30))

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
                    _process_course(course_records, fout, track_speeds, track_best, horse_speeds, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, track_speeds, track_best, horse_speeds, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, track_speeds, track_best, horse_speeds, fills):
    r0 = records[0]
    hippo = (r0.get("hippodrome_normalise") or "").lower().strip()
    dc = _dist_cat(r0.get("distance"))
    track_key = f"{hippo}|{dc}" if hippo and dc else None

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        red_km = _safe(rec.get("reduction_km_ms"))

        if red_km and red_km > 0 and track_key:
            th = track_speeds.get(track_key)
            if th and len(th) >= 10:
                speeds = list(th)
                tb = track_best.get(track_key)
                if tb:
                    feat["crv_vs_track_best"] = round(red_km - tb, 0)
                    fills["crv_vs_track_best"] += 1
                avg_speed = sum(speeds) / len(speeds)
                feat["crv_vs_track_avg"] = round(red_km - avg_speed, 0)
                fills["crv_vs_track_avg"] += 1
                slower = sum(1 for s in speeds if s > red_km)
                feat["crv_speed_pct"] = round(slower / len(speeds), 4)
                fills["crv_speed_pct"] += 1

        if horse and len(horse) >= 2 and red_km and red_km > 0:
            hs = horse_speeds.get(horse)
            if hs and len(hs) >= 3:
                speeds = list(hs)
                best = min(speeds)
                feat["crv_vs_pb"] = round(red_km - best, 0)
                fills["crv_vs_pb"] += 1
                feat["crv_is_pb"] = 1 if red_km <= best else 0
                fills["crv_is_pb"] += 1
                feat["crv_avg_speed"] = round(sum(speeds) / len(speeds), 0)
                fills["crv_avg_speed"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    for rec in records:
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        red_km = _safe(rec.get("reduction_km_ms"))
        if red_km and red_km > 0:
            if track_key:
                track_speeds[track_key].append(red_km)
                tb = track_best.get(track_key)
                if tb is None or red_km < tb:
                    track_best[track_key] = red_km
            if horse and len(horse) >= 2:
                horse_speeds[horse].append(red_km)


if __name__ == "__main__":
    main()
