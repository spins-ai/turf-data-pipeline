#!/usr/bin/env python3
"""Speed form composite: contextual speed features combining temps_ms, reduction_km_ms
and spd_speed_figure relative to current race conditions and horse history."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/speed_form_composite")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _dist_bucket(d):
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "stay"
    return "long"


class _HorseSpeedState:
    __slots__ = ("speeds", "speed_by_dist", "best_speed", "total",
                 "speed_figures", "times_by_dist")

    def __init__(self):
        self.speeds = deque(maxlen=20)  # reduction_km_ms
        self.speed_figures = deque(maxlen=20)
        self.speed_by_dist = defaultdict(lambda: deque(maxlen=10))
        self.times_by_dist = defaultdict(lambda: deque(maxlen=10))
        self.best_speed = None
        self.total = 0


def main():
    logger = setup_logging("speed_form_composite_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "speed_form_composite_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseSpeedState] = {}

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        current_course = None
        course_records: list[dict] = []

        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                cuid = rec.get("course_uid", "")

                if cuid != current_course and course_records:
                    _process_course(course_records, fout, horse_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, fills):
    distance = _safe(records[0].get("distance"))
    dist_b = _dist_bucket(distance)

    # Field speed stats
    field_speeds = []
    field_figs = []
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        st = horse_states.get(hid)
        if st and st.speeds:
            avg_spd = sum(st.speeds) / len(st.speeds)
            field_speeds.append(avg_spd)
        if st and st.speed_figures:
            avg_fig = sum(st.speed_figures) / len(st.speed_figures)
            field_figs.append(avg_fig)

    field_spd_avg = sum(field_speeds) / len(field_speeds) if field_speeds else None
    field_fig_avg = sum(field_figs) / len(field_figs) if field_figs else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""

        if not hid:
            features_list.append(feat)
            continue

        st = horse_states.get(hid)
        if st is not None and st.total >= 3 and st.speeds:
            speeds = list(st.speeds)
            avg_speed = sum(speeds) / len(speeds)

            # 1. Average speed vs field
            if field_spd_avg is not None and field_spd_avg > 0:
                feat["sfc_speed_vs_field"] = round(avg_speed - field_spd_avg, 1)
                fills["sfc_speed_vs_field"] += 1

            # 2. Best speed ever
            if st.best_speed is not None:
                feat["sfc_best_speed"] = round(st.best_speed, 0)
                fills["sfc_best_speed"] += 1

                # 3. Current avg vs personal best
                feat["sfc_avg_vs_best"] = round(avg_speed - st.best_speed, 1)
                fills["sfc_avg_vs_best"] += 1

            # 4. Speed at THIS distance
            if dist_b and dist_b in st.speed_by_dist:
                dist_speeds = list(st.speed_by_dist[dist_b])
                if dist_speeds:
                    avg_at_dist = sum(dist_speeds) / len(dist_speeds)
                    feat["sfc_speed_at_distance"] = round(avg_at_dist, 0)
                    fills["sfc_speed_at_distance"] += 1

                    # 5. Speed at this distance vs overall
                    feat["sfc_dist_speed_diff"] = round(avg_at_dist - avg_speed, 1)
                    fills["sfc_dist_speed_diff"] += 1

            # 6. Speed trend (last 3 vs prev 3)
            if len(speeds) >= 6:
                recent = sum(speeds[:3]) / 3
                older = sum(speeds[3:6]) / 3
                if older > 0:
                    feat["sfc_speed_trend"] = round((recent - older) / older * 100, 2)
                    fills["sfc_speed_trend"] += 1

            # 7. Speed consistency (CV)
            if len(speeds) >= 3:
                mean_s = sum(speeds) / len(speeds)
                if mean_s > 0:
                    std_s = math.sqrt(sum((s - mean_s) ** 2 for s in speeds) / len(speeds))
                    feat["sfc_speed_cv"] = round(std_s / mean_s, 4)
                    fills["sfc_speed_cv"] += 1

            # 8. Speed figure composite
            if st.speed_figures:
                figs = list(st.speed_figures)
                feat["sfc_avg_speed_figure"] = round(sum(figs) / len(figs), 2)
                fills["sfc_avg_speed_figure"] += 1

                if field_fig_avg is not None:
                    feat["sfc_figure_vs_field"] = round(sum(figs) / len(figs) - field_fig_avg, 2)
                    fills["sfc_figure_vs_field"] += 1

            # 9. Recent speed (last 2 races)
            if len(speeds) >= 2:
                recent_avg = sum(speeds[:2]) / 2
                feat["sfc_recent_speed"] = round(recent_avg, 0)
                fills["sfc_recent_speed"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue

        speed = _safe(rec.get("reduction_km_ms"))
        fig = _safe(rec.get("spd_speed_figure"))

        if hid not in horse_states:
            horse_states[hid] = _HorseSpeedState()
        st = horse_states[hid]

        if speed is not None:
            st.speeds.append(speed)
            if st.best_speed is None or speed < st.best_speed:  # Lower = faster
                st.best_speed = speed
            if dist_b:
                st.speed_by_dist[dist_b].append(speed)

        if fig is not None:
            st.speed_figures.append(fig)

        st.total += 1


if __name__ == "__main__":
    main()
