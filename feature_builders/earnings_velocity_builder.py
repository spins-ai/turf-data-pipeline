#!/usr/bin/env python3
from __future__ import annotations
import argparse, gc, json, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/earnings_velocity")
_LOG_EVERY = 500_000

def _safe(val):
    try: return float(val)
    except: return None

def main():
    logger = setup_logging("earnings_velocity_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "earnings_velocity_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # State: track last known earnings per horse
    horse_prev_earnings = {}  # horse_id -> last gains_carriere
    horse_earnings_history = defaultdict(lambda: deque(maxlen=20))  # rolling earnings snapshots
    horse_velocity_history = defaultdict(lambda: deque(maxlen=10))  # rolling velocity

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
                    _process_course(course_records, fout, horse_prev_earnings,
                                   horse_earnings_history, horse_velocity_history, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,} lines, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_prev_earnings,
                           horse_earnings_history, horse_velocity_history, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v/total*100:.1f}%")


def _process_course(records, fout, prev_earnings, earnings_hist, velocity_hist, fills):
    # Compute field average earnings for relative features
    field_earnings = []
    for rec in records:
        gc_val = _safe(rec.get("gains_carriere_euros"))
        if gc_val is not None:
            field_earnings.append(gc_val)
    field_avg = sum(field_earnings) / len(field_earnings) if field_earnings else None

    features_list = []
    for rec in records:
        hid = rec.get("horse_id", "")
        feat = {"partant_uid": rec.get("partant_uid", "")}

        gc_val = _safe(rec.get("gains_carriere_euros"))
        ga_val = _safe(rec.get("gains_annee_euros"))
        nb_courses = _safe(rec.get("nb_courses_carriere"))
        nb_wins = _safe(rec.get("nb_victoires_carriere"))
        nb_places = _safe(rec.get("nb_places_carriere"))
        age = _safe(rec.get("age"))
        engagement = _safe(rec.get("engagement"))

        # 1. Earnings per race
        if gc_val is not None and nb_courses and nb_courses > 0:
            feat["ev_earnings_per_race"] = gc_val / nb_courses
            fills["ev_earnings_per_race"] += 1

        # 2. Win earnings ratio
        if gc_val is not None and nb_wins and nb_wins > 0:
            feat["ev_earnings_per_win"] = gc_val / nb_wins
            fills["ev_earnings_per_win"] += 1

        # 3. Place earnings ratio
        if gc_val is not None and nb_places and nb_places > 0:
            feat["ev_earnings_per_place"] = gc_val / nb_places
            fills["ev_earnings_per_place"] += 1

        # 4. Earnings velocity (delta since last seen)
        prev = prev_earnings.get(hid)
        if prev is not None and gc_val is not None:
            velocity = gc_val - prev
            feat["ev_earnings_velocity"] = velocity
            fills["ev_earnings_velocity"] += 1

            # 8. Acceleration
            vhist = list(velocity_hist.get(hid, []))
            if vhist:
                feat["ev_earnings_acceleration"] = velocity - vhist[-1]
                feat["ev_avg_velocity"] = sum(vhist) / len(vhist)
                fills["ev_earnings_acceleration"] += 1

        # 5. Earnings growth rate
        if prev is not None and gc_val is not None and prev > 0:
            feat["ev_earnings_growth_rate"] = (gc_val - prev) / prev
            fills["ev_earnings_growth_rate"] += 1

        # 6. Earnings per year
        if gc_val is not None and age and age > 1:
            racing_years = age - 1  # horses start racing at ~2
            feat["ev_earnings_per_year"] = gc_val / max(racing_years, 1)
            fills["ev_earnings_per_year"] += 1

        # 7. ROI proxy
        if gc_val is not None and engagement and engagement > 0:
            feat["ev_roi_proxy"] = gc_val / engagement
            fills["ev_roi_proxy"] += 1

        # 9. Rolling earnings trend
        ehist = list(earnings_hist.get(hid, []))
        if len(ehist) >= 3:
            recent = ehist[-3:]
            feat["ev_earnings_trend_3"] = recent[-1] - recent[0]
            fills["ev_earnings_trend_3"] += 1
        if len(ehist) >= 5:
            recent5 = ehist[-5:]
            feat["ev_earnings_trend_5"] = recent5[-1] - recent5[0]
            fills["ev_earnings_trend_5"] += 1

        # 10. Earnings vs field
        if gc_val is not None and field_avg and field_avg > 0:
            feat["ev_earnings_vs_field"] = gc_val / field_avg
            fills["ev_earnings_vs_field"] += 1

        # Annual earnings ratio
        if gc_val is not None and ga_val is not None and gc_val > 0:
            feat["ev_annual_earnings_ratio"] = ga_val / gc_val
            fills["ev_annual_earnings_ratio"] += 1

        # Win rate value (earnings efficiency)
        if nb_wins is not None and nb_courses and nb_courses > 0:
            win_rate = nb_wins / nb_courses
            if gc_val is not None and gc_val > 0:
                feat["ev_earnings_efficiency"] = win_rate * gc_val / nb_courses
                fills["ev_earnings_efficiency"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features emitted
    for rec in records:
        hid = rec.get("horse_id", "")
        gc_val = _safe(rec.get("gains_carriere_euros"))
        if gc_val is not None:
            prev = prev_earnings.get(hid)
            if prev is not None:
                velocity = gc_val - prev
                velocity_hist[hid].append(velocity)
            prev_earnings[hid] = gc_val
            earnings_hist[hid].append(gc_val)


if __name__ == "__main__":
    main()
