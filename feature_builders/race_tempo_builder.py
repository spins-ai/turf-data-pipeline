#!/usr/bin/env python3
"""Race tempo prediction builder - STREAMING. 7 features:
nb_front_runners, predicted_pace, pace_pressure, pace_advantage,
distance_pace_interaction, field_speed_composite, pace_collapse_risk.

Uses historical corde/position data to estimate race pace scenario.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_tempo")
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
    logger = setup_logging("race_tempo_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "race_tempo_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Horse front-running tendency: horse -> deque of (was_front, was_winner)
    horse_front = defaultdict(lambda: [0, 0])  # [front_count, total]
    # Dist × pace interaction
    dist_pace = defaultdict(lambda: [0, 0])  # dist_cat -> [fast_pace_wins, total]

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
                    _process_course(course_records, fout, horse_front, dist_pace, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_front, dist_pace, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_front, dist_pace, fills):
    nb_partants = len(records)
    distance = _safe(records[0].get("distance"))
    dc = _dist_cat(distance)

    # Estimate front-running tendencies
    front_probs = []
    for rec in records:
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        if horse and len(horse) >= 2:
            h = horse_front.get(horse)
            if h and h[1] >= 3:
                front_probs.append(h[0] / h[1])
            else:
                front_probs.append(None)
        else:
            front_probs.append(None)

    # Count likely front-runners (>30% front rate)
    known_probs = [p for p in front_probs if p is not None]
    nb_likely_front = sum(1 for p in known_probs if p > 0.3)
    avg_front_rate = sum(known_probs) / len(known_probs) if known_probs else None

    # SNAPSHOT features
    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        # 1. Nb of likely front-runners in race
        if len(known_probs) >= 3:
            feat["rt_nb_front_runners"] = nb_likely_front
            fills["rt_nb_front_runners"] += 1

            # 2. Pace pressure (ratio of front runners to field)
            feat["rt_pace_pressure"] = round(nb_likely_front / nb_partants, 4)
            fills["rt_pace_pressure"] += 1

            # 3. Average field front tendency
            feat["rt_field_front_avg"] = round(avg_front_rate, 4)
            fills["rt_field_front_avg"] += 1

        # 4. This horse's front tendency
        my_prob = front_probs[i]
        if my_prob is not None:
            feat["rt_horse_front_rate"] = round(my_prob, 4)
            fills["rt_horse_front_rate"] += 1

            # 5. Pace advantage: front-runner in slow pace, or closer in fast pace
            if len(known_probs) >= 3:
                if nb_likely_front <= 1 and my_prob > 0.3:
                    feat["rt_pace_advantage"] = round(0.5 + my_prob, 4)
                elif nb_likely_front >= 3 and my_prob < 0.2:
                    feat["rt_pace_advantage"] = round(0.5 + (1 - my_prob) * 0.3, 4)
                else:
                    feat["rt_pace_advantage"] = 0.5
                fills["rt_pace_advantage"] += 1

        # 6. Distance × pace interaction
        if dc:
            h = dist_pace.get(dc)
            if h and h[1] >= 20:
                feat["rt_dist_pace_wr"] = round(h[0] / h[1], 4)
                fills["rt_dist_pace_wr"] += 1

        # 7. Pace collapse risk (many front-runners + long distance)
        if len(known_probs) >= 3 and distance:
            dist_factor = min(distance / 2000, 1.5) if distance else 1.0
            feat["rt_collapse_risk"] = round(nb_likely_front * dist_factor / nb_partants, 4)
            fills["rt_collapse_risk"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        place_corde = _safe(rec.get("place_corde"))
        is_winner = bool(rec.get("is_gagnant"))

        # Estimate "front-runner" from low corde number
        is_front = place_corde is not None and place_corde <= 4

        if horse and len(horse) >= 2:
            horse_front[horse][1] += 1
            if is_front:
                horse_front[horse][0] += 1

    # Distance × front-runner pace interaction
    if dc and nb_likely_front >= 0:
        is_fast_pace = nb_likely_front >= 3
        winner_was_front = False
        for i, rec in enumerate(records):
            if bool(rec.get("is_gagnant")):
                p = front_probs[i]
                if p is not None and p > 0.3:
                    winner_was_front = True
        if is_fast_pace:
            dist_pace[dc][1] += 1
            if winner_was_front:
                dist_pace[dc][0] += 1


if __name__ == "__main__":
    main()
