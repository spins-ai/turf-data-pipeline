#!/usr/bin/env python3
"""Distance aptitude confidence builder - STREAMING. 7 features:
horse_pref_distance, horse_distance_range, distance_match_score,
horse_dist_cat_wr, distance_experience, optimal_dist_deviation,
distance_confidence.

Builds a horse's distance preference profile with confidence intervals.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/distance_aptitude_confidence")
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
    logger = setup_logging("distance_aptitude_confidence_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "distance_aptitude_confidence_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # horse -> {dist_cat -> [wins, places, total]}
    horse_dist = defaultdict(lambda: defaultdict(lambda: [0, 0, 0]))
    # horse -> list of (distance_m, position) for winning/placed races
    horse_win_dists = defaultdict(list)  # bounded to 30

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
                    _process_course(course_records, fout, horse_dist, horse_win_dists, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_dist, horse_win_dists, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_dist, horse_win_dists, fills):
    r0 = records[0]
    curr_dist = _safe(r0.get("distance"))
    curr_dc = _dist_cat(curr_dist)

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()

        if horse and len(horse) >= 2:
            dist_data = horse_dist.get(horse)
            win_dists = horse_win_dists.get(horse, [])

            if dist_data:
                total_runs = sum(t for w, p, t in dist_data.values())

                if total_runs >= 3:
                    # 1. Current distance category WR
                    if curr_dc and curr_dc in dist_data:
                        w, p, t = dist_data[curr_dc]
                        if t >= 2:
                            feat["dac_dist_cat_wr"] = round(w / t, 4)
                            fills["dac_dist_cat_wr"] += 1

                    # 2. Distance experience (nb runs at this dist category)
                    if curr_dc and curr_dc in dist_data:
                        feat["dac_dist_experience"] = dist_data[curr_dc][2]
                        fills["dac_dist_experience"] += 1

                    # 3. Best distance category
                    best_wr = 0
                    best_dc = None
                    for dc, (w, p, t) in dist_data.items():
                        if t >= 2:
                            wr = w / t
                            if wr > best_wr:
                                best_wr = wr
                                best_dc = dc

                    # 4. Optimal distance from wins
                    if win_dists and len(win_dists) >= 2:
                        avg_win_dist = sum(win_dists) / len(win_dists)
                        feat["dac_pref_distance"] = round(avg_win_dist, 0)
                        fills["dac_pref_distance"] += 1

                        # Distance range
                        d_range = max(win_dists) - min(win_dists)
                        feat["dac_distance_range"] = round(d_range, 0)
                        fills["dac_distance_range"] += 1

                        # 5. Match score (how close is current distance to preference)
                        if curr_dist:
                            deviation = abs(curr_dist - avg_win_dist)
                            feat["dac_dist_deviation"] = round(deviation, 0)
                            fills["dac_dist_deviation"] += 1

                            # Normalized match (0-1, 1=perfect)
                            match = max(0, 1 - deviation / 1000)
                            feat["dac_dist_match"] = round(match, 4)
                            fills["dac_dist_match"] += 1

                        # 6. Confidence (more wins = higher confidence)
                        confidence = min(len(win_dists) / 10, 1.0)
                        feat["dac_confidence"] = round(confidence, 4)
                        fills["dac_confidence"] += 1

                        # 7. Std of winning distances
                        if len(win_dists) >= 3:
                            mean = sum(win_dists) / len(win_dists)
                            var = sum((d - mean) ** 2 for d in win_dists) / len(win_dists)
                            feat["dac_dist_std"] = round(math.sqrt(var), 0)
                            fills["dac_dist_std"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE
    for rec in records:
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        is_winner = bool(rec.get("is_gagnant"))
        is_placed = bool(rec.get("is_place"))

        if horse and len(horse) >= 2 and curr_dc:
            horse_dist[horse][curr_dc][2] += 1
            if is_winner: horse_dist[horse][curr_dc][0] += 1
            if is_placed: horse_dist[horse][curr_dc][1] += 1

            if is_winner and curr_dist:
                wl = horse_win_dists[horse]
                wl.append(int(curr_dist))
                if len(wl) > 30:
                    horse_win_dists[horse] = wl[-30:]


if __name__ == "__main__":
    main()
