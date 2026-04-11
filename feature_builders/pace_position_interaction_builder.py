#!/usr/bin/env python3
"""Pace-position interaction features: how a horse's running style interacts with
draw position, distance, and field size. Front-runners from wide draws lose more;
closers need bigger fields. Critical for race simulation models."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pace_position_interaction")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _dist_bucket(d):
    if d is None:
        return None
    if d < 1400:
        return "sprint"
    if d < 1800:
        return "mile"
    if d < 2200:
        return "inter"
    return "stay"


class _HorsePaceState:
    __slots__ = (
        "avg_early_pos", "total", "positions",
        "wins_inside", "total_inside", "wins_outside", "total_outside",
        "wins_small_field", "total_small_field", "wins_big_field", "total_big_field",
        "style_scores",  # deque of early position proxies
    )

    def __init__(self):
        self.avg_early_pos = None
        self.total = 0
        self.positions = deque(maxlen=20)  # normalized positions
        self.wins_inside = 0
        self.total_inside = 0
        self.wins_outside = 0
        self.total_outside = 0
        self.wins_small_field = 0  # <10 runners
        self.total_small_field = 0
        self.wins_big_field = 0    # >=10 runners
        self.total_big_field = 0
        self.style_scores = deque(maxlen=20)


def main():
    logger = setup_logging("pace_position_interaction_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "pace_position_interaction_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorsePaceState] = {}

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
    field_size = len(records)
    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""

        if not hid:
            features_list.append(feat)
            continue

        draw = _safe(rec.get("numero_corde")) or _safe(rec.get("numero"))
        distance = _safe(rec.get("distance"))
        dist_b = _dist_bucket(distance)

        st = horse_states.get(hid)

        if st is not None and st.total >= 3:
            # Running style proxy (based on historical positions — lower = front-runner tendency)
            style_list = list(st.style_scores)
            if style_list:
                avg_style = sum(style_list) / len(style_list)

                # 1. Running style score (0=front-runner, 1=closer)
                feat["ppi_running_style"] = round(avg_style, 4)
                fills["ppi_running_style"] += 1

                # 2. Style × draw interaction (front-runners penalized by wide draw)
                if draw is not None and field_size > 0:
                    norm_draw = draw / field_size
                    feat["ppi_style_x_draw"] = round(avg_style * norm_draw, 4)
                    fills["ppi_style_x_draw"] += 1

                    # 3. Front-runner from wide draw (penalty signal)
                    if avg_style < 0.35 and norm_draw > 0.6:
                        feat["ppi_frontrunner_wide_draw"] = 1
                    else:
                        feat["ppi_frontrunner_wide_draw"] = 0
                    fills["ppi_frontrunner_wide_draw"] += 1

                    # 4. Closer in small field (penalty signal)
                    if avg_style > 0.65 and field_size < 8:
                        feat["ppi_closer_small_field"] = 1
                    else:
                        feat["ppi_closer_small_field"] = 0
                    fills["ppi_closer_small_field"] += 1

                # 5. Style × distance interaction
                if dist_b:
                    style_dist_bonus = 0.0
                    if dist_b == "sprint" and avg_style < 0.35:
                        style_dist_bonus = 0.3  # Front-runners good in sprints
                    elif dist_b in ("stay",) and avg_style > 0.5:
                        style_dist_bonus = 0.2  # Closers good in staying races
                    elif dist_b == "sprint" and avg_style > 0.65:
                        style_dist_bonus = -0.2  # Closers bad in sprints
                    feat["ppi_style_dist_bonus"] = round(style_dist_bonus, 3)
                    fills["ppi_style_dist_bonus"] += 1

                # 6. Style × field size interaction
                feat["ppi_style_x_field"] = round(avg_style * (field_size / 16.0), 4)
                fills["ppi_style_x_field"] += 1

            # 7. Inside draw performance
            if st.total_inside >= 3:
                feat["ppi_wr_inside_draw"] = round(st.wins_inside / st.total_inside, 4)
                fills["ppi_wr_inside_draw"] += 1

            # 8. Outside draw performance
            if st.total_outside >= 3:
                feat["ppi_wr_outside_draw"] = round(st.wins_outside / st.total_outside, 4)
                fills["ppi_wr_outside_draw"] += 1

            # 9. Draw preference (inside vs outside win rate diff)
            if st.total_inside >= 3 and st.total_outside >= 3:
                inside_wr = st.wins_inside / st.total_inside
                outside_wr = st.wins_outside / st.total_outside
                feat["ppi_draw_preference"] = round(inside_wr - outside_wr, 4)
                fills["ppi_draw_preference"] += 1

            # 10. Small vs big field performance
            if st.total_small_field >= 3 and st.total_big_field >= 3:
                small_wr = st.wins_small_field / st.total_small_field
                big_wr = st.wins_big_field / st.total_big_field
                feat["ppi_field_size_pref"] = round(small_wr - big_wr, 4)
                fills["ppi_field_size_pref"] += 1

            # 11. Current draw advantage (is this draw favorable for this horse?)
            if draw is not None and field_size > 0:
                norm_draw = draw / field_size
                is_inside = norm_draw <= 0.4
                if is_inside and st.total_inside >= 3:
                    feat["ppi_draw_advantage"] = round(st.wins_inside / st.total_inside, 4)
                elif not is_inside and st.total_outside >= 3:
                    feat["ppi_draw_advantage"] = round(st.wins_outside / st.total_outside, 4)
                if "ppi_draw_advantage" in feat:
                    fills["ppi_draw_advantage"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue

        pos = _safe(rec.get("position_arrivee"))
        field = _safe(rec.get("nombre_partants"))
        draw = _safe(rec.get("numero_corde")) or _safe(rec.get("numero"))
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorsePaceState()
        st = horse_states[hid]

        norm_pos = pos / field if pos is not None and field and field > 0 else None

        if norm_pos is not None:
            st.positions.append(norm_pos)
            st.style_scores.append(norm_pos)  # Proxy: low finish = front-runner tendency

        # Track draw performance
        if draw is not None and field is not None and field > 0:
            norm_draw = draw / field
            if norm_draw <= 0.4:
                st.total_inside += 1
                st.wins_inside += int(is_winner)
            else:
                st.total_outside += 1
                st.wins_outside += int(is_winner)

        # Track field size performance
        if field is not None:
            if field < 10:
                st.total_small_field += 1
                st.wins_small_field += int(is_winner)
            else:
                st.total_big_field += 1
                st.wins_big_field += int(is_winner)

        st.total += 1


if __name__ == "__main__":
    main()
