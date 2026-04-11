#!/usr/bin/env python3
"""Days-since features: temporal gaps that strongly predict performance.
How many days since last race, last win, last place, first ever race.
Also tracks racing frequency (races per month). Streaming, low RAM."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/days_since")
_LOG_EVERY = 500_000


def _parse_date(s):
    """Parse YYYY-MM-DD to day ordinal for fast arithmetic."""
    if not s or not isinstance(s, str) or len(s) < 10:
        return None
    try:
        y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
        # Approximate day ordinal (good enough for deltas)
        return y * 365 + m * 30 + d
    except (ValueError, IndexError):
        return None


class _HorseTimeState:
    __slots__ = (
        "last_race_day", "last_win_day", "last_place_day",
        "first_race_day", "race_days", "total",
    )

    def __init__(self):
        self.last_race_day = None
        self.last_win_day = None
        self.last_place_day = None
        self.first_race_day = None
        self.race_days = []  # up to last 20 race days for frequency calc
        self.total = 0


class _JockeyTimeState:
    __slots__ = ("last_race_day", "last_win_day", "race_count_30d")

    def __init__(self):
        self.last_race_day = None
        self.last_win_day = None
        self.race_count_30d = 0


def main():
    logger = setup_logging("days_since_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "days_since_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseTimeState] = {}
    jockey_states: dict[str, _JockeyTimeState] = {}

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
                    _process_course(course_records, fout, horse_states, jockey_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states, jockey_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, jockey_states, fills):
    race_date = records[0].get("date_reunion_iso", "")
    today = _parse_date(race_date)

    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = rec.get("jockey_driver") or rec.get("nom_jockey") or ""

        if hid and today is not None:
            st = horse_states.get(hid)
            if st is not None and st.last_race_day is not None:
                # 1. Days since last race
                days_lr = today - st.last_race_day
                if days_lr >= 0:
                    feat["ds_days_since_last_race"] = days_lr
                    fills["ds_days_since_last_race"] += 1

                    # 2. Is fresh (>30 days rest)?
                    feat["ds_is_fresh"] = 1 if days_lr > 30 else 0
                    fills["ds_is_fresh"] += 1

                    # 3. Is quick turnaround (<10 days)?
                    feat["ds_is_quick_return"] = 1 if days_lr < 10 else 0
                    fills["ds_is_quick_return"] += 1

                # 4. Days since last win
                if st.last_win_day is not None:
                    days_lw = today - st.last_win_day
                    if days_lw >= 0:
                        feat["ds_days_since_last_win"] = days_lw
                        fills["ds_days_since_last_win"] += 1

                # 5. Days since last place (top 3)
                if st.last_place_day is not None:
                    days_lp = today - st.last_place_day
                    if days_lp >= 0:
                        feat["ds_days_since_last_place"] = days_lp
                        fills["ds_days_since_last_place"] += 1

                # 6. Career length in days
                if st.first_race_day is not None:
                    career = today - st.first_race_day
                    if career >= 0:
                        feat["ds_career_length_days"] = career
                        fills["ds_career_length_days"] += 1

                        # 7. Career intensity (races per 100 days)
                        if career > 30:
                            feat["ds_career_intensity"] = round(st.total / career * 100, 2)
                            fills["ds_career_intensity"] += 1

                # 8. Racing frequency (races in last ~60 days approximation)
                if st.race_days:
                    recent = [d for d in st.race_days if today - d <= 60]
                    feat["ds_races_last_60d"] = len(recent)
                    fills["ds_races_last_60d"] += 1

                    # 9. Average gap between races (regularity)
                    if len(st.race_days) >= 3:
                        gaps = [st.race_days[i] - st.race_days[i-1]
                                for i in range(1, len(st.race_days))
                                if st.race_days[i] - st.race_days[i-1] > 0]
                        if gaps:
                            feat["ds_avg_gap_days"] = round(sum(gaps) / len(gaps), 1)
                            fills["ds_avg_gap_days"] += 1

                            # 10. Current gap vs average (is this rest normal?)
                            if st.last_race_day is not None:
                                current_gap = today - st.last_race_day
                                avg_gap = sum(gaps) / len(gaps)
                                if avg_gap > 0:
                                    feat["ds_gap_vs_normal"] = round(current_gap / avg_gap, 3)
                                    fills["ds_gap_vs_normal"] += 1

        # 11. Jockey days since last win
        if jockey:
            js = jockey_states.get(jockey)
            if js and js.last_win_day is not None and today is not None:
                days_jw = today - js.last_win_day
                if days_jw >= 0:
                    feat["ds_jockey_days_since_win"] = days_jw
                    fills["ds_jockey_days_since_win"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = rec.get("jockey_driver") or rec.get("nom_jockey") or ""
        is_winner = bool(rec.get("is_gagnant"))
        is_placed = False
        try:
            is_placed = int(rec.get("position_arrivee", 99)) <= 3
        except (TypeError, ValueError):
            pass

        race_date = rec.get("date_reunion_iso", "")
        day = _parse_date(race_date)

        if hid and day is not None:
            if hid not in horse_states:
                horse_states[hid] = _HorseTimeState()
            st = horse_states[hid]
            st.last_race_day = day
            if is_winner:
                st.last_win_day = day
            if is_placed:
                st.last_place_day = day
            if st.first_race_day is None:
                st.first_race_day = day
            st.race_days.append(day)
            if len(st.race_days) > 20:
                st.race_days = st.race_days[-20:]
            st.total += 1

        if jockey and day is not None:
            if jockey not in jockey_states:
                jockey_states[jockey] = _JockeyTimeState()
            js = jockey_states[jockey]
            js.last_race_day = day
            if is_winner:
                js.last_win_day = day


if __name__ == "__main__":
    main()
