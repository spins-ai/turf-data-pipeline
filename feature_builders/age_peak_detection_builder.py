#!/usr/bin/env python3
"""Age peak detection features: model the age-performance curve,
detect if horse is before/at/past peak, age×speed interaction,
age×gains efficiency, and age class analysis."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/age_peak_detection")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


# Global age→performance stats
_age_wins: dict[int, int] = defaultdict(int)
_age_total: dict[int, int] = defaultdict(int)
_age_speeds: dict[int, list] = defaultdict(list)


class _HorseAgeState:
    __slots__ = ("age_pos_history", "best_age", "best_wr", "total")

    def __init__(self):
        self.age_pos_history = {}  # age -> (wins, total)
        self.best_age = None
        self.best_wr = 0.0
        self.total = 0


def main():
    logger = setup_logging("age_peak_detection_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "age_peak_detection_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseAgeState] = {}

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
    # Field age stats
    field_ages = []
    for rec in records:
        a = _safe(rec.get("age"))
        if a: field_ages.append(a)

    field_avg_age = sum(field_ages) / len(field_ages) if field_ages else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        age = _safe(rec.get("age"))
        speed_fig = _safe(rec.get("spd_speed_figure"))
        gains = _safe(rec.get("gains_carriere_euros"))
        nb_courses = _safe(rec.get("nb_courses_carriere"))
        discipline = (rec.get("discipline") or "").lower()

        if age is not None:
            age_int = int(age)

            # 1. Age vs field
            if field_avg_age:
                feat["apd_age_vs_field"] = round(age - field_avg_age, 1)
                fills["apd_age_vs_field"] += 1

            # 2. Global age win rate
            if _age_total.get(age_int, 0) >= 100:
                feat["apd_age_global_wr"] = round(_age_wins[age_int] / _age_total[age_int], 5)
                fills["apd_age_global_wr"] += 1

            # 3. Age² (quadratic for peak detection)
            feat["apd_age_sq"] = age_int * age_int
            fills["apd_age_sq"] += 1

            # 4. Is prime age (3-5 for galop, 4-8 for trot)
            if "trot" in discipline:
                feat["apd_is_prime"] = 1 if 4 <= age_int <= 8 else 0
            else:
                feat["apd_is_prime"] = 1 if 3 <= age_int <= 5 else 0
            fills["apd_is_prime"] += 1

            # 5. Age × speed interaction
            if speed_fig:
                feat["apd_age_x_speed"] = round(age * speed_fig, 1)
                fills["apd_age_x_speed"] += 1

                # Speed relative to age cohort
                age_spds = _age_speeds.get(age_int, [])
                if len(age_spds) >= 20:
                    avg_spd = sum(age_spds) / len(age_spds)
                    feat["apd_speed_vs_age_cohort"] = round(speed_fig - avg_spd, 2)
                    fills["apd_speed_vs_age_cohort"] += 1

            # 6. Gains efficiency by age
            if gains and nb_courses and nb_courses >= 3:
                racing_years = max(age - 1, 1)
                feat["apd_gains_per_age_year"] = round(gains / racing_years, 0)
                fills["apd_gains_per_age_year"] += 1

            # Horse-specific age analysis
            if hid:
                st = horse_states.get(hid)
                if st and st.total >= 5:
                    # 7. Is at best age?
                    if st.best_age is not None:
                        feat["apd_past_peak"] = 1 if age_int > st.best_age + 1 else 0
                        feat["apd_years_from_peak"] = age_int - st.best_age
                        fills["apd_past_peak"] += 1
                        fills["apd_years_from_peak"] += 1

                    # 8. Career stage
                    if nb_courses:
                        if nb_courses < 10:
                            feat["apd_career_stage"] = 0  # early
                        elif nb_courses < 30:
                            feat["apd_career_stage"] = 1  # developing
                        elif nb_courses < 60:
                            feat["apd_career_stage"] = 2  # peak
                        else:
                            feat["apd_career_stage"] = 3  # veteran
                        fills["apd_career_stage"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        age = _safe(rec.get("age"))
        is_winner = bool(rec.get("is_gagnant"))
        speed_fig = _safe(rec.get("spd_speed_figure"))

        if age is not None:
            age_int = int(age)
            _age_wins[age_int] += int(is_winner)
            _age_total[age_int] += 1
            if speed_fig:
                spds = _age_speeds[age_int]
                spds.append(speed_fig)
                if len(spds) > 100:
                    _age_speeds[age_int] = spds[-100:]

        if hid and age:
            age_int = int(age)
            if hid not in horse_states:
                horse_states[hid] = _HorseAgeState()
            st = horse_states[hid]
            if age_int not in st.age_pos_history:
                st.age_pos_history[age_int] = [0, 0]
            st.age_pos_history[age_int][0] += int(is_winner)
            st.age_pos_history[age_int][1] += 1

            # Update best age
            for a, (w, t) in st.age_pos_history.items():
                if t >= 3:
                    wr = w / t
                    if wr > st.best_wr:
                        st.best_wr = wr
                        st.best_age = a
            st.total += 1


if __name__ == "__main__":
    main()
