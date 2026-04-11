#!/usr/bin/env python3
"""Gains/ROI features: career earnings analysis, earnings per race,
earnings efficiency, ROI by cote bracket, and gains trajectory.
Crosses gains with age, distance, discipline, and field quality."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/gains_roi")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


class _HorseGainsState:
    __slots__ = ("prev_gains_career", "prev_gains_year", "total",
                 "cote_when_won", "cote_total")

    def __init__(self):
        self.prev_gains_career = None
        self.prev_gains_year = None
        self.total = 0
        self.cote_when_won = []  # cotes when this horse won
        self.cote_total = []     # all cotes


def main():
    logger = setup_logging("gains_roi_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "gains_roi_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseGainsState] = {}

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
    # Field gains stats
    field_gains = []
    for rec in records:
        g = _safe(rec.get("gains_carriere_euros"))
        if g: field_gains.append(g)

    field_avg_gains = sum(field_gains) / len(field_gains) if field_gains else None
    field_med_gains = sorted(field_gains)[len(field_gains) // 2] if field_gains else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        gains_career = _safe(rec.get("gains_carriere_euros"))
        gains_year = _safe(rec.get("gains_annee_euros"))
        nb_courses = _safe(rec.get("nb_courses_carriere"))
        nb_wins = _safe(rec.get("nb_victoires_carriere"))
        nb_places = _safe(rec.get("nb_places_carriere"))
        age = _safe(rec.get("age"))
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))

        # 1. Gains per race
        if gains_career and nb_courses and nb_courses >= 3:
            gpr = gains_career / nb_courses
            feat["gr_gains_per_race"] = round(gpr, 0)
            fills["gr_gains_per_race"] += 1

            # 2. Gains per win
            if nb_wins and nb_wins >= 1:
                feat["gr_gains_per_win"] = round(gains_career / nb_wins, 0)
                fills["gr_gains_per_win"] += 1

            # 3. Gains vs field
            if field_avg_gains:
                feat["gr_gains_vs_field_avg"] = round(gains_career - field_avg_gains, 0)
                feat["gr_gains_ratio_field"] = round(gains_career / field_avg_gains, 3) if field_avg_gains > 0 else None
                fills["gr_gains_vs_field_avg"] += 1
                if feat.get("gr_gains_ratio_field") is not None:
                    fills["gr_gains_ratio_field"] += 1

            # 4. Log gains (diminishing returns)
            feat["gr_log_gains"] = round(math.log1p(gains_career), 3)
            fills["gr_log_gains"] += 1

        # 5. Year gains ratio (current form)
        if gains_career and gains_year:
            feat["gr_year_pct_of_career"] = round(gains_year / gains_career, 4)
            fills["gr_year_pct_of_career"] += 1

        # 6. Gains per age year
        if gains_career and age and age >= 2:
            racing_years = max(age - 1, 1)
            feat["gr_gains_per_year"] = round(gains_career / racing_years, 0)
            fills["gr_gains_per_year"] += 1

        # 7. Win rate × gains efficiency
        if nb_wins is not None and nb_courses and nb_courses >= 5:
            wr = nb_wins / nb_courses
            feat["gr_career_wr"] = round(wr, 4)
            fills["gr_career_wr"] += 1

            if nb_places is not None:
                feat["gr_career_pr"] = round(nb_places / nb_courses, 4)
                fills["gr_career_pr"] += 1

        # 8. Gains trajectory (comparing with previous observation)
        if hid:
            st = horse_states.get(hid)
            if st and st.total >= 2:
                if st.prev_gains_career and gains_career:
                    delta = gains_career - st.prev_gains_career
                    feat["gr_gains_delta"] = round(delta, 0)
                    fills["gr_gains_delta"] += 1

                # 9. ROI proxy from historical cotes
                if len(st.cote_total) >= 5:
                    total_bet = len(st.cote_total)
                    total_return = sum(st.cote_when_won)
                    roi = (total_return - total_bet) / total_bet
                    feat["gr_roi_historical"] = round(roi, 4)
                    fills["gr_roi_historical"] += 1

        # 10. Gains rank in field
        if gains_career and field_gains:
            rank = sum(1 for g in field_gains if g > gains_career) + 1
            feat["gr_gains_rank"] = rank
            fills["gr_gains_rank"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue
        gains_career = _safe(rec.get("gains_carriere_euros"))
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseGainsState()
        st = horse_states[hid]
        st.prev_gains_career = gains_career
        st.prev_gains_year = _safe(rec.get("gains_annee_euros"))
        st.total += 1
        if cote:
            st.cote_total.append(1)  # 1 unit bet
            if is_winner:
                st.cote_when_won.append(cote)
            # Keep limited
            if len(st.cote_total) > 50:
                st.cote_total = st.cote_total[-50:]
            if len(st.cote_when_won) > 20:
                st.cote_when_won = st.cote_when_won[-20:]


if __name__ == "__main__":
    main()
