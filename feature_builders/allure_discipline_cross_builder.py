#!/usr/bin/env python3
"""Allure × discipline cross features: trot/galop performance differences,
discipline switch detection, and allure-specific form patterns.
Also crosses discipline with speed, gains, and jockey stats."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/allure_discipline_cross")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _norm_disc(val):
    if not val:
        return ""
    v = val.lower().strip()
    if "attel" in v:
        return "trot_attele"
    if "mont" in v and "trot" in v:
        return "trot_monte"
    if "plat" in v:
        return "plat"
    if "haie" in v:
        return "haies"
    if "steeple" in v or "chase" in v:
        return "steeple"
    if "cross" in v:
        return "cross"
    if "trot" in v:
        return "trot"
    if "galop" in v:
        return "galop"
    return v


class _HorseDiscState:
    __slots__ = ("prev_disc", "disc_wins", "disc_total", "disc_speeds",
                 "total", "wins")

    def __init__(self):
        self.prev_disc = None
        self.disc_wins = defaultdict(int)
        self.disc_total = defaultdict(int)
        self.disc_speeds = defaultdict(list)
        self.total = 0
        self.wins = 0


class _JockeyDiscState:
    __slots__ = ("disc_wins", "disc_total")

    def __init__(self):
        self.disc_wins = defaultdict(int)
        self.disc_total = defaultdict(int)


def main():
    logger = setup_logging("allure_discipline_cross_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "allure_discipline_cross_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseDiscState] = {}
    jockey_states: dict[str, _JockeyDiscState] = {}

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
                    _process_course(course_records, fout, horse_states,
                                    jockey_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states,
                            jockey_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, jockey_states, fills):
    disc = _norm_disc(records[0].get("discipline"))
    allure = (records[0].get("allure") or "").strip().lower()

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey_driver") or "").strip()
        speed = _safe(rec.get("reduction_km_ms"))
        speed_fig = _safe(rec.get("spd_speed_figure"))
        race_type = (rec.get("race") or rec.get("pgr_race") or "").strip().lower()

        # 1. Allure encoding
        if allure:
            feat["adc_is_trot"] = 1 if "trot" in allure else 0
            feat["adc_is_galop"] = 1 if "galop" in allure else 0
            fills["adc_is_trot"] += 1
            fills["adc_is_galop"] += 1

        # 2. Breed × discipline match
        if race_type and disc:
            is_trotteur = "trotteur" in race_type
            is_trot_race = "trot" in disc
            feat["adc_breed_disc_match"] = 1 if (is_trotteur == is_trot_race) else 0
            fills["adc_breed_disc_match"] += 1

        # Horse discipline features
        if hid and disc:
            st = horse_states.get(hid)
            if st and st.total >= 3:
                # 3. Win rate in this discipline
                dt = st.disc_total.get(disc, 0)
                if dt >= 3:
                    feat["adc_horse_disc_wr"] = round(st.disc_wins[disc] / dt, 5)
                    feat["adc_horse_disc_runs"] = dt
                    fills["adc_horse_disc_wr"] += 1
                    fills["adc_horse_disc_runs"] += 1

                # 4. Discipline switch
                if st.prev_disc is not None:
                    is_switch = disc != st.prev_disc
                    feat["adc_disc_switch"] = 1 if is_switch else 0
                    fills["adc_disc_switch"] += 1

                # 5. Best discipline (where horse wins most)
                best_disc = None
                best_wr = -1
                for d, t in st.disc_total.items():
                    if t >= 3:
                        wr = st.disc_wins.get(d, 0) / t
                        if wr > best_wr:
                            best_wr = wr
                            best_disc = d
                if best_disc:
                    feat["adc_is_best_disc"] = 1 if disc == best_disc else 0
                    fills["adc_is_best_disc"] += 1

                # 6. Speed in this discipline
                disc_spds = st.disc_speeds.get(disc, [])
                if disc_spds:
                    feat["adc_disc_avg_speed"] = round(sum(disc_spds) / len(disc_spds), 0)
                    fills["adc_disc_avg_speed"] += 1

                # 7. Discipline diversity
                feat["adc_n_disciplines"] = len(st.disc_total)
                fills["adc_n_disciplines"] += 1

        # Jockey discipline features
        if jockey and disc:
            jst = jockey_states.get(jockey)
            if jst:
                jt = jst.disc_total.get(disc, 0)
                if jt >= 10:
                    feat["adc_jockey_disc_wr"] = round(jst.disc_wins[disc] / jt, 5)
                    fills["adc_jockey_disc_wr"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey_driver") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))
        speed_fig = _safe(rec.get("spd_speed_figure"))

        if hid and disc:
            if hid not in horse_states:
                horse_states[hid] = _HorseDiscState()
            st = horse_states[hid]
            st.disc_wins[disc] += int(is_winner)
            st.disc_total[disc] += 1
            st.prev_disc = disc
            st.total += 1
            st.wins += int(is_winner)
            if speed_fig:
                spds = st.disc_speeds[disc]
                spds.append(speed_fig)
                if len(spds) > 10:
                    st.disc_speeds[disc] = spds[-10:]

        if jockey and disc:
            if jockey not in jockey_states:
                jockey_states[jockey] = _JockeyDiscState()
            jst = jockey_states[jockey]
            jst.disc_wins[disc] += int(is_winner)
            jst.disc_total[disc] += 1


if __name__ == "__main__":
    main()
