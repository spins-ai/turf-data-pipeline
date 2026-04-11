#!/usr/bin/env python3
"""Pattern discovery features - STREAMING VERSION. 12 features from
discovered patterns: DOW effect, career stage, age×sex×distance,
field×fav interaction, trainer seasonality, jockey×dist×terrain,
career WR bucket, field upset rate.

No OOM: streaming course-by-course, partants_master already sorted by date."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pattern_discovery")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _career_stage(nb):
    v = _safe(nb)
    if v is None: return None
    n = int(v)
    if n == 0: return 0  # debut
    if n < 5: return 1   # early
    if n < 20: return 2  # developing
    if n < 50: return 3  # mature
    return 4              # veteran


def _dist_cat(distance):
    d = _safe(distance)
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "long"
    return "marathon"


def _odds_bracket(cote):
    if cote is None or cote <= 0: return None
    if cote < 3: return "fav"
    if cote < 8: return "mid"
    if cote < 20: return "long"
    return "outsider"


def _field_size_cat(nb):
    v = _safe(nb)
    if v is None: return None
    n = int(v)
    if n < 8: return "small"
    if n < 12: return "medium"
    if n < 16: return "large"
    return "xlarge"


def _career_wr_bucket(nb_wins, nb_courses):
    w = _safe(nb_wins)
    nc = _safe(nb_courses)
    if w is None or nc is None or nc < 3:
        return None
    wr = w / nc
    if wr < 0.05: return 0
    if wr < 0.10: return 1
    if wr < 0.15: return 2
    if wr < 0.20: return 3
    if wr < 0.30: return 4
    return 5


def main():
    logger = setup_logging("pattern_discovery_builder_v2")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "pattern_discovery_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Lightweight accumulators (bounded number of keys)
    dow_hist = defaultdict(lambda: [0, 0])       # dow -> [wins, total]
    stage_hist = defaultdict(lambda: [0, 0])      # stage -> [wins, total]
    asd_hist = defaultdict(lambda: [0, 0])        # age|sex|dist -> [wins, total]
    field_fav_hist = defaultdict(lambda: [0, 0])  # field_size|odds_bracket -> [wins, total]
    trainer_month = defaultdict(lambda: [0, 0])   # trainer|month -> [wins, total]
    trainer_all = defaultdict(lambda: [0, 0])     # trainer -> [wins, total]
    jdt_hist = defaultdict(lambda: [0, 0])        # jockey|dist|terrain -> [wins, total]
    career_wr_hist = defaultdict(lambda: [0, 0])  # bucket -> [wins, total]
    field_upset = defaultdict(lambda: [0, 0])     # field_size -> [fav_wins, total]

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
                    _process_course(course_records, fout,
                                    dow_hist, stage_hist, asd_hist,
                                    field_fav_hist, trainer_month, trainer_all,
                                    jdt_hist, career_wr_hist, field_upset, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout,
                            dow_hist, stage_hist, asd_hist,
                            field_fav_hist, trainer_month, trainer_all,
                            jdt_hist, career_wr_hist, field_upset, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, dow_hist, stage_hist, asd_hist,
                    field_fav_hist, trainer_month, trainer_all,
                    jdt_hist, career_wr_hist, field_upset, fills):
    r0 = records[0]
    date_iso = str(r0.get("date_reunion_iso", "") or "")[:10]
    nb_partants = len(records)
    fs_cat = _field_size_cat(nb_partants)

    try:
        dt = datetime.strptime(date_iso, "%Y-%m-%d")
        dow = dt.weekday()
        month = dt.month
    except (ValueError, TypeError):
        dow = None
        month = None

    # Find favourite
    fav_idx = None
    fav_cote = float("inf")
    for i, rec in enumerate(records):
        c = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        if c is not None and c > 0 and c < fav_cote:
            fav_cote = c
            fav_idx = i

    # SNAPSHOT features
    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        stage = _career_stage(rec.get("nb_courses_carriere"))
        age = rec.get("age")
        sex = (rec.get("sexe") or "").lower().strip()
        dc = _dist_cat(rec.get("distance"))
        ob = _odds_bracket(cote)
        trainer = (rec.get("entraineur") or "").upper().strip()
        jockey = (rec.get("jockey_driver") or "").upper().strip()
        terrain = (rec.get("terrain") or rec.get("etat_terrain") or "").lower().strip()

        # Keys
        asd_key = f"{age}|{sex}|{dc}" if age and sex and dc else None
        ff_key = f"{fs_cat}|{ob}" if fs_cat and ob else None
        tm_key = f"{trainer}|{month}" if trainer and month else None
        jdt_key = f"{jockey}|{dc}|{terrain}" if jockey and dc and terrain else None
        crb = _career_wr_bucket(rec.get("nb_victoires_carriere"), rec.get("nb_courses_carriere"))

        # 1. DOW win rate
        if dow is not None:
            h = dow_hist.get(dow)
            if h and h[1] >= 50:
                feat["pd_dow_wr"] = round(h[0] / h[1], 4)
                fills["pd_dow_wr"] += 1

        # 2-3. Career stage
        if stage is not None:
            feat["pd_career_stage"] = stage
            fills["pd_career_stage"] += 1
            h = stage_hist.get(stage)
            if h and h[1] >= 50:
                feat["pd_stage_wr"] = round(h[0] / h[1], 4)
                fills["pd_stage_wr"] += 1

        # 4. Age × sex × distance WR
        if asd_key:
            h = asd_hist.get(asd_key)
            if h and h[1] >= 20:
                feat["pd_asd_wr"] = round(h[0] / h[1], 4)
                fills["pd_asd_wr"] += 1

        # 5. Field × fav interaction
        if ff_key:
            h = field_fav_hist.get(ff_key)
            if h and h[1] >= 20:
                feat["pd_field_fav_wr"] = round(h[0] / h[1], 4)
                fills["pd_field_fav_wr"] += 1

        # 6-7. Trainer monthly seasonality
        if tm_key:
            h = trainer_month.get(tm_key)
            to = trainer_all.get(trainer)
            if h and h[1] >= 10:
                tm_wr = h[0] / h[1]
                feat["pd_trainer_month_wr"] = round(tm_wr, 4)
                fills["pd_trainer_month_wr"] += 1
                if to and to[1] >= 20:
                    overall_wr = to[0] / to[1]
                    feat["pd_trainer_month_delta"] = round(tm_wr - overall_wr, 4)
                    fills["pd_trainer_month_delta"] += 1

        # 8-9. Jockey × distance × terrain
        if jdt_key:
            h = jdt_hist.get(jdt_key)
            if h and h[1] >= 5:
                feat["pd_jdt_wr"] = round(h[0] / h[1], 4)
                feat["pd_jdt_n"] = h[1]
                fills["pd_jdt_wr"] += 1
                fills["pd_jdt_n"] += 1

        # 10-11. Career WR bucket
        if crb is not None:
            feat["pd_career_wr_bucket"] = crb
            fills["pd_career_wr_bucket"] += 1
            h = career_wr_hist.get(crb)
            if h and h[1] >= 30:
                feat["pd_career_wr_signal"] = round(h[0] / h[1], 4)
                fills["pd_career_wr_signal"] += 1

        # 12. Field upset rate
        if fs_cat:
            h = field_upset.get(fs_cat)
            if h and h[1] >= 20:
                feat["pd_upset_rate"] = round(1 - h[0] / h[1], 4)
                fills["pd_upset_rate"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators AFTER writing
    fav_won = False
    for i, rec in enumerate(records):
        is_winner = bool(rec.get("is_gagnant"))
        cote = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        stage = _career_stage(rec.get("nb_courses_carriere"))
        age = rec.get("age")
        sex = (rec.get("sexe") or "").lower().strip()
        dc = _dist_cat(rec.get("distance"))
        ob = _odds_bracket(cote)
        trainer = (rec.get("entraineur") or "").upper().strip()
        jockey = (rec.get("jockey_driver") or "").upper().strip()
        terrain = (rec.get("terrain") or rec.get("etat_terrain") or "").lower().strip()
        crb = _career_wr_bucket(rec.get("nb_victoires_carriere"), rec.get("nb_courses_carriere"))

        if dow is not None:
            dow_hist[dow][1] += 1
            if is_winner: dow_hist[dow][0] += 1

        if stage is not None:
            stage_hist[stage][1] += 1
            if is_winner: stage_hist[stage][0] += 1

        asd_key = f"{age}|{sex}|{dc}" if age and sex and dc else None
        if asd_key:
            asd_hist[asd_key][1] += 1
            if is_winner: asd_hist[asd_key][0] += 1

        ff_key = f"{fs_cat}|{ob}" if fs_cat and ob else None
        if ff_key:
            field_fav_hist[ff_key][1] += 1
            if is_winner: field_fav_hist[ff_key][0] += 1

        tm_key = f"{trainer}|{month}" if trainer and month else None
        if tm_key:
            trainer_month[tm_key][1] += 1
            if is_winner: trainer_month[tm_key][0] += 1
        if trainer:
            trainer_all[trainer][1] += 1
            if is_winner: trainer_all[trainer][0] += 1

        jdt_key = f"{jockey}|{dc}|{terrain}" if jockey and dc and terrain else None
        if jdt_key:
            jdt_hist[jdt_key][1] += 1
            if is_winner: jdt_hist[jdt_key][0] += 1

        if crb is not None:
            career_wr_hist[crb][1] += 1
            if is_winner: career_wr_hist[crb][0] += 1

        if i == fav_idx and is_winner:
            fav_won = True

    # Field upset
    if fav_idx is not None and fs_cat:
        field_upset[fs_cat][1] += 1
        if fav_won:
            field_upset[fs_cat][0] += 1


if __name__ == "__main__":
    main()
