#!/usr/bin/env python3
"""Jockey booking momentum builder - STREAMING. 7 features:
jockey_new_trainer_flag, jockey_new_horse_flag, jockey_demand_score,
jockey_recent_bookings, jockey_upgrade_signal, trainer_jockey_loyalty,
jockey_hot_bookings.

Detects when top jockeys are booked for first time = confidence signal.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/jockey_booking_momentum")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("jockey_booking_momentum_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "jockey_booking_momentum_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Accumulators
    jockey_horses = defaultdict(set)       # jockey -> set of horses ridden
    jockey_trainers = defaultdict(set)     # jockey -> set of trainers worked with
    trainer_jockeys = defaultdict(set)     # trainer -> set of jockeys used
    jockey_recent = defaultdict(lambda: deque(maxlen=50))  # jockey -> deque of (is_win,)
    horse_jockeys = defaultdict(set)       # horse -> set of jockeys that rode it
    jockey_wr = defaultdict(lambda: [0, 0])  # jockey -> [wins, total]

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
                    _process_course(course_records, fout, jockey_horses, jockey_trainers,
                                    trainer_jockeys, jockey_recent, horse_jockeys,
                                    jockey_wr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, jockey_horses, jockey_trainers,
                            trainer_jockeys, jockey_recent, horse_jockeys,
                            jockey_wr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, jockey_horses, jockey_trainers,
                    trainer_jockeys, jockey_recent, horse_jockeys,
                    jockey_wr, fills):
    # SNAPSHOT features
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        jockey = (rec.get("jockey_driver") or "").upper().strip()
        trainer = (rec.get("entraineur") or "").upper().strip()
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()

        if jockey and len(jockey) >= 3:
            # 1. First time this jockey rides this horse
            if horse and len(horse) >= 2:
                is_new_horse = horse not in jockey_horses.get(jockey, set())
                feat["jbm_new_horse"] = 1 if is_new_horse else 0
                fills["jbm_new_horse"] += 1

                # Was there a jockey change on this horse?
                prev_jockeys = horse_jockeys.get(horse, set())
                if prev_jockeys and jockey not in prev_jockeys:
                    feat["jbm_jockey_switch"] = 1
                    fills["jbm_jockey_switch"] += 1

                    # 2. Upgrade signal: new jockey has better WR than previous
                    jwr = jockey_wr.get(jockey)
                    if jwr and jwr[1] >= 20:
                        new_wr = jwr[0] / jwr[1]
                        # Compare with avg of previous jockeys
                        prev_wrs = []
                        for pj in prev_jockeys:
                            pw = jockey_wr.get(pj)
                            if pw and pw[1] >= 10:
                                prev_wrs.append(pw[0] / pw[1])
                        if prev_wrs:
                            avg_prev = sum(prev_wrs) / len(prev_wrs)
                            feat["jbm_upgrade_signal"] = round(new_wr - avg_prev, 4)
                            fills["jbm_upgrade_signal"] += 1
                else:
                    feat["jbm_jockey_switch"] = 0
                    fills["jbm_jockey_switch"] += 1

            # 3. First time this jockey works with this trainer
            if trainer and len(trainer) >= 3:
                is_new_trainer = trainer not in jockey_trainers.get(jockey, set())
                feat["jbm_new_trainer"] = 1 if is_new_trainer else 0
                fills["jbm_new_trainer"] += 1

                # 4. Trainer loyalty (how many different jockeys trainer uses)
                nb_jockeys = len(trainer_jockeys.get(trainer, set()))
                if nb_jockeys >= 1:
                    feat["jbm_trainer_loyalty"] = nb_jockeys
                    fills["jbm_trainer_loyalty"] += 1

            # 5. Jockey recent form (WR last 20)
            recent = jockey_recent.get(jockey)
            if recent and len(recent) >= 5:
                wr = sum(recent) / len(recent)
                feat["jbm_jockey_form"] = round(wr, 4)
                fills["jbm_jockey_form"] += 1

            # 6. Jockey demand (unique horses ridden)
            nb_horses = len(jockey_horses.get(jockey, set()))
            if nb_horses >= 1:
                feat["jbm_demand"] = nb_horses
                fills["jbm_demand"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        jockey = (rec.get("jockey_driver") or "").upper().strip()
        trainer = (rec.get("entraineur") or "").upper().strip()
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        is_winner = bool(rec.get("is_gagnant"))

        if jockey and len(jockey) >= 3:
            if horse and len(horse) >= 2:
                jockey_horses[jockey].add(horse)
                horse_jockeys[horse].add(jockey)
            if trainer and len(trainer) >= 3:
                jockey_trainers[jockey].add(trainer)
                trainer_jockeys[trainer].add(jockey)
            jockey_recent[jockey].append(int(is_winner))
            jockey_wr[jockey][1] += 1
            if is_winner:
                jockey_wr[jockey][0] += 1


if __name__ == "__main__":
    main()
