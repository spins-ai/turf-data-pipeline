#!/usr/bin/env python3
"""Time-decay features: exponentially decayed win/place rates for all entities
(horse, jockey, trainer, hippo) with multiple decay windows."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/time_decay_all_entities")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _parse_date_ord(rec):
    d = rec.get("date_reunion_iso") or rec.get("date_reunion") or rec.get("date") or ""
    if not d:
        return None
    try:
        from datetime import datetime
        if "T" in str(d):
            d = str(d).split("T")[0]
        dt = datetime.strptime(str(d)[:10], "%Y-%m-%d")
        return dt.toordinal()
    except (ValueError, TypeError):
        return None


class _DecayState:
    __slots__ = ("events",)  # list of (date_ord, is_win, is_place)

    def __init__(self):
        self.events = []

    def add(self, date_ord, is_win, is_place):
        self.events.append((date_ord, is_win, is_place))
        # Keep last 50 to limit memory
        if len(self.events) > 50:
            self.events = self.events[-50:]

    def decay_wr(self, now, half_life_days):
        if not self.events or now is None:
            return None, None
        lam = math.log(2) / half_life_days
        w_sum = 0.0
        wp_sum = 0.0
        total_w = 0.0
        for dt, iw, ip in self.events:
            age = now - dt
            if age < 0:
                continue
            w = math.exp(-lam * age)
            w_sum += w * iw
            wp_sum += w * ip
            total_w += w
        if total_w < 1e-9:
            return None, None
        return w_sum / total_w, wp_sum / total_w


def main():
    logger = setup_logging("time_decay_all_entities_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "time_decay_all_entities_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_decay: dict[str, _DecayState] = {}
    jockey_decay: dict[str, _DecayState] = {}
    trainer_decay: dict[str, _DecayState] = {}

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
                    _process_course(course_records, fout, horse_decay,
                                    jockey_decay, trainer_decay, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_decay,
                            jockey_decay, trainer_decay, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_decay, jockey_decay, trainer_decay, fills):
    race_date = _parse_date_ord(records[0])

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()

        if race_date:
            # Horse decay (30-day and 90-day half-life)
            if hid and hid in horse_decay:
                hd = horse_decay[hid]
                wr30, pr30 = hd.decay_wr(race_date, 30)
                wr90, pr90 = hd.decay_wr(race_date, 90)
                if wr30 is not None:
                    feat["td_horse_wr_30d"] = round(wr30, 5)
                    feat["td_horse_pr_30d"] = round(pr30, 5)
                    fills["td_horse_wr_30d"] += 1
                    fills["td_horse_pr_30d"] += 1
                if wr90 is not None:
                    feat["td_horse_wr_90d"] = round(wr90, 5)
                    fills["td_horse_wr_90d"] += 1

            # Jockey decay (30-day and 90-day)
            if jockey and jockey in jockey_decay:
                jd = jockey_decay[jockey]
                wr30, pr30 = jd.decay_wr(race_date, 30)
                wr90, _ = jd.decay_wr(race_date, 90)
                if wr30 is not None:
                    feat["td_jockey_wr_30d"] = round(wr30, 5)
                    feat["td_jockey_pr_30d"] = round(pr30, 5)
                    fills["td_jockey_wr_30d"] += 1
                    fills["td_jockey_pr_30d"] += 1
                if wr90 is not None:
                    feat["td_jockey_wr_90d"] = round(wr90, 5)
                    fills["td_jockey_wr_90d"] += 1

            # Trainer decay (90-day and 180-day)
            if trainer and trainer in trainer_decay:
                td = trainer_decay[trainer]
                wr90, pr90 = td.decay_wr(race_date, 90)
                wr180, _ = td.decay_wr(race_date, 180)
                if wr90 is not None:
                    feat["td_trainer_wr_90d"] = round(wr90, 5)
                    feat["td_trainer_pr_90d"] = round(pr90, 5)
                    fills["td_trainer_wr_90d"] += 1
                    fills["td_trainer_pr_90d"] += 1
                if wr180 is not None:
                    feat["td_trainer_wr_180d"] = round(wr180, 5)
                    fills["td_trainer_wr_180d"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()
        is_winner = int(bool(rec.get("is_gagnant")))
        pos = _safe(rec.get("place_arrivee"))
        is_place = int(pos is not None and pos <= 3)

        if race_date:
            if hid:
                if hid not in horse_decay:
                    horse_decay[hid] = _DecayState()
                horse_decay[hid].add(race_date, is_winner, is_place)
            if jockey:
                if jockey not in jockey_decay:
                    jockey_decay[jockey] = _DecayState()
                jockey_decay[jockey].add(race_date, is_winner, is_place)
            if trainer:
                if trainer not in trainer_decay:
                    trainer_decay[trainer] = _DecayState()
                trainer_decay[trainer].add(race_date, is_winner, is_place)


if __name__ == "__main__":
    main()
