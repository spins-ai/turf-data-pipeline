#!/usr/bin/env python3
"""Equipment first-time effect builder - STREAMING. 6 features:
first_time_blinkers, first_time_equipment_change, equipment_combo_hash,
equipment_change_wr, blinkers_first_time_wr, equipment_stability.

Tracks horse equipment history to detect first-time gear changes.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/equipment_first_time")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _parse_equip(rec):
    """Extract equipment string from record."""
    equip = (rec.get("equipement") or rec.get("cnd_cond_depart") or "").lower().strip()
    return equip if equip else None


def main():
    logger = setup_logging("equipment_first_time_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "equipment_first_time_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Horse → set of previously used equipment
    horse_equip_hist = {}  # horse -> set of seen equipment strings
    # Horse → last equipment
    horse_last_equip = {}  # horse -> last equipment string
    # Equipment change WR accumulators
    equip_change_wr = [0, 0]  # [wins, total] when equipment changes
    equip_no_change_wr = [0, 0]  # [wins, total] when no change
    blinkers_first_wr = [0, 0]  # [wins, total] first time blinkers

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
                    _process_course(course_records, fout, horse_equip_hist,
                                    horse_last_equip, equip_change_wr,
                                    equip_no_change_wr, blinkers_first_wr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_equip_hist,
                            horse_last_equip, equip_change_wr,
                            equip_no_change_wr, blinkers_first_wr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_equip_hist, horse_last_equip,
                    equip_change_wr, equip_no_change_wr, blinkers_first_wr, fills):
    # SNAPSHOT features
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        equip = _parse_equip(rec)

        if horse and len(horse) >= 2:
            last_equip = horse_last_equip.get(horse)
            seen_equip = horse_equip_hist.get(horse, set())

            # 1. Equipment changed from last race
            if last_equip is not None and equip is not None:
                changed = equip != last_equip
                feat["eft_equip_changed"] = 1 if changed else 0
                fills["eft_equip_changed"] += 1

                # 2. Equipment change WR signal
                if changed and equip_change_wr[1] >= 50:
                    feat["eft_change_wr"] = round(equip_change_wr[0] / equip_change_wr[1], 4)
                    fills["eft_change_wr"] += 1
                elif not changed and equip_no_change_wr[1] >= 50:
                    feat["eft_change_wr"] = round(equip_no_change_wr[0] / equip_no_change_wr[1], 4)
                    fills["eft_change_wr"] += 1

            # 3. First time with this equipment
            if equip:
                is_first_time = equip not in seen_equip
                feat["eft_first_time_equip"] = 1 if is_first_time else 0
                fills["eft_first_time_equip"] += 1

                # 4. First time blinkers
                has_blinkers = "oeill" in equip or "blink" in equip
                if has_blinkers and is_first_time:
                    feat["eft_first_blinkers"] = 1
                    fills["eft_first_blinkers"] += 1
                    if blinkers_first_wr[1] >= 30:
                        feat["eft_blinkers_first_wr"] = round(blinkers_first_wr[0] / blinkers_first_wr[1], 4)
                        fills["eft_blinkers_first_wr"] += 1

            # 5. Equipment stability (nb different equipment used)
            if seen_equip:
                feat["eft_equip_variety"] = len(seen_equip)
                fills["eft_equip_variety"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        equip = _parse_equip(rec)
        is_winner = bool(rec.get("is_gagnant"))

        if horse and len(horse) >= 2:
            last_equip = horse_last_equip.get(horse)
            seen_equip = horse_equip_hist.get(horse, set())

            if equip:
                if last_equip is not None:
                    if equip != last_equip:
                        equip_change_wr[1] += 1
                        if is_winner: equip_change_wr[0] += 1
                    else:
                        equip_no_change_wr[1] += 1
                        if is_winner: equip_no_change_wr[0] += 1

                # First time blinkers tracking
                has_blinkers = "oeill" in equip or "blink" in equip
                if has_blinkers and equip not in seen_equip:
                    blinkers_first_wr[1] += 1
                    if is_winner: blinkers_first_wr[0] += 1

                # Update history
                if horse not in horse_equip_hist:
                    horse_equip_hist[horse] = set()
                horse_equip_hist[horse].add(equip)
                horse_last_equip[horse] = equip


if __name__ == "__main__":
    main()
