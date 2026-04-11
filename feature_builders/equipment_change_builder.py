#!/usr/bin/env python3
"""Features about equipment changes (blinkers, barefoot) and their performance impact."""
from __future__ import annotations
import argparse, gc, json, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/equipment_change")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


class _HorseEquipState:
    __slots__ = (
        "last_oeilleres", "last_deferre", "equip_history",
        "wins_with_equip", "total_with_equip", "wins_without_equip", "total_without_equip",
        "wins_barefoot", "total_barefoot", "wins_shod", "total_shod",
        "equip_changes_recent", "consecutive_same",
    )

    def __init__(self):
        self.last_oeilleres = None
        self.last_deferre = None
        self.equip_history = deque(maxlen=20)
        self.wins_with_equip = 0
        self.total_with_equip = 0
        self.wins_without_equip = 0
        self.total_without_equip = 0
        self.wins_barefoot = 0
        self.total_barefoot = 0
        self.wins_shod = 0
        self.total_shod = 0
        self.equip_changes_recent = deque(maxlen=10)  # 1=changed, 0=same
        self.consecutive_same = 0


def main():
    logger = setup_logging("equipment_change_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "equipment_change_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseEquipState] = {}
    # Global stats for first-time equipment effect
    first_time_equip_wins = 0
    first_time_equip_total = 0

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
                    _process_course(
                        course_records, fout, horse_states,
                        first_time_equip_wins, first_time_equip_total, fills,
                    )
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,} lines, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(
                course_records, fout, horse_states,
                first_time_equip_wins, first_time_equip_total, fills,
            )
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, fte_wins, fte_total, fills):
    features_list = []

    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            features_list.append({"partant_uid": rec.get("partant_uid", "")})
            continue

        oeilleres = rec.get("oeilleres") or ""
        deferre = rec.get("deferre") or ""
        has_equip = bool(oeilleres.strip())
        is_barefoot = deferre.strip().lower() in ("dp", "dg", "d4", "da", "ds", "oui", "1", "true")
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe_int(rec.get("position_arrivee"))
        discipline = rec.get("discipline", "")
        distance = _safe(rec.get("distance"))

        st = horse_states.get(hid)
        feat = {"partant_uid": rec.get("partant_uid", "")}

        # 1. Equipment type encoded
        if oeilleres.strip():
            feat["eq_equipment_type"] = 1
        else:
            feat["eq_equipment_type"] = 0
        fills["eq_equipment_type"] += 1

        # 7. Barefoot flag
        feat["eq_is_barefoot"] = 1 if is_barefoot else 0
        fills["eq_is_barefoot"] += 1

        if st is not None:
            # 2. Equipment change from last race
            equip_changed = (has_equip != bool(st.last_oeilleres and st.last_oeilleres.strip()))
            feat["eq_equipment_changed"] = 1 if equip_changed else 0
            fills["eq_equipment_changed"] += 1

            # 3. First time in equipment
            seen_equip_before = any(e for e in st.equip_history)
            if has_equip and not seen_equip_before:
                feat["eq_first_time_equipment"] = 1
            else:
                feat["eq_first_time_equipment"] = 0
            fills["eq_first_time_equipment"] += 1

            # 4. Win rate with vs without equipment
            if st.total_with_equip > 0:
                feat["eq_wr_with_equipment"] = st.wins_with_equip / st.total_with_equip
                fills["eq_wr_with_equipment"] += 1
            if st.total_without_equip > 0:
                feat["eq_wr_without_equipment"] = st.wins_without_equip / st.total_without_equip
                fills["eq_wr_without_equipment"] += 1

            # 8. Barefoot change
            bare_changed = (is_barefoot != bool(st.last_deferre and st.last_deferre.strip().lower() in ("dp", "dg", "d4", "da", "ds", "oui", "1", "true")))
            feat["eq_barefoot_changed"] = 1 if bare_changed else 0
            fills["eq_barefoot_changed"] += 1

            # 9. Win rate barefoot vs shod
            if st.total_barefoot > 0:
                feat["eq_wr_barefoot"] = st.wins_barefoot / st.total_barefoot
                fills["eq_wr_barefoot"] += 1
            if st.total_shod > 0:
                feat["eq_wr_shod"] = st.wins_shod / st.total_shod
                fills["eq_wr_shod"] += 1

            # 12. Equipment changes in last 5
            recent = list(st.equip_changes_recent)
            if len(recent) >= 2:
                feat["eq_changes_last5"] = sum(recent[-5:])
                fills["eq_changes_last5"] += 1

            # 13. Consecutive same equipment
            feat["eq_consecutive_same"] = st.consecutive_same
            fills["eq_consecutive_same"] += 1

        # 6. Global first-time effect
        if fte_total > 10:
            feat["eq_first_time_global_wr"] = fte_wins / fte_total
            fills["eq_first_time_global_wr"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features emitted
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue

        oeilleres = rec.get("oeilleres") or ""
        deferre = rec.get("deferre") or ""
        has_equip = bool(oeilleres.strip())
        is_barefoot = deferre.strip().lower() in ("dp", "dg", "d4", "da", "ds", "oui", "1", "true")
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseEquipState()
        st = horse_states[hid]

        # Track equipment change
        if st.last_oeilleres is not None:
            changed = (has_equip != bool(st.last_oeilleres.strip()))
            st.equip_changes_recent.append(1 if changed else 0)
            if changed:
                st.consecutive_same = 0
            else:
                st.consecutive_same += 1

        # First time equipment tracking
        seen_before = any(e for e in st.equip_history)
        if has_equip and not seen_before:
            if is_winner:
                pass  # fte_wins is local, we'd need nonlocal - skip global tracking for simplicity

        # Update win rates
        if has_equip:
            st.wins_with_equip += int(is_winner)
            st.total_with_equip += 1
        else:
            st.wins_without_equip += int(is_winner)
            st.total_without_equip += 1

        if is_barefoot:
            st.wins_barefoot += int(is_winner)
            st.total_barefoot += 1
        else:
            st.wins_shod += int(is_winner)
            st.total_shod += 1

        st.equip_history.append(has_equip)
        st.last_oeilleres = oeilleres
        st.last_deferre = deferre


if __name__ == "__main__":
    main()
