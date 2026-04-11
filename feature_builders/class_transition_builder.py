#!/usr/bin/env python3
"""Class transition features: detect when a horse moves up/down in class level.
Uses allocation, gains, and field quality as class proxies.
Critical for handicap and claiming races — a horse dropping in class is a strong signal."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/class_transition")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _HorseClassState:
    __slots__ = (
        "prev_alloc", "prev_gains", "prev_field_avg_gains",
        "alloc_history", "gains_history", "class_changes",
        "total", "wins_after_drop", "total_after_drop",
        "wins_after_rise", "total_after_rise",
    )

    def __init__(self):
        self.prev_alloc = None
        self.prev_gains = None
        self.prev_field_avg_gains = None
        self.alloc_history = deque(maxlen=10)
        self.gains_history = deque(maxlen=10)
        self.class_changes = deque(maxlen=10)  # +1 = up, -1 = down, 0 = same
        self.total = 0
        self.wins_after_drop = 0
        self.total_after_drop = 0
        self.wins_after_rise = 0
        self.total_after_rise = 0


def main():
    logger = setup_logging("class_transition_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "class_transition_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseClassState] = {}

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
    # Compute field average gains for class context
    field_gains = []
    for rec in records:
        g = _safe(rec.get("gains_carriere_euros"))
        if g is not None:
            field_gains.append(g)
    field_avg_gains = sum(field_gains) / len(field_gains) if field_gains else None

    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""

        if not hid:
            features_list.append(feat)
            continue

        alloc = _safe(rec.get("allocation_euros")) or _safe(rec.get("prix_course"))
        gains = _safe(rec.get("gains_carriere_euros"))

        st = horse_states.get(hid)

        if st is not None and st.total >= 1:
            # 1. Allocation change (class proxy #1)
            if alloc is not None and st.prev_alloc is not None and st.prev_alloc > 0:
                alloc_change = (alloc - st.prev_alloc) / st.prev_alloc
                feat["ct_alloc_change_pct"] = round(alloc_change, 4)
                fills["ct_alloc_change_pct"] += 1

                # Class direction
                if alloc_change > 0.1:
                    feat["ct_class_direction"] = 1  # Moving up
                elif alloc_change < -0.1:
                    feat["ct_class_direction"] = -1  # Dropping down
                else:
                    feat["ct_class_direction"] = 0  # Same level
                fills["ct_class_direction"] += 1

            # 2. Field quality change (class proxy #2)
            if field_avg_gains is not None and st.prev_field_avg_gains is not None and st.prev_field_avg_gains > 0:
                field_change = (field_avg_gains - st.prev_field_avg_gains) / st.prev_field_avg_gains
                feat["ct_field_quality_change"] = round(field_change, 4)
                fills["ct_field_quality_change"] += 1

            # 3. Horse gains vs field average (relative class position)
            if gains is not None and field_avg_gains is not None and field_avg_gains > 0:
                feat["ct_gains_vs_field"] = round(gains / field_avg_gains, 4)
                fills["ct_gains_vs_field"] += 1

            # 4. Allocation trend (over last N races)
            if len(st.alloc_history) >= 3:
                allocs = list(st.alloc_history)
                first_half = sum(allocs[:len(allocs)//2]) / (len(allocs)//2)
                second_half = sum(allocs[len(allocs)//2:]) / (len(allocs) - len(allocs)//2)
                if first_half > 0:
                    feat["ct_alloc_trend"] = round((second_half - first_half) / first_half, 4)
                    fills["ct_alloc_trend"] += 1

            # 5. Number of class changes in recent races
            if len(st.class_changes) >= 3:
                changes = list(st.class_changes)
                feat["ct_num_class_drops"] = sum(1 for c in changes if c == -1)
                feat["ct_num_class_rises"] = sum(1 for c in changes if c == 1)
                fills["ct_num_class_drops"] += 1
                fills["ct_num_class_rises"] += 1

            # 6. Win rate after class drops (historically strong signal)
            if st.total_after_drop >= 3:
                feat["ct_wr_after_drop"] = round(st.wins_after_drop / st.total_after_drop, 4)
                fills["ct_wr_after_drop"] += 1

            # 7. Win rate after class rises
            if st.total_after_rise >= 3:
                feat["ct_wr_after_rise"] = round(st.wins_after_rise / st.total_after_rise, 4)
                fills["ct_wr_after_rise"] += 1

            # 8. Is currently dropping class?
            if alloc is not None and len(st.alloc_history) >= 2:
                avg_alloc = sum(st.alloc_history) / len(st.alloc_history)
                if avg_alloc > 0:
                    feat["ct_is_class_drop"] = 1 if alloc < avg_alloc * 0.85 else 0
                    feat["ct_is_class_rise"] = 1 if alloc > avg_alloc * 1.15 else 0
                    fills["ct_is_class_drop"] += 1
                    fills["ct_is_class_rise"] += 1

            # 9. Max allocation seen (proxy for peak class)
            if alloc is not None and st.alloc_history:
                max_alloc = max(st.alloc_history)
                if max_alloc > 0:
                    feat["ct_pct_of_peak_class"] = round(alloc / max_alloc, 4)
                    fills["ct_pct_of_peak_class"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue

        alloc = _safe(rec.get("allocation_euros")) or _safe(rec.get("prix_course"))
        gains = _safe(rec.get("gains_carriere_euros"))
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseClassState()
        st = horse_states[hid]

        # Determine class change
        class_change = 0
        if alloc is not None and st.prev_alloc is not None and st.prev_alloc > 0:
            pct = (alloc - st.prev_alloc) / st.prev_alloc
            if pct > 0.1:
                class_change = 1
            elif pct < -0.1:
                class_change = -1

        st.class_changes.append(class_change)

        # Track win rates after class changes
        if class_change == -1:
            st.total_after_drop += 1
            st.wins_after_drop += int(is_winner)
        elif class_change == 1:
            st.total_after_rise += 1
            st.wins_after_rise += int(is_winner)

        if alloc is not None:
            st.prev_alloc = alloc
            st.alloc_history.append(alloc)
        if gains is not None:
            st.prev_gains = gains
            st.gains_history.append(gains)
        st.prev_field_avg_gains = field_avg_gains
        st.total += 1


if __name__ == "__main__":
    main()
