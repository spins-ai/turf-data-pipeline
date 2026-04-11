#!/usr/bin/env python3
"""Opponent strength features: calibrate a horse's level by looking at the quality
of opponents it has beaten or lost to. A horse with 20% win rate against strong
fields is very different from 20% against weak fields. Critical for ELO-like rating."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/opponent_strength")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _HorseOpponentState:
    __slots__ = (
        "wins", "total", "beaten_wr_sum", "beaten_count",
        "lost_to_wr_sum", "lost_to_count",
        "field_avg_wr_history",
    )

    def __init__(self):
        self.wins = 0
        self.total = 0
        self.beaten_wr_sum = 0.0    # Sum of win rates of horses this horse has beaten
        self.beaten_count = 0
        self.lost_to_wr_sum = 0.0   # Sum of win rates of horses that beat this horse
        self.lost_to_count = 0
        self.field_avg_wr_history = deque(maxlen=20)  # Avg field quality per race


def main():
    logger = setup_logging("opponent_strength_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "opponent_strength_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_stats: dict[str, _HorseOpponentState] = {}
    # Simple running win rate tracker (separate from opponent state)
    horse_wr: dict[str, tuple[int, int]] = {}  # hid -> (wins, total)

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
                    _process_course(course_records, fout, horse_stats, horse_wr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_stats, horse_wr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_stats, horse_wr, fills):
    # Step 1: Get current win rates for all horses in this race
    field_wrs = {}
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if hid and hid in horse_wr:
            w, t = horse_wr[hid]
            if t >= 3:
                field_wrs[hid] = w / t

    # Field average win rate (quality of this race)
    if field_wrs:
        field_avg_wr = sum(field_wrs.values()) / len(field_wrs)
    else:
        field_avg_wr = None

    # Step 2: Compute features
    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""

        if not hid:
            features_list.append(feat)
            continue

        st = horse_stats.get(hid)

        if st is not None and st.total >= 3:
            # 1. Average quality of beaten opponents
            if st.beaten_count >= 5:
                feat["os_avg_beaten_wr"] = round(st.beaten_wr_sum / st.beaten_count, 5)
                fills["os_avg_beaten_wr"] += 1

            # 2. Average quality of opponents that beat this horse
            if st.lost_to_count >= 5:
                feat["os_avg_lost_to_wr"] = round(st.lost_to_wr_sum / st.lost_to_count, 5)
                fills["os_avg_lost_to_wr"] += 1

            # 3. Strength of schedule (avg field quality faced)
            if st.field_avg_wr_history:
                feat["os_strength_of_schedule"] = round(
                    sum(st.field_avg_wr_history) / len(st.field_avg_wr_history), 5
                )
                fills["os_strength_of_schedule"] += 1

            # 4. Quality-adjusted win rate: horse's wr relative to opponents' quality
            my_wr = horse_wr.get(hid)
            if my_wr:
                w, t = my_wr
                if t >= 3 and st.field_avg_wr_history:
                    avg_opp = sum(st.field_avg_wr_history) / len(st.field_avg_wr_history)
                    raw_wr = w / t
                    if avg_opp > 0:
                        feat["os_quality_adjusted_wr"] = round(raw_wr / avg_opp, 4)
                        fills["os_quality_adjusted_wr"] += 1

            # 5. Beaten quality ratio: quality of beaten / quality of lost to
            if st.beaten_count >= 5 and st.lost_to_count >= 5:
                beaten_avg = st.beaten_wr_sum / st.beaten_count
                lost_avg = st.lost_to_wr_sum / st.lost_to_count
                if lost_avg > 0:
                    feat["os_beat_loss_quality_ratio"] = round(beaten_avg / lost_avg, 4)
                    fills["os_beat_loss_quality_ratio"] += 1

        # 6. Current field quality (for all horses)
        if field_avg_wr is not None:
            feat["os_current_field_quality"] = round(field_avg_wr, 5)
            fills["os_current_field_quality"] += 1

            # 7. Horse's wr vs current field quality
            my_wr_data = horse_wr.get(hid)
            if my_wr_data:
                w, t = my_wr_data
                if t >= 3:
                    feat["os_wr_vs_field_quality"] = round(w / t - field_avg_wr, 5)
                    fills["os_wr_vs_field_quality"] += 1

        # 8. Number of opponents with known stats (field depth)
        feat["os_field_depth"] = len(field_wrs)
        fills["os_field_depth"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Step 3: Update state AFTER features
    # Determine results for opponent tracking
    results = []
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        pos = _safe(rec.get("position_arrivee"))
        results.append((hid, pos))

    for i, (hid, pos) in enumerate(results):
        if not hid:
            continue

        is_winner = bool(records[i].get("is_gagnant"))

        # Update simple wr tracker
        if hid not in horse_wr:
            horse_wr[hid] = (0, 0)
        w, t = horse_wr[hid]
        horse_wr[hid] = (w + int(is_winner), t + 1)

        # Update opponent state
        if hid not in horse_stats:
            horse_stats[hid] = _HorseOpponentState()
        st = horse_stats[hid]
        st.wins += int(is_winner)
        st.total += 1

        if field_avg_wr is not None:
            st.field_avg_wr_history.append(field_avg_wr)

        # Track beaten/lost to opponents
        if pos is not None:
            for j, (opp_hid, opp_pos) in enumerate(results):
                if i == j or not opp_hid or opp_pos is None:
                    continue
                opp_wr = field_wrs.get(opp_hid)
                if opp_wr is None:
                    continue
                if pos < opp_pos:  # This horse beat the opponent
                    st.beaten_wr_sum += opp_wr
                    st.beaten_count += 1
                elif pos > opp_pos:  # Opponent beat this horse
                    st.lost_to_wr_sum += opp_wr
                    st.lost_to_count += 1


if __name__ == "__main__":
    main()
