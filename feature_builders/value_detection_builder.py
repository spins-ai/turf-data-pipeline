#!/usr/bin/env python3
"""Value detection features: identify when the market is mispricing a horse.
Compares historical win probability to current implied probability.
Essential for betting models (Kelly, value betting, CLV)."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/value_detection")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


class _HorseValue:
    __slots__ = (
        "wins", "total", "implied_probs", "actual_outcomes",
        "recent_cotes", "recent_positions",
        "clv_history",  # closing line value history
    )

    def __init__(self):
        self.wins = 0
        self.total = 0
        self.implied_probs = deque(maxlen=30)
        self.actual_outcomes = deque(maxlen=30)
        self.recent_cotes = deque(maxlen=20)
        self.recent_positions = deque(maxlen=20)
        self.clv_history = deque(maxlen=20)


class _OddsBucketTracker:
    __slots__ = ("wins", "total")

    def __init__(self):
        self.wins = 0
        self.total = 0


def _odds_bucket(cote):
    if cote is None:
        return None
    if cote < 2:
        return "hot_fav"
    if cote < 4:
        return "fav"
    if cote < 8:
        return "contender"
    if cote < 15:
        return "mid"
    if cote < 30:
        return "outsider"
    return "longshot"


def main():
    logger = setup_logging("value_detection_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "value_detection_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseValue] = {}
    odds_buckets: dict[str, _OddsBucketTracker] = defaultdict(_OddsBucketTracker)

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
                    _process_course(course_records, fout, horse_states, odds_buckets, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states, odds_buckets, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, odds_buckets, fills):
    features_list = []

    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        feat = {"partant_uid": rec.get("partant_uid", "")}

        cote_fin = _safe(rec.get("cote_finale"))
        cote_ref = _safe(rec.get("cote_reference"))
        implied_prob = 1.0 / cote_fin if cote_fin and cote_fin > 0 else None

        if not hid:
            features_list.append(feat)
            continue

        st = horse_states.get(hid)
        bucket = _odds_bucket(cote_fin)

        if st is not None and st.total >= 3:
            # 1. Horse's true win rate
            true_wr = st.wins / st.total
            feat["vd_horse_true_wr"] = round(true_wr, 5)
            fills["vd_horse_true_wr"] += 1

            # 2. Value edge: true_wr - implied_prob (positive = undervalued)
            if implied_prob is not None:
                edge = true_wr - implied_prob
                feat["vd_value_edge"] = round(edge, 5)
                fills["vd_value_edge"] += 1

                # 3. Value ratio: true_wr / implied_prob (>1 = value)
                if implied_prob > 0:
                    feat["vd_value_ratio"] = round(true_wr / implied_prob, 4)
                    fills["vd_value_ratio"] += 1

            # 4. Kelly criterion sizing: edge / (cote - 1) if positive
            if cote_fin and cote_fin > 1 and implied_prob is not None:
                edge = true_wr - implied_prob
                if edge > 0:
                    kelly = edge / (cote_fin - 1)
                    feat["vd_kelly_fraction"] = round(min(kelly, 0.25), 5)
                    fills["vd_kelly_fraction"] += 1

            # 5. Calibration error: how well do the odds predict this horse?
            if len(st.implied_probs) >= 5:
                avg_impl = sum(st.implied_probs) / len(st.implied_probs)
                avg_actual = sum(st.actual_outcomes) / len(st.actual_outcomes)
                feat["vd_calibration_error"] = round(avg_impl - avg_actual, 5)
                fills["vd_calibration_error"] += 1

            # 6. CLV (Closing Line Value) trend
            if len(st.clv_history) >= 3:
                avg_clv = sum(st.clv_history) / len(st.clv_history)
                feat["vd_avg_clv"] = round(avg_clv, 5)
                fills["vd_avg_clv"] += 1

            # 7. Odds consistency: std of recent odds
            if len(st.recent_cotes) >= 3:
                cotes_list = list(st.recent_cotes)
                mean_c = sum(cotes_list) / len(cotes_list)
                std_c = math.sqrt(sum((c - mean_c) ** 2 for c in cotes_list) / len(cotes_list))
                feat["vd_odds_consistency"] = round(std_c / mean_c if mean_c > 0 else 0, 4)
                fills["vd_odds_consistency"] += 1

            # 8. Recent value trend: is horse becoming more/less undervalued?
            if len(st.implied_probs) >= 6:
                recent_impl = list(st.implied_probs)[-3:]
                older_impl = list(st.implied_probs)[-6:-3]
                recent_avg = sum(recent_impl) / len(recent_impl)
                older_avg = sum(older_impl) / len(older_impl)
                feat["vd_value_trend"] = round(recent_avg - older_avg, 5)
                fills["vd_value_trend"] += 1

        # 9. Odds bucket historical win rate (market-wide)
        if bucket:
            bt = odds_buckets.get(bucket)
            if bt and bt.total >= 50:
                feat["vd_bucket_actual_wr"] = round(bt.wins / bt.total, 5)
                fills["vd_bucket_actual_wr"] += 1

                # 10. Market bias at this odds level
                if implied_prob is not None:
                    expected = implied_prob
                    actual = bt.wins / bt.total
                    feat["vd_market_bias"] = round(actual - expected, 5)
                    fills["vd_market_bias"] += 1

        # 11. CLV current race (cote_reference vs cote_finale)
        if cote_ref and cote_fin:
            clv = (cote_ref - cote_fin) / cote_ref  # positive = price shortened (smart money)
            feat["vd_current_clv"] = round(clv, 5)
            fills["vd_current_clv"] += 1

        # 12. Expected value at current odds
        if st is not None and st.total >= 5 and cote_fin:
            true_wr = st.wins / st.total
            ev = true_wr * (cote_fin - 1) - (1 - true_wr)
            feat["vd_expected_value"] = round(ev, 4)
            fills["vd_expected_value"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue

        cote_fin = _safe(rec.get("cote_finale"))
        cote_ref = _safe(rec.get("cote_reference"))
        is_winner = bool(rec.get("is_gagnant"))
        bucket = _odds_bucket(cote_fin)

        if hid not in horse_states:
            horse_states[hid] = _HorseValue()
        st = horse_states[hid]

        st.wins += int(is_winner)
        st.total += 1

        if cote_fin:
            impl = 1.0 / cote_fin
            st.implied_probs.append(impl)
            st.actual_outcomes.append(1.0 if is_winner else 0.0)
            st.recent_cotes.append(cote_fin)

        if cote_ref and cote_fin:
            clv = (cote_ref - cote_fin) / cote_ref
            st.clv_history.append(clv)

        pos = rec.get("position_arrivee")
        try:
            st.recent_positions.append(int(pos))
        except (TypeError, ValueError):
            pass

        # Update global odds bucket
        if bucket:
            odds_buckets[bucket].wins += int(is_winner)
            odds_buckets[bucket].total += 1


if __name__ == "__main__":
    main()
