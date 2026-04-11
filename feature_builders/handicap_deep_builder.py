#!/usr/bin/env python3
"""Handicap deep features: handicap value relative to field, evolution over time,
interaction with distance, and historical win rates by handicap bracket.
Critical for handicap/claiming races (~15% of data)."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/handicap_deep")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _handi_bucket(v):
    if v is None:
        return None
    if v < 20: return "low"
    if v < 40: return "mid_low"
    if v < 60: return "mid"
    if v < 80: return "mid_high"
    return "high"


class _HorseHandiState:
    __slots__ = ("prev_handi", "handi_history", "wins", "total",
                 "wins_by_bucket", "total_by_bucket")

    def __init__(self):
        self.prev_handi = None
        self.handi_history = deque(maxlen=15)
        self.wins = 0
        self.total = 0
        self.wins_by_bucket = defaultdict(int)
        self.total_by_bucket = defaultdict(int)


def main():
    logger = setup_logging("handicap_deep_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "handicap_deep_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseHandiState] = {}
    # Global bucket stats
    bucket_wins: dict[str, int] = defaultdict(int)
    bucket_total: dict[str, int] = defaultdict(int)

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
                                    bucket_wins, bucket_total, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states,
                            bucket_wins, bucket_total, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, bucket_wins, bucket_total, fills):
    # Field handicap stats
    field_handis = []
    for rec in records:
        h = _safe(rec.get("handicap_valeur"))
        if h is not None:
            field_handis.append(h)

    field_avg = sum(field_handis) / len(field_handis) if field_handis else None
    field_min = min(field_handis) if field_handis else None
    field_max = max(field_handis) if field_handis else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        handi = _safe(rec.get("handicap_valeur"))
        distance = _safe(rec.get("distance"))
        poids = _safe(rec.get("poids_porte_kg"))
        surcharge = _safe(rec.get("surcharge_decharge_kg"))

        if handi is not None:
            bucket = _handi_bucket(handi)

            # 1. Handicap relatif au champ
            if field_avg is not None and field_avg > 0:
                feat["hd_handi_vs_field_avg"] = round(handi - field_avg, 2)
                fills["hd_handi_vs_field_avg"] += 1

            if field_min is not None:
                feat["hd_handi_vs_field_min"] = round(handi - field_min, 2)
                fills["hd_handi_vs_field_min"] += 1

            # 2. Rang handicap dans le champ
            if field_handis:
                rank = sum(1 for h in field_handis if h > handi) + 1
                feat["hd_handi_rank"] = rank
                feat["hd_handi_percentile"] = round(1 - rank / len(field_handis), 4) if len(field_handis) > 1 else 0.5
                fills["hd_handi_rank"] += 1
                fills["hd_handi_percentile"] += 1

            # 3. Handicap × distance interaction
            if distance is not None and distance > 0:
                feat["hd_handi_per_km"] = round(handi / (distance / 1000), 2)
                fills["hd_handi_per_km"] += 1

            # 4. Global bucket win rate
            if bucket and bucket_total.get(bucket, 0) >= 50:
                feat["hd_bucket_wr"] = round(bucket_wins[bucket] / bucket_total[bucket], 5)
                fills["hd_bucket_wr"] += 1

            # 5. Surcharge/décharge
            if surcharge is not None:
                feat["hd_surcharge_kg"] = surcharge
                fills["hd_surcharge_kg"] += 1

            # Horse-specific features
            st = horse_states.get(hid) if hid else None
            if st is not None and st.total >= 2:
                # 6. Handicap evolution
                if st.prev_handi is not None and st.prev_handi > 0:
                    change = handi - st.prev_handi
                    feat["hd_handi_change"] = round(change, 2)
                    fills["hd_handi_change"] += 1
                    feat["hd_handi_rising"] = 1 if change > 0 else 0
                    fills["hd_handi_rising"] += 1

                # 7. Handicap trend
                if len(st.handi_history) >= 3:
                    hist = list(st.handi_history)
                    first = sum(hist[:len(hist)//2]) / (len(hist)//2)
                    second = sum(hist[len(hist)//2:]) / (len(hist) - len(hist)//2)
                    if first > 0:
                        feat["hd_handi_trend"] = round((second - first) / first, 4)
                        fills["hd_handi_trend"] += 1

                # 8. Horse win rate at this bucket
                bt = st.total_by_bucket.get(bucket, 0)
                if bt >= 3:
                    feat["hd_horse_bucket_wr"] = round(st.wins_by_bucket.get(bucket, 0) / bt, 4)
                    fills["hd_horse_bucket_wr"] += 1

                # 9. Optimal handicap (bucket with best wr)
                best_bucket = None
                best_wr = -1
                for b, t in st.total_by_bucket.items():
                    if t >= 3:
                        wr = st.wins_by_bucket.get(b, 0) / t
                        if wr > best_wr:
                            best_wr = wr
                            best_bucket = b
                if best_bucket:
                    feat["hd_is_optimal_bucket"] = 1 if bucket == best_bucket else 0
                    fills["hd_is_optimal_bucket"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        handi = _safe(rec.get("handicap_valeur"))
        is_winner = bool(rec.get("is_gagnant"))

        if hid and handi is not None:
            if hid not in horse_states:
                horse_states[hid] = _HorseHandiState()
            st = horse_states[hid]
            bucket = _handi_bucket(handi)
            st.prev_handi = handi
            st.handi_history.append(handi)
            st.wins += int(is_winner)
            st.total += 1
            if bucket:
                st.wins_by_bucket[bucket] += int(is_winner)
                st.total_by_bucket[bucket] += 1
                bucket_wins[bucket] += int(is_winner)
                bucket_total[bucket] += 1


if __name__ == "__main__":
    main()
