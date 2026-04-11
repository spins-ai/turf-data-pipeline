#!/usr/bin/env python3
"""Weight impact deep features: carried weight relative to field,
weight×distance interaction, horse performance at different weights."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/poids_impact_deep")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _weight_bucket(w):
    if w is None: return None
    if w < 54: return "light"
    if w < 58: return "medium"
    if w < 62: return "heavy"
    return "very_heavy"


class _HorseWeightState:
    __slots__ = ("weights", "wins_by_bucket", "total_by_bucket",
                 "prev_weight", "total")

    def __init__(self):
        self.weights = deque(maxlen=15)
        self.wins_by_bucket = defaultdict(int)
        self.total_by_bucket = defaultdict(int)
        self.prev_weight = None
        self.total = 0


def main():
    logger = setup_logging("poids_impact_deep_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "poids_impact_deep_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseWeightState] = {}
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
    distance = _safe(records[0].get("distance"))

    # Field weight stats
    field_weights = []
    for rec in records:
        w = _safe(rec.get("poids_porte_kg"))
        if w is not None:
            field_weights.append(w)

    field_avg = sum(field_weights) / len(field_weights) if field_weights else None
    field_min = min(field_weights) if field_weights else None
    field_max = max(field_weights) if field_weights else None

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        weight = _safe(rec.get("poids_porte_kg"))

        if weight is not None:
            bucket = _weight_bucket(weight)

            # 1. Weight vs field
            if field_avg is not None:
                feat["pid_weight_vs_avg"] = round(weight - field_avg, 2)
                fills["pid_weight_vs_avg"] += 1

            if field_min is not None:
                feat["pid_weight_vs_min"] = round(weight - field_min, 2)
                fills["pid_weight_vs_min"] += 1

            # 2. Weight rank in field
            if field_weights:
                rank = sum(1 for w in field_weights if w < weight) + 1
                feat["pid_weight_rank"] = rank
                fills["pid_weight_rank"] += 1

            # 3. Weight × distance
            if distance is not None:
                feat["pid_weight_per_km"] = round(weight / (distance / 1000), 2)
                fills["pid_weight_per_km"] += 1

            # 4. Weight spread in field
            if field_max is not None and field_min is not None and field_max > field_min:
                feat["pid_weight_norm"] = round((weight - field_min) / (field_max - field_min), 4)
                fills["pid_weight_norm"] += 1

            # 5. Global bucket win rate
            if bucket and bucket_total.get(bucket, 0) >= 50:
                feat["pid_bucket_wr"] = round(bucket_wins[bucket] / bucket_total[bucket], 5)
                fills["pid_bucket_wr"] += 1

            # Horse-specific
            if hid:
                st = horse_states.get(hid)
                if st and st.total >= 3:
                    # 6. Weight change
                    if st.prev_weight is not None:
                        feat["pid_weight_change"] = round(weight - st.prev_weight, 2)
                        fills["pid_weight_change"] += 1

                    # 7. Avg carried weight
                    wlist = list(st.weights)
                    if wlist:
                        avg_w = sum(wlist) / len(wlist)
                        feat["pid_avg_weight"] = round(avg_w, 2)
                        feat["pid_weight_vs_own_avg"] = round(weight - avg_w, 2)
                        fills["pid_avg_weight"] += 1
                        fills["pid_weight_vs_own_avg"] += 1

                    # 8. Best bucket
                    best_b = None
                    best_wr = -1
                    for b, t in st.total_by_bucket.items():
                        if t >= 3:
                            wr = st.wins_by_bucket.get(b, 0) / t
                            if wr > best_wr:
                                best_wr = wr
                                best_b = b
                    if best_b:
                        feat["pid_optimal_weight_bucket"] = 1 if bucket == best_b else 0
                        fills["pid_optimal_weight_bucket"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        weight = _safe(rec.get("poids_porte_kg"))
        is_winner = bool(rec.get("is_gagnant"))

        if hid and weight is not None:
            if hid not in horse_states:
                horse_states[hid] = _HorseWeightState()
            st = horse_states[hid]
            bucket = _weight_bucket(weight)
            st.weights.append(weight)
            st.prev_weight = weight
            st.total += 1
            if bucket:
                st.wins_by_bucket[bucket] += int(is_winner)
                st.total_by_bucket[bucket] += 1
                bucket_wins[bucket] += int(is_winner)
                bucket_total[bucket] += 1


if __name__ == "__main__":
    main()
