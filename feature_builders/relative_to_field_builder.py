#!/usr/bin/env python3
"""Relative-to-field features: for each horse in a race, compute how its historical
stats compare to the OTHER horses in the same race. This is the #1 feature type
that GBM models love — relative positioning within the competitive context.
Uses running stats from prior races (no leakage)."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/relative_to_field")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _HorseRunningStats:
    __slots__ = ("wins", "places", "total", "pos_sum", "speed_sum", "speed_n",
                 "gains", "recent_pos")

    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0
        self.pos_sum = 0.0
        self.speed_sum = 0.0
        self.speed_n = 0
        self.gains = 0.0
        self.recent_pos = deque(maxlen=10)


def main():
    logger = setup_logging("relative_to_field_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "relative_to_field_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_stats: dict[str, _HorseRunningStats] = {}

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
                    _process_course(course_records, fout, horse_stats, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_stats, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_stats, fills):
    # Step 1: Gather stats for all horses in this race
    field_data = []
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        st = horse_stats.get(hid)
        wr = st.wins / st.total if st and st.total >= 3 else None
        pr = st.places / st.total if st and st.total >= 3 else None
        avg_pos = st.pos_sum / st.total if st and st.total >= 3 else None
        avg_speed = st.speed_sum / st.speed_n if st and st.speed_n >= 3 else None
        exp = st.total if st else 0
        gains = st.gains if st else 0.0
        recent_avg = sum(st.recent_pos) / len(st.recent_pos) if st and len(st.recent_pos) >= 2 else None

        field_data.append({
            "hid": hid, "wr": wr, "pr": pr, "avg_pos": avg_pos,
            "avg_speed": avg_speed, "exp": exp, "gains": gains,
            "recent_avg": recent_avg,
        })

    # Step 2: Compute field-level stats (for horses with enough history)
    def _field_stats(key):
        vals = [d[key] for d in field_data if d[key] is not None]
        if len(vals) < 2:
            return None, None, None, None
        mean = sum(vals) / len(vals)
        std = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))
        return mean, std, min(vals), max(vals)

    wr_mean, wr_std, wr_min, wr_max = _field_stats("wr")
    pr_mean, pr_std, _, _ = _field_stats("pr")
    pos_mean, pos_std, _, _ = _field_stats("avg_pos")
    spd_mean, spd_std, _, _ = _field_stats("avg_speed")
    exp_vals = [d["exp"] for d in field_data]
    exp_mean = sum(exp_vals) / len(exp_vals) if exp_vals else 0
    gains_vals = [d["gains"] for d in field_data if d["gains"] > 0]
    gains_mean = sum(gains_vals) / len(gains_vals) if gains_vals else 0
    rec_mean, rec_std, _, _ = _field_stats("recent_avg")

    # Step 3: Compute relative features
    features_list = []
    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}
        d = field_data[i]

        # Win rate relative to field
        if d["wr"] is not None and wr_mean is not None:
            feat["rtf_wr_vs_field"] = round(d["wr"] - wr_mean, 5)
            fills["rtf_wr_vs_field"] += 1
            if wr_std and wr_std > 0:
                feat["rtf_wr_zscore"] = round((d["wr"] - wr_mean) / wr_std, 4)
                fills["rtf_wr_zscore"] += 1
            # Rank: best wr in field
            field_wrs = sorted([x["wr"] for x in field_data if x["wr"] is not None], reverse=True)
            if field_wrs:
                rank = field_wrs.index(d["wr"]) + 1 if d["wr"] in field_wrs else len(field_wrs)
                feat["rtf_wr_rank"] = rank
                feat["rtf_wr_percentile"] = round(1 - rank / len(field_wrs), 4) if len(field_wrs) > 1 else 0.5
                fills["rtf_wr_rank"] += 1
                fills["rtf_wr_percentile"] += 1

        # Place rate relative to field
        if d["pr"] is not None and pr_mean is not None:
            feat["rtf_pr_vs_field"] = round(d["pr"] - pr_mean, 5)
            fills["rtf_pr_vs_field"] += 1

        # Avg position relative to field
        if d["avg_pos"] is not None and pos_mean is not None:
            feat["rtf_pos_vs_field"] = round(d["avg_pos"] - pos_mean, 5)
            fills["rtf_pos_vs_field"] += 1

        # Speed relative to field
        if d["avg_speed"] is not None and spd_mean is not None:
            feat["rtf_speed_vs_field"] = round(d["avg_speed"] - spd_mean, 4)
            fills["rtf_speed_vs_field"] += 1
            if spd_std and spd_std > 0:
                feat["rtf_speed_zscore"] = round((d["avg_speed"] - spd_mean) / spd_std, 4)
                fills["rtf_speed_zscore"] += 1

        # Experience relative to field
        if exp_mean > 0:
            feat["rtf_exp_ratio"] = round(d["exp"] / exp_mean, 4)
            fills["rtf_exp_ratio"] += 1

        # Gains relative to field
        if d["gains"] > 0 and gains_mean > 0:
            feat["rtf_gains_ratio"] = round(d["gains"] / gains_mean, 4)
            fills["rtf_gains_ratio"] += 1

        # Recent form relative to field
        if d["recent_avg"] is not None and rec_mean is not None:
            feat["rtf_recent_vs_field"] = round(d["recent_avg"] - rec_mean, 5)
            fills["rtf_recent_vs_field"] += 1
            if rec_std and rec_std > 0:
                feat["rtf_recent_zscore"] = round((d["recent_avg"] - rec_mean) / rec_std, 4)
                fills["rtf_recent_zscore"] += 1

        # Is best in field? (binary)
        if d["wr"] is not None and wr_max is not None:
            feat["rtf_is_best_wr"] = 1 if d["wr"] == wr_max else 0
            fills["rtf_is_best_wr"] += 1

        # Composite strength score (normalized 0-1)
        scores = []
        if d["wr"] is not None and wr_std and wr_std > 0:
            scores.append((d["wr"] - wr_mean) / wr_std)
        if d["avg_speed"] is not None and spd_std and spd_std > 0:
            scores.append((d["avg_speed"] - spd_mean) / spd_std)
        if d["recent_avg"] is not None and rec_std and rec_std > 0:
            # Lower position = better, so negate
            scores.append(-(d["recent_avg"] - rec_mean) / rec_std)
        if scores:
            feat["rtf_composite_strength"] = round(sum(scores) / len(scores), 4)
            fills["rtf_composite_strength"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue

        pos = _safe(rec.get("position_arrivee"))
        field = _safe(rec.get("nombre_partants"))
        speed = _safe(rec.get("reduction_km_ms"))
        gains = _safe(rec.get("gains_carriere_euros"))
        is_winner = bool(rec.get("is_gagnant"))
        is_placed = False
        try:
            is_placed = int(rec.get("position_arrivee", 99)) <= 3
        except (TypeError, ValueError):
            pass

        if hid not in horse_stats:
            horse_stats[hid] = _HorseRunningStats()
        st = horse_stats[hid]

        st.wins += int(is_winner)
        st.places += int(is_placed)
        st.total += 1
        if pos is not None and field and field > 0:
            norm = pos / field
            st.pos_sum += norm
            st.recent_pos.append(norm)
        if speed is not None:
            st.speed_sum += speed
            st.speed_n += 1
        if gains is not None:
            st.gains = gains  # Career gains (cumulative from data)


if __name__ == "__main__":
    main()
