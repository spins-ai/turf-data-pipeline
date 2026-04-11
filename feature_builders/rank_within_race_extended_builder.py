#!/usr/bin/env python3
"""Extended within-race ranking: rank each horse on multiple dimensions
relative to its field (historical wr, speed, cote, age, weight, etc.)."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/rank_within_race_extended")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _HorseRankState:
    __slots__ = ("wins", "places", "total", "recent_pos", "avg_speed")

    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0
        self.recent_pos = deque(maxlen=10)
        self.avg_speed = None


def main():
    logger = setup_logging("rank_within_race_extended_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "rank_within_race_extended_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseRankState] = {}

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


def _rank_field(values, reverse=False):
    """Rank values (1=best). Higher=better if reverse=True."""
    indexed = [(v, i) for i, v in enumerate(values) if v is not None]
    if not indexed:
        return {}
    indexed.sort(key=lambda x: x[0], reverse=reverse)
    ranks = {}
    for rank, (val, idx) in enumerate(indexed, 1):
        ranks[idx] = rank
    return ranks


def _process_course(records, fout, horse_states, fills):
    n = len(records)
    if n < 2:
        for rec in records:
            fout.write(json.dumps({"partant_uid": rec.get("partant_uid", "")},
                                  ensure_ascii=False) + "\n")
        return

    # Collect dimensions for ranking
    wr_vals = []
    pr_vals = []
    avg_pos_vals = []
    speed_vals = []
    cote_vals = []
    age_vals = []
    weight_vals = []
    runs_vals = []

    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        st = horse_states.get(hid) if hid else None

        if st and st.total >= 3:
            wr_vals.append(st.wins / st.total)
            pr_vals.append(st.places / st.total)
            rp = list(st.recent_pos)
            avg_pos_vals.append(sum(rp) / len(rp) if rp else None)
            speed_vals.append(st.avg_speed)
            runs_vals.append(st.total)
        else:
            wr_vals.append(None)
            pr_vals.append(None)
            avg_pos_vals.append(None)
            speed_vals.append(None)
            runs_vals.append(None)

        cote_vals.append(_safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference")) or _safe(rec.get("cote_probable")))
        age_vals.append(_safe(rec.get("age")))
        weight_vals.append(_safe(rec.get("poids_porte_kg")))

    # Compute ranks
    wr_ranks = _rank_field(wr_vals, reverse=True)
    pr_ranks = _rank_field(pr_vals, reverse=True)
    pos_ranks = _rank_field(avg_pos_vals, reverse=False)  # lower pos = better
    cote_ranks = _rank_field(cote_vals, reverse=False)  # lower cote = favorite
    age_ranks = _rank_field(age_vals, reverse=False)
    weight_ranks = _rank_field(weight_vals, reverse=False)  # lighter = better?
    runs_ranks = _rank_field(runs_vals, reverse=True)  # more exp = better

    features_list = []
    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        if i in wr_ranks:
            feat["rr_wr_rank"] = wr_ranks[i]
            feat["rr_wr_pctl"] = round(1 - wr_ranks[i] / n, 4)
            fills["rr_wr_rank"] += 1
            fills["rr_wr_pctl"] += 1
        if i in pr_ranks:
            feat["rr_pr_rank"] = pr_ranks[i]
            fills["rr_pr_rank"] += 1
        if i in pos_ranks:
            feat["rr_avgpos_rank"] = pos_ranks[i]
            fills["rr_avgpos_rank"] += 1
        if i in cote_ranks:
            feat["rr_cote_rank"] = cote_ranks[i]
            fills["rr_cote_rank"] += 1
        if i in age_ranks:
            feat["rr_age_rank"] = age_ranks[i]
            fills["rr_age_rank"] += 1
        if i in weight_ranks:
            feat["rr_weight_rank"] = weight_ranks[i]
            fills["rr_weight_rank"] += 1
        if i in runs_ranks:
            feat["rr_exp_rank"] = runs_ranks[i]
            fills["rr_exp_rank"] += 1

        # Composite rank (average of available ranks)
        all_r = []
        for rk in [wr_ranks, pr_ranks, pos_ranks, cote_ranks]:
            if i in rk:
                all_r.append(rk[i] / n)  # normalize to [0,1]
        if len(all_r) >= 2:
            feat["rr_composite_rank"] = round(sum(all_r) / len(all_r), 4)
            fills["rr_composite_rank"] += 1

        # Dominance: how many dimensions is this horse #1?
        top1_count = sum(1 for rk in [wr_ranks, pr_ranks, pos_ranks, cote_ranks]
                         if i in rk and rk[i] == 1)
        if any(i in rk for rk in [wr_ranks, pr_ranks, pos_ranks, cote_ranks]):
            feat["rr_top1_count"] = top1_count
            fills["rr_top1_count"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe(rec.get("place_arrivee"))
        speed = _safe(rec.get("reduction_km_ms"))

        if hid not in horse_states:
            horse_states[hid] = _HorseRankState()
        st = horse_states[hid]
        st.wins += int(is_winner)
        st.places += int(pos is not None and pos <= 3)
        st.total += 1
        if pos is not None:
            st.recent_pos.append(pos)
        if speed is not None:
            if st.avg_speed is None:
                st.avg_speed = speed
            else:
                st.avg_speed = 0.85 * st.avg_speed + 0.15 * speed


if __name__ == "__main__":
    main()
