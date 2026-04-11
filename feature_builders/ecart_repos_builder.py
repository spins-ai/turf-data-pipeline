#!/usr/bin/env python3
"""Rest gap features: days between races, optimal rest patterns,
freshness indicators, and layoff performance impact."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/ecart_repos")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _parse_date(rec):
    """Extract date as ordinal for gap calculation."""
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


def _gap_bucket(days):
    if days is None: return None
    if days <= 7: return "week"
    if days <= 14: return "2weeks"
    if days <= 28: return "month"
    if days <= 60: return "2months"
    if days <= 120: return "layoff"
    return "long_layoff"


class _HorseRestState:
    __slots__ = ("last_date", "gaps", "wins_by_gap", "total_by_gap",
                 "race_dates", "total")

    def __init__(self):
        self.last_date = None
        self.gaps = deque(maxlen=15)
        self.wins_by_gap = defaultdict(int)
        self.total_by_gap = defaultdict(int)
        self.race_dates = deque(maxlen=20)
        self.total = 0


def main():
    logger = setup_logging("ecart_repos_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "ecart_repos_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseRestState] = {}
    gap_wins: dict[str, int] = defaultdict(int)
    gap_total: dict[str, int] = defaultdict(int)

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
                                    gap_wins, gap_total, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states,
                            gap_wins, gap_total, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, gap_wins, gap_total, fills):
    race_date = _parse_date(records[0])

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""

        if hid and race_date:
            st = horse_states.get(hid)
            if st and st.last_date is not None:
                gap_days = race_date - st.last_date

                if 0 < gap_days < 1000:
                    gap_b = _gap_bucket(gap_days)

                    # 1. Days since last race
                    feat["er_days_since_last"] = gap_days
                    fills["er_days_since_last"] += 1

                    # 2. Gap bucket
                    feat["er_is_fresh"] = 1 if gap_days <= 14 else 0
                    feat["er_is_layoff"] = 1 if gap_days > 60 else 0
                    fills["er_is_fresh"] += 1
                    fills["er_is_layoff"] += 1

                    # 3. Log gap (diminishing returns)
                    feat["er_log_gap"] = round(math.log1p(gap_days), 3)
                    fills["er_log_gap"] += 1

                    # 4. Gap vs horse's average gap
                    if st.gaps:
                        avg_gap = sum(st.gaps) / len(st.gaps)
                        if avg_gap > 0:
                            feat["er_gap_vs_avg"] = round(gap_days - avg_gap, 1)
                            feat["er_gap_ratio"] = round(gap_days / avg_gap, 3)
                            fills["er_gap_vs_avg"] += 1
                            fills["er_gap_ratio"] += 1

                    # 5. Racing frequency (races in last 90 days)
                    recent_count = sum(1 for d in st.race_dates
                                       if race_date - d <= 90)
                    feat["er_races_90d"] = recent_count
                    fills["er_races_90d"] += 1

                    # 6. Global gap bucket win rate
                    if gap_b and gap_total.get(gap_b, 0) >= 50:
                        feat["er_gap_bucket_wr"] = round(
                            gap_wins[gap_b] / gap_total[gap_b], 5)
                        fills["er_gap_bucket_wr"] += 1

                    # 7. Horse's win rate at this gap bucket
                    if gap_b and st.total >= 5:
                        ht = st.total_by_gap.get(gap_b, 0)
                        if ht >= 3:
                            feat["er_horse_gap_wr"] = round(
                                st.wins_by_gap.get(gap_b, 0) / ht, 5)
                            fills["er_horse_gap_wr"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        is_winner = bool(rec.get("is_gagnant"))

        if hid and race_date:
            if hid not in horse_states:
                horse_states[hid] = _HorseRestState()
            st = horse_states[hid]

            if st.last_date is not None:
                gap = race_date - st.last_date
                if 0 < gap < 1000:
                    st.gaps.append(gap)
                    gap_b = _gap_bucket(gap)
                    if gap_b:
                        st.wins_by_gap[gap_b] += int(is_winner)
                        st.total_by_gap[gap_b] += 1
                        gap_wins[gap_b] += int(is_winner)
                        gap_total[gap_b] += 1

            st.last_date = race_date
            st.race_dates.append(race_date)
            st.total += 1


if __name__ == "__main__":
    main()
