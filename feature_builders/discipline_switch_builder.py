#!/usr/bin/env python3
"""Discipline switch features: detect when a horse changes discipline (trot_attele <-> trot_monte,
plat <-> obstacle, etc.) and how it performs. Discipline switches are a strong predictor
of performance — some horses excel after switching, others crash."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/discipline_switch")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _HorseDisciplineState:
    __slots__ = (
        "prev_disc", "disc_history", "total",
        "disc_wins", "disc_total",  # wins/total per discipline
        "switch_count", "wins_after_switch", "total_after_switch",
        "is_switching",
    )

    def __init__(self):
        self.prev_disc = None
        self.disc_history = deque(maxlen=20)
        self.total = 0
        self.disc_wins = defaultdict(int)
        self.disc_total = defaultdict(int)
        self.switch_count = 0
        self.wins_after_switch = 0
        self.total_after_switch = 0
        self.is_switching = False


def main():
    logger = setup_logging("discipline_switch_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "discipline_switch_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseDisciplineState] = {}

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
    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        disc = rec.get("discipline", "")

        if not hid or not disc:
            features_list.append(feat)
            continue

        st = horse_states.get(hid)

        if st is not None and st.total >= 1 and st.prev_disc:
            # 1. Is this a discipline switch?
            is_switch = 1 if disc != st.prev_disc else 0
            feat["dsw_is_switch"] = is_switch
            fills["dsw_is_switch"] += 1

            # 2. Number of discipline switches in career
            feat["dsw_switch_count"] = st.switch_count
            fills["dsw_switch_count"] += 1

            # 3. Has horse raced in this discipline before?
            feat["dsw_has_disc_experience"] = 1 if st.disc_total.get(disc, 0) > 0 else 0
            fills["dsw_has_disc_experience"] += 1

            # 4. Win rate in current discipline
            dt = st.disc_total.get(disc, 0)
            if dt >= 3:
                feat["dsw_disc_wr"] = round(st.disc_wins.get(disc, 0) / dt, 4)
                fills["dsw_disc_wr"] += 1

            # 5. Win rate in previous discipline (comparison)
            pt = st.disc_total.get(st.prev_disc, 0)
            if pt >= 3:
                feat["dsw_prev_disc_wr"] = round(st.disc_wins.get(st.prev_disc, 0) / pt, 4)
                fills["dsw_prev_disc_wr"] += 1

            # 6. Win rate difference between disciplines
            if dt >= 3 and pt >= 3:
                curr_wr = st.disc_wins.get(disc, 0) / dt
                prev_wr = st.disc_wins.get(st.prev_disc, 0) / pt
                feat["dsw_wr_diff"] = round(curr_wr - prev_wr, 4)
                fills["dsw_wr_diff"] += 1

            # 7. Experience in current discipline
            feat["dsw_disc_experience"] = dt
            fills["dsw_disc_experience"] += 1

            # 8. Dominant discipline (the one with most races)
            if st.disc_total:
                dominant = max(st.disc_total, key=st.disc_total.get)
                feat["dsw_is_dominant_disc"] = 1 if disc == dominant else 0
                fills["dsw_is_dominant_disc"] += 1

            # 9. Switch frequency (switches / total races)
            if st.total >= 5:
                feat["dsw_switch_frequency"] = round(st.switch_count / st.total, 4)
                fills["dsw_switch_frequency"] += 1

            # 10. Historical win rate after switches
            if st.total_after_switch >= 3:
                feat["dsw_wr_after_switch"] = round(st.wins_after_switch / st.total_after_switch, 4)
                fills["dsw_wr_after_switch"] += 1

            # 11. Discipline diversity (number of distinct disciplines)
            n_discs = len([d for d, c in st.disc_total.items() if c > 0])
            feat["dsw_disc_diversity"] = n_discs
            fills["dsw_disc_diversity"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        disc = rec.get("discipline", "")
        if not hid or not disc:
            continue

        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseDisciplineState()
        st = horse_states[hid]

        is_switch = st.prev_disc is not None and disc != st.prev_disc
        if is_switch:
            st.switch_count += 1
            st.is_switching = True

        if st.is_switching:
            st.total_after_switch += 1
            st.wins_after_switch += int(is_winner)
            st.is_switching = False  # Only count the first race after switch

        st.disc_wins[disc] += int(is_winner)
        st.disc_total[disc] += 1
        st.disc_history.append(disc)
        st.prev_disc = disc
        st.total += 1


if __name__ == "__main__":
    main()
