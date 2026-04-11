#!/usr/bin/env python3
"""Debut/maiden features: detect first-time runners, maiden status,
new combinations (horse-jockey, horse-distance), and debut performance patterns."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/inedit_debutant")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def _dist_bucket(d):
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "stay"
    return "long"


class _HorseDebutState:
    __slots__ = ("total", "wins", "has_won", "distances_seen",
                 "jockeys_seen", "hippos_seen")

    def __init__(self):
        self.total = 0
        self.wins = 0
        self.has_won = False
        self.distances_seen = set()
        self.jockeys_seen = set()
        self.hippos_seen = set()


def main():
    logger = setup_logging("inedit_debutant_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "inedit_debutant_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseDebutState] = {}

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
    distance = _safe(records[0].get("distance"))
    dist_b = _dist_bucket(distance)
    hippo = (records[0].get("hippodrome_normalise") or records[0].get("hippodrome") or records[0].get("nom_hippodrome") or "").strip()

    # Count debutants in field
    field_debutants = 0
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if hid and hid not in horse_states:
            field_debutants += 1

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()

        if hid:
            st = horse_states.get(hid)

            # 1. Is debut
            is_debut = 1 if st is None else 0
            feat["id_is_debut"] = is_debut
            fills["id_is_debut"] += 1

            # 2. Career length
            if st:
                feat["id_career_runs"] = st.total
                fills["id_career_runs"] += 1

                # 3. Is maiden (never won)
                feat["id_is_maiden"] = 0 if st.has_won else 1
                fills["id_is_maiden"] += 1

                # 4. New distance
                if dist_b:
                    feat["id_new_distance"] = 0 if dist_b in st.distances_seen else 1
                    fills["id_new_distance"] += 1

                # 5. New jockey
                if jockey:
                    feat["id_new_jockey"] = 0 if jockey in st.jockeys_seen else 1
                    fills["id_new_jockey"] += 1

                # 6. New hippodrome
                if hippo:
                    feat["id_new_hippo"] = 0 if hippo in st.hippos_seen else 1
                    fills["id_new_hippo"] += 1

                # 7. Distances explored
                feat["id_n_distances"] = len(st.distances_seen)
                feat["id_n_jockeys"] = len(st.jockeys_seen)
                fills["id_n_distances"] += 1
                fills["id_n_jockeys"] += 1

            # 8. Debutants in field
            feat["id_field_debutants"] = field_debutants
            feat["id_field_debutant_pct"] = round(field_debutants / len(records), 4) if records else 0
            fills["id_field_debutants"] += 1
            fills["id_field_debutant_pct"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        if not hid:
            continue
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseDebutState()
        st = horse_states[hid]
        st.total += 1
        if is_winner:
            st.wins += 1
            st.has_won = True
        if dist_b:
            st.distances_seen.add(dist_b)
        if jockey:
            st.jockeys_seen.add(jockey)
        if hippo:
            st.hippos_seen.add(hippo)


if __name__ == "__main__":
    main()
