#!/usr/bin/env python3
"""Features about owner (proprietaire) racing patterns and stable form."""
from __future__ import annotations
import argparse, gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/owner_pattern")
_LOG_EVERY = 500_000
_SHRINK_K = 20


def _safe(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


class _OwnerState:
    __slots__ = (
        "wins", "total", "places",
        "horses_seen", "trainers_seen", "hippos_seen",
        "discipline_counts", "recent_results",
        "total_gains", "total_engagement",
        "per_hippo_wins", "per_hippo_total",
        "per_dist_wins", "per_dist_total",
    )

    def __init__(self):
        self.wins = 0
        self.total = 0
        self.places = 0
        self.horses_seen = set()
        self.trainers_seen = set()
        self.hippos_seen = set()
        self.discipline_counts = defaultdict(int)  # disc -> count
        self.recent_results = deque(maxlen=30)  # (won, placed)
        self.total_gains = 0.0
        self.total_engagement = 0.0
        self.per_hippo_wins = defaultdict(int)
        self.per_hippo_total = defaultdict(int)
        self.per_dist_wins = defaultdict(int)
        self.per_dist_total = defaultdict(int)


def _dist_bucket(d):
    if d is None:
        return None
    if d < 1400:
        return "sprint"
    if d < 1800:
        return "mile"
    if d < 2200:
        return "inter"
    if d < 2800:
        return "staying"
    return "long"


def _shrunk(wins, total, global_rate, k=_SHRINK_K):
    if total == 0:
        return None
    return (total * (wins / total) + k * global_rate) / (total + k)


def main():
    logger = setup_logging("owner_pattern_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "owner_pattern_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    owner_states: dict[str, _OwnerState] = {}
    global_wins = 0
    global_total = 0

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
                    gw, gt = _process_course(
                        course_records, fout, owner_states,
                        global_wins, global_total, fills,
                    )
                    global_wins = gw
                    global_total = gt
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,} lines, written {written:,}")
                    gc.collect()

        if course_records:
            gw, gt = _process_course(
                course_records, fout, owner_states,
                global_wins, global_total, fills,
            )
            global_wins = gw
            global_total = gt
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, owner_states, global_wins, global_total, fills):
    hippo = records[0].get("hippodrome_normalise", "")
    disc = records[0].get("discipline", "")
    global_rate = global_wins / global_total if global_total > 0 else 0.08

    # Check for multiple runners by same owner in this race
    owner_counts = defaultdict(int)
    for rec in records:
        owner = rec.get("proprietaire") or rec.get("nom_proprietaire") or ""
        if owner:
            owner_counts[owner] += 1

    features_list = []
    for rec in records:
        owner = rec.get("proprietaire") or rec.get("nom_proprietaire") or ""
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        trainer = rec.get("entraineur") or rec.get("nom_entraineur") or ""
        distance = _safe(rec.get("distance"))
        dist_b = _dist_bucket(distance)
        gains = _safe(rec.get("gains_carriere_euros"))
        engagement = _safe(rec.get("engagement"))

        feat = {"partant_uid": rec.get("partant_uid", "")}

        if not owner:
            features_list.append(feat)
            continue

        st = owner_states.get(owner)

        if st is not None and st.total > 0:
            # 1. Owner shrunk win rate
            feat["op_owner_wr"] = _shrunk(st.wins, st.total, global_rate)
            fills["op_owner_wr"] += 1

            # 2. Owner place rate
            feat["op_owner_place_rate"] = _shrunk(st.places, st.total, global_rate * 3)
            fills["op_owner_place_rate"] += 1

            # 3. Active horses
            feat["op_owner_active_horses"] = len(st.horses_seen)
            fills["op_owner_active_horses"] += 1

            # 4. Total races
            feat["op_owner_total_races"] = st.total
            fills["op_owner_total_races"] += 1

            # 5. Trainer diversification
            feat["op_owner_nb_trainers"] = len(st.trainers_seen)
            fills["op_owner_nb_trainers"] += 1

            # 6. Owner-trainer loyalty
            if trainer and st.total > 0:
                trainer_count = sum(1 for _ in st.recent_results)  # approximation
                # Better: count in discipline_counts or add trainer tracking
                feat["op_owner_trainer_loyalty"] = None  # Would need more state

            # 7. Avg horse quality
            if st.total > 0 and st.total_gains > 0:
                feat["op_owner_avg_gains"] = st.total_gains / len(st.horses_seen) if st.horses_seen else None
                if feat.get("op_owner_avg_gains") is not None:
                    fills["op_owner_avg_gains"] += 1

            # 8. Hippodrome experience
            feat["op_owner_knows_hippo"] = 1 if hippo in st.hippos_seen else 0
            fills["op_owner_knows_hippo"] += 1

            # 9. Discipline preference
            total_disc = sum(st.discipline_counts.values())
            if total_disc > 0 and disc:
                feat["op_owner_disc_pref"] = st.discipline_counts.get(disc, 0) / total_disc
                fills["op_owner_disc_pref"] += 1

            # 10. Owner at hippodrome win rate
            ht = st.per_hippo_total.get(hippo, 0)
            if ht > 0:
                feat["op_owner_hippo_wr"] = st.per_hippo_wins.get(hippo, 0) / ht
                fills["op_owner_hippo_wr"] += 1

            # 12. Stable form (last 20 results)
            recent = list(st.recent_results)
            if len(recent) >= 5:
                recent_wins = sum(1 for w, _ in recent[-20:] if w)
                feat["op_owner_stable_form"] = recent_wins / len(recent[-20:])
                fills["op_owner_stable_form"] += 1

            # 13. Investment level
            if st.total_engagement > 0 and st.total > 0:
                feat["op_owner_avg_engagement"] = st.total_engagement / st.total
                fills["op_owner_avg_engagement"] += 1

            # 14. Distance bucket win rate
            if dist_b and st.per_dist_total.get(dist_b, 0) > 0:
                feat["op_owner_dist_wr"] = st.per_dist_wins[dist_b] / st.per_dist_total[dist_b]
                fills["op_owner_dist_wr"] += 1

        # 11. Multiple runner flag
        feat["op_owner_multi_runner"] = 1 if owner_counts.get(owner, 0) > 1 else 0
        fills["op_owner_multi_runner"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features emitted
    for rec in records:
        owner = rec.get("proprietaire") or rec.get("nom_proprietaire") or ""
        if not owner:
            continue

        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        trainer = rec.get("entraineur") or rec.get("nom_entraineur") or ""
        distance = _safe(rec.get("distance"))
        dist_b = _dist_bucket(distance)
        is_winner = bool(rec.get("is_gagnant"))
        pos = rec.get("position_arrivee")
        is_placed = False
        try:
            is_placed = int(pos) <= 3
        except (TypeError, ValueError):
            pass
        gains = _safe(rec.get("gains_carriere_euros"))
        engagement = _safe(rec.get("engagement"))

        if owner not in owner_states:
            owner_states[owner] = _OwnerState()
        st = owner_states[owner]

        st.wins += int(is_winner)
        st.places += int(is_placed)
        st.total += 1
        global_wins += int(is_winner)
        global_total += 1

        if hid:
            st.horses_seen.add(hid)
        if trainer:
            st.trainers_seen.add(trainer)
        st.hippos_seen.add(hippo)

        disc = rec.get("discipline", "")
        if disc:
            st.discipline_counts[disc] += 1

        st.recent_results.append((is_winner, is_placed))

        if gains is not None:
            st.total_gains = max(st.total_gains, gains * len(st.horses_seen) if st.horses_seen else gains)

        if engagement is not None:
            st.total_engagement += engagement

        st.per_hippo_wins[hippo] += int(is_winner)
        st.per_hippo_total[hippo] += 1

        if dist_b:
            st.per_dist_wins[dist_b] += int(is_winner)
            st.per_dist_total[dist_b] += 1

    return global_wins, global_total


if __name__ == "__main__":
    main()
