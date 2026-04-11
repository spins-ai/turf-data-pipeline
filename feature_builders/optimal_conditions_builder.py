#!/usr/bin/env python3
"""Optimal conditions features: for each horse, compute how similar the current race
conditions are to the conditions where the horse historically performs best.
Critical for prediction models - captures horse preferences."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/optimal_conditions")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


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
        return "stay"
    return "long"


class _HorseConditions:
    __slots__ = (
        "best_dist", "best_hippo", "best_surface", "best_disc",
        "dist_results", "hippo_results", "surface_results", "disc_results",
        "winning_distances", "winning_hippos", "winning_surfaces",
        "total_races",
    )

    def __init__(self):
        # condition -> [norm_positions]
        self.dist_results = defaultdict(lambda: deque(maxlen=30))
        self.hippo_results = defaultdict(lambda: deque(maxlen=30))
        self.surface_results = defaultdict(lambda: deque(maxlen=30))
        self.disc_results = defaultdict(lambda: deque(maxlen=30))
        self.winning_distances = deque(maxlen=20)
        self.winning_hippos = deque(maxlen=20)
        self.winning_surfaces = deque(maxlen=20)
        self.total_races = 0

    def best_condition(self, results_dict):
        """Find the condition with the lowest average norm position (=best)."""
        best_key = None
        best_avg = float("inf")
        for key, positions in results_dict.items():
            if len(positions) >= 2:
                avg = sum(positions) / len(positions)
                if avg < best_avg:
                    best_avg = avg
                    best_key = key
        return best_key, best_avg if best_key else (None, None)


def main():
    logger = setup_logging("optimal_conditions_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "optimal_conditions_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseConditions] = {}

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
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        feat = {"partant_uid": rec.get("partant_uid", "")}

        if not hid:
            features_list.append(feat)
            continue

        distance = _safe(rec.get("distance"))
        dist_b = _dist_bucket(distance)
        hippo = rec.get("hippodrome_normalise", "")
        surface = rec.get("type_piste", "")
        disc = rec.get("discipline", "")

        st = horse_states.get(hid)

        if st is not None and st.total_races >= 3:
            # 1. Is this the horse's best distance?
            best_dist, best_dist_avg = st.best_condition(st.dist_results)
            if best_dist and dist_b:
                feat["oc_is_best_distance"] = 1 if dist_b == best_dist else 0
                fills["oc_is_best_distance"] += 1

                # Avg performance at this distance vs best distance
                this_dist_pos = list(st.dist_results.get(dist_b, []))
                if this_dist_pos:
                    feat["oc_dist_perf_vs_best"] = round(
                        sum(this_dist_pos) / len(this_dist_pos) - best_dist_avg, 4
                    )
                    fills["oc_dist_perf_vs_best"] += 1

            # 2. Is this the horse's best hippodrome?
            best_hippo, best_hippo_avg = st.best_condition(st.hippo_results)
            if best_hippo and hippo:
                feat["oc_is_best_hippo"] = 1 if hippo == best_hippo else 0
                fills["oc_is_best_hippo"] += 1

                this_hippo_pos = list(st.hippo_results.get(hippo, []))
                if this_hippo_pos:
                    feat["oc_hippo_perf_vs_best"] = round(
                        sum(this_hippo_pos) / len(this_hippo_pos) - best_hippo_avg, 4
                    )
                    fills["oc_hippo_perf_vs_best"] += 1

            # 3. Is this the horse's best surface?
            best_surf, best_surf_avg = st.best_condition(st.surface_results)
            if best_surf and surface:
                feat["oc_is_best_surface"] = 1 if surface == best_surf else 0
                fills["oc_is_best_surface"] += 1

            # 4. Is this the horse's best discipline?
            best_disc, _ = st.best_condition(st.disc_results)
            if best_disc and disc:
                feat["oc_is_best_discipline"] = 1 if disc == best_disc else 0
                fills["oc_is_best_discipline"] += 1

            # 5. Conditions match score (how many optimal conditions match)
            match_score = 0
            match_total = 0
            if best_dist:
                match_total += 1
                if dist_b == best_dist:
                    match_score += 1
            if best_hippo:
                match_total += 1
                if hippo == best_hippo:
                    match_score += 1
            if best_surf:
                match_total += 1
                if surface == best_surf:
                    match_score += 1
            if best_disc:
                match_total += 1
                if disc == best_disc:
                    match_score += 1
            if match_total > 0:
                feat["oc_conditions_match_score"] = round(match_score / match_total, 4)
                fills["oc_conditions_match_score"] += 1

            # 6. Has horse won at this distance before?
            feat["oc_has_won_at_distance"] = 1 if dist_b in [_dist_bucket(d) for d in st.winning_distances] else 0
            fills["oc_has_won_at_distance"] += 1

            # 7. Has horse won at this hippodrome before?
            feat["oc_has_won_at_hippo"] = 1 if hippo in st.winning_hippos else 0
            fills["oc_has_won_at_hippo"] += 1

            # 8. Number of wins at this distance bucket
            dist_w = sum(1 for d in st.winning_distances if _dist_bucket(d) == dist_b)
            feat["oc_wins_at_distance"] = dist_w
            fills["oc_wins_at_distance"] += 1

            # 9. Performance at this exact distance vs overall avg
            this_dist_hist = list(st.dist_results.get(dist_b, []))
            all_positions = []
            for positions in st.dist_results.values():
                all_positions.extend(positions)
            if this_dist_hist and all_positions:
                overall_avg = sum(all_positions) / len(all_positions)
                this_avg = sum(this_dist_hist) / len(this_dist_hist)
                feat["oc_dist_vs_overall"] = round(this_avg - overall_avg, 4)
                fills["oc_dist_vs_overall"] += 1

            # 10. Experience at these conditions
            this_exp = len(list(st.dist_results.get(dist_b, [])))
            feat["oc_experience_at_conditions"] = this_exp
            fills["oc_experience_at_conditions"] += 1

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
        distance = _safe(rec.get("distance"))
        dist_b = _dist_bucket(distance)
        hippo = rec.get("hippodrome_normalise", "")
        surface = rec.get("type_piste", "")
        disc = rec.get("discipline", "")
        is_winner = bool(rec.get("is_gagnant"))

        if hid not in horse_states:
            horse_states[hid] = _HorseConditions()
        st = horse_states[hid]

        norm_pos = pos / field if pos is not None and field and field > 0 else None

        if norm_pos is not None:
            if dist_b:
                st.dist_results[dist_b].append(norm_pos)
            if hippo:
                st.hippo_results[hippo].append(norm_pos)
            if surface:
                st.surface_results[surface].append(norm_pos)
            if disc:
                st.disc_results[disc].append(norm_pos)

        if is_winner:
            if distance is not None:
                st.winning_distances.append(distance)
            if hippo:
                st.winning_hippos.append(hippo)
            if surface:
                st.winning_surfaces.append(surface)

        st.total_races += 1


if __name__ == "__main__":
    main()
