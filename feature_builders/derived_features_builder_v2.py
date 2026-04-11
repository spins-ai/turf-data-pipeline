#!/usr/bin/env python3
"""Derived features - STREAMING VERSION. 9 derived features from field
interactions and ratios. No OOM: processes course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/derived_features")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _safe_pos(val):
    """Safe float, must be > 0."""
    v = _safe(val)
    return v if v is not None and v > 0 else None


def main():
    logger = setup_logging("derived_features_builder_v2")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "derived_features_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        current_course = None
        course_records = []

        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                cuid = rec.get("course_uid", "")

                if cuid != current_course and course_records:
                    _process_course(course_records, fout, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, fills):
    # Pre-compute gains_per_race for ranking within course
    gains_per_race = []
    for rec in records:
        g = _safe(rec.get("gains_carriere_euros"))
        nc = _safe_pos(rec.get("nb_courses_carriere"))
        if g is not None and nc is not None:
            gains_per_race.append(g / nc)
        else:
            gains_per_race.append(None)

    # Rank gains_per_race within course (0.0 = best, 1.0 = worst)
    scored = [(i, v) for i, v in enumerate(gains_per_race) if v is not None]
    gains_rank = [None] * len(records)
    if len(scored) >= 2:
        scored.sort(key=lambda x: -x[1])
        n = len(scored)
        for rank, (i, _) in enumerate(scored):
            gains_rank[i] = rank / (n - 1)

    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        g = _safe(rec.get("gains_carriere_euros"))
        nc = _safe_pos(rec.get("nb_courses_carriere"))
        cf = _safe(rec.get("cote_finale")) or _safe(rec.get("cote_reference"))
        cr = _safe(rec.get("cote_reference"))
        pm5 = _safe_pos(rec.get("seq_position_moy_5"))
        nv = _safe_pos(rec.get("nb_victoires_carriere"))
        ppk = _safe(rec.get("poids_porte_kg"))
        dist = _safe_pos(rec.get("distance"))
        ga = _safe(rec.get("gains_annee_euros"))
        cd = _safe(rec.get("spd_is_class_drop"))
        ie = _safe(rec.get("is_inedit"))
        p2 = _safe(rec.get("nb_places_2eme"))
        p3 = _safe(rec.get("nb_places_3eme"))

        # 1. class_drop × gains
        if cd is not None and g is not None:
            feat["df_class_drop_x_gains"] = round(cd * g, 2)
            fills["df_class_drop_x_gains"] += 1

        # 2. cote vs form
        if cf is not None and pm5 is not None:
            feat["df_cote_vs_form"] = round(cf / pm5, 4)
            fills["df_cote_vs_form"] += 1

        # 3. inedit × experience
        if ie is not None and nc is not None:
            feat["df_inedit_x_exp"] = round(ie * nc, 1)
            fills["df_inedit_x_exp"] += 1

        # 4. places 2+3 rate
        if p2 is not None and p3 is not None and nc is not None:
            feat["df_places_23_rate"] = round((p2 + p3) / nc, 4)
            fills["df_places_23_rate"] += 1

        # 5. gains per race rank
        if gains_rank[i] is not None:
            feat["df_gains_rank"] = round(gains_rank[i], 4)
            fills["df_gains_rank"] += 1

        # 6. gains per victory
        if g is not None and nv is not None:
            feat["df_gains_per_win"] = round(g / nv, 2)
            fills["df_gains_per_win"] += 1

        # 7. cote ratio (drift)
        if cf is not None and cr is not None:
            feat["df_cote_ratio"] = round(cf / cr, 4)
            fills["df_cote_ratio"] += 1

        # 8. poids par distance
        if ppk is not None and dist is not None:
            feat["df_poids_par_km"] = round(ppk / (dist / 1000), 4)
            fills["df_poids_par_km"] += 1

        # 9. gains momentum
        if ga is not None and g is not None and g > 0:
            feat["df_gains_momentum"] = round(ga / g, 4)
            fills["df_gains_momentum"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
