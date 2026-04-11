#!/usr/bin/env python3
"""Multi-surface versatility builder - STREAMING. 6 features:
horse_nb_surfaces, horse_nb_disciplines, horse_versatility_score,
horse_surface_specialist, horse_best_surface_wr, surface_match.

Tracks horses across different surfaces/disciplines.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/multi_surface_versatility")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _surface(rec):
    disc = (rec.get("discipline") or "").lower()
    if "trot" in disc:
        return "trot"
    piste = (rec.get("type_piste") or "").lower()
    if "gazon" in piste or "herbe" in piste or "turf" in piste:
        return "turf"
    if "sable" in piste or "psx" in piste or "fibr" in piste:
        return "aw"
    if "obstacle" in disc or "haie" in disc or "steeple" in disc:
        return "jump"
    if "plat" in disc:
        return "flat"
    return piste[:8] if piste else None


def _discipline(rec):
    disc = (rec.get("discipline") or "").lower()
    if "trot attelé" in disc or "trot attele" in disc: return "trot_att"
    if "trot monté" in disc or "trot monte" in disc: return "trot_mon"
    if "trot" in disc: return "trot"
    if "steeple" in disc: return "steeple"
    if "haie" in disc: return "haies"
    if "cross" in disc: return "cross"
    if "plat" in disc: return "plat"
    return disc[:10] if disc else None


def main():
    logger = setup_logging("multi_surface_versatility_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "multi_surface_versatility_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # horse -> {surface -> [wins, total]}
    horse_surface = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    # horse -> set of disciplines
    horse_disc = defaultdict(set)

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
                    _process_course(course_records, fout, horse_surface, horse_disc, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_surface, horse_disc, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_surface, horse_disc, fills):
    r0 = records[0]
    curr_surf = _surface(r0)
    curr_disc = _discipline(r0)

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()

        if horse and len(horse) >= 2:
            surf_data = horse_surface.get(horse)
            discs = horse_disc.get(horse, set())

            if surf_data:
                surfaces = dict(surf_data)
                nb_surfaces = len(surfaces)
                total_runs = sum(t for w, t in surfaces.values())

                if total_runs >= 3:
                    # 1. Number of different surfaces
                    feat["msv_nb_surfaces"] = nb_surfaces
                    fills["msv_nb_surfaces"] += 1

                    # 2. Number of different disciplines
                    feat["msv_nb_disciplines"] = len(discs)
                    fills["msv_nb_disciplines"] += 1

                    # 3. Versatility score (entropy-like)
                    if nb_surfaces >= 2:
                        probs = [t / total_runs for w, t in surfaces.values()]
                        entropy = -sum(p * math.log(p + 1e-10) for p in probs)
                        feat["msv_versatility"] = round(entropy, 4)
                        fills["msv_versatility"] += 1

                    # 4. Best surface WR
                    best_wr = 0
                    best_surf = None
                    for s, (w, t) in surfaces.items():
                        if t >= 3:
                            wr = w / t
                            if wr > best_wr:
                                best_wr = wr
                                best_surf = s
                    if best_wr > 0:
                        feat["msv_best_surface_wr"] = round(best_wr, 4)
                        fills["msv_best_surface_wr"] += 1

                        # 5. Is running on best surface?
                        if curr_surf and best_surf:
                            feat["msv_on_best_surface"] = 1 if curr_surf == best_surf else 0
                            fills["msv_on_best_surface"] += 1

                    # 6. Current surface WR
                    if curr_surf and curr_surf in surfaces:
                        sw, st = surfaces[curr_surf]
                        if st >= 2:
                            feat["msv_current_surface_wr"] = round(sw / st, 4)
                            fills["msv_current_surface_wr"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE
    for rec in records:
        horse = (rec.get("nom_cheval") or rec.get("cheval") or "").upper().strip()
        is_winner = bool(rec.get("is_gagnant"))

        if horse and len(horse) >= 2:
            if curr_surf:
                horse_surface[horse][curr_surf][1] += 1
                if is_winner: horse_surface[horse][curr_surf][0] += 1
            if curr_disc:
                horse_disc[horse].add(curr_disc)


if __name__ == "__main__":
    main()
