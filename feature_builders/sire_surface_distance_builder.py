#!/usr/bin/env python3
"""Sire × Surface × Distance 3D builder - STREAMING. 6 features:
sire_surf_dist_wr, sire_surf_dist_n, sire_surf_wr, sire_dist_only_wr,
sire_versatility, sire_specialist_score.

3-way interaction: sire × surface_type × distance_category.
No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/sire_surface_distance")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _norm(name):
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 else None


def _dist_cat(distance):
    d = _safe(distance)
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "long"
    return "marathon"


def _surface(rec):
    """Extract surface type from record."""
    disc = (rec.get("discipline") or "").lower()
    if "trot" in disc:
        return "trot"
    piste = (rec.get("type_piste") or "").lower()
    if "gazon" in piste or "herbe" in piste or "turf" in piste:
        return "turf"
    if "sable" in piste or "psx" in piste or "fibr" in piste:
        return "aw"  # all-weather
    if "obstacle" in disc or "haie" in disc or "steeple" in disc:
        return "jump"
    if "plat" in disc:
        return "flat"
    return piste[:8] if piste else None


def main():
    logger = setup_logging("sire_surface_distance_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "sire_surface_distance_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Accumulators
    sire_ssd = defaultdict(lambda: [0, 0])   # sire|surface|dist -> [wins, total]
    sire_surf = defaultdict(lambda: [0, 0])  # sire|surface -> [wins, total]
    sire_dist = defaultdict(lambda: [0, 0])  # sire|dist -> [wins, total]
    sire_all = defaultdict(lambda: [0, 0])   # sire -> [wins, total]
    sire_combos = defaultdict(set)           # sire -> set of surface|dist combos won in

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
                    _process_course(course_records, fout, sire_ssd, sire_surf,
                                    sire_dist, sire_all, sire_combos, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, sire_ssd, sire_surf,
                            sire_dist, sire_all, sire_combos, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, sire_ssd, sire_surf, sire_dist,
                    sire_all, sire_combos, fills):
    r0 = records[0]
    dc = _dist_cat(r0.get("distance"))
    surf = _surface(r0)

    # SNAPSHOT features
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        pere = _norm(rec.get("pere") or rec.get("nom_pere") or rec.get("sire"))

        if pere:
            # 1. Sire × Surface × Distance WR (3-way)
            if surf and dc:
                key3 = f"{pere}|{surf}|{dc}"
                h = sire_ssd.get(key3)
                if h and h[1] >= 5:
                    feat["ssd_sire_surf_dist_wr"] = round(h[0] / h[1], 4)
                    feat["ssd_sire_surf_dist_n"] = h[1]
                    fills["ssd_sire_surf_dist_wr"] += 1
                    fills["ssd_sire_surf_dist_n"] += 1

            # 2. Sire × Surface WR
            if surf:
                key2 = f"{pere}|{surf}"
                h = sire_surf.get(key2)
                if h and h[1] >= 10:
                    feat["ssd_sire_surf_wr"] = round(h[0] / h[1], 4)
                    fills["ssd_sire_surf_wr"] += 1

            # 3. Sire × Distance WR
            if dc:
                key2 = f"{pere}|{dc}"
                h = sire_dist.get(key2)
                if h and h[1] >= 10:
                    feat["ssd_sire_dist_wr"] = round(h[0] / h[1], 4)
                    fills["ssd_sire_dist_wr"] += 1

            # 4. Sire versatility (nb of different surface|dist combos with wins)
            combos = sire_combos.get(pere, set())
            if combos:
                feat["ssd_sire_versatility"] = len(combos)
                fills["ssd_sire_versatility"] += 1

            # 5. Specialist score (how concentrated are sire's wins)
            sa = sire_all.get(pere)
            if sa and sa[1] >= 20:
                overall_wr = sa[0] / sa[1]
                if surf and dc:
                    key3 = f"{pere}|{surf}|{dc}"
                    h = sire_ssd.get(key3)
                    if h and h[1] >= 5 and overall_wr > 0:
                        specific_wr = h[0] / h[1]
                        feat["ssd_specialist_score"] = round(specific_wr / overall_wr, 4)
                        fills["ssd_specialist_score"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        pere = _norm(rec.get("pere") or rec.get("nom_pere") or rec.get("sire"))
        is_winner = bool(rec.get("is_gagnant"))

        if pere:
            sire_all[pere][1] += 1
            if is_winner: sire_all[pere][0] += 1

            if surf and dc:
                key3 = f"{pere}|{surf}|{dc}"
                sire_ssd[key3][1] += 1
                if is_winner:
                    sire_ssd[key3][0] += 1
                    sire_combos[pere].add(f"{surf}|{dc}")

            if surf:
                key2 = f"{pere}|{surf}"
                sire_surf[key2][1] += 1
                if is_winner: sire_surf[key2][0] += 1

            if dc:
                key2 = f"{pere}|{dc}"
                sire_dist[key2][1] += 1
                if is_winner: sire_dist[key2][0] += 1


if __name__ == "__main__":
    main()
