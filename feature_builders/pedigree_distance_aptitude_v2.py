#!/usr/bin/env python3
"""Pedigree distance aptitude - STREAMING VERSION. 6 features:
sire WR by distance, sire WR by terrain, dam-sire WR,
inbreeding, stamina index, speed index.

No OOM: streaming course-by-course."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_distance_aptitude")
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
    if d < 1300: return "sprint"
    if d < 1900: return "mile"
    if d < 2500: return "inter"
    return "staying"


def _terrain(rec):
    t = (rec.get("terrain") or rec.get("etat_terrain") or rec.get("type_piste") or "").lower().strip()
    if not t: return None
    if "lourd" in t or "heavy" in t: return "heavy"
    if "souple" in t or "soft" in t: return "soft"
    if "bon" in t or "good" in t: return "good"
    if "sec" in t or "firm" in t: return "firm"
    return t[:10]


def main():
    logger = setup_logging("pedigree_distance_aptitude_v2")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "pedigree_distance_aptitude_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Accumulators (sire/damsire × distance/terrain)
    sire_dist = defaultdict(lambda: [0, 0])   # sire|dist_cat -> [wins, total]
    sire_terrain = defaultdict(lambda: [0, 0])  # sire|terrain -> [wins, total]
    damsire_all = defaultdict(lambda: [0, 0])   # damsire -> [wins, total]
    sire_win_dists = defaultdict(list)          # sire -> [winning distances]
    damsire_win_dists = defaultdict(list)        # damsire -> [winning distances]

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
                    _process_course(course_records, fout, sire_dist, sire_terrain,
                                    damsire_all, sire_win_dists, damsire_win_dists, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, sire_dist, sire_terrain,
                            damsire_all, sire_win_dists, damsire_win_dists, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, sire_dist, sire_terrain,
                    damsire_all, sire_win_dists, damsire_win_dists, fills):
    dc = _dist_cat(records[0].get("distance"))
    terr = _terrain(records[0])
    distance_m = _safe(records[0].get("distance"))

    # SNAPSHOT features
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        pere = _norm(rec.get("pere") or rec.get("nom_pere") or rec.get("sire"))
        pere_mere = _norm(rec.get("pere_mere") or rec.get("dam_sire") or rec.get("pgr_pere_mere"))

        # 1. Sire win rate at this distance
        if pere and dc:
            key = f"{pere}|{dc}"
            h = sire_dist.get(key)
            if h and h[1] >= 5:
                feat["pda_sire_dist_wr"] = round(h[0] / h[1], 4)
                fills["pda_sire_dist_wr"] += 1

        # 2. Sire win rate on this terrain
        if pere and terr:
            key = f"{pere}|{terr}"
            h = sire_terrain.get(key)
            if h and h[1] >= 5:
                feat["pda_sire_terrain_wr"] = round(h[0] / h[1], 4)
                fills["pda_sire_terrain_wr"] += 1

        # 3. Dam-sire overall WR
        if pere_mere:
            h = damsire_all.get(pere_mere)
            if h and h[1] >= 10:
                feat["pda_damsire_wr"] = round(h[0] / h[1], 4)
                fills["pda_damsire_wr"] += 1

        # 4. Stamina index (avg winning distance of sire's offspring)
        if pere:
            wdists = sire_win_dists.get(pere, [])
            if len(wdists) >= 5:
                avg_dist = sum(wdists) / len(wdists)
                feat["pda_sire_stamina"] = round(avg_dist / 2500, 3)  # normalized
                fills["pda_sire_stamina"] += 1

        # 5. Dam-sire stamina
        if pere_mere:
            wdists = damsire_win_dists.get(pere_mere, [])
            if len(wdists) >= 5:
                avg_dist = sum(wdists) / len(wdists)
                feat["pda_damsire_stamina"] = round(avg_dist / 2500, 3)
                fills["pda_damsire_stamina"] += 1

        # 6. Combined stamina × current distance match
        if pere and distance_m:
            wdists = sire_win_dists.get(pere, [])
            if len(wdists) >= 5:
                avg_dist = sum(wdists) / len(wdists)
                # Closer to 0 = better match
                feat["pda_dist_match"] = round(abs(distance_m - avg_dist) / 1000, 3)
                fills["pda_dist_match"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        pere = _norm(rec.get("pere") or rec.get("nom_pere") or rec.get("sire"))
        pere_mere = _norm(rec.get("pere_mere") or rec.get("dam_sire") or rec.get("pgr_pere_mere"))
        is_winner = bool(rec.get("is_gagnant"))

        if pere and dc:
            key = f"{pere}|{dc}"
            sire_dist[key][1] += 1
            if is_winner: sire_dist[key][0] += 1

        if pere and terr:
            key = f"{pere}|{terr}"
            sire_terrain[key][1] += 1
            if is_winner: sire_terrain[key][0] += 1

        if pere_mere:
            damsire_all[pere_mere][1] += 1
            if is_winner: damsire_all[pere_mere][0] += 1

        if is_winner and distance_m:
            d = int(distance_m)
            if pere:
                wl = sire_win_dists[pere]
                wl.append(d)
                if len(wl) > 50: sire_win_dists[pere] = wl[-50:]
            if pere_mere:
                wl = damsire_win_dists[pere_mere]
                wl.append(d)
                if len(wl) > 50: damsire_win_dists[pere_mere] = wl[-50:]


if __name__ == "__main__":
    main()
