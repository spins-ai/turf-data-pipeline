#!/usr/bin/env python3
"""Pedigree advanced features - STREAMING VERSION.
15+ features: sire/dam/damsire stats, stamina/precocity indices,
inbreeding, lineage depth, discipline match, cross-nicking.

No OOM: streaming course-by-course with sire/dam accumulators."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pedigree_advanced")
_LOG_EVERY = 500_000

# Known sire profiles
SIRE_STAMINA = {
    "SIYOUNI": 0.35, "WOOTTON BASSETT": 0.40, "LOPE DE VEGA": 0.45,
    "DUBAWI": 0.50, "FRANKEL": 0.55, "GALILEO": 0.65,
    "DEEP IMPACT": 0.55, "KINGMAN": 0.40,
    "READY CASH": 0.60, "BOLD EAGLE": 0.55, "TIMOKO": 0.65,
    "LOVE YOU": 0.70, "OURASI": 0.75, "JASMIN DE FLORE": 0.60,
    "SAINT DES SAINTS": 0.80, "NETWORK": 0.75, "KAPGARDE": 0.85,
    "TURGEON": 0.80, "POLIGLOTE": 0.70,
}
SIRE_PRECOCITY = {
    "SIYOUNI": 0.80, "WOOTTON BASSETT": 0.75, "LOPE DE VEGA": 0.70,
    "DUBAWI": 0.65, "FRANKEL": 0.60, "GALILEO": 0.45,
    "READY CASH": 0.55, "BOLD EAGLE": 0.60,
}


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


def main():
    logger = setup_logging("pedigree_advanced_builder_v2")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "pedigree_advanced_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Accumulators for sire/dam/damsire (bounded by number of unique entities)
    sire_stats = defaultdict(lambda: [0, 0, 0, 0.0])  # [total, wins, places, gains]
    dam_stats = defaultdict(lambda: [0, 0, 0])  # [total, wins, places]
    damsire_stats = defaultdict(lambda: [0, 0])  # [total, wins]
    nick_stats = defaultdict(lambda: [0, 0])  # sire|damsire -> [total, wins]

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
                    _process_course(course_records, fout, sire_stats, dam_stats,
                                    damsire_stats, nick_stats, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, sire_stats, dam_stats,
                            damsire_stats, nick_stats, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, sire_stats, dam_stats, damsire_stats, nick_stats, fills):
    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        pere = _norm(rec.get("pere") or rec.get("nom_pere") or rec.get("sire"))
        mere = _norm(rec.get("mere") or rec.get("nom_mere") or rec.get("dam"))
        pere_mere = _norm(rec.get("pere_mere") or rec.get("dam_sire") or rec.get("broodmare_sire") or rec.get("pgr_pere_mere"))
        gp_p = _norm(rec.get("grand_pere_paternel") or rec.get("grandsire"))
        gp_m = _norm(rec.get("grand_pere_maternel"))
        discipline = (rec.get("discipline") or "").lower()

        # 1. Sire stats
        if pere:
            ss = sire_stats.get(pere)
            if ss and ss[0] >= 5:
                feat["pa_sire_nb"] = ss[0]
                feat["pa_sire_wr"] = round(ss[1] / ss[0], 4)
                feat["pa_sire_pr"] = round(ss[2] / ss[0], 4)
                feat["pa_sire_avg_gains"] = round(ss[3] / ss[0], 0)
                fills["pa_sire_nb"] += 1
                fills["pa_sire_wr"] += 1
                fills["pa_sire_pr"] += 1
                fills["pa_sire_avg_gains"] += 1

        # 2. Dam stats
        if mere:
            ds = dam_stats.get(mere)
            if ds and ds[0] >= 3:
                feat["pa_dam_nb"] = ds[0]
                feat["pa_dam_wr"] = round(ds[1] / ds[0], 4)
                feat["pa_dam_pr"] = round(ds[2] / ds[0], 4)
                fills["pa_dam_nb"] += 1
                fills["pa_dam_wr"] += 1
                fills["pa_dam_pr"] += 1

        # 3. Dam-sire stats
        if pere_mere:
            dss = damsire_stats.get(pere_mere)
            if dss and dss[0] >= 10:
                feat["pa_damsire_nb"] = dss[0]
                feat["pa_damsire_wr"] = round(dss[1] / dss[0], 4)
                fills["pa_damsire_nb"] += 1
                fills["pa_damsire_wr"] += 1

        # 4. Stamina / precocity indices
        if pere:
            st = SIRE_STAMINA.get(pere)
            if st is not None:
                feat["pa_sire_stamina"] = st
                fills["pa_sire_stamina"] += 1

                # Discipline match
                if "plat" in discipline:
                    feat["pa_disc_match"] = round(1.0 - st, 2)
                elif "obstacle" in discipline or "haie" in discipline or "steeple" in discipline:
                    feat["pa_disc_match"] = round(st, 2)
                elif "trot" in discipline:
                    feat["pa_disc_match"] = round(0.5 + (st - 0.5) * 0.5, 2)
                if "pa_disc_match" in feat:
                    fills["pa_disc_match"] += 1

            pr = SIRE_PRECOCITY.get(pere)
            if pr is not None:
                feat["pa_sire_precocity"] = pr
                fills["pa_sire_precocity"] += 1

        if pere_mere:
            ds_st = SIRE_STAMINA.get(pere_mere)
            if ds_st is not None:
                feat["pa_damsire_stamina"] = ds_st
                fills["pa_damsire_stamina"] += 1

        # 5. Inbreeding
        ancestors = set()
        inbred = False
        for anc in [pere, mere, pere_mere, gp_p, gp_m]:
            if anc:
                if anc in ancestors:
                    inbred = True
                ancestors.add(anc)
        feat["pa_inbreeding"] = 1 if inbred else 0
        fills["pa_inbreeding"] += 1

        # 6. Lineage depth
        depth = sum(1 for x in [pere, mere, pere_mere, gp_p, gp_m] if x)
        feat["pa_lineage_depth"] = depth
        feat["pa_full_pedigree"] = 1 if depth >= 4 else 0
        fills["pa_lineage_depth"] += 1
        fills["pa_full_pedigree"] += 1

        # 7. Cross-nicking
        if pere and pere_mere:
            nk = nick_stats.get(f"{pere}|{pere_mere}")
            if nk and nk[0] >= 3:
                feat["pa_nick_wr"] = round(nk[1] / nk[0], 4)
                feat["pa_nick_n"] = nk[0]
                fills["pa_nick_wr"] += 1
                fills["pa_nick_n"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # UPDATE accumulators
    for rec in records:
        pere = _norm(rec.get("pere") or rec.get("nom_pere") or rec.get("sire"))
        mere = _norm(rec.get("mere") or rec.get("nom_mere") or rec.get("dam"))
        pere_mere = _norm(rec.get("pere_mere") or rec.get("dam_sire") or rec.get("broodmare_sire") or rec.get("pgr_pere_mere"))
        is_winner = bool(rec.get("is_gagnant"))
        is_placed = bool(rec.get("is_place"))
        gains = _safe(rec.get("gains_carriere_euros")) or 0

        if pere:
            s = sire_stats[pere]
            s[0] += 1
            s[1] += int(is_winner)
            s[2] += int(is_placed)
            s[3] += gains

        if mere:
            d = dam_stats[mere]
            d[0] += 1
            d[1] += int(is_winner)
            d[2] += int(is_placed)

        if pere_mere:
            ds = damsire_stats[pere_mere]
            ds[0] += 1
            ds[1] += int(is_winner)

        if pere and pere_mere:
            nk = nick_stats[f"{pere}|{pere_mere}"]
            nk[0] += 1
            nk[1] += int(is_winner)


if __name__ == "__main__":
    main()
