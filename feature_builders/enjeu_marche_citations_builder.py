#!/usr/bin/env python3
"""Enjeux/marché/citations features: betting volume analysis, market share,
citations data, enjeu patterns, and smart money detection from mch_ + cit_ fields."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/enjeu_marche_citations")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


def main():
    logger = setup_logging("enjeu_marche_citations_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "enjeu_marche_citations_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

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
    # Collect field-level enjeu data
    total_enjeu = None
    enjeux = []
    pct_masses = []

    for rec in records:
        e = _safe(rec.get("mch_enjeu_combinaison"))
        if e: enjeux.append(e)
        p = _safe(rec.get("mch_pct_masse"))
        if p: pct_masses.append(p)

    total_field_enjeu = sum(enjeux) if enjeux else None

    # Cote stats
    cote_moy = _safe(records[0].get("mch_cote_moyenne_course"))
    cote_med = _safe(records[0].get("mch_cote_mediane_course"))

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        # MCH features
        enjeu = _safe(rec.get("mch_enjeu_combinaison"))
        pct_masse = _safe(rec.get("mch_pct_masse"))
        rang_combi = _safe(rec.get("mch_rang_combinaison"))
        rang_cote = _safe(rec.get("mch_rang_cote"))
        total_pari = _safe(rec.get("mch_total_enjeu_pari"))
        is_favori = rec.get("mch_is_favori")
        is_outsider = rec.get("mch_is_outsider")
        ecart_moy = _safe(rec.get("mch_ecart_cote_moyenne"))
        nb_partants = _safe(rec.get("mch_nb_partants_course"))
        mch_proba = _safe(rec.get("mch_proba_implicite"))

        # 1. Enjeu features
        if enjeu is not None:
            feat["emc_enjeu"] = round(enjeu, 0)
            feat["emc_log_enjeu"] = round(math.log1p(enjeu), 3)
            fills["emc_enjeu"] += 1
            fills["emc_log_enjeu"] += 1

            if total_field_enjeu and total_field_enjeu > 0:
                feat["emc_enjeu_share"] = round(enjeu / total_field_enjeu, 5)
                fills["emc_enjeu_share"] += 1

        # 2. Pct masse (market share)
        if pct_masse is not None:
            feat["emc_pct_masse"] = round(pct_masse, 4)
            fills["emc_pct_masse"] += 1

        # 3. Rang combinaison vs rang cote (disagreement = smart money?)
        if rang_combi is not None:
            feat["emc_rang_combi"] = int(rang_combi)
            fills["emc_rang_combi"] += 1

        if rang_cote is not None:
            feat["emc_rang_cote"] = int(rang_cote)
            fills["emc_rang_cote"] += 1

        if rang_combi is not None and rang_cote is not None:
            # If money rank < cote rank = smart money flowing in
            feat["emc_money_vs_odds_gap"] = int(rang_cote - rang_combi)
            fills["emc_money_vs_odds_gap"] += 1
            feat["emc_smart_money_signal"] = 1 if rang_combi < rang_cote - 2 else 0
            fills["emc_smart_money_signal"] += 1

        # 4. Market favori/outsider
        if is_favori is not None:
            feat["emc_is_mch_favori"] = 1 if is_favori else 0
            fills["emc_is_mch_favori"] += 1
        if is_outsider is not None:
            feat["emc_is_mch_outsider"] = 1 if is_outsider else 0
            fills["emc_is_mch_outsider"] += 1

        # 5. Ecart vs moyenne (value signal)
        if ecart_moy is not None:
            feat["emc_ecart_cote_moy"] = round(ecart_moy, 2)
            fills["emc_ecart_cote_moy"] += 1

        # 6. MCH proba implicite
        if mch_proba is not None:
            feat["emc_mch_proba"] = round(mch_proba, 5)
            fills["emc_mch_proba"] += 1

        # 7. Total pari (race attractiveness)
        if total_pari is not None:
            feat["emc_total_pari"] = round(total_pari, 0)
            feat["emc_log_total_pari"] = round(math.log1p(total_pari), 3)
            fills["emc_total_pari"] += 1
            fills["emc_log_total_pari"] += 1

        # === CITATIONS ===
        cit_enjeu = _safe(rec.get("cit_enjeu_total"))
        cit_ratio = _safe(rec.get("cit_ratio_marche"))
        cit_favori = rec.get("cit_is_favori_citations")

        if cit_enjeu is not None:
            feat["emc_cit_enjeu"] = round(cit_enjeu, 0)
            feat["emc_cit_log_enjeu"] = round(math.log1p(cit_enjeu), 3)
            fills["emc_cit_enjeu"] += 1
            fills["emc_cit_log_enjeu"] += 1

        if cit_ratio is not None:
            feat["emc_cit_ratio"] = round(cit_ratio, 3)
            fills["emc_cit_ratio"] += 1

        if cit_favori is not None:
            feat["emc_cit_favori"] = 1 if cit_favori else 0
            fills["emc_cit_favori"] += 1

        # 8. Combinaison array analysis
        combi = rec.get("mch_combinaison")
        if isinstance(combi, list) and combi:
            feat["emc_combi_size"] = len(combi)
            fills["emc_combi_size"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
