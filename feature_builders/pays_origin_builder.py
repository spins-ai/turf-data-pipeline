#!/usr/bin/env python3
"""Country of origin features: performance stats by horse nationality,
foreign runner advantage/disadvantage, country×distance affinity."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/pays_origin")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _dist_bucket(d):
    if d is None: return None
    if d < 1400: return "sprint"
    if d < 1800: return "mile"
    if d < 2200: return "inter"
    if d < 2800: return "stay"
    return "long"


class _CountryStats:
    __slots__ = ("wins", "places", "total", "wins_by_dist", "total_by_dist")

    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0
        self.wins_by_dist = defaultdict(int)
        self.total_by_dist = defaultdict(int)


def main():
    logger = setup_logging("pays_origin_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "pays_origin_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    country_stats: dict[str, _CountryStats] = {}
    global_wr_num = 0
    global_wr_den = 0

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
                    gwr = global_wr_num / global_wr_den if global_wr_den > 100 else None
                    _process_course(course_records, fout, country_stats, gwr, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            gwr = global_wr_num / global_wr_den if global_wr_den > 100 else None
            _process_course(course_records, fout, country_stats, gwr, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _extract_country(rec):
    """Extract country from various fields."""
    # Try direct field
    pays = (rec.get("pays") or rec.get("pays_origine") or
            rec.get("nationalite") or "").strip().upper()
    if pays:
        return pays

    # Try from horse name suffix (e.g. "HORSE_NAME (IRE)")
    nom = rec.get("nom_cheval") or ""
    if "(" in nom and ")" in nom:
        start = nom.rfind("(")
        end = nom.rfind(")")
        if end > start:
            code = nom[start + 1:end].strip().upper()
            if 1 <= len(code) <= 4:
                return code
    return ""


def _process_course(records, fout, country_stats, global_wr, fills):
    distance = _safe(records[0].get("distance"))
    dist_b = _dist_bucket(distance)

    # Detect if field is mixed-country
    countries_in_field = set()
    for rec in records:
        c = _extract_country(rec)
        if c:
            countries_in_field.add(c)
    is_international = len(countries_in_field) > 1

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        country = _extract_country(rec)

        if country:
            cst = country_stats.get(country)
            if cst and cst.total >= 30:
                wr = cst.wins / cst.total
                feat["po_country_wr"] = round(wr, 5)
                feat["po_country_pr"] = round(cst.places / cst.total, 5)
                feat["po_country_runs"] = cst.total
                fills["po_country_wr"] += 1
                fills["po_country_pr"] += 1
                fills["po_country_runs"] += 1

                # Country vs global
                if global_wr is not None:
                    feat["po_country_vs_global"] = round(wr - global_wr, 5)
                    fills["po_country_vs_global"] += 1

                # Country × distance
                if dist_b and cst.total_by_dist.get(dist_b, 0) >= 10:
                    feat["po_country_dist_wr"] = round(
                        cst.wins_by_dist[dist_b] / cst.total_by_dist[dist_b], 5)
                    fills["po_country_dist_wr"] += 1

            # Is foreign
            feat["po_is_foreign"] = 1 if country != "FR" else 0
            fills["po_is_foreign"] += 1

            # International field
            feat["po_international_field"] = 1 if is_international else 0
            fills["po_international_field"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        country = _extract_country(rec)
        if not country:
            continue
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe(rec.get("place_arrivee"))
        is_place = pos is not None and pos <= 3

        if country not in country_stats:
            country_stats[country] = _CountryStats()
        cst = country_stats[country]
        cst.wins += int(is_winner)
        cst.places += int(is_place)
        cst.total += 1
        if dist_b:
            cst.wins_by_dist[dist_b] += int(is_winner)
            cst.total_by_dist[dist_b] += 1


if __name__ == "__main__":
    main()
