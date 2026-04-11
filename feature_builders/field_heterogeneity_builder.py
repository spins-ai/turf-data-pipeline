#!/usr/bin/env python3
"""Field heterogeneity features: measure how diverse/competitive the field is
on multiple dimensions (odds spread, experience spread, class spread)."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/field_heterogeneity")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _std(vals):
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / len(vals))


def _cv(vals):
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    if m == 0:
        return None
    s = math.sqrt(sum((v - m) ** 2 for v in vals) / len(vals))
    return s / abs(m)


def main():
    logger = setup_logging("field_heterogeneity_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "field_heterogeneity_features.jsonl"
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
    # Collect field-level stats
    cotes = []
    ages = []
    weights = []
    allocs = []

    for rec in records:
        c = _safe(rec.get("cote_probable")) or _safe(rec.get("cote_actuelle"))
        if c is not None and c > 0:
            cotes.append(c)
        a = _safe(rec.get("age"))
        if a is not None:
            ages.append(a)
        w = _safe(rec.get("poids_porte_kg"))
        if w is not None and w > 0:
            weights.append(w)
        al = _safe(rec.get("allocation"))
        if al is not None:
            allocs.append(al)

    # Compute field-level metrics
    field_feats = {}

    # Odds spread
    if len(cotes) >= 3:
        field_feats["fh_odds_cv"] = round(_cv(cotes), 4)
        field_feats["fh_odds_range"] = round(max(cotes) - min(cotes), 2)
        field_feats["fh_odds_std"] = round(_std(cotes), 3)

        # Herfindahl index on implied probs
        ip = [1.0 / c for c in cotes]
        total_ip = sum(ip)
        if total_ip > 0:
            shares = [p / total_ip for p in ip]
            hhi = sum(s ** 2 for s in shares)
            field_feats["fh_hhi"] = round(hhi, 5)
            # Normalized: 1=monopoly, 0=perfect competition
            n = len(shares)
            if n > 1:
                field_feats["fh_hhi_norm"] = round((hhi - 1/n) / (1 - 1/n), 5)

    # Age spread
    if len(ages) >= 3:
        field_feats["fh_age_std"] = round(_std(ages), 3)
        field_feats["fh_age_range"] = round(max(ages) - min(ages), 0)

    # Weight spread
    if len(weights) >= 3:
        field_feats["fh_weight_cv"] = round(_cv(weights), 4)

    # Field size
    field_feats["fh_field_size"] = len(records)

    # Write features (same field-level features for all runners)
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        feat.update(field_feats)
        for k in field_feats:
            fills[k] += 1
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
