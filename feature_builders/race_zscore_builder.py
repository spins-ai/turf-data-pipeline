#!/usr/bin/env python3
"""Race-level z-scores: normalize each horse's key metrics relative to the race field.
Essential for tree models and neural nets - removes race-level effects."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/race_zscore")
_LOG_EVERY = 500_000

# Fields to z-score within each race
_ZSCORE_FIELDS = [
    ("gains_carriere_euros", "rz_gains"),
    ("nb_courses_carriere", "rz_experience"),
    ("nb_victoires_carriere", "rz_wins"),
    ("age", "rz_age"),
    ("cote_finale", "rz_cote"),
    ("poids_porte_kg", "rz_weight"),
    ("handicap_valeur", "rz_handicap"),
    ("proba_implicite", "rz_proba"),
    ("ecart_precedent", "rz_layoff"),
    ("nb_places_carriere", "rz_places"),
]


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _zscore(value, mean, std):
    if value is None or std is None or std == 0:
        return None
    return (value - mean) / std


def _field_stats(values):
    """Return (mean, std) for a list of non-None values."""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None, None
    m = sum(clean) / len(clean)
    var = sum((x - m) ** 2 for x in clean) / len(clean)
    return m, math.sqrt(var) if var > 0 else None


def main():
    logger = setup_logging("race_zscore_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "race_zscore_features.jsonl"
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
    n = len(records)

    # Extract values for each field
    field_values = {}
    for src_field, _ in _ZSCORE_FIELDS:
        field_values[src_field] = [_safe(rec.get(src_field)) for rec in records]

    # Compute field stats
    field_stats = {}
    for src_field, _ in _ZSCORE_FIELDS:
        field_stats[src_field] = _field_stats(field_values[src_field])

    # Also compute rank percentiles
    for i, rec in enumerate(records):
        feat = {"partant_uid": rec.get("partant_uid", "")}

        for src_field, out_prefix in _ZSCORE_FIELDS:
            val = field_values[src_field][i]
            mean, std = field_stats[src_field]

            # Z-score
            z = _zscore(val, mean, std)
            if z is not None:
                feat[f"{out_prefix}_zscore"] = round(z, 4)
                fills[f"{out_prefix}_zscore"] += 1

            # Percentile rank within field
            if val is not None:
                clean = [v for v in field_values[src_field] if v is not None]
                if len(clean) > 1:
                    rank = sum(1 for v in clean if v < val)
                    feat[f"{out_prefix}_pctrank"] = round(rank / (len(clean) - 1), 4)
                    fills[f"{out_prefix}_pctrank"] += 1

            # Min-max normalized within field
            if val is not None:
                clean = [v for v in field_values[src_field] if v is not None]
                mn, mx = min(clean), max(clean)
                if mx > mn:
                    feat[f"{out_prefix}_minmax"] = round((val - mn) / (mx - mn), 4)
                    fills[f"{out_prefix}_minmax"] += 1

        # Composite: average z-score across all available dimensions
        zscores = [feat.get(f"{p}_zscore") for _, p in _ZSCORE_FIELDS]
        valid_z = [z for z in zscores if z is not None]
        if valid_z:
            feat["rz_composite_zscore"] = round(sum(valid_z) / len(valid_z), 4)
            fills["rz_composite_zscore"] += 1

        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
