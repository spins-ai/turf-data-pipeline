#!/usr/bin/env python3
"""
scripts/feature_type_metadata.py
=================================
Generates feature metadata catalog from partants_master.jsonl.

For each field in the first 1000 records, determines:
  - dtype (numeric / categorical / boolean / date / text)
  - cardinality (number of distinct values observed)
  - sample values (up to 5)
  - category: static vs dynamic vs id
  - origin: raw (from source) vs engineered (computed by builders) vs label (target)

Outputs:
  - data_master/feature_metadata.json  (structured catalog)
  - data_master/feature_types.csv      (flat summary)

Memory budget: < 1 GB (reads only first 1000 lines).

Usage:
    python scripts/feature_type_metadata.py
    python scripts/feature_type_metadata.py --lines 2000
    python scripts/feature_type_metadata.py --master data_master/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config import DATA_MASTER_DIR, PARTANTS_MASTER  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("feature_type_metadata")

# ---------------------------------------------------------------------------
# Classification heuristics
# ---------------------------------------------------------------------------

# Fields that are identifiers / keys (not real features)
ID_FIELDS = {
    "partant_uid", "course_uid", "reunion_uid", "cle_partant",
    "horse_id", "source", "timestamp_collecte",
}

# Label / target fields
LABEL_FIELDS = {
    "position_arrivee", "temps_ms", "reduction_km_ms",
    "is_gagnant", "is_place", "is_disqualifie",
    "cote_finale", "cote_reference", "proba_implicite",
}

# Static fields: horse intrinsic attributes that don't change race-to-race
STATIC_FIELDS = {
    "nom_cheval", "pere", "mere", "pere_mere", "eleveur",
    "sexe", "race", "robe", "pays_cheval", "pays_entrainement",
    "date_reunion_iso",
}

# Engineered feature prefixes (from feature builders)
ENGINEERED_PREFIXES = (
    "seq_", "met_", "ped_", "gnn_", "spd_", "rap_", "mch_",
    "cnd_", "pgr_", "elo_", "feat_", "fld_", "trn_", "jky_",
    "hda_", "hdd_", "hjk_", "htr_", "vbt_", "mti_", "pdm_",
)

# Date-looking patterns
DATE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"  # ISO date prefix
)

BOOL_VALUES = {"true", "false", "0", "1", "oui", "non", "yes", "no"}


def classify_dtype(values: list) -> str:
    """Infer dtype from a sample of non-None values."""
    if not values:
        return "unknown"

    # Check booleans first
    str_vals = {str(v).lower().strip() for v in values}
    if str_vals <= BOOL_VALUES:
        return "boolean"

    # Check dates
    date_count = sum(1 for v in values if isinstance(v, str) and DATE_RE.match(v))
    if date_count > len(values) * 0.7:
        return "date"

    # Check numeric
    numeric_count = 0
    for v in values:
        if isinstance(v, (int, float)):
            numeric_count += 1
        elif isinstance(v, str):
            try:
                float(v)
                numeric_count += 1
            except (ValueError, TypeError):
                pass
    if numeric_count > len(values) * 0.7:
        return "numeric"

    # Check text vs categorical: high-cardinality strings = text
    # Will be refined below with cardinality info
    return "text_or_categorical"


def classify_category(field_name: str) -> str:
    """Classify field as static / dynamic / id / label / engineered."""
    if field_name in ID_FIELDS:
        return "id"
    if field_name in LABEL_FIELDS:
        return "label"
    if field_name in STATIC_FIELDS:
        return "static"
    if field_name.startswith(ENGINEERED_PREFIXES):
        return "engineered"
    # Default: dynamic (changes per race)
    return "dynamic"


def classify_origin(field_name: str) -> str:
    """Classify origin: raw / engineered / label."""
    if field_name in LABEL_FIELDS:
        return "label"
    if field_name.startswith(ENGINEERED_PREFIXES):
        return "engineered"
    if field_name in ID_FIELDS:
        return "raw"
    return "raw"


def analyze_fields(master_path: Path, max_lines: int) -> dict:
    """Read first max_lines of master JSONL, compute per-field statistics."""

    # Per-field accumulators
    field_values: dict[str, list] = defaultdict(list)
    field_counters: dict[str, Counter] = defaultdict(Counter)
    field_types_seen: dict[str, Counter] = defaultdict(Counter)
    field_null_count: dict[str, int] = defaultdict(int)
    total_records = 0

    log.info("Reading up to %d lines from %s ...", max_lines, master_path)

    with open(master_path, "r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if i >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            total_records += 1

            for key, val in record.items():
                if val is None or val == "":
                    field_null_count[key] += 1
                    continue
                # Track Python type
                field_types_seen[key][type(val).__name__] += 1
                # Track value for cardinality (limit stored values for RAM)
                str_val = str(val)
                field_counters[key][str_val] += 1
                # Keep up to 200 sample values for dtype inference
                if len(field_values[key]) < 200:
                    field_values[key].append(val)

    log.info("Read %d records, discovered %d fields.", total_records, len(field_counters))

    # Build metadata per field
    all_fields = sorted(
        set(field_counters.keys()) | set(field_null_count.keys())
    )
    metadata: dict[str, dict] = {}

    for field in all_fields:
        counter = field_counters.get(field, Counter())
        cardinality = len(counter)
        samples = field_values.get(field, [])
        dtype_raw = classify_dtype(samples)

        # Refine text_or_categorical using cardinality
        if dtype_raw == "text_or_categorical":
            if cardinality <= 50:
                dtype_raw = "categorical"
            elif cardinality <= 500 and total_records > 0:
                ratio = cardinality / min(total_records, max_lines)
                dtype_raw = "categorical" if ratio < 0.1 else "text"
            else:
                dtype_raw = "text"

        null_count = field_null_count.get(field, 0)
        fill_rate = round(1.0 - (null_count / total_records), 4) if total_records else 0.0

        # Top 5 sample values
        top_values = [v for v, _ in counter.most_common(5)]

        category = classify_category(field)
        origin = classify_origin(field)

        is_numeric = dtype_raw == "numeric"
        is_categorical = dtype_raw in ("categorical", "boolean")

        metadata[field] = {
            "dtype": dtype_raw,
            "cardinality": cardinality,
            "fill_rate": fill_rate,
            "null_count": null_count,
            "total_records": total_records,
            "sample_values": top_values,
            "python_types_seen": dict(field_types_seen.get(field, {})),
            "category": category,
            "origin": origin,
            "is_numeric": is_numeric,
            "is_categorical": is_categorical,
        }

    return metadata


def write_json_catalog(metadata: dict, output_path: Path) -> None:
    """Write structured JSON catalog."""
    catalog = {
        "generated_at": datetime.now().isoformat(),
        "generator": "scripts/feature_type_metadata.py",
        "total_fields": len(metadata),
        "summary": {
            "numeric": sum(1 for m in metadata.values() if m["dtype"] == "numeric"),
            "categorical": sum(1 for m in metadata.values() if m["dtype"] == "categorical"),
            "boolean": sum(1 for m in metadata.values() if m["dtype"] == "boolean"),
            "date": sum(1 for m in metadata.values() if m["dtype"] == "date"),
            "text": sum(1 for m in metadata.values() if m["dtype"] == "text"),
            "unknown": sum(1 for m in metadata.values() if m["dtype"] == "unknown"),
        },
        "by_category": {
            "static": sum(1 for m in metadata.values() if m["category"] == "static"),
            "dynamic": sum(1 for m in metadata.values() if m["category"] == "dynamic"),
            "engineered": sum(1 for m in metadata.values() if m["category"] == "engineered"),
            "label": sum(1 for m in metadata.values() if m["category"] == "label"),
            "id": sum(1 for m in metadata.values() if m["category"] == "id"),
        },
        "by_origin": {
            "raw": sum(1 for m in metadata.values() if m["origin"] == "raw"),
            "engineered": sum(1 for m in metadata.values() if m["origin"] == "engineered"),
            "label": sum(1 for m in metadata.values() if m["origin"] == "label"),
        },
        "fields": metadata,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, indent=2, ensure_ascii=False, default=str)
    log.info("Wrote JSON catalog: %s (%d fields)", output_path, len(metadata))


def write_csv_summary(metadata: dict, output_path: Path) -> None:
    """Write flat CSV summary."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "name", "dtype", "category", "origin",
            "is_numeric", "is_categorical", "cardinality", "fill_rate",
        ])
        for field in sorted(metadata.keys()):
            m = metadata[field]
            writer.writerow([
                field,
                m["dtype"],
                m["category"],
                m["origin"],
                m["is_numeric"],
                m["is_categorical"],
                m["cardinality"],
                m["fill_rate"],
            ])
    log.info("Wrote CSV summary: %s", output_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate feature metadata catalog from partants_master.jsonl"
    )
    parser.add_argument(
        "--master", type=Path, default=PARTANTS_MASTER,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--lines", type=int, default=1000,
        help="Number of lines to sample (default: 1000)",
    )
    parser.add_argument(
        "--output-json", type=Path, default=DATA_MASTER_DIR / "feature_metadata.json",
        help="Output path for JSON catalog",
    )
    parser.add_argument(
        "--output-csv", type=Path, default=DATA_MASTER_DIR / "feature_types.csv",
        help="Output path for CSV summary",
    )
    args = parser.parse_args()

    if not args.master.exists():
        log.error("Master file not found: %s", args.master)
        sys.exit(1)

    metadata = analyze_fields(args.master, args.lines)
    write_json_catalog(metadata, args.output_json)
    write_csv_summary(metadata, args.output_csv)

    # Print summary
    dtypes = Counter(m["dtype"] for m in metadata.values())
    categories = Counter(m["category"] for m in metadata.values())
    origins = Counter(m["origin"] for m in metadata.values())

    print(f"\n{'='*60}")
    print(f"Feature Metadata Summary ({len(metadata)} fields)")
    print(f"{'='*60}")
    print(f"\nBy dtype:     {dict(dtypes)}")
    print(f"By category:  {dict(categories)}")
    print(f"By origin:    {dict(origins)}")
    print(f"\nOutputs:")
    print(f"  JSON: {args.output_json}")
    print(f"  CSV:  {args.output_csv}")


if __name__ == "__main__":
    main()
