#!/usr/bin/env python3
"""
audit_trail_builder.py — Pilier 5 : Auditabilite

For each record in partants_master (sample 1000), trace which source files
contributed to each field.  Build a provenance map:
    field_name -> source_script -> source_file

Outputs quality/audit_trail_report.md

Streams line-by-line with reservoir sampling to keep RAM under 2 GB.

Usage:
    python scripts/audit_trail_builder.py
"""

from __future__ import annotations

import json
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PARTANTS_MASTER, QUALITY_DIR

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SAMPLE_SIZE = 1000
RESERVOIR_SEED = 42
OUTPUT_REPORT = QUALITY_DIR / "audit_trail_report.md"

# ---------------------------------------------------------------------------
# Field -> source mapping (based on prefix conventions in the pipeline)
# ---------------------------------------------------------------------------
# The mega-merge prefixes fields by origin.  We also know the core fields
# come from the 04_resultats / participants API, enrichment fields from
# various merge scripts, etc.

PREFIX_TO_SOURCE: dict[str, tuple[str, str]] = {
    "rap_":  ("merge_rapports_master.py",       "output/21_rapports_definitifs, output/38_rapports_internet"),
    "mch_":  ("merge_marche_master.py",         "output/07_cotes_marche, output/28_combinaisons_marche"),
    "cnd_":  ("mega_merge_partants_master.py",  "output/48_conditions_texte"),
    "pgr_":  ("merge_pedigree_master.py",       "output/08_pedigree, output/14_pedigree"),
    "met_":  ("merge_meteo_master.py",          "output/00_enrichissement_meteo, output/13_meteo_historique"),
    "ped_":  ("merge_pedigree_master.py",       "output/08_pedigree, output/36_pedigree_query"),
    "seq_":  ("mega_merge_partants_master.py",  "output/41_sequences"),
    "gnn_":  ("mega_merge_partants_master.py",  "output/45_graphe_gnn"),
    "spd_":  ("mega_merge_partants_master.py",  "output/46_track_bias_speed"),
}

CORE_FIELDS_SOURCE = ("mega_merge_partants_master.py", "output/04_resultats (API PMU participants)")
ENRICHMENT_FIELDS = {
    "cote_finale", "cote_reference", "proba_implicite",
}
ENRICHMENT_SOURCE = ("merge_marche_master.py", "output/07_cotes_marche")


def infer_source(field_name: str) -> tuple[str, str]:
    """Return (script, source_files) for a given field name."""
    for prefix, source in PREFIX_TO_SOURCE.items():
        if field_name.startswith(prefix):
            return source
    if field_name in ENRICHMENT_FIELDS:
        return ENRICHMENT_SOURCE
    return CORE_FIELDS_SOURCE


def reservoir_sample(filepath: Path, k: int, seed: int) -> list[dict]:
    """Reservoir sampling of k records from a JSONL file — O(1) extra memory."""
    reservoir: list[dict] = []
    rng = random.Random(seed)
    n = 0
    with open(filepath, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            n += 1
            if n <= k:
                reservoir.append(rec)
            else:
                j = rng.randint(0, n - 1)
                if j < k:
                    reservoir[j] = rec
            # Progress
            if n % 500_000 == 0:
                print(f"  ... scanned {n:,} records")
    print(f"  Total scanned: {n:,} — sampled {len(reservoir):,}")
    return reservoir


def build_provenance_map(records: list[dict]) -> dict:
    """
    Build field_name -> {script, source_files, fill_rate, sample_values}.
    """
    field_counter: Counter = Counter()
    field_non_null: Counter = Counter()
    field_samples: dict[str, list] = defaultdict(list)
    total = len(records)

    for rec in records:
        for k, v in rec.items():
            field_counter[k] += 1
            if v is not None and v != "" and v != []:
                field_non_null[k] += 1
                if len(field_samples[k]) < 3:
                    field_samples[k].append(v)

    provenance: dict[str, dict] = {}
    for field in sorted(field_counter.keys()):
        script, sources = infer_source(field)
        fill = field_non_null[field] / total * 100 if total else 0
        provenance[field] = {
            "script": script,
            "source_files": sources,
            "fill_rate_pct": round(fill, 1),
            "present_in_n": field_counter[field],
            "non_null_in_n": field_non_null[field],
            "sample_values": field_samples[field],
        }
    return provenance


def detect_output_dirs(project_root: Path) -> list[dict]:
    """Scan output/ for all subdirectories and their file counts/sizes."""
    output_dir = project_root / "output"
    if not output_dir.exists():
        return []
    dirs_info = []
    for d in sorted(output_dir.iterdir()):
        if not d.is_dir():
            continue
        files = list(d.rglob("*"))
        file_count = sum(1 for f in files if f.is_file())
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        dirs_info.append({
            "name": d.name,
            "file_count": file_count,
            "total_size_mb": round(total_size / 1_048_576, 1),
        })
    return dirs_info


def write_report(
    provenance: dict,
    output_dirs: list[dict],
    sample_size: int,
    elapsed: float,
) -> None:
    """Write the audit trail report in Markdown."""
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total_fields = len(provenance)

    # Group fields by source script
    by_script: dict[str, list[str]] = defaultdict(list)
    for field, info in provenance.items():
        by_script[info["script"]].append(field)

    lines: list[str] = []
    lines.append("# Audit Trail Report (Pilier 5 — Auditabilite)")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"Sample size: {sample_size:,} records from `partants_master.jsonl`")
    lines.append(f"Elapsed: {elapsed:.1f}s")
    lines.append("")

    # --- Summary ---
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total unique fields | {total_fields} |")
    lines.append(f"| Source scripts (distinct) | {len(by_script)} |")
    lines.append(f"| Output directories | {len(output_dirs)} |")
    lines.append("")

    # --- Fields by source script ---
    lines.append("## Fields by Source Script")
    lines.append("")
    for script in sorted(by_script.keys()):
        fields = by_script[script]
        lines.append(f"### `{script}` ({len(fields)} fields)")
        lines.append("")
        lines.append("| Field | Fill Rate | Source Files |")
        lines.append("|-------|-----------|-------------|")
        for f in sorted(fields):
            info = provenance[f]
            lines.append(
                f"| `{f}` | {info['fill_rate_pct']:.1f}% | {info['source_files']} |"
            )
        lines.append("")

    # --- Fill rate distribution ---
    lines.append("## Fill Rate Distribution")
    lines.append("")
    buckets = {"100%": 0, "90-99%": 0, "50-89%": 0, "10-49%": 0, "<10%": 0}
    for info in provenance.values():
        fr = info["fill_rate_pct"]
        if fr >= 100:
            buckets["100%"] += 1
        elif fr >= 90:
            buckets["90-99%"] += 1
        elif fr >= 50:
            buckets["50-89%"] += 1
        elif fr >= 10:
            buckets["10-49%"] += 1
        else:
            buckets["<10%"] += 1
    lines.append("| Bucket | Count |")
    lines.append("|--------|-------|")
    for bucket, count in buckets.items():
        lines.append(f"| {bucket} | {count} |")
    lines.append("")

    # --- Output directory inventory ---
    lines.append("## Output Directory Inventory")
    lines.append("")
    lines.append("| Directory | Files | Size (MB) |")
    lines.append("|-----------|-------|-----------|")
    for d in output_dirs:
        lines.append(f"| `{d['name']}` | {d['file_count']} | {d['total_size_mb']} |")
    lines.append("")

    # --- Provenance detail (top 30 by fill rate, ascending) ---
    lines.append("## Lowest Fill Rate Fields (potential gaps)")
    lines.append("")
    sorted_by_fill = sorted(provenance.items(), key=lambda x: x[1]["fill_rate_pct"])
    lines.append("| Field | Fill Rate | Script | Source |")
    lines.append("|-------|-----------|--------|--------|")
    for field, info in sorted_by_fill[:30]:
        lines.append(
            f"| `{field}` | {info['fill_rate_pct']:.1f}% "
            f"| `{info['script']}` | {info['source_files']} |"
        )
    lines.append("")

    OUTPUT_REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to {OUTPUT_REPORT}")


def main() -> None:
    print("=" * 60)
    print("Pilier 5 — Audit Trail Builder")
    print("=" * 60)
    t0 = time.time()

    if not PARTANTS_MASTER.exists():
        print(f"ERROR: {PARTANTS_MASTER} not found.")
        sys.exit(1)

    print(f"\n[1/3] Reservoir sampling {SAMPLE_SIZE} records ...")
    records = reservoir_sample(PARTANTS_MASTER, SAMPLE_SIZE, RESERVOIR_SEED)

    print(f"\n[2/3] Building provenance map for {len(records)} records ...")
    provenance = build_provenance_map(records)

    print(f"\n[3/3] Scanning output directories ...")
    output_dirs = detect_output_dirs(PROJECT_ROOT)

    elapsed = time.time() - t0
    write_report(provenance, output_dirs, len(records), elapsed)

    print(f"\nDone in {elapsed:.1f}s — {len(provenance)} fields mapped.")


if __name__ == "__main__":
    main()
