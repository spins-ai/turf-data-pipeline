#!/usr/bin/env python3
"""
Fill Rate Dashboard - Measure data quality across partants_master.jsonl
Generates docs/FILL_RATES.md with detailed fill rate analysis.

Usage:
    python scripts/fill_rate_dashboard.py [--sample N] [--full]

Default: samples 5000 records for speed. Use --full for complete scan (slow, 25GB).
"""

import json
import os
import sys
import random
import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ─── Config ──────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_MASTER = BASE_DIR / "data_master" / "partants_master.jsonl"
PMU_ENRICHED = BASE_DIR / "output" / "101_pmu_api" / "pmu_participants_enriched.jsonl"
PMU_PARTICIPANTS = BASE_DIR / "output" / "101_pmu_api" / "pmu_participants.jsonl"
GENY_FLAT = BASE_DIR / "output" / "26_geny" / "geny_flat.jsonl"
PERF_DETAIL = BASE_DIR / "output" / "22_performances_detaillees" / "performances_detaillees.jsonl"
OUTPUT_MD = BASE_DIR / "docs" / "FILL_RATES.md"

# 30 most important ML fields
ML_KEY_FIELDS = [
    "cote_finale", "cote_reference", "proba_implicite",
    "position_arrivee", "is_gagnant", "nombre_partants",
    "distance", "discipline", "hippodrome_normalise",
    "nom_cheval", "jockey_driver", "entraineur",
    "age", "sexe", "poids_porte_kg",
    "deferre", "temps_ms", "reduction_km_ms",
    "handicap_valeur", "avis_entraineur",
    "pere", "mere", "race", "robe",
    "gains_carriere_euros", "nb_courses_carriere", "nb_victoires_carriere",
    "musique", "oeilleres", "incident",
]

# Field name mapping: master field -> (source_file_label, source_field)
# Used to find which scrapers can fill missing fields
FIELD_SOURCE_MAP = {
    "cote_finale": [("101_pmu_participants", "cote_direct")],
    "cote_reference": [("101_pmu_participants", "cote_reference")],
    "deferre": [("101_pmu_enriched", "deferre"), ("101_pmu_participants", "deferre")],
    "temps_ms": [("101_pmu_enriched", "tempsObtenu"), ("101_pmu_participants", "tempsObtenu")],
    "reduction_km_ms": [("101_pmu_enriched", "reductionKilometrique"), ("101_pmu_participants", "reductionKm")],
    "handicap_valeur": [("101_pmu_enriched", "handicapValeur")],
    "avis_entraineur": [("101_pmu_enriched", "avisEntraineur"), ("101_pmu_participants", "avisEntraineur")],
    "oeilleres": [("101_pmu_enriched", "oeilleres"), ("101_pmu_participants", "oeilleres")],
    "musique": [("101_pmu_participants", "musique")],
    "pere": [("101_pmu_participants", "nomPere")],
    "mere": [("101_pmu_participants", "nomMere")],
    "race": [("101_pmu_participants", "race"), ("26_geny", "col_11")],
    "robe": [],
    "incident": [],
    "poids_porte_kg": [("101_pmu_enriched", "poidsConditionMonte"), ("26_geny", "poids")],
    "gains_carriere_euros": [("101_pmu_participants", "gainsCarriere")],
    "nb_courses_carriere": [("101_pmu_participants", "nombreCourses")],
    "nb_victoires_carriere": [("101_pmu_participants", "nombreVictoires")],
    "sexe": [("101_pmu_participants", "sexe"), ("26_geny", "sexe_age")],
    "age": [("101_pmu_participants", "age")],
    "proba_implicite": [],  # computed field
    "position_arrivee": [("101_pmu_participants", "ordreArrivee")],
    "is_gagnant": [],  # derived from position_arrivee
    "nombre_partants": [],
    "distance": [],
    "discipline": [],
    "hippodrome_normalise": [],
    "nom_cheval": [("101_pmu_participants", "nom"), ("26_geny", "nom_cheval")],
    "jockey_driver": [("101_pmu_participants", "driver"), ("26_geny", "jockey")],
    "entraineur": [("101_pmu_participants", "entraineur"), ("26_geny", "entraineur")],
}


def is_filled(value):
    """Check if a value is meaningfully filled (not None, not empty, not 'N/A')."""
    if value is None:
        return False
    if isinstance(value, str):
        v = value.strip()
        return v != "" and v.lower() not in ("n/a", "na", "none", "null", "inconnu", "unknown", "")
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


def sample_jsonl(filepath, n=5000, seed=42):
    """Reservoir sampling of n records from a JSONL file. Memory-efficient."""
    print(f"  Sampling {n} records from {filepath.name}...")
    reservoir = []
    random.seed(seed)
    total = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            total = i + 1
            line = line.strip()
            if not line:
                continue
            if i < n:
                try:
                    reservoir.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
            else:
                j = random.randint(0, i)
                if j < n:
                    try:
                        reservoir[j] = json.loads(line)
                    except json.JSONDecodeError:
                        continue
            if total % 500000 == 0:
                print(f"    ... scanned {total:,} lines")
    print(f"  Total lines scanned: {total:,}, sampled: {len(reservoir)}")
    return reservoir, total


def compute_fill_rates(records, fields):
    """Compute fill rate for each field."""
    counts = {f: 0 for f in fields}
    n = len(records)
    if n == 0:
        return {f: 0.0 for f in fields}

    for rec in records:
        for f in fields:
            if is_filled(rec.get(f)):
                counts[f] += 1

    return {f: (counts[f] / n) * 100.0 for f in fields}


def compute_all_field_rates(records):
    """Compute fill rate for ALL fields found in records."""
    all_fields = set()
    for rec in records:
        all_fields.update(rec.keys())

    all_fields = sorted(all_fields)
    return compute_fill_rates(records, all_fields)


def load_pmu_enriched_keys(filepath):
    """Load join keys from PMU enriched data. Returns dict of (date, numReunion, numCourse, numPmu) -> record."""
    print(f"  Loading PMU enriched keys from {filepath.name}...")
    keys = {}
    filled_fields = defaultdict(int)
    total = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1
            key = (rec.get("date"), rec.get("numReunion"), rec.get("numCourse"), rec.get("numPmu"))
            keys[key] = rec
            for field, val in rec.items():
                if is_filled(val):
                    filled_fields[field] += 1
    print(f"  Loaded {total:,} PMU enriched records, {len(keys):,} unique keys")
    return keys, filled_fields, total


def load_pmu_participants_keys(filepath):
    """Load join keys from PMU participants. Returns dict of (date, num_reunion, num_course, numPmu) -> record."""
    print(f"  Loading PMU participants keys from {filepath.name}...")
    keys = {}
    filled_fields = defaultdict(int)
    total = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1
            key = (rec.get("date"), rec.get("num_reunion"), rec.get("num_course"), rec.get("numPmu"))
            keys[key] = rec
            for field, val in rec.items():
                if is_filled(val):
                    filled_fields[field] += 1
    print(f"  Loaded {total:,} PMU participants records, {len(keys):,} unique keys")
    return keys, filled_fields, total


def check_master_join_potential(master_sample, pmu_enriched_keys, pmu_participants_keys):
    """Check how many master records can join to PMU data."""
    matches_enriched = 0
    matches_participants = 0

    for rec in master_sample:
        date = rec.get("date_reunion_iso")
        num_r = rec.get("numero_reunion")
        num_c = rec.get("numero_course")
        num_pmu = rec.get("num_pmu")

        key = (date, num_r, num_c, num_pmu)
        if key in pmu_enriched_keys:
            matches_enriched += 1
        if key in pmu_participants_keys:
            matches_participants += 1

    n = len(master_sample)
    return {
        "enriched_matches": matches_enriched,
        "enriched_rate": (matches_enriched / n * 100) if n > 0 else 0,
        "participants_matches": matches_participants,
        "participants_rate": (matches_participants / n * 100) if n > 0 else 0,
        "sample_size": n,
    }


def estimate_gain_from_pmu(master_sample, pmu_enriched_keys, pmu_participants_keys, fill_rates):
    """Estimate which fields could be improved by merging PMU data."""
    gains = {}

    # Map from master field to PMU enriched field
    enriched_map = {
        "deferre": "deferre",
        "temps_ms": "tempsObtenu",
        "reduction_km_ms": "reductionKilometrique",
        "handicap_valeur": "handicapValeur",
        "avis_entraineur": "avisEntraineur",
        "oeilleres": "oeilleres",
        "poids_porte_kg": "poidsConditionMonte",
    }

    participants_map = {
        "cote_reference": "cote_reference",
        "deferre": "deferre",
        "temps_ms": "tempsObtenu",
        "reduction_km_ms": "reductionKm",
        "avis_entraineur": "avisEntraineur",
        "oeilleres": "oeilleres",
        "musique": "musique",
        "pere": "nomPere",
        "mere": "nomMere",
        "race": "race",
        "gains_carriere_euros": "gainsCarriere",
        "nb_courses_carriere": "nombreCourses",
        "nb_victoires_carriere": "nombreVictoires",
        "sexe": "sexe",
        "age": "age",
        "position_arrivee": "ordreArrivee",
        "nom_cheval": "nom",
        "jockey_driver": "driver",
        "entraineur": "entraineur",
        "poids_porte_kg": "poidsConditionMonte",
        "cote_finale": "cote_direct",
    }

    for master_field in ML_KEY_FIELDS:
        current_rate = fill_rates.get(master_field, 0)
        could_fill = 0
        source_label = None

        for rec in master_sample:
            if is_filled(rec.get(master_field)):
                continue  # already filled

            date = rec.get("date_reunion_iso")
            num_r = rec.get("numero_reunion")
            num_c = rec.get("numero_course")
            num_pmu = rec.get("num_pmu")
            key = (date, num_r, num_c, num_pmu)

            # Check enriched first
            if master_field in enriched_map:
                pmu_rec = pmu_enriched_keys.get(key)
                if pmu_rec and is_filled(pmu_rec.get(enriched_map[master_field])):
                    could_fill += 1
                    source_label = "pmu_enriched"
                    continue

            # Check participants
            if master_field in participants_map:
                pmu_rec = pmu_participants_keys.get(key)
                if pmu_rec and is_filled(pmu_rec.get(participants_map[master_field])):
                    could_fill += 1
                    source_label = "pmu_participants"
                    continue

        n = len(master_sample)
        potential_rate = current_rate + (could_fill / n * 100) if n > 0 else current_rate
        if could_fill > 0:
            gains[master_field] = {
                "current_rate": current_rate,
                "potential_rate": min(potential_rate, 100.0),
                "gain_pct": potential_rate - current_rate,
                "records_fillable": could_fill,
                "source": source_label or "pmu_data",
            }

    return gains


def compute_fill_rates_by_era(records, fields):
    """Compute fill rates split by date era: pre-2020 vs 2020+."""
    eras = {"pre_2020": [], "post_2020": []}
    for rec in records:
        date = rec.get("date_reunion_iso", "")
        if date < "2020-01-01":
            eras["pre_2020"].append(rec)
        else:
            eras["post_2020"].append(rec)

    result = {}
    for era, recs in eras.items():
        result[era] = {"count": len(recs), "rates": compute_fill_rates(recs, fields) if recs else {f: 0 for f in fields}}
    return result


def generate_markdown(fill_rates, all_rates, join_info, gains, total_records, sample_size,
                      era_rates=None):
    """Generate the markdown report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = []
    lines.append("# Fill Rate Dashboard - partants_master.jsonl")
    lines.append("")
    lines.append(f"**Generated**: {now}")
    lines.append(f"**Total records**: {total_records:,}")
    lines.append(f"**Sample size**: {sample_size:,}")
    lines.append("")

    # ── Section 1: Key ML Fields ──
    lines.append("## 1. Key ML Fields (30 most important)")
    lines.append("")
    lines.append("| # | Field | Fill Rate | Status |")
    lines.append("|---|-------|-----------|--------|")

    sorted_fields = sorted(fill_rates.items(), key=lambda x: -x[1])
    for i, (field, rate) in enumerate(sorted_fields, 1):
        if rate >= 90:
            status = "OK"
        elif rate >= 50:
            status = "MEDIUM"
        else:
            status = "LOW"
        bar = format_bar(rate)
        lines.append(f"| {i} | `{field}` | {bar} {rate:.1f}% | {status} |")

    lines.append("")

    # ── Summary stats ──
    high = sum(1 for r in fill_rates.values() if r >= 90)
    med = sum(1 for r in fill_rates.values() if 50 <= r < 90)
    low = sum(1 for r in fill_rates.values() if r < 50)
    lines.append(f"**Summary**: {high} fields >= 90%, {med} fields 50-89%, {low} fields < 50%")
    lines.append("")

    # ── Section 1b: Fill rates by era ──
    if era_rates:
        lines.append("### Fill Rates by Era (pre-2020 vs 2020+)")
        lines.append("")
        lines.append("PMU enriched data covers 2020-2021, PMU participants covers 2020-2026.")
        lines.append("Pre-2020 records have no PMU API source available.")
        lines.append("")
        pre = era_rates.get("pre_2020", {})
        post = era_rates.get("post_2020", {})
        lines.append(f"- **Pre-2020**: {pre.get('count', 0):,} records in sample")
        lines.append(f"- **2020+**: {post.get('count', 0):,} records in sample")
        lines.append("")
        lines.append("| Field | Pre-2020 | 2020+ | Delta |")
        lines.append("|-------|----------|-------|-------|")
        pre_rates = pre.get("rates", {})
        post_rates = post.get("rates", {})
        for field, _ in sorted(fill_rates.items(), key=lambda x: -x[1]):
            pr = pre_rates.get(field, 0)
            po = post_rates.get(field, 0)
            delta = po - pr
            sign = "+" if delta > 0 else ""
            lines.append(f"| `{field}` | {pr:.1f}% | {po:.1f}% | {sign}{delta:.1f}% |")
        lines.append("")

    # ── Section 2: Fields < 50% - Untapped Sources ──
    lines.append("## 2. Fields Below 50% - Potential Sources")
    lines.append("")

    low_fields = [(f, r) for f, r in sorted_fields if r < 50]
    if low_fields:
        lines.append("| Field | Current Rate | Potential Sources |")
        lines.append("|-------|-------------|-------------------|")
        for field, rate in low_fields:
            sources = FIELD_SOURCE_MAP.get(field, [])
            if sources:
                src_str = ", ".join(f"`{s[0]}:{s[1]}`" for s in sources)
            else:
                src_str = "_No external source identified_"
            lines.append(f"| `{field}` | {rate:.1f}% | {src_str} |")
        lines.append("")
    else:
        lines.append("All key ML fields are above 50% fill rate.")
        lines.append("")

    # ── Section 2b: Fields 50-89% ──
    med_fields = [(f, r) for f, r in sorted_fields if 50 <= r < 90]
    if med_fields:
        lines.append("### Fields 50-89% (improvement candidates)")
        lines.append("")
        lines.append("| Field | Current Rate | Potential Sources |")
        lines.append("|-------|-------------|-------------------|")
        for field, rate in med_fields:
            sources = FIELD_SOURCE_MAP.get(field, [])
            if sources:
                src_str = ", ".join(f"`{s[0]}:{s[1]}`" for s in sources)
            else:
                src_str = "_Derived / no external source_"
            lines.append(f"| `{field}` | {rate:.1f}% | {src_str} |")
        lines.append("")

    # ── Section 3: PMU Data Join Analysis ──
    lines.append("## 3. PMU Enriched Data Join Analysis")
    lines.append("")
    lines.append(f"- **PMU enriched** (pmu_participants_enriched.jsonl): ~235K records")
    lines.append(f"  - Join key: `(date, numReunion, numCourse, numPmu)`")
    lines.append(f"  - Match rate on sample: **{join_info['enriched_rate']:.1f}%** ({join_info['enriched_matches']:,}/{join_info['sample_size']:,})")
    lines.append(f"- **PMU participants** (pmu_participants.jsonl): ~1.39M records")
    lines.append(f"  - Join key: `(date, num_reunion, num_course, numPmu)`")
    lines.append(f"  - Match rate on sample: **{join_info['participants_rate']:.1f}%** ({join_info['participants_matches']:,}/{join_info['sample_size']:,})")
    lines.append("")

    # ── Section 4: Potential Gains from PMU merge ──
    lines.append("## 4. Potential Gains from PMU Data Merge")
    lines.append("")
    if gains:
        lines.append("| Field | Current | After Merge | Gain | Fillable Records |")
        lines.append("|-------|---------|-------------|------|-----------------|")
        sorted_gains = sorted(gains.items(), key=lambda x: -x[1]["gain_pct"])
        for field, g in sorted_gains:
            lines.append(
                f"| `{field}` | {g['current_rate']:.1f}% | {g['potential_rate']:.1f}% | "
                f"+{g['gain_pct']:.1f}% | {g['records_fillable']:,}/{sample_size:,} |"
            )
        lines.append("")

        total_gain_fields = len([g for g in gains.values() if g["gain_pct"] > 1.0])
        lines.append(f"**{total_gain_fields} fields** could gain > 1% fill rate from PMU data merge.")
        lines.append("")
    else:
        lines.append("No significant gains identified from PMU data merge.")
        lines.append("")

    # ── Section 5: Full Field Inventory ──
    lines.append("## 5. Full Field Inventory (all fields, top 50 by fill rate)")
    lines.append("")
    sorted_all = sorted(all_rates.items(), key=lambda x: -x[1])
    lines.append("| # | Field | Fill Rate |")
    lines.append("|---|-------|-----------|")
    for i, (field, rate) in enumerate(sorted_all[:50], 1):
        lines.append(f"| {i} | `{field}` | {rate:.1f}% |")
    lines.append("")
    lines.append(f"*Total fields found: {len(all_rates)}*")
    lines.append("")

    # ── Section 6: Bottom 30 fields ──
    lines.append("## 6. Lowest Fill Rate Fields (bottom 30)")
    lines.append("")
    lines.append("| # | Field | Fill Rate |")
    lines.append("|---|-------|-----------|")
    for i, (field, rate) in enumerate(sorted_all[-30:], 1):
        lines.append(f"| {i} | `{field}` | {rate:.1f}% |")
    lines.append("")

    # ── Section 7: Recommendations ──
    lines.append("## 7. Recommendations")
    lines.append("")
    lines.append("### Quick Wins (merge existing data)")
    if gains:
        quick_wins = [(f, g) for f, g in gains.items() if g["gain_pct"] > 2.0]
        for field, g in sorted(quick_wins, key=lambda x: -x[1]["gain_pct"]):
            lines.append(f"- **`{field}`**: +{g['gain_pct']:.1f}% from `{g['source']}`")
    lines.append("")

    lines.append("### Scraper Improvements Needed")
    for field, rate in low_fields:
        sources = FIELD_SOURCE_MAP.get(field, [])
        if not sources:
            lines.append(f"- **`{field}`** ({rate:.1f}%): No external source identified - consider adding a scraper or deriving from existing data")
    lines.append("")

    lines.append("### Data Pipeline Actions")
    lines.append("1. Run `scripts/merge/merge_pmu_enriched.py` to integrate PMU enriched data")
    lines.append("2. Cross-reference with `output/26_geny/geny_flat.jsonl` for cotes and equipment data")
    lines.append("3. Run `scripts/enrich_deferre.py` and `scripts/enrich_incident.py` for missing equipment/incident data")
    lines.append("4. Consider re-scraping PMU API for dates with missing `temps_ms` and `reduction_km_ms`")
    lines.append("")

    return "\n".join(lines)


def format_bar(rate):
    """Create a text progress bar."""
    filled = int(rate / 10)
    empty = 10 - filled
    return "[" + "#" * filled + "." * empty + "]"


def main():
    parser = argparse.ArgumentParser(description="Fill Rate Dashboard")
    parser.add_argument("--sample", type=int, default=5000, help="Sample size (default: 5000)")
    parser.add_argument("--full", action="store_true", help="Scan all records (slow)")
    args = parser.parse_args()

    sample_size = args.sample if not args.full else None

    print("=" * 60)
    print("FILL RATE DASHBOARD")
    print("=" * 60)

    # ── Step 1: Sample partants_master ──
    print("\n[1/4] Sampling partants_master.jsonl...")
    if args.full:
        print("  Full scan mode - this will be slow!")

    master_sample, total_records = sample_jsonl(DATA_MASTER, n=sample_size or 999999999)
    actual_sample = len(master_sample)

    # ── Step 2: Compute fill rates ──
    print("\n[2/4] Computing fill rates...")
    fill_rates = compute_fill_rates(master_sample, ML_KEY_FIELDS)
    all_rates = compute_all_field_rates(master_sample)

    # Print quick summary
    print("\n  Key ML Field Fill Rates:")
    for field, rate in sorted(fill_rates.items(), key=lambda x: -x[1]):
        status = "OK" if rate >= 90 else ("MED" if rate >= 50 else "LOW")
        print(f"    {field:30s} {rate:6.1f}%  [{status}]")

    # ── Step 3: Load PMU data and check join potential ──
    print("\n[3/4] Analyzing PMU data join potential...")

    pmu_enriched_keys = {}
    pmu_enriched_filled = {}
    pmu_enriched_total = 0
    if PMU_ENRICHED.exists():
        pmu_enriched_keys, pmu_enriched_filled, pmu_enriched_total = load_pmu_enriched_keys(PMU_ENRICHED)

    pmu_participants_keys = {}
    pmu_participants_filled = {}
    pmu_participants_total = 0
    if PMU_PARTICIPANTS.exists():
        pmu_participants_keys, pmu_participants_filled, pmu_participants_total = load_pmu_participants_keys(PMU_PARTICIPANTS)

    join_info = check_master_join_potential(master_sample, pmu_enriched_keys, pmu_participants_keys)
    print(f"\n  PMU enriched match rate: {join_info['enriched_rate']:.1f}%")
    print(f"  PMU participants match rate: {join_info['participants_rate']:.1f}%")

    # ── Step 4: Estimate gains ──
    print("\n[4/4] Estimating potential gains from PMU merge...")
    gains = estimate_gain_from_pmu(master_sample, pmu_enriched_keys, pmu_participants_keys, fill_rates)

    if gains:
        print("\n  Potential gains:")
        for field, g in sorted(gains.items(), key=lambda x: -x[1]["gain_pct"]):
            print(f"    {field:30s} {g['current_rate']:6.1f}% -> {g['potential_rate']:6.1f}% (+{g['gain_pct']:.1f}%)")

    # ── Compute era breakdown ──
    print("\nComputing fill rates by era (pre-2020 vs 2020+)...")
    era_rates = compute_fill_rates_by_era(master_sample, ML_KEY_FIELDS)
    for era, info in era_rates.items():
        print(f"  {era}: {info['count']} records")

    # ── Generate report ──
    print("\nGenerating markdown report...")
    md = generate_markdown(fill_rates, all_rates, join_info, gains, total_records, actual_sample,
                           era_rates=era_rates)

    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\nReport saved to: {OUTPUT_MD}")
    print("Done!")


if __name__ == "__main__":
    main()
