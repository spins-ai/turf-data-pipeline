#!/usr/bin/env python3
"""
Collect pipeline statistics from partants_master.jsonl (streaming, max ~2GB RAM).
Outputs docs/STATS.md.
"""

import json
import random
import time
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DATA_MASTER = BASE / "data_master"
OUTPUT_DIR = BASE / "output"
PARTANTS = DATA_MASTER / "partants_master.jsonl"
FEATURES_PARQUET = (
    BASE
    / "pipeline"
    / "phase_02_feature_engineering"
    / "09_advanced_feature_generator"
    / "data"
    / "features_matrix.parquet"
)
DOCS_DIR = BASE / "docs"
DOCS_DIR.mkdir(exist_ok=True)

SAMPLE_SIZE = 1000
RESERVOIR_SEED = 42


def stream_partants():
    """Stream partants_master.jsonl line by line, collecting stats with counters only."""
    course_uids = set()
    chevaux = set()
    jockeys = set()
    hippodromes = set()
    min_date = None
    max_date = None
    total_lines = 0
    num_fields_first = 0

    # Reservoir sampling for fill-rate
    reservoir = []
    random.seed(RESERVOIR_SEED)

    print(f"Streaming {PARTANTS} ...")
    t0 = time.time()
    last_report = t0

    with open(PARTANTS, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_lines += 1

            if total_lines == 1:
                num_fields_first = len(rec)

            # 1. course_uid
            cuid = rec.get("course_uid")
            if cuid:
                course_uids.add(cuid)

            # 3. nom_cheval
            nc = rec.get("nom_cheval")
            if nc:
                chevaux.add(nc)

            # 4. jockey_driver
            jd = rec.get("jockey_driver")
            if jd:
                jockeys.add(jd)

            # 5. hippodrome_normalise
            hn = rec.get("hippodrome_normalise")
            if hn:
                hippodromes.add(hn)

            # 6. date_reunion_iso
            dr = rec.get("date_reunion_iso")
            if dr:
                if min_date is None or dr < min_date:
                    min_date = dr
                if max_date is None or dr > max_date:
                    max_date = dr

            # Reservoir sampling for fill-rate (sample SAMPLE_SIZE records)
            if total_lines <= SAMPLE_SIZE:
                reservoir.append(rec)
            else:
                j = random.randint(1, total_lines)
                if j <= SAMPLE_SIZE:
                    reservoir[j - 1] = rec

            # Progress report every 60s
            now = time.time()
            if now - last_report > 60:
                elapsed = now - t0
                rate = total_lines / elapsed
                print(
                    f"  [{int(elapsed)}s] {total_lines:,} lines, "
                    f"{len(course_uids):,} courses, {len(chevaux):,} chevaux, "
                    f"{rate:,.0f} lines/s"
                )
                last_report = now

    elapsed = time.time() - t0
    print(
        f"Done: {total_lines:,} lines in {elapsed:.0f}s "
        f"({total_lines/elapsed:,.0f} lines/s)"
    )

    return {
        "total_partants": total_lines,
        "total_courses": len(course_uids),
        "total_chevaux": len(chevaux),
        "total_jockeys": len(jockeys),
        "total_hippodromes": len(hippodromes),
        "min_date": min_date,
        "max_date": max_date,
        "num_fields": num_fields_first,
        "reservoir": reservoir,
    }


def compute_fill_rate(reservoir, num_fields):
    """Average fill rate from sampled records (per-record, then averaged)."""
    if not reservoir:
        return 0.0
    rates = []
    for rec in reservoir:
        n = len(rec)
        if n == 0:
            continue
        filled = sum(1 for v in rec.values() if v is not None and v != "" and v != [])
        rates.append(filled / n)
    return (sum(rates) / len(rates)) * 100 if rates else 0.0


def count_active_sources(output_dir):
    """Count output subdirectories that contain at least one data file."""
    count = 0
    if not output_dir.exists():
        return 0
    for d in sorted(output_dir.iterdir()):
        if not d.is_dir():
            continue
        has_data = False
        for ext in ("*.json", "*.jsonl", "*.csv", "*.parquet"):
            if list(d.glob(ext)):
                has_data = True
                break
        if has_data:
            count += 1
    return count


def get_dir_size(path):
    """Get total size of a directory."""
    total = 0
    if not path.exists():
        return total
    for f in path.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total


def human_size(nbytes):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def count_features():
    """Try to count features from the parquet, fallback to first-line field count."""
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(str(FEATURES_PARQUET))
        return pf.metadata.num_columns
    except Exception:
        return None


def main():
    print("=" * 60)
    print("TURF DATA PIPELINE - Statistics Collection")
    print("=" * 60)

    # Stream partants_master.jsonl
    stats = stream_partants()

    # Fill rate
    fill_rate = compute_fill_rate(stats["reservoir"], stats["num_fields"])

    # Features count
    features_count = count_features()
    if features_count is None:
        features_count = stats["num_fields"]
        features_note = " (from partants_master fields; features_matrix not yet built)"
    else:
        features_note = " (from features_matrix.parquet)"

    # Active sources
    active_sources = count_active_sources(OUTPUT_DIR)

    # Directory sizes
    print("Computing directory sizes...")
    data_master_size = get_dir_size(DATA_MASTER)
    output_size = get_dir_size(OUTPUT_DIR)

    # Build markdown
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_range = f"{stats['min_date']} -> {stats['max_date']}"

    md = f"""# Statistiques du Pipeline Turf Data

> Genere automatiquement le {now}

## Vue d'ensemble

| Metrique | Valeur |
|---|---|
| Total courses | **{stats['total_courses']:,}** |
| Total partants | **{stats['total_partants']:,}** |
| Chevaux uniques | **{stats['total_chevaux']:,}** |
| Jockeys/Drivers uniques | **{stats['total_jockeys']:,}** |
| Hippodromes uniques | **{stats['total_hippodromes']:,}** |
| Plage de dates | **{date_range}** |
| Total features | **{features_count:,}**{features_note} |
| Taux de remplissage moyen | **{fill_rate:.1f}%** (echantillon de {len(stats['reservoir']):,} enregistrements) |

## Stockage

| Repertoire | Taille |
|---|---|
| `data_master/` | {human_size(data_master_size)} |
| `output/` | {human_size(output_size)} |
| **Total** | **{human_size(data_master_size + output_size)}** |

## Sources de donnees

- **Sources actives** : {active_sources} (repertoires dans `output/` contenant des fichiers de donnees)

## Details techniques

- Fichier principal : `data_master/partants_master.jsonl` ({human_size(PARTANTS.stat().st_size)})
- Nombre de champs par enregistrement : {stats['num_fields']}
- Script de collecte : `scripts/collect_stats.py`
"""

    out_path = DOCS_DIR / "STATS.md"
    out_path.write_text(md, encoding="utf-8")
    print(f"\nStats written to {out_path}")
    print(f"  Courses:     {stats['total_courses']:,}")
    print(f"  Partants:    {stats['total_partants']:,}")
    print(f"  Chevaux:     {stats['total_chevaux']:,}")
    print(f"  Jockeys:     {stats['total_jockeys']:,}")
    print(f"  Hippodromes: {stats['total_hippodromes']:,}")
    print(f"  Dates:       {date_range}")
    print(f"  Features:    {features_count}")
    print(f"  Fill rate:   {fill_rate:.1f}%")
    print(f"  Sources:     {active_sources}")


if __name__ == "__main__":
    main()
