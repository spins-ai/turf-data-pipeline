"""
fix_roi_target.py
=================
Recovers ROI (rapport_simple_gagnant) in targets.jsonl from two sources:
  1. D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet
  2. D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/  (fallback)

Steps
-----
1. Read targets.jsonl and print "before" fill-rate stats.
2. Check partants_master.parquet for rapport_simple_gagnant / rapport_simple.
3. If still missing, scan builder_outputs/ Parquet files for the same fields.
4. Patch each target record with recovered values; compute roi_simple.
5. Write targets_enriched.jsonl and print "after" stats.

DuckDB settings: memory_limit='4GB', threads=2
"""

import json
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed.  Run: pip install duckdb")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
TARGETS_IN      = Path("D:/turf-data-pipeline/04_FEATURES/targets/targets.jsonl")
TARGETS_OUT     = Path("D:/turf-data-pipeline/04_FEATURES/targets/targets_enriched.jsonl")
MASTER_PARQUET  = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
BUILDER_DIR     = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")

# Fields we look for (in priority order)
RAPPORT_CANDIDATES = [
    "rapport_simple_gagnant",
    "rap_rapport_simple_gagnant",
    "rapport_simple",
    "dividende_simple_gagnant",
]

# ---------------------------------------------------------------------------
# DuckDB connection
# ---------------------------------------------------------------------------
con = duckdb.connect()
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")

# ---------------------------------------------------------------------------
# Helper: list columns of a Parquet file
# ---------------------------------------------------------------------------

def parquet_columns(path: str) -> list[str]:
    try:
        return [
            r[0]
            for r in con.execute(
                f"SELECT column_name FROM parquet_schema('{path}')"
            ).fetchall()
        ]
    except Exception:
        return []


def find_rapport_col(columns: list[str]) -> str | None:
    col_lower = {c.lower(): c for c in columns}
    for candidate in RAPPORT_CANDIDATES:
        if candidate.lower() in col_lower:
            return col_lower[candidate.lower()]
    return None


# ---------------------------------------------------------------------------
# Step 1 – Load targets and print BEFORE stats
# ---------------------------------------------------------------------------
print("=" * 60)
print("Step 1: Loading targets.jsonl …")

if not TARGETS_IN.exists():
    print(f"ERROR: targets file not found: {TARGETS_IN}")
    sys.exit(1)

records: list[dict] = []
with open(TARGETS_IN, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            records.append(json.loads(line))

n_total = len(records)
print(f"  Total records: {n_total:,}")

# Before stats
def count_filled(recs, field):
    return sum(1 for r in recs if r.get(field) not in (None, "", "null"))

n_before_rapport = count_filled(records, "rapport_simple_gagnant")
n_before_roi     = count_filled(records, "roi_simple")

print(f"\n--- BEFORE ---")
print(f"  rapport_simple_gagnant filled : {n_before_rapport:,} / {n_total:,} "
      f"({100*n_before_rapport/max(n_total,1):.1f}%)")
print(f"  roi_simple filled             : {n_before_roi:,} / {n_total:,} "
      f"({100*n_before_roi/max(n_total,1):.1f}%)")

# Build index of records missing ROI
missing_uids: set[str] = {
    r["partant_uid"]
    for r in records
    if r.get("rapport_simple_gagnant") is None
    and r.get("partant_uid")
}
print(f"\n  Records missing rapport_simple_gagnant: {len(missing_uids):,}")

# ---------------------------------------------------------------------------
# Step 2 – Recover from partants_master.parquet
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 2: Querying partants_master.parquet …")

master_lookup: dict[str, float | None] = {}   # partant_uid -> rapport value

if MASTER_PARQUET.exists():
    cols = parquet_columns(str(MASTER_PARQUET))
    rapport_col = find_rapport_col(cols)

    if rapport_col:
        print(f"  Found column '{rapport_col}' in master parquet.")
        try:
            df = con.execute(f"""
                SELECT partant_uid, {rapport_col}
                FROM read_parquet('{MASTER_PARQUET}')
                WHERE partant_uid IS NOT NULL
                  AND {rapport_col} IS NOT NULL
            """).fetchdf()
            for _, row in df.iterrows():
                master_lookup[str(row["partant_uid"])] = float(row[rapport_col])
            print(f"  Loaded {len(master_lookup):,} rows with non-null {rapport_col}")
        except Exception as e:
            print(f"  WARNING: query failed – {e}")
    else:
        print(f"  WARNING: no rapport column found in master parquet.")
        print(f"  Available columns (sample): {cols[:20]}")
else:
    print(f"  WARNING: master parquet not found at {MASTER_PARQUET}")

recovered_from_master = sum(1 for uid in missing_uids if uid in master_lookup)
print(f"  Can recover {recovered_from_master:,} / {len(missing_uids):,} missing records from master")

# ---------------------------------------------------------------------------
# Step 3 – Fallback: scan builder_outputs/ for rapport fields
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 3: Scanning builder_outputs/ for rapport fields …")

builder_lookup: dict[str, float | None] = {}   # partant_uid -> rapport value

still_missing = missing_uids - set(master_lookup.keys())
print(f"  Still missing after master: {len(still_missing):,}")

if BUILDER_DIR.exists() and still_missing:
    parquet_files = sorted(BUILDER_DIR.glob("**/*.parquet"))
    print(f"  Found {len(parquet_files)} Parquet files in builder_outputs/")

    for pf in parquet_files:
        cols = parquet_columns(str(pf))
        rapport_col = find_rapport_col(cols)

        # Must also have a partant_uid column to be useful
        if rapport_col and any(c.lower() == "partant_uid" for c in cols):
            print(f"  Querying {pf.name}  (col={rapport_col}) …")
            try:
                df = con.execute(f"""
                    SELECT partant_uid, {rapport_col}
                    FROM read_parquet('{str(pf)}')
                    WHERE partant_uid IS NOT NULL
                      AND {rapport_col} IS NOT NULL
                """).fetchdf()

                added = 0
                for _, row in df.iterrows():
                    uid = str(row["partant_uid"])
                    if uid in still_missing and uid not in builder_lookup:
                        builder_lookup[uid] = float(row[rapport_col])
                        added += 1
                if added:
                    print(f"    → recovered {added:,} records")
                    still_missing -= set(builder_lookup.keys())

                if not still_missing:
                    print("  All missing records recovered – stopping scan.")
                    break
            except Exception as e:
                print(f"    WARNING: {pf.name} – {e}")
else:
    if not BUILDER_DIR.exists():
        print(f"  WARNING: builder_outputs dir not found at {BUILDER_DIR}")
    else:
        print("  No missing records remain – skipping scan.")

recovered_from_builders = len(builder_lookup)
print(f"  Recovered from builder_outputs: {recovered_from_builders:,}")

# ---------------------------------------------------------------------------
# Step 4 – Patch records and compute roi_simple
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("Step 4: Patching records …")

# Combined lookup (master takes priority over builders)
combined: dict[str, float | None] = {**builder_lookup, **master_lookup}

n_patched_rapport = 0
n_patched_roi     = 0

for record in records:
    uid = str(record.get("partant_uid", ""))

    # --- Fill rapport_simple_gagnant if missing ---
    if record.get("rapport_simple_gagnant") is None and uid in combined:
        record["rapport_simple_gagnant"] = combined[uid]
        n_patched_rapport += 1

    # --- Compute roi_simple ---
    rapport = record.get("rapport_simple_gagnant")
    is_win  = record.get("is_gagnant", 0)

    if record.get("roi_simple") is None:
        if is_win and rapport is not None:
            try:
                record["roi_simple"] = round(float(rapport) / 100.0 - 1.0, 6)
            except (TypeError, ValueError):
                record["roi_simple"] = -1.0
        else:
            record["roi_simple"] = -1.0
        n_patched_roi += 1

print(f"  rapport_simple_gagnant patched : {n_patched_rapport:,}")
print(f"  roi_simple patched             : {n_patched_roi:,}")

# ---------------------------------------------------------------------------
# Step 5 – Write output
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print(f"Step 5: Writing {TARGETS_OUT} …")

TARGETS_OUT.parent.mkdir(parents=True, exist_ok=True)
with open(TARGETS_OUT, "w", encoding="utf-8") as f:
    for record in records:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

print(f"  Written {len(records):,} records.")

# ---------------------------------------------------------------------------
# AFTER stats
# ---------------------------------------------------------------------------
n_after_rapport = count_filled(records, "rapport_simple_gagnant")
n_after_roi     = count_filled(records, "roi_simple")

print(f"\n--- AFTER ---")
print(f"  rapport_simple_gagnant filled : {n_after_rapport:,} / {n_total:,} "
      f"({100*n_after_rapport/max(n_total,1):.1f}%)")
print(f"  roi_simple filled             : {n_after_roi:,} / {n_total:,} "
      f"({100*n_after_roi/max(n_total,1):.1f}%)")

# Net gain
print(f"\n  Net gain rapport : +{n_after_rapport - n_before_rapport:,}")
print(f"  Net gain roi     : +{n_after_roi     - n_before_roi:,}")

# Spot-check: 5 winners
print("\nSpot-check – 5 winners with positive roi_simple:")
shown = 0
for rec in records:
    if rec.get("is_gagnant") == 1 and (rec.get("roi_simple") or -1) > 0:
        print(f"  uid={rec.get('partant_uid')}  "
              f"rapport={rec.get('rapport_simple_gagnant')}  "
              f"roi_simple={rec.get('roi_simple')}")
        shown += 1
        if shown >= 5:
            break
if shown == 0:
    print("  (no winners with positive ROI found – check data)")

con.close()
print("\nDone.")
