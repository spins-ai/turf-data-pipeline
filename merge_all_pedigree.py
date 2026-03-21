#!/usr/bin/env python3
"""
merge_all_pedigree.py
Merge 4 pedigree data sources into one unified pedigree file.

Sources:
  1. output/08_pedigree/  — sire & dam stats (win rates, descendants)
  2. output/12_pedigree/  — pedigrees_consolide (544 records, lignee_male)
  3. output/14_pedigree/  — pedigrees_pq (24k records, 4 generations deep)
  4. output/36_pedigree_query/ — pedigree_query_data (2.4k records, HTML-contaminated)

Output:
  output/pedigree_complete/pedigree_complet.json  (+ .csv + .parquet)
"""

import json
import os
import re
from pathlib import Path
from collections import Counter

from utils.normalize import normalize_name

import pandas as pd

BASE = Path(__file__).resolve().parent
OUT_DIR = BASE / "output" / "pedigree_complete"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SYMLINK_DIR = (
    BASE
    / "pipeline"
    / "phase_02_feature_engineering"
    / "14_pedigree_feature_builder"
    / "data"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_dosage_from_sire_field(raw: str) -> dict:
    """
    Step-36 sire field contains embedded DP/DI/CD data in HTML noise.
    Extract Dosage Profile, Dosage Index, Center of Distribution.
    """
    out = {}
    if not raw:
        return out
    m = re.search(
        r"DP\s*=\s*([\d\-\s]+?)\s*\((\d+)\)\s*DI\s*=\s*([\d.]+)\s*CD\s*=\s*([\-\d.]+)",
        raw,
    )
    if m:
        out["dosage_profile"] = m.group(1).strip()
        out["dosage_total"] = int(m.group(2))
        out["dosage_index"] = float(m.group(3))
        out["center_of_distribution"] = float(m.group(4))
    return out


# ---------------------------------------------------------------------------
# Load sources
# ---------------------------------------------------------------------------

def load_step08():
    """Load sire and dam stats keyed by normalized name."""
    sires_path = BASE / "output" / "08_pedigree" / "pedigree_peres.json"
    dams_path = BASE / "output" / "08_pedigree" / "pedigree_meres.json"

    with open(sires_path, encoding="utf-8") as f:
        sires_raw = json.load(f)
    with open(dams_path, encoding="utf-8") as f:
        dams_raw = json.load(f)

    sires = {}
    for r in sires_raw:
        key = normalize_name(r.get("nom_pere", ""))
        if key:
            sires[key] = {
                "sire_nb_descendants_courses": r.get("nb_descendants_courses"),
                "sire_nb_descendants_victoires": r.get("nb_descendants_victoires"),
                "sire_taux_victoire_descendants": r.get("taux_victoire_descendants"),
                "sire_distances_predilection": r.get("distances_predilection"),
                "sire_disciplines": r.get("disciplines"),
                "sire_hippodromes_forts": r.get("hippodromes_forts"),
            }

    dams = {}
    for r in dams_raw:
        key = normalize_name(r.get("nom_mere", ""))
        if key:
            dams[key] = {
                "dam_nb_descendants_courses": r.get("nb_descendants_courses"),
                "dam_nb_descendants_victoires": r.get("nb_descendants_victoires"),
                "dam_taux_victoire_descendants": r.get("taux_victoire_descendants"),
                "dam_distances_predilection": r.get("distances_predilection"),
                "dam_disciplines": r.get("disciplines"),
                "dam_hippodromes_forts": r.get("hippodromes_forts"),
            }

    print(f"  Step 08: {len(sires):,} sires, {len(dams):,} dams loaded")
    return sires, dams


def load_step12():
    """Load consolidated pedigree (544 records) keyed by normalized horse name."""
    path = BASE / "output" / "12_pedigree" / "pedigrees_consolide.json"
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    records = {}
    for r in raw:
        key = normalize_name(r.get("nom", ""))
        if not key:
            continue
        records[key] = {
            "horse_id_12": r.get("horse_id"),
            "pere": r.get("pere"),
            "mere": r.get("mere"),
            "pere_mere": r.get("pere_mere") or None,
            "mere_pere": r.get("mere_pere") or None,
            "pere_pere": r.get("pere_pere") or None,
            "mere_mere": r.get("mere_mere") or None,
            "lignee_male": r.get("lignee_male") or None,
            "pays_naissance": r.get("pays_naissance") or None,
            "annee_naissance": r.get("annee_naissance"),
            "sexe": r.get("sexe") or None,
            "race": r.get("race") or None,
        }
    print(f"  Step 12: {len(records):,} records loaded")
    return records


def load_step14():
    """Load PedigreeQuery pedigree (24k, 4 gen deep) keyed by normalized name."""
    path = BASE / "output" / "14_pedigree" / "pedigrees_pq.json"
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    records = {}
    for r in raw:
        key = normalize_name(r.get("nom_cheval", ""))
        if not key:
            continue
        records[key] = {
            "horse_id_14": r.get("horse_id"),
            "pere": r.get("pere"),
            "mere": r.get("mere"),
            "pere_mere": r.get("pere_mere") or None,
            "grand_pere_paternel": r.get("grand_pere_paternel") or None,
            "grand_mere_paternelle": r.get("grand_mere_paternelle") or None,
            "grand_pere_maternel": r.get("grand_pere_maternel") or None,
            "grand_mere_maternelle": r.get("grand_mere_maternelle") or None,
            "arriere_gpp_pp": r.get("arriere_gpp_pp") or None,
            "arriere_gpm_pp": r.get("arriere_gpm_pp") or None,
            "arriere_gpp_mp": r.get("arriere_gpp_mp") or None,
            "arriere_gpm_mp": r.get("arriere_gpm_mp") or None,
            "arriere_gpp_pm": r.get("arriere_gpp_pm") or None,
            "arriere_gpm_pm": r.get("arriere_gpm_pm") or None,
            "arriere_gpp_mm": r.get("arriere_gpp_mm") or None,
            "arriere_gpm_mm": r.get("arriere_gpm_mm") or None,
            "source_14": r.get("source"),
            "found_14": r.get("found"),
        }
    print(f"  Step 14: {len(records):,} records loaded")
    return records


def load_step36():
    """
    Load step 36 data. The sire/dam/grandparent fields are contaminated with
    HTML navigation artifacts so we do NOT use them for pedigree tree data.
    We only extract: country, birth_year, and Dosage Profile (DP/DI/CD).
    """
    path = BASE / "output" / "36_pedigree_query" / "pedigree_query_data.json"
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    records = {}
    for r in raw:
        key = normalize_name(r.get("name", ""))
        if not key:
            continue
        entry = {
            "country_36": r.get("country") or None,
            "birth_year_36": r.get("birth_year"),
        }
        # Extract dosage info from the contaminated sire field
        dosage = extract_dosage_from_sire_field(r.get("sire", ""))
        entry.update(dosage)
        records[key] = entry

    n_dosage = sum(1 for v in records.values() if "dosage_index" in v)
    print(f"  Step 36: {len(records):,} records loaded ({n_dosage:,} with dosage data)")
    return records


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

def merge_all():
    print("Loading data sources...")
    sires, dams = load_step08()
    step12 = load_step12()
    step14 = load_step14()
    step36 = load_step36()

    # Collect all horse names from step 12, 14, 36
    all_keys = set(step14.keys()) | set(step12.keys()) | set(step36.keys())
    print(f"\nTotal unique horses (before sire/dam enrichment): {len(all_keys):,}")

    merged = []
    for key in sorted(all_keys):
        record = {"nom_normalise": key}

        # --- Pedigree tree: priority step14 > step12 > (step36 excluded for tree) ---
        r14 = step14.get(key, {})
        r12 = step12.get(key, {})
        r36 = step36.get(key, {})

        # Original display name: prefer step14 nom_cheval
        # We need to recover the original casing
        record["nom"] = None
        if key in step14:
            # recover from raw — we use the normalized key, but original is upper
            record["nom"] = key.upper()  # approximate; overwritten below if possible
        elif key in step12:
            record["nom"] = key.upper()

        # Horse IDs
        if r14.get("horse_id_14"):
            record["horse_id_14"] = r14["horse_id_14"]
        if r12.get("horse_id_12"):
            record["horse_id_12"] = r12["horse_id_12"]

        # Pedigree tree (step14 priority, fallback step12)
        for field in ["pere", "mere"]:
            record[field] = r14.get(field) or r12.get(field)

        # Grandparents — step14 has better naming
        record["pere_mere"] = r14.get("pere_mere") or r12.get("pere_mere")
        record["grand_pere_paternel"] = r14.get("grand_pere_paternel") or r12.get("pere_pere")
        record["grand_mere_paternelle"] = r14.get("grand_mere_paternelle")
        record["grand_pere_maternel"] = r14.get("grand_pere_maternel") or r12.get("mere_pere")
        record["grand_mere_maternelle"] = r14.get("grand_mere_maternelle") or r12.get("mere_mere")

        # Great-grandparents (step14 only)
        for field in [
            "arriere_gpp_pp", "arriere_gpm_pp",
            "arriere_gpp_mp", "arriere_gpm_mp",
            "arriere_gpp_pm", "arriere_gpm_pm",
            "arriere_gpp_mm", "arriere_gpm_mm",
        ]:
            val = r14.get(field)
            if val:
                record[field] = val

        # Lignee male (step12 only)
        if r12.get("lignee_male"):
            record["lignee_male"] = r12["lignee_male"]

        # Metadata from step12
        for field in ["pays_naissance", "annee_naissance", "sexe", "race"]:
            if r12.get(field) is not None:
                record[field] = r12[field]

        # Step 36: country, birth_year, dosage
        if r36.get("country_36"):
            record.setdefault("pays_naissance", r36["country_36"])
        if r36.get("birth_year_36") is not None:
            record.setdefault("annee_naissance", r36["birth_year_36"])
        for field in ["dosage_profile", "dosage_total", "dosage_index", "center_of_distribution"]:
            if r36.get(field) is not None:
                record[field] = r36[field]

        # Step 14 metadata
        if r14.get("found_14") is not None:
            record["found_pedigreequery"] = r14["found_14"]

        # --- Sire stats (from step 08) ---
        pere_key = normalize_name(record.get("pere") or "")
        if pere_key and pere_key in sires:
            for k, v in sires[pere_key].items():
                record[k] = v

        # --- Dam stats (from step 08) ---
        mere_key = normalize_name(record.get("mere") or "")
        if mere_key and mere_key in dams:
            for k, v in dams[mere_key].items():
                record[k] = v

        merged.append(record)

    print(f"Total merged records: {len(merged):,}")
    return merged


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export(merged: list):
    # JSON
    json_path = OUT_DIR / "pedigree_complet.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"\nJSON saved: {json_path}  ({json_path.stat().st_size / 1_048_576:.1f} MB)")

    # CSV + Parquet via pandas
    # Flatten list columns for CSV
    df = pd.DataFrame(merged)

    # Convert list columns to semicolon-separated strings for CSV
    list_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, list)).any()]
    df_csv = df.copy()
    for col in list_cols:
        df_csv[col] = df_csv[col].apply(
            lambda x: ";".join(str(i) for i in x) if isinstance(x, list) else x
        )

    csv_path = OUT_DIR / "pedigree_complet.csv"
    df_csv.to_csv(csv_path, index=False)
    print(f"CSV saved:  {csv_path}  ({csv_path.stat().st_size / 1_048_576:.1f} MB)")

    pq_path = OUT_DIR / "pedigree_complet.parquet"
    df.to_parquet(pq_path, index=False)
    print(f"Parquet saved: {pq_path}  ({pq_path.stat().st_size / 1_048_576:.1f} MB)")

    return df


def print_stats(df: pd.DataFrame):
    print("\n" + "=" * 70)
    print("COVERAGE STATISTICS")
    print("=" * 70)
    print(f"Total unique horses: {len(df):,}")
    print(f"Total fields: {len(df.columns)}")
    print()
    print(f"{'Field':<35} {'Non-null':>10} {'Coverage':>10}")
    print("-" * 57)
    for col in df.columns:
        non_null = df[col].notna().sum()
        # Also exclude empty strings
        if df[col].dtype == object:
            non_null = (df[col].notna() & (df[col] != "") & (df[col] != "None")).sum()
        pct = non_null / len(df) * 100
        print(f"{col:<35} {non_null:>10,} {pct:>9.1f}%")

    # Source overlap stats
    print("\n" + "=" * 70)
    print("SOURCE OVERLAP")
    print("=" * 70)
    has_14 = df["horse_id_14"].notna().sum() if "horse_id_14" in df.columns else 0
    has_12 = df["horse_id_12"].notna().sum() if "horse_id_12" in df.columns else 0
    has_dosage = df["dosage_index"].notna().sum() if "dosage_index" in df.columns else 0
    has_sire_stats = df["sire_taux_victoire_descendants"].notna().sum() if "sire_taux_victoire_descendants" in df.columns else 0
    has_dam_stats = df["dam_taux_victoire_descendants"].notna().sum() if "dam_taux_victoire_descendants" in df.columns else 0

    print(f"  From step 14 (PedigreeQuery 4-gen):  {has_14:>6,}")
    print(f"  From step 12 (consolide):             {has_12:>6,}")
    print(f"  From step 36 (dosage data):           {has_dosage:>6,}")
    print(f"  With sire stats (step 08):            {has_sire_stats:>6,}")
    print(f"  With dam stats (step 08):             {has_dam_stats:>6,}")


def update_symlinks():
    """Add symlinks for the unified pedigree file in the feature builder data dir."""
    print("\n" + "=" * 70)
    print("UPDATING SYMLINKS")
    print("=" * 70)

    rel_prefix = "../../../../output/pedigree_complete"
    files = ["pedigree_complet.json", "pedigree_complet.csv", "pedigree_complet.parquet"]

    for fname in files:
        link_path = SYMLINK_DIR / fname
        target = f"{rel_prefix}/{fname}"
        if link_path.exists() or link_path.is_symlink():
            link_path.unlink()
        link_path.symlink_to(target)
        print(f"  {link_path.name} -> {target}")

    print("Symlinks updated.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    merged = merge_all()
    df = export(merged)
    print_stats(df)
    update_symlinks()
    print("\nDone.")
