#!/usr/bin/env python3
"""
Cross-Source Validation — Referential Integrity & Consistency Checks
Samples 1000 records from partants_master and validates:
  1. course_uid exists in courses_master
  2. pedigree consistency (pere/mere match pedigree_master)
  3. position consistency (position_arrivee coherent values)
Reports match rates per check.
"""

import os
import sys
import random
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data_master")

PARTANTS_FILE = os.path.join(DATA_DIR, "partants_master.parquet")
COURSES_FILE = os.path.join(DATA_DIR, "courses_master.parquet")
PEDIGREE_FILE = os.path.join(DATA_DIR, "pedigree_master.parquet")

SAMPLE_SIZE = 1000
REPORT_DIR = os.path.join(BASE_DIR, "output", "quality")
os.makedirs(REPORT_DIR, exist_ok=True)


def load_parquet_lazy(path, columns=None):
    """Load a parquet file with optional column selection."""
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(path, columns=columns)
        return table.to_pandas()
    except Exception as e:
        print(f"[ERROR] Cannot load {path}: {e}")
        return None


def check_course_uid_integrity(sample_df, courses_df):
    """Check that course_uid in sample exists in courses_master."""
    if "course_uid" not in sample_df.columns:
        return {"check": "course_uid_exists", "status": "SKIP",
                "reason": "column course_uid not in partants_master"}
    if courses_df is None:
        return {"check": "course_uid_exists", "status": "SKIP",
                "reason": "courses_master not available"}

    courses_uids = set(courses_df["course_uid"].dropna().unique())
    sample_uids = sample_df["course_uid"].dropna()
    total = len(sample_uids)
    if total == 0:
        return {"check": "course_uid_exists", "status": "SKIP",
                "reason": "no non-null course_uid in sample"}

    matched = sample_uids.isin(courses_uids).sum()
    missing = total - matched
    rate = matched / total * 100

    missing_examples = list(
        sample_uids[~sample_uids.isin(courses_uids)].head(10).values
    )

    return {
        "check": "course_uid_exists",
        "total_checked": int(total),
        "matched": int(matched),
        "missing": int(missing),
        "match_rate_pct": round(rate, 2),
        "missing_examples": missing_examples[:5],
    }


def check_pedigree_consistency(sample_df, pedigree_df):
    """Check that pere/mere in partants match pedigree_master."""
    if pedigree_df is None:
        return {"check": "pedigree_consistency", "status": "SKIP",
                "reason": "pedigree_master not available"}

    results = {"check": "pedigree_consistency", "sub_checks": {}}

    # Build lookup from pedigree_master
    # Detect column names (could be pere/sire, mere/dam, nom_cheval/horse_name)
    ped_cols = pedigree_df.columns.tolist()

    # Horse name column in pedigree
    horse_col_ped = None
    for c in ["nom_cheval", "horse_name", "name", "cheval"]:
        if c in ped_cols:
            horse_col_ped = c
            break

    pere_col_ped = None
    for c in ["pere", "sire", "father"]:
        if c in ped_cols:
            pere_col_ped = c
            break

    mere_col_ped = None
    for c in ["mere", "dam", "mother"]:
        if c in ped_cols:
            mere_col_ped = c
            break

    # Check pere match
    if "pere" in sample_df.columns and pere_col_ped and horse_col_ped:
        # Build pedigree lookup: horse -> pere
        ped_lookup = {}
        for _, row in pedigree_df[[horse_col_ped, pere_col_ped]].dropna().iterrows():
            name = str(row[horse_col_ped]).strip().upper()
            pere = str(row[pere_col_ped]).strip().upper()
            if name and pere:
                ped_lookup[name] = pere

        checked = 0
        matched = 0
        mismatched_examples = []

        for _, row in sample_df[["nom_cheval", "pere"]].dropna().iterrows():
            horse = str(row["nom_cheval"]).strip().upper()
            pere_partant = str(row["pere"]).strip().upper()
            if not horse or not pere_partant:
                continue
            if horse in ped_lookup:
                checked += 1
                if ped_lookup[horse] == pere_partant:
                    matched += 1
                else:
                    if len(mismatched_examples) < 5:
                        mismatched_examples.append({
                            "horse": horse,
                            "pere_partants": pere_partant,
                            "pere_pedigree": ped_lookup[horse],
                        })

        if checked > 0:
            results["sub_checks"]["pere_match"] = {
                "total_checked": checked,
                "matched": matched,
                "match_rate_pct": round(matched / checked * 100, 2),
                "mismatched_examples": mismatched_examples,
            }
        else:
            results["sub_checks"]["pere_match"] = {
                "status": "SKIP",
                "reason": "no horses found in both sources",
            }
    else:
        results["sub_checks"]["pere_match"] = {
            "status": "SKIP",
            "reason": "required columns not available",
        }

    # Check mere match
    if "mere" in sample_df.columns and mere_col_ped and horse_col_ped:
        ped_lookup_mere = {}
        for _, row in pedigree_df[[horse_col_ped, mere_col_ped]].dropna().iterrows():
            name = str(row[horse_col_ped]).strip().upper()
            mere = str(row[mere_col_ped]).strip().upper()
            if name and mere:
                ped_lookup_mere[name] = mere

        checked = 0
        matched = 0
        mismatched_examples = []

        for _, row in sample_df[["nom_cheval", "mere"]].dropna().iterrows():
            horse = str(row["nom_cheval"]).strip().upper()
            mere_partant = str(row["mere"]).strip().upper()
            if not horse or not mere_partant:
                continue
            if horse in ped_lookup_mere:
                checked += 1
                if ped_lookup_mere[horse] == mere_partant:
                    matched += 1
                else:
                    if len(mismatched_examples) < 5:
                        mismatched_examples.append({
                            "horse": horse,
                            "mere_partants": mere_partant,
                            "mere_pedigree": ped_lookup_mere[horse],
                        })

        if checked > 0:
            results["sub_checks"]["mere_match"] = {
                "total_checked": checked,
                "matched": matched,
                "match_rate_pct": round(matched / checked * 100, 2),
                "mismatched_examples": mismatched_examples,
            }
        else:
            results["sub_checks"]["mere_match"] = {
                "status": "SKIP",
                "reason": "no horses found in both sources",
            }
    else:
        results["sub_checks"]["mere_match"] = {
            "status": "SKIP",
            "reason": "required columns not available",
        }

    return results


def check_position_consistency(sample_df):
    """Check that position_arrivee values are coherent."""
    if "position_arrivee" not in sample_df.columns:
        return {"check": "position_consistency", "status": "SKIP",
                "reason": "column position_arrivee not in partants_master"}

    positions = sample_df["position_arrivee"].dropna()
    total = len(positions)
    if total == 0:
        return {"check": "position_consistency", "status": "SKIP",
                "reason": "all position_arrivee values are null"}

    # Valid positions: 1-30 typically, 0 for DNF/disqualified in some systems
    valid_mask = (positions >= 0) & (positions <= 30)
    valid_count = valid_mask.sum()
    invalid_count = total - valid_count

    # Check is_gagnant consistency
    gagnant_check = None
    if "is_gagnant" in sample_df.columns:
        winners = sample_df.dropna(subset=["position_arrivee", "is_gagnant"])
        if len(winners) > 0:
            pos1_and_gagnant = ((winners["position_arrivee"] == 1) &
                                (winners["is_gagnant"] == True)).sum()
            pos1_total = (winners["position_arrivee"] == 1).sum()
            gagnant_total = (winners["is_gagnant"] == True).sum()
            gagnant_check = {
                "pos1_count": int(pos1_total),
                "is_gagnant_count": int(gagnant_total),
                "pos1_and_gagnant": int(pos1_and_gagnant),
                "consistent": pos1_total == gagnant_total == pos1_and_gagnant,
            }

    # Check nombre_partants vs max position
    partants_check = None
    if "nombre_partants" in sample_df.columns:
        both = sample_df.dropna(subset=["position_arrivee", "nombre_partants"])
        if len(both) > 0:
            over = (both["position_arrivee"] > both["nombre_partants"]).sum()
            partants_check = {
                "checked": int(len(both)),
                "position_exceeds_partants": int(over),
            }

    invalid_examples = list(
        positions[~valid_mask].head(10).values
    )

    return {
        "check": "position_consistency",
        "total_checked": int(total),
        "valid_range_0_30": int(valid_count),
        "out_of_range": int(invalid_count),
        "valid_rate_pct": round(valid_count / total * 100, 2),
        "invalid_examples": [float(x) for x in invalid_examples[:5]],
        "gagnant_consistency": gagnant_check,
        "partants_consistency": partants_check,
    }


def main():
    import json

    print("=" * 60)
    print("CROSS-SOURCE VALIDATION")
    print(f"  Sample size: {SAMPLE_SIZE}")
    print(f"  Data dir: {DATA_DIR}")
    print("=" * 60)

    # Load partants_master
    print("\nLoading partants_master...")
    partants_df = load_parquet_lazy(PARTANTS_FILE)
    if partants_df is None:
        print("[FATAL] Cannot load partants_master.parquet")
        sys.exit(1)

    total_rows = len(partants_df)
    print(f"  Total rows: {total_rows:,}")

    # Sample
    if total_rows > SAMPLE_SIZE:
        random.seed(42)
        indices = random.sample(range(total_rows), SAMPLE_SIZE)
        sample_df = partants_df.iloc[indices].copy()
    else:
        sample_df = partants_df.copy()
    print(f"  Sample size: {len(sample_df):,}")

    # Load courses_master
    print("\nLoading courses_master...")
    courses_cols = ["course_uid"]
    courses_df = load_parquet_lazy(COURSES_FILE, columns=courses_cols)
    if courses_df is not None:
        print(f"  Courses loaded: {len(courses_df):,} rows")

    # Load pedigree_master
    print("\nLoading pedigree_master...")
    pedigree_df = load_parquet_lazy(PEDIGREE_FILE)
    if pedigree_df is not None:
        print(f"  Pedigree loaded: {len(pedigree_df):,} rows")
        print(f"  Pedigree columns: {pedigree_df.columns.tolist()}")

    # Run checks
    print("\n" + "=" * 60)
    print("RUNNING CHECKS")
    print("=" * 60)

    results = {
        "timestamp": datetime.now().isoformat(),
        "sample_size": len(sample_df),
        "total_partants": total_rows,
        "checks": [],
    }

    # Check 1: course_uid integrity
    print("\n[1/3] Checking course_uid referential integrity...")
    r1 = check_course_uid_integrity(sample_df, courses_df)
    results["checks"].append(r1)
    if "match_rate_pct" in r1:
        print(f"  -> Match rate: {r1['match_rate_pct']}% "
              f"({r1['matched']}/{r1['total_checked']})")
    else:
        print(f"  -> {r1.get('status', 'N/A')}: {r1.get('reason', '')}")

    # Check 2: pedigree consistency
    print("\n[2/3] Checking pedigree consistency (pere/mere)...")
    r2 = check_pedigree_consistency(sample_df, pedigree_df)
    results["checks"].append(r2)
    for sub_name, sub_result in r2.get("sub_checks", {}).items():
        if "match_rate_pct" in sub_result:
            print(f"  -> {sub_name}: {sub_result['match_rate_pct']}% "
                  f"({sub_result['matched']}/{sub_result['total_checked']})")
        else:
            print(f"  -> {sub_name}: {sub_result.get('status', 'N/A')}: "
                  f"{sub_result.get('reason', '')}")

    # Check 3: position consistency
    print("\n[3/3] Checking position consistency...")
    r3 = check_position_consistency(sample_df)
    results["checks"].append(r3)
    if "valid_rate_pct" in r3:
        print(f"  -> Valid positions: {r3['valid_rate_pct']}% "
              f"({r3['valid_range_0_30']}/{r3['total_checked']})")
        if r3.get("gagnant_consistency"):
            gc = r3["gagnant_consistency"]
            status = "OK" if gc["consistent"] else "MISMATCH"
            print(f"  -> is_gagnant consistency: {status} "
                  f"(pos1={gc['pos1_count']}, gagnant={gc['is_gagnant_count']})")
        if r3.get("partants_consistency"):
            pc = r3["partants_consistency"]
            print(f"  -> position > nombre_partants: {pc['position_exceeds_partants']}"
                  f"/{pc['checked']}")
    else:
        print(f"  -> {r3.get('status', 'N/A')}: {r3.get('reason', '')}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for check in results["checks"]:
        name = check.get("check", "?")
        if "match_rate_pct" in check:
            print(f"  {name}: {check['match_rate_pct']}%")
        elif "valid_rate_pct" in check:
            print(f"  {name}: {check['valid_rate_pct']}%")
        elif "sub_checks" in check:
            for sub_name, sub in check["sub_checks"].items():
                if "match_rate_pct" in sub:
                    print(f"  {name}/{sub_name}: {sub['match_rate_pct']}%")
                else:
                    print(f"  {name}/{sub_name}: SKIPPED")
        else:
            print(f"  {name}: SKIPPED")

    # Save report
    report_file = os.path.join(REPORT_DIR, "cross_source_validation.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nReport saved: {report_file}")


if __name__ == "__main__":
    main()
