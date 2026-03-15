#!/usr/bin/env python3
"""
Merge rapports_definitifs (21) and rapports_internet (38) into a single
unified rapport file with one record per course.

- rapports_definitifs: already one row per course, columns are rapport fields
- rapports_internet: multiple rows per course (one per bet type per combination),
  pivoted into numbered columns per bet type.

Output: output/rapports_merged/rapports_complets.json
"""

import json
import os
import sys
import time
from collections import defaultdict

BASE_DIR = "/Users/quentinherve/models hybride"
FILE_21 = os.path.join(BASE_DIR, "output/21_rapports_definitifs/rapports_definitifs.json")
FILE_38 = os.path.join(BASE_DIR, "output/38_rapports_internet/rapports_internet.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "output/rapports_merged")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "rapports_complets.json")


def stream_json_array(filepath, report_every=100000):
    """
    Stream-parse a JSON array file using ijson if available,
    otherwise fall back to chunked manual parsing.
    Uses a simple incremental JSON parser to avoid loading everything at once.
    """
    # Use a streaming approach: read the file and parse objects one by one
    # with a simple state machine for top-level array of objects.
    import ijson
    count = 0
    with open(filepath, "rb") as f:
        for obj in ijson.items(f, "item"):
            count += 1
            if count % report_every == 0:
                print(f"  ... streamed {count:,} records from {os.path.basename(filepath)}")
            yield obj
    print(f"  Total: {count:,} records from {os.path.basename(filepath)}")


def try_install_ijson():
    try:
        import ijson
        return True
    except ImportError:
        print("Installing ijson for streaming JSON parsing...")
        os.system(f"{sys.executable} -m pip install ijson -q")
        try:
            import ijson
            return True
        except ImportError:
            return False


def load_rapports_definitifs():
    """Load file 21 into a dict keyed by course_uid."""
    print("\n[1/4] Loading rapports_definitifs (21)...")
    t0 = time.time()
    result = {}
    for record in stream_json_array(FILE_21):
        uid = record["course_uid"]
        # Prefix rapport fields to clarify source (skip metadata fields)
        result[uid] = record
    print(f"  Loaded {len(result):,} courses in {time.time()-t0:.1f}s")
    return result


def pivot_rapports_internet():
    """
    Stream file 38, pivot to one dict per course_uid.
    For each typePari, rows are numbered sequentially (1-based).
    Columns created:
      - ri_{type_lower}_{n}_dividende
      - ri_{type_lower}_{n}_combinaison
      - ri_{type_lower}_{n}_nb_gagnants  (if not null)
      - ri_{type_lower}_{n}_mise_base
      - ri_{type_lower}_{n}_rembourse
    Also stores: ri_{type_lower}_count = number of rows for that type
    """
    print("\n[2/4] Streaming & pivoting rapports_internet (38)...")
    t0 = time.time()

    # Group rows by course_uid, then by typePari (maintain order)
    courses = defaultdict(lambda: defaultdict(list))

    for record in stream_json_array(FILE_38, report_every=200000):
        uid = record["course_uid"]
        tp = record["typePari"]
        courses[uid][tp].append(record)

    print(f"  Grouped {len(courses):,} courses in {time.time()-t0:.1f}s")

    # Now pivot each course
    print("  Pivoting into flat columns...")
    pivoted = {}
    for uid, type_dict in courses.items():
        row = {}
        for tp, rows in type_dict.items():
            tp_lower = tp.lower()
            row[f"ri_{tp_lower}_count"] = len(rows)
            for i, r in enumerate(rows, 1):
                prefix = f"ri_{tp_lower}_{i}"
                row[f"{prefix}_dividende"] = r.get("dividende")
                row[f"{prefix}_combinaison"] = r.get("combinaison")
                if r.get("nb_gagnants") is not None:
                    row[f"{prefix}_nb_gagnants"] = r["nb_gagnants"]
                row[f"{prefix}_mise_base"] = r.get("miseBase")
                row[f"{prefix}_rembourse"] = r.get("rembourse")
        pivoted[uid] = row

    print(f"  Pivoted {len(pivoted):,} courses in {time.time()-t0:.1f}s")
    return pivoted


def merge_and_write(definitifs, internet):
    """Merge both sources and write output."""
    print("\n[3/4] Merging datasets...")
    t0 = time.time()

    # Collect all course_uids from both sources
    all_uids = set(definitifs.keys()) | set(internet.keys())

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Stats
    both_count = 0
    only_21 = 0
    only_38 = 0

    # Write as streaming JSON array
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        out.write("[\n")
        first = True
        for uid in sorted(all_uids):
            rec_21 = definitifs.get(uid)
            rec_38 = internet.get(uid)

            # Build merged record
            merged = {}

            if rec_21:
                # Copy all fields from definitifs
                merged.update(rec_21)

            if rec_38:
                if not rec_21:
                    # Need course_uid at least
                    merged["course_uid"] = uid
                merged.update(rec_38)

            # Track source
            has_21 = rec_21 is not None
            has_38 = rec_38 is not None
            merged["source_rapports_definitifs"] = has_21
            merged["source_rapports_internet"] = has_38

            if has_21 and has_38:
                both_count += 1
            elif has_21:
                only_21 += 1
            else:
                only_38 += 1

            if not first:
                out.write(",\n")
            json.dump(merged, out, ensure_ascii=False)
            first = False

        out.write("\n]\n")

    elapsed = time.time() - t0
    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"  Written {len(all_uids):,} records in {elapsed:.1f}s")
    print(f"  Output file: {OUTPUT_FILE}")
    print(f"  Output size: {file_size/1024/1024:.1f} MB")

    return all_uids, both_count, only_21, only_38


def print_stats(all_uids, both_count, only_21, only_38, definitifs, internet):
    """Print detailed merge statistics."""
    print("\n[4/4] === MERGE STATISTICS ===")
    print(f"  Total courses in merged file:    {len(all_uids):,}")
    print(f"  Courses with BOTH sources:       {both_count:,}")
    print(f"  Courses only in definitifs (21):  {only_21:,}")
    print(f"  Courses only in internet (38):    {only_38:,}")
    print(f"  Courses in definitifs (21):       {len(definitifs):,}")
    print(f"  Courses in internet (38):         {len(internet):,}")

    # Field coverage: sample some records from output
    print("\n  --- Field coverage (from definitifs 21) ---")
    fields_21 = [
        "rapport_simple_gagnant", "rapport_couple_gagnant",
        "rapport_tierce_ordre", "rapport_quarte_ordre", "rapport_quinte_ordre",
        "rapport_multi_4", "rapport_2sur4_min"
    ]
    for field in fields_21:
        count = sum(1 for r in definitifs.values() if r.get(field) is not None)
        pct = 100.0 * count / len(definitifs) if definitifs else 0
        print(f"    {field}: {count:,} / {len(definitifs):,} ({pct:.1f}%)")

    print("\n  --- Field coverage (from internet 38 - pivoted) ---")
    # Check coverage of main bet types
    ri_types = [
        "ri_e_simple_gagnant_count", "ri_e_couple_gagnant_count",
        "ri_e_trio_count", "ri_e_trio_ordre_count",
        "ri_e_multi_count", "ri_e_quarte_plus_count",
        "ri_e_quinte_plus_count", "ri_e_deux_sur_quatre_count",
        "ri_e_mini_multi_count", "ri_e_pick5_count"
    ]
    for field in ri_types:
        count = sum(1 for r in internet.values() if r.get(field) is not None)
        pct = 100.0 * count / len(internet) if internet else 0
        label = field.replace("ri_", "").replace("_count", "")
        print(f"    {label}: {count:,} / {len(internet):,} ({pct:.1f}%)")


def main():
    if not try_install_ijson():
        print("ERROR: Could not install ijson. Falling back to full load.")
        sys.exit(1)

    definitifs = load_rapports_definitifs()
    internet = pivot_rapports_internet()

    all_uids, both_count, only_21, only_38 = merge_and_write(definitifs, internet)
    print_stats(all_uids, both_count, only_21, only_38, definitifs, internet)

    print("\nDone!")


if __name__ == "__main__":
    main()
