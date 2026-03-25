#!/usr/bin/env python3
"""
scripts/propagate_features_to_master.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Propagate computed feature columns from features_matrix.jsonl into the
partants master (partants_normalises.jsonl), joining on partant_uid.

The features_matrix contains 376 columns per partant, including both raw
fields (already present in the master) and derived features (computed by
feature builders).  This script only propagates columns that do NOT already
exist in the master record, avoiding overwrites of source-of-truth fields.

Merge strategy:
  - Load features_matrix.jsonl into a dict keyed by partant_uid.
  - Stream partants_normalises.jsonl line by line.
  - For each partant, look up its partant_uid in the features dict.
  - Add any feature columns not already present in the master record.
  - Write enriched records to partants_enriched_features.jsonl.

This ensures the master file gains all derived features (elo, speed figures,
delta, ranking, musique parsed, meteo, etc.) without duplicating raw data.
"""

from __future__ import annotations

import json
import os
import sys
import time

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FEATURES_PATH = os.path.join(BASE, "output", "features", "features_matrix.jsonl")
MASTER_IN = os.path.join(BASE, "output", "02_merged_intermediate", "partants_normalises.jsonl")
MASTER_OUT = os.path.join(BASE, "output", "02_merged_intermediate", "partants_enriched_features.jsonl")

# Fields from features_matrix that are identity/key fields (not features).
# These are already in the master and should never be overwritten.
_SKIP_FIELDS = {
    "partant_uid", "course_uid", "reunion_uid", "cle_partant", "source",
    "date_reunion_iso", "hippodrome_normalise", "numero_reunion",
    "numero_course", "distance", "discipline", "horse_id", "nom_cheval",
    "num_pmu",
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    t0 = time.time()

    # --- Validate inputs ---
    for label, path in [("Features", FEATURES_PATH), ("Master", MASTER_IN)]:
        if not os.path.isfile(path):
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    # --- Phase 1: load features into memory keyed by partant_uid ---
    print(f"[1/2] Loading features from {FEATURES_PATH} ...")

    features_lookup: dict[str, dict] = {}
    total_feat = 0
    feature_cols: set[str] = set()

    with open(FEATURES_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_feat += 1
            uid = rec.get("partant_uid")
            if not uid:
                continue

            # Keep only derived feature columns (exclude identity fields)
            feat = {k: v for k, v in rec.items() if k not in _SKIP_FIELDS}
            if not feature_cols:
                feature_cols = set(feat.keys())

            features_lookup[str(uid)] = feat

    print(f"       {total_feat:,} feature records loaded, "
          f"{len(features_lookup):,} indexed by partant_uid, "
          f"{len(feature_cols):,} feature columns")

    # --- Phase 2: stream master, enrich, write out ---
    print(f"[2/2] Streaming master -> enriched output ...")

    total = 0
    matched = 0
    cols_added = 0

    os.makedirs(os.path.dirname(MASTER_OUT), exist_ok=True)

    with open(MASTER_IN, "r", encoding="utf-8") as fin, \
         open(MASTER_OUT, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                fout.write(line + "\n")
                total += 1
                continue

            total += 1

            uid = rec.get("partant_uid")
            if uid:
                feat = features_lookup.get(str(uid))
                if feat:
                    added = 0
                    for k, v in feat.items():
                        if k not in rec and v is not None:
                            rec[k] = v
                            added += 1
                    if added > 0:
                        matched += 1
                        cols_added += added

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    elapsed = time.time() - t0
    pct = (matched / total * 100) if total else 0
    avg_cols = (cols_added / matched) if matched else 0
    print(f"Done in {elapsed:.1f}s. {total:,} partants, {matched:,} enriched ({pct:.1f}%).")
    print(f"Average {avg_cols:.0f} feature columns added per matched partant.")
    print(f"Output: {MASTER_OUT}")


if __name__ == "__main__":
    main()
