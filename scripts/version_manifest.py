#!/usr/bin/env python3
"""Generate version manifest for the feature pipeline outputs."""

import json
import hashlib
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = Path("D:/turf-data-pipeline/04_FEATURES")
MANIFEST_PATH = OUTPUT_DIR / "version_manifest.json"

# Key pipeline files
PIPELINE_FILES = {
    "features_selected.parquet": "Final ML-ready features (454 cols + target_roi, 2.93M rows)",
    "features_normalized.parquet": "Z-score normalized features (2577 cols)",
    "features_encoded.parquet": "Categoricals encoded (2577 cols)",
    "features_clean.parquet": "After noise reduction (2816 cols)",
    "features_capped.parquet": "After outlier capping (3181 cols)",
    "features_imputed.parquet": "After NaN imputation (3181 cols)",
    "features_consolidated.parquet": "All 297 builders consolidated (3181 cols)",
    "feature_importance.csv": "LightGBM feature importance ranking",
    "data_drift_audit.csv": "Train/test PSI drift analysis",
    "noise_audit.csv": "Near-zero variance + high NaN audit",
    "normalization_stats.csv": "Train-set mean/std for z-score",
    "encoding_log.csv": "Label/frequency encoding mappings",
    "parquet_metadata.csv": "Per-column statistics",
    "outlier_capping_thresholds.csv": "5-sigma capping thresholds",
}

def file_hash(path: Path, chunk_size=65536) -> str:
    """Compute MD5 of first 10MB (fast approximation)."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        total = 0
        while total < 10 * 1024 * 1024:
            data = f.read(chunk_size)
            if not data:
                break
            h.update(data)
            total += len(data)
    return h.hexdigest()

def main():
    manifest = {
        "pipeline_version": "1.0.0",
        "generated_at": datetime.now().isoformat(),
        "pipeline_steps": [
            "1. Consolidation: 297 builders -> 1 Parquet (3181 cols, 7.7 GB)",
            "2. DuckDB index: permanent DB with partant_uid index",
            "3. Schema fix: verified clean (no fixes needed)",
            "4. Imputation: 1877 cols imputed (zero/elo/median rules)",
            "5. Outlier capping: 137 features clipped at 5-sigma (13.6M values)",
            "6. Noise reduction: 421 features dropped (NZV + high NaN)",
            "7. Categorical encoding: 28 label + 9 freq encoded, 239 string dropped",
            "8. Z-score normalization: 1910 features normalized (train-set stats)",
            "9. Feature selection: LightGBM top 500 features",
            "10. Drift removal: 47 high-PSI features dropped",
            "11. ROI target: target_roi column added",
        ],
        "final_dataset": {
            "file": "features_selected.parquet",
            "rows": 2930290,
            "columns": 455,
            "features": 453,
            "targets": ["comblage__is_gagnant (binary)", "target_roi (continuous)"],
            "id_column": "partant_uid",
        },
        "splits": {
            "train_uids": "splits/train_uids.txt (2,394,129 UIDs)",
            "test_uids": "splits/test_uids.txt (416,807 UIDs)",
            "method": "temporal split (earlier = train, later = test)",
        },
        "files": {},
    }

    for fname, description in PIPELINE_FILES.items():
        fpath = OUTPUT_DIR / fname
        if fpath.exists():
            size_mb = fpath.stat().st_size / 1024 / 1024
            manifest["files"][fname] = {
                "description": description,
                "size_mb": round(size_mb, 1),
                "md5_prefix": file_hash(fpath),
                "modified": datetime.fromtimestamp(fpath.stat().st_mtime).isoformat(),
            }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"Version manifest: {MANIFEST_PATH}")
    print(f"Files tracked: {len(manifest['files'])}")
    print(json.dumps(manifest["final_dataset"], indent=2))

if __name__ == "__main__":
    main()
