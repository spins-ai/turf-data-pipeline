# Backup Procedures

## Overview

Backups are managed by `scripts/backup_data.py`, which creates timestamped, compressed copies of all master data files from `data_master/`. Backups are stored under `backups/backup_YYYYMMDD_HHMMSS/`.

## Running a Backup

### Preview (dry run)

```bash
python scripts/backup_data.py --dry-run
```

This lists every file that would be backed up, its size, and whether it will be gzip-compressed or hash-only. No files are written.

### Full backup

```bash
python scripts/backup_data.py
```

Creates a new timestamped directory under `backups/` and processes all eligible files.

## What Gets Backed Up

### Eligible files

All files in `data_master/` with these extensions:
- `.jsonl`, `.json`, `.parquet`, `.csv`

### Excluded

- DuckDB files (`turf_data.duckdb`, `.wal`)
- Temporary files (`tmp_*`, `temp_*`, `.tmp*`, `.temp*`)
- Hidden directories, `indexes/`, `__pycache__/`, `cache/`

### Size-Based Handling

| File size | Action | Output |
|-----------|--------|--------|
| <= 500 MB | **gzip copy** | `<filename>.gz` (compressed with level 6) |
| > 500 MB | **hash only** | `<filename>.sha256` (SHA-256 checksum file) |

Large files (>500 MB) are not copied to save disk space. Instead, a SHA-256 hash is recorded so integrity can be verified later. To restore these files, the originals must be available (e.g., from the output directories or a full disk backup).

### Streaming and Memory

The backup uses an 8 MB streaming buffer for both hashing and compression, keeping RAM usage well under 2 GB regardless of file size.

## Backup Manifest

Each backup directory contains a `manifest.json` with metadata for every file:

```json
{
  "file": "partants_master.jsonl",
  "original_size": 123456789,
  "backup_type": "gzip_copy",
  "compressed_size": 45678901,
  "sha256": "abc123..."
}
```

For hash-only files, `backup_type` is `"hash_only"` and `compressed_size` is omitted.

## Checksum Verification

Separate from backups, the file `security/checksums.json` stores checksums for data files. The daily maintenance script (`scripts/daily_maintenance.py`) verifies 3 random files against `CHECKSUMS.sha256` each day.

To verify a backup manually, compare the SHA-256 in the manifest against the current file:

```bash
# On Linux/macOS:
sha256sum data_master/partants_master.jsonl

# On Windows (PowerShell):
Get-FileHash data_master\partants_master.jsonl -Algorithm SHA256
```

## Recovery Procedures

### Restoring from a gzip backup

```bash
# Decompress a single file
python -c "
import gzip, shutil
with gzip.open('backups/backup_20260320_143000/partants_master.jsonl.gz', 'rb') as f_in:
    with open('data_master/partants_master.jsonl', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
"
```

Or using command-line tools:

```bash
gunzip -k backups/backup_20260320_143000/partants_master.jsonl.gz
mv backups/backup_20260320_143000/partants_master.jsonl data_master/
```

### Restoring hash-only files

Files over 500 MB are not copied -- only their hashes are stored. To restore:

1. Re-run the relevant merge script (e.g., `merge_pedigree_master.py`, `mega_merge.py`).
2. Verify the regenerated file hash matches the stored `.sha256` value.

### Full restore from backup

```bash
# List available backups
ls backups/

# Restore all gzip files from a specific backup
for f in backups/backup_20260320_143000/*.gz; do
    gunzip -k "$f"
    mv "${f%.gz}" data_master/
done
```

After restoring, run `scripts/diagnostic.py` to verify integrity.

## Recommended Backup Frequency

| Event | Action |
|-------|--------|
| **After completing a batch of scrapers** | Run full backup |
| **After nettoyage/enrichment passes** | Run full backup (new checkpoint) |
| **After feature engineering rebuild** | Run full backup |
| **Weekly (routine)** | Run full backup |
| **Before any destructive operation** | Run full backup |

The TODO references specific backup checkpoints:
- Backup #1: after all scrapers finish (step 1.4)
- Backup #2: after nettoyage (step 3.5)
- Backup #3: after mega-merge (step 5.5)
- Backup #4: after feature engineering (step 6.5)
- Final backup: after quality validation (step 11.4)

## Backup Storage

Backups are stored locally under `backups/`. For disaster recovery, consider copying the backup directory to an external drive or cloud storage. The manifest makes it easy to verify completeness after transfer.
