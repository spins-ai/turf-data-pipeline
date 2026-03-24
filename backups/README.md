# Backup Strategy

## Overview

This directory stores compressed backups of the `data_master/` directory,
which contains all master data files for the turf data pipeline (courses,
partants, performances, pedigrees, weather, market data, etc.).

Backups are created by `scripts/backup_data.py`.

## How it works

### Creating a backup

```bash
# Full backup (gzip-compressed copies of all data files)
python scripts/backup_data.py

# Preview what would be backed up (no files written)
python scripts/backup_data.py --dry-run
```

Each run creates a timestamped directory: `backups/backup_YYYYMMDD_HHMMSS/`.

### What gets backed up

- All `.json`, `.jsonl`, `.csv`, and `.parquet` files in `data_master/`.
- Indexes, cache, and temp files are excluded.
- Files named `turf_data.duckdb` or starting with `tmp_`/`temp_` are skipped.

### Compression

- Files up to 500 MB are gzip-compressed (typically 60-80% size reduction).
- Files over 500 MB get a SHA-256 hash file only (`.sha256`), since copying
  multi-gigabyte files is impractical for routine backups.

### Manifest

Every backup contains a `manifest.json` with:

- Timestamp of creation.
- List of all backed-up files with original size, compressed size, and SHA-256
  checksum.
- Total original vs. backup size.

### Verification

Compare checksums to verify backup integrity:

```bash
# The manifest.json in each backup contains SHA-256 hashes
# Compare against current files to detect drift
```

## Retention policy

- Keep at least the 3 most recent backups.
- The initial reference backup (`backup_complet_20260315`) should be preserved
  as a restore point.
- Older backups can be removed manually once verified.

## Restore procedure

1. Identify the backup to restore from (check `manifest.json` dates).
2. Decompress `.gz` files back into `data_master/`:
   ```bash
   gunzip backups/backup_XXXXXXXX_XXXXXX/filename.ext.gz
   cp backups/backup_XXXXXXXX_XXXXXX/filename.ext data_master/
   ```
3. For hash-only files (over 500 MB), restore from the original source or
   re-run the relevant scraper/merge script.
4. Verify checksums after restore using the manifest.

## Directory structure

```
backups/
  README.md                      <- this file
  backup_20260315_120000/        <- timestamped backup
    manifest.json                <- file list + checksums
    courses_master.jsonl.gz      <- compressed data file
    partants_master.parquet.gz   <- compressed data file
    large_file.sha256            <- hash-only for big files
  backup_20260320_090000/        <- another backup
    ...
```

## Notes

- Backups are excluded from git via `.gitignore` (only this README is tracked).
- The backup script uses streaming I/O (8 MB buffer) to keep memory usage low,
  even for multi-gigabyte files.
- Run `make backup` as a shortcut (defined in the project Makefile).
