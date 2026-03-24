# Maintenance Procedures

## Daily Maintenance

Run `scripts/daily_maintenance.py` every day (or schedule via cron/Task Scheduler):

```bash
python scripts/daily_maintenance.py
```

### What it does

1. **Clean `__pycache__`** -- removes all `__pycache__` directories to reclaim disk space.
2. **Clean temp files** -- deletes `.tmp` files older than 24 hours.
3. **Check disk space** -- warns if free space drops below 50 GB.
4. **Check data freshness** -- warns if `partants_master` is older than 7 days.
5. **Verify checksums** -- spot-checks 3 random files against `CHECKSUMS.sha256`.
6. **Run diagnostic health check** -- invokes `diagnostic.py` internally.
7. **Write log** -- outputs summary to `logs/daily_maintenance_YYYYMMDD.log`.

### Interpreting results

The log reports counts of cleaned items, warnings, and errors. Check `logs/daily_maintenance_YYYYMMDD.log` for details. Any warnings about stale data or failed checksums should be investigated promptly.

## Weekly Maintenance

### Run diagnostics

```bash
python scripts/diagnostic.py
```

Performs 10 health checks:

1. Master files exist in `data_master/`
2. Output directories exist
3. No zero-byte files
4. Python module imports work (`utils.*`)
5. `py_compile` on a sample of 20 Python scripts
6. Master file freshness (< 7 days)
7. Record counts (partants_master, features_matrix, labels)
8. Disk space (warns if < 50 GB free)
9. Available RAM
10. Git status (uncommitted changes)

Exit code 0 means all checks pass; exit code 1 means at least one failure.

### Check telemetry

```bash
python scripts/telemetry_collector.py
```

Collects and reports:

- Disk usage per output directory
- Record counts per JSONL file
- Last modified dates
- Daily data growth rate
- Scraper success rates
- Pipeline throughput

Output is saved to `logs/telemetry_YYYYMMDD.json` and printed as a human-readable summary. Compare week-over-week to spot anomalies (e.g., a scraper that stopped producing data).

## Monthly Maintenance

### Run full quality suite

```bash
python quality/run_all_tests.py
```

This runs all quality checks in the `quality/` directory:

- `test_json_integrity.py` -- all JSON/JSONL files are valid
- `test_zero_bytes.py` -- no zero-byte files
- `test_dates_valid.py` -- all dates are parseable and in expected range
- `test_values_range.py` -- cotes > 0, distances > 0, etc.
- `test_record_counts.py` -- record counts are consistent across sources
- `test_features_quality.py` -- no NaN/Inf in numeric features
- `test_cross_source.py` -- cross-source validation
- `sanity_checks_metier.py` -- domain-specific sanity checks

### Update checksums

After verifying data integrity, regenerate checksums:

```bash
python security/backup_checksums.py
```

This updates `security/checksums.json` with fresh SHA-256 hashes for all master files. The daily maintenance script uses these for spot-check verification.

### Review logs

Check the `logs/` directory for any accumulated warnings or errors from daily maintenance runs. Archive old logs if the directory grows too large.

## After a Crash: Recovery Procedures

### 1. Assess the damage

```bash
python scripts/diagnostic.py
```

This will report which master files are missing, corrupted (zero bytes), or stale.

### 2. Check for checkpoint files

Most scrapers save progress to checkpoint files (`.checkpoint.json` in their output directory). These allow resuming from the last successful point:

```bash
# Example: check Visual Crossing checkpoint
cat output/112_visual_crossing/.checkpoint.json

# Example: check NOAA checkpoint
cat output/96_noaa_weather/.checkpoint.json
```

### 3. Re-run failed scrapers

Scrapers with checkpoint support will automatically resume from where they left off:

```bash
# Just re-run the script -- it reads its checkpoint automatically
python 112_visual_crossing_scraper.py
python 96_noaa_weather_scraper.py
```

For scrapers without checkpoint support, check the output directory for partial data and decide whether to re-run from scratch or manually resume.

### 4. Re-run the pipeline from the failure point

The pipeline has a defined order (see `docs/PIPELINE.md`). Identify which stage failed and re-run from that point:

```bash
# Stage 1: Re-merge master files if corrupted
python merge_pedigree_master.py
python merge_meteo_master.py
python mega_merge.py

# Stage 2: Re-run enrichment
python enrichissement_champs.py

# Stage 3: Re-run feature engineering
python master_feature_builder.py

# Stage 4: Re-generate labels
python generate_labels.py
```

### 5. Verify recovery

```bash
# Run diagnostics
python scripts/diagnostic.py

# Run quality checks
python quality/run_all_tests.py

# Verify checksums still match (or regenerate)
python security/backup_checksums.py
```

### 6. Restore from backup (last resort)

If data cannot be regenerated, restore from the most recent backup:

```bash
# See docs/BACKUP.md for detailed restore procedures
python scripts/backup_data.py --dry-run  # verify backup exists
```

## Scheduled Maintenance Summary

| Frequency | Task | Command |
|-----------|------|---------|
| Daily | Maintenance (cleanup, freshness, checksums) | `python scripts/daily_maintenance.py` |
| Weekly | Full diagnostics | `python scripts/diagnostic.py` |
| Weekly | Telemetry collection | `python scripts/telemetry_collector.py` |
| Monthly | Full quality test suite | `python quality/run_all_tests.py` |
| Monthly | Update checksums | `python security/backup_checksums.py` |
| After scraper runs | Backup | `python scripts/backup_data.py` |
| After crash | Diagnostic + checkpoint recovery | See "After a Crash" above |

## Automation

To automate daily maintenance on Windows, use Task Scheduler:

```
Program: python
Arguments: scripts/daily_maintenance.py
Start in: C:\Users\celia\turf-data-pipeline
Trigger: Daily at 02:00
```

On Linux/macOS, use cron:

```cron
0 2 * * * cd /path/to/turf-data-pipeline && python scripts/daily_maintenance.py
```
