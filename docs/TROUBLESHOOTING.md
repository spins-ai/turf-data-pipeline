# Troubleshooting Guide

Common problems encountered in this pipeline and their solutions.

---

## 1. CRLF Line Endings (Windows)

**Symptom**: JSONL files contain `\r\n` instead of `\n`. Downstream parsers choke on extra `\r` inside JSON strings or field values.

**Root cause**: Python `open()` on Windows defaults to `\r\n` line endings in text mode.

**Fix applied**: All JSONL writers now use `newline="\n"` parameter:
```python
open(path, "w", encoding="utf-8", newline="\n")
```

**Prevention**: A `.gitattributes` file enforces LF endings for `*.jsonl` and `*.py` files. The shared `utils/output.py` helpers (`save_jsonl`, `append_jsonl`) always use `newline="\n"`.

---

## 2. Cloudflare Blocking (403/503 errors)

**Symptom**: Scrapers return HTTP 403 or 503. Response body contains Cloudflare challenge page.

**Affected sites**: Zeturf, Equidia, Oddschecker, France Galop, Paris-Turf, Turfomania, TurfInfo, and others.

**Solutions (in order of preference)**:

1. **Playwright** (preferred): 14 scrapers have been migrated to Playwright via `utils/playwright.py` shared helpers. Playwright renders the full JS challenge automatically.
   ```python
   from utils.playwright import create_browser, fetch_page
   ```

2. **cloudscraper** (fallback): For sites with lighter protection, `cloudscraper` can bypass basic challenges:
   ```python
   import cloudscraper
   session = cloudscraper.create_scraper()
   ```

3. **HTML cache**: Some scrapers cache raw HTML locally. Use `--export` flag to re-extract data from cache without re-fetching.

**Prevention**: Use `utils.scraping.create_session()` which auto-selects cloudscraper with proper headers and retry logic.

---

## 3. Checkpoint / Resume Corruption

**Symptom**: Script with `--resume` restarts from the beginning or crashes on checkpoint load.

**Root cause (fixed)**: Several issues were found:
- Checkpoint file written without `encoding="utf-8"` on Windows (cp1252 mangling dates).
- PMU scraper ignoring checkpoint date when it was before the default start date.
- 33 scrapers had a bug where `load_checkpoint()` returned wrong type.

**Fix applied**: All 33+ scrapers patched. Checkpoints now use the shared `utils.scraping.load_checkpoint()` / `save_checkpoint()` which handle encoding and validation.

**If a checkpoint is corrupted**: Delete the checkpoint file manually:
```bash
rm data/checkpoints/<script_name>_checkpoint.json
```
Then restart the script (it will begin from the default start date).

---

## 4. RAM / Memory Limits

**Symptom**: Script killed by OS or Python `MemoryError`. Typically on `mega_merge` or `master_feature_builder`.

**Constraints**: Max 3 heavy scripts at a time. Never exceed available RAM.

**Solutions applied**:
- `master_feature_builder.py` rewritten as streaming 2-pass (~3 GB RAM vs 50 GB).
- `mega_merge` uses `readline()` with 1 MB buffering instead of `json.load()`.
- `export_parquet_chunks.py` splits large files into manageable Parquet chunks.

**Best practices**:
- Monitor with `status_report.py` which shows current RAM usage.
- Process JSONL files line-by-line, never `json.load()` a multi-GB file.
- Use `ijson` for streaming JSON parsing when needed.

---

## 5. Encoding Errors (UTF-8 / Latin-1 / cp1252)

**Symptom**: `UnicodeDecodeError` or `UnicodeEncodeError`, especially on Windows.

**Root cause**: French text (accents: e, a, c) in horse names, hippodrome names, jockey names. Windows console defaults to cp1252.

**Fixes applied**:
- All `open()` calls now specify `encoding="utf-8"` explicitly (80+ files patched).
- `logging_setup.py` console handler uses UTF-8 with `errors="replace"`.
- `mega_merge` has latin-1 fallback for legacy PMU data files.

**Quick fix for new scripts**:
```python
# Always specify encoding
with open(path, encoding="utf-8") as f:
    ...

# For console output on Windows
import sys
sys.stdout.reconfigure(encoding="utf-8")
```

---

## 6. Hardcoded / Relative Paths

**Symptom**: `FileNotFoundError` when running scripts from a different directory, or paths pointing to Mac locations (`/Users/...`).

**Root cause**: Original scripts used relative paths like `"data/output.jsonl"` or Mac-specific absolute paths.

**Fix applied**: 100+ files patched to use:
```python
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "data" / "output"
```

**Prevention**: Always derive paths from `__file__`, never use hardcoded or relative paths. The shared `utils/` modules set paths correctly.

---

## 7. Silent Exception Swallowing

**Symptom**: Script runs but produces empty or incomplete output. No errors in console.

**Root cause**: Bare `except: pass` blocks hiding real errors (network failures, parse errors, missing fields).

**Fix applied**:
- 24 bare `except` clauses replaced with specific exception types.
- 50+ silent exception handlers now log the error at DEBUG or ERROR level.

**Best practice**: Always log exceptions:
```python
except (ValueError, KeyError) as e:
    logger.debug("Skipping record: %s", e)
```

---

## 8. PMU API Scraper Issues

**Symptom**: PMU scraper (`101_pmu_api_scraper.py`) crashes or produces corrupt data.

**Known issues (all fixed)**:
- Corrupted JSON cache files caused `json.JSONDecodeError` -- now catches `OSError` and removes corrupt file.
- Windows file locks when removing cache -- uses `os.replace()` instead of `os.remove()` + `os.rename()`.
- `--resume` ignored checkpoint date -- fixed date comparison logic.

**If PMU data looks wrong**: Run the scraper with `--resume` to pick up where it left off. Delete `data/checkpoints/pmu_api_checkpoint.json` to start fresh.

---

## 9. Windows File Locking

**Symptom**: `PermissionError: [WinError 32]` when trying to overwrite or delete a file.

**Root cause**: Another process (or the same script) still holds the file open. Common with cache files and JSONL appends.

**Fix applied**: Use context managers (`with open(...)`) everywhere. For cache replacement, use atomic `os.replace()`.

**Workaround**: Close all Python processes, then retry. Or use `handle.exe` from Sysinternals to find the locking process.

---

## 10. Deprecated `datetime.utcnow()`

**Symptom**: `DeprecationWarning: datetime.utcnow() is deprecated` (Python 3.12+).

**Fix applied**: Replaced across 60 files with:
```python
from datetime import datetime, timezone
datetime.now(timezone.utc)
```
The shared helper `utils.normalize.utc_now_iso()` returns a timezone-aware ISO string.

---

## 11. Missing Python Packages

**Symptom**: `ModuleNotFoundError` for cloudscraper, playwright, ijson, etc.

**Fix**: Install all dependencies:
```bash
pip install -r requirements.txt
```

For Playwright, also install browsers:
```bash
python -m playwright install chromium
```

---

## 12. Feature Builder Produces 0% Output

**Symptom**: A feature builder runs but outputs all nulls or empty values.

**Known causes (fixed)**:
- `42_croisement_racing_post_pmu.py` had wrong join key format.
- `49_ecart_cotes_internet_national.py` expected different data format.
- `44_croisement_pedigree_partants.py` had latin-1 encoding issue.

**Diagnostic**: Check the log file for the builder. Look for "0 records matched" or similar. Verify input file paths and field names match expected schema (see `docs/DATA_DICTIONARY.md`).

---

## Quick Diagnostic Commands

```bash
# Check pipeline health
python status_report.py

# Validate all data files
python validate_data_final.py

# Check data completeness
python data_completeness_report.py

# Audit scraper outputs
python scraper_results_audit.py

# Verify checksums
sha256sum -c CHECKSUMS.sha256
```
