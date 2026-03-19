# Data Quality Report

Generated: 2026-03-19 06:55:24
Data directory: `C:\Users\celia\Desktop\models hybride\backup_20260314`

## Summary

| Metric | Value |
|--------|-------|
| Total tests | 7 |
| Passed | 1 |
| Failed | 0 |
| Warnings | 0 |
| Other | 6 |
| Total time | 1810.3s |

**Overall: PASS**

## Test Results

| Test | Status | Time |
|------|--------|------|
| JSON/JSONL Integrity | ??? | 300.0s |
| Zero-Byte Files | PASS | 6.6s |
| Record Counts | ??? | 300.0s |
| Feature Quality | ??? | 301.6s |
| Date Validity | ??? | 300.3s |
| Value Ranges | ??? | 301.7s |
| Cross-Source Consistency | ??? | 300.0s |

## Details

### JSON/JSONL Integrity

**Status: UNKNOWN** | Time: 300.0s

Validates all JSON/JSONL files are parseable and not truncated


**Errors:**
```
TIMEOUT after 300s
```

### Zero-Byte Files

**Status: PASS** | Time: 6.6s

Finds all 0-byte files that indicate failed writes

```
=== Zero-Byte File Test ===
Scanning: C:\Users\celia\Desktop\models hybride\backup_20260314

  PASS  No 0-byte files found (662390 files scanned)

--- Summary ---
Total files scanned: 662390
Zero-byte files: 0

Overall: PASS
```

### Record Counts

**Status: UNKNOWN** | Time: 300.0s

Counts records per file and compares with expected minimums


**Errors:**
```
TIMEOUT after 300s
```

### Feature Quality

**Status: UNKNOWN** | Time: 301.6s

Checks for NaN/Inf in numeric features and high null rates


**Errors:**
```
TIMEOUT after 300s
```

### Date Validity

**Status: UNKNOWN** | Time: 300.3s

Validates date fields are ISO format and in range 2004-2026


**Errors:**
```
TIMEOUT after 300s
```

### Value Ranges

**Status: UNKNOWN** | Time: 301.7s

Checks cotes > 0, distances > 0, no invalid negative values


**Errors:**
```
TIMEOUT after 300s
```

### Cross-Source Consistency

**Status: UNKNOWN** | Time: 300.0s

Cross-validates records between PMU, Le Trot, and other sources


**Errors:**
```
TIMEOUT after 300s
```
