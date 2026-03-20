# Data Quality Report

Generated: 2026-03-20 05:24:33
Data directory: `C:\Users\celia\turf-data-pipeline\.claude\worktrees\naughty-bardeen\output`

## Summary

| Metric | Value |
|--------|-------|
| Total tests | 7 |
| Passed | 0 |
| Failed | 1 |
| Warnings | 0 |
| Other | 6 |
| Total time | 1824.5s |

**Overall: FAIL**

## Test Results

| Test | Status | Time |
|------|--------|------|
| JSON/JSONL Integrity | ??? | 300.0s |
| Zero-Byte Files | FAIL | 26.2s |
| Record Counts | ??? | 296.5s |
| Feature Quality | ??? | 300.4s |
| Date Validity | ??? | 300.9s |
| Value Ranges | ??? | 300.5s |
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

**Status: FAIL** | Time: 26.2s

Finds all 0-byte files that indicate failed writes

```
=== Zero-Byte File Test ===
Scanning: C:\Users\celia\turf-data-pipeline\.claude\worktrees\naughty-bardeen\output

  FAIL  Found 122 zero-byte file(s):

        - 28_combinaisons_marche\cache_corrupted\08062014_R11_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R11_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R11_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C2.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C4.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R12_C8.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C2.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C4.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R1_C8.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C2.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C4.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R2_C8.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C2.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C4.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R3_C8.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R4_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R4_C2.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R4_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R4_C4.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R4_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R4_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R4_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R5_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R5_C2.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R5_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R6_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R7_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R8_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R8_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062014_R9_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062015_R1_C1.json
        - 28_combinaisons_marche\cache_corrupted\08062015_R1_C2.json
        - 28_combinaisons_marche\cache_corrupted\08062015_R1_C3.json
        - 28_combinaisons_marche\cache_corrupted\08062015_R1_C4.json
        - 28_combinaisons_marche\cache_corrupted\08062015_R1_C5.json
... (73 more lines)
        - 28_combinaisons_marche\cache_corrupted\08062025_R1_C5.json
        - 28_combinaisons_marche\cache_corrupted\08062025_R1_C6.json
        - 28_combinaisons_marche\cache_corrupted\08062025_R1_C7.json
        - 28_combinaisons_marche\cache_corrupted\08062025_R1_C8.json

--- Summary ---
Total files scanned: 817719
Zero-byte files: 122

Overall: FAIL
```

### Record Counts

**Status: UNKNOWN** | Time: 296.5s

Counts records per file and compares with expected minimums

```
=== Record Count Test ===
Scanning: C:\Users\celia\turf-data-pipeline\.claude\worktrees\naughty-bardeen\output

  Status     Records    Expected  File
  ------     -------    --------  ----
  WARN             1         100  01_calendrier_reunions\.checkpoint_calendrier.json
  PASS          3583         100  01_calendrier_reunions\.checkpoint_patch_pmu.json
  WARN             3         100  01_calendrier_reunions\cartographie_variables.json
  WARN             1         100  01_calendrier_reunions\equidia_terrain_cache.json
  WARN             1         100  01_calendrier_reunions\meteo_cache.json
  WARN             1         100  01_calendrier_reunions\nasa_meteo_cache.json
  WARN             1         100  01_calendrier_reunions\pmu_condition_cache.json
  WARN             1         100  01_calendrier_reunions\rapport_qualite_reunions.json
  PASS         75329         100  01_calendrier_reunions\reunions_brut.json
  PASS         41477         100  01_calendrier_reunions\reunions_normalisees.json
  PASS         34187         100  01_calendrier_reunions\reunions_normalisees_meteo.json
  PASS         41477         100  01_calendrier_reunions\reunions_references_02.json
  PASS          7291         100  01_calendrier_reunions\reunions_references_02_2013_2016.json
  PASS             1         N/A  02b_liste_courses_2013\.checkpoint_02b.json
  PASS         29553         500  02b_liste_courses_2013\courses_brut.json
  PASS         49825         500  02b_liste_courses_2013\courses_normalisees.json
  PASS         49825         N/A  02b_liste_courses_2013\courses_references_04.json
  PASS        370614        1000  02b_liste_courses_2013\partants_brut.json
  PASS         89527        1000  02b_liste_courses_2013\partants_normalises.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-19_R1.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-19_R10.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-19_R4.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-19_R5.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-20_R3.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-20_R5.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-21_R1.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-21_R2.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-21_R3.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-21_R5.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-22_R1.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-22_R2.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-22_R3.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-22_R4.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-22_R6.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-22_R7.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-23_R1.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-23_R2.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-23_R3.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-23_R6.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R1.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R10.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R11.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R2.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R3.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R4.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R6.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R7.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-24_R9.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-25_R1.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-25_R3.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-25_R5.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-26_R2.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-26_R3.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-26_R4.json
  PASS             1         N/A  02b_liste_courses_2013\cache\2013-02-26_R5.json
... (40309 more lines)
  PASS             1         N/A  02_liste_courses\cache\2026-03-02_R6.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-02_R7.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-03_R1.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-03_R2.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-03_R3.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-03_R4.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-03_R5.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-03_R6.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-04_R1.json
  PASS             1         N/A  02_liste_courses\cache\2026-03-04_R2.json
```

### Feature Quality

**Status: UNKNOWN** | Time: 300.4s

Checks for NaN/Inf in numeric features and high null rates


**Errors:**
```
TIMEOUT after 300s
```

### Date Validity

**Status: UNKNOWN** | Time: 300.9s

Validates date fields are ISO format and in range 2004-2026


**Errors:**
```
TIMEOUT after 300s
```

### Value Ranges

**Status: UNKNOWN** | Time: 300.5s

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
