# Class Imbalance & Split Analysis Report

- **Total records**: 4,861,360
- **Average field size**: 12.1

## 1. Label Distribution

### Winners vs Losers

| Category | Count | % |
|---|---:|---:|
| Winners | 256,778 | 5.28% |
| Losers | 2,583,357 | 53.14% |
| Unknown | 2,021,225 | 41.58% |
| **Imbalance ratio** | **10.06:1** | |
| Expected win rate (1/N) | | 0.0827 |
| Actual win rate | | 0.0528 |

### Placed vs Not Placed

| Category | Count | % |
|---|---:|---:|
| Placed | 770,164 | 15.84% |
| Not placed | 2,069,971 | 42.58% |
| Unknown | 2,021,225 | 41.58% |
| **Imbalance ratio** | **2.69:1** | |

### DNF (Did Not Finish)

| Category | Count | % |
|---|---:|---:|
| DNF | 366,521 | 7.54% |
| Finished | 2,473,614 | 50.88% |
| Unknown | 2,021,225 | 41.58% |

### Position Buckets

- Records with position: 2,393,845
- Top 1: 256,778 (10.73%)
- Top 3: 770,164 (32.17%)
- Top 5: 1,279,273 (53.44%)

## 2. Recommended Temporal Split (70/15/15)

- Date range:  "cote_finale": null to 2026-03-12
- Unique dates: 8,417

| Split | Date cutoff | Records | % |
|---|---|---:|---:|
| Train | <= 2019-08-24 | 3,402,845 | 70.0% |
| Val | (2019-08-24, 2023-01-29] | 728,932 | 15.0% |
| Test | > 2023-01-29 | 729,583 | 15.0% |

```python
# Suggested usage in pipeline:
TRAIN_END = "2019-08-24"
VAL_END   = "2023-01-29"
```

## 3. Horse Overlap Between Train & Test (informational)

Skipped (use --check-horse-leakage to enable).

## 4. Recommended Class Weights

### is_winner

- Weight positive (1): 5.5303
- Weight negative (0): 0.5497
- Ratio: 10.06x
- `class_weight={0: 0.5497, 1: 5.5303}`

### is_place

- Weight positive (1): 1.8439
- Weight negative (0): 0.686
- Ratio: 2.69x
- `class_weight={0: 0.686, 1: 1.8439}`

### is_dnf

- Weight positive (1): 3.8745
- Weight negative (0): 0.5741
- Ratio: 6.75x

### Recommendations

- High win imbalance (10.06:1). Consider: focal loss, SMOTE on features, or stratified sampling.
- Use class_weight='balanced' in sklearn or equivalent.
- DNF rate is 7.5%. Consider a two-stage model: first predict DNF, then predict position among finishers.
