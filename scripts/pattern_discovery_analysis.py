#!/usr/bin/env python3
"""
Pattern discovery analysis on partants_master.jsonl.
Samples 5000 records, then also does targeted analyses on full dataset subsets.
"""
import json
import random
import sys
from collections import defaultdict
from datetime import datetime

DATA = "C:/Users/celia/turf-data-pipeline/.claude/worktrees/naughty-bardeen/data_master/partants_master.jsonl"

random.seed(42)

# ── Phase 1: reservoir-sample 5000 records ────────────────────────────
print("=== Sampling 5000 records (reservoir sampling) ===")
reservoir = []
n = 0
with open(DATA, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        n += 1
        rec = json.loads(line)
        if n <= 5000:
            reservoir.append(rec)
        else:
            j = random.randint(0, n - 1)
            if j < 5000:
                reservoir[j] = rec
        if n % 500000 == 0:
            print(f"  scanned {n:,} records...")

print(f"  Total records in file: {n:,}")
print(f"  Sample size: {len(reservoir)}")

# ── Identify available fields ──────────────────────────────────────────
print("\n=== Key fields availability ===")
fields_check = [
    "date_reunion_iso", "is_gagnant", "is_place", "position_arrivee",
    "cote_finale", "cote_reference", "proba_implicite",
    "jockey_driver", "entraineur", "nom_cheval", "horse_id",
    "hippodrome_normalise", "discipline", "distance",
    "age", "sexe", "temps_ms", "nombre_partants",
    "ecart_precedent", "heure_depart",
    "type_piste", "nb_courses_carriere", "nb_victoires_carriere",
]
for f in fields_check:
    non_null = sum(1 for r in reservoir if r.get(f) is not None and r.get(f) != "")
    print(f"  {f}: {non_null}/{len(reservoir)} ({100*non_null/len(reservoir):.1f}%)")

# ── Helper ─────────────────────────────────────────────────────────────
def safe_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def parse_hour(rec):
    """Extract decimal hour from heure_depart or date_reunion_iso."""
    h = rec.get("heure_depart")
    if h and isinstance(h, str) and ":" in h:
        parts = h.split(":")
        try:
            return int(parts[0]) + int(parts[1]) / 60
        except:
            return None
    # Try to extract from date_reunion_iso if it has time
    d = rec.get("date_reunion_iso", "")
    if isinstance(d, str) and "T" in d:
        parts = d.split("T")[1].split(":")
        try:
            return int(parts[0]) + int(parts[1]) / 60
        except:
            return None
    # Check temp_heure_course
    hc = rec.get("temp_heure_course")
    if hc is not None:
        try:
            return float(hc)
        except:
            pass
    return None

# =====================================================================
# 1. TIME-BASED PATTERNS
# =====================================================================
print("\n" + "=" * 70)
print("1. TIME-BASED PATTERNS")
print("=" * 70)

# 1a. Performance by time of day
print("\n--- 1a. Win rate by time of day ---")
time_buckets = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    h = parse_hour(r)
    if h is None:
        continue
    if h < 12:
        bucket = "morning (<12h)"
    elif h < 14:
        bucket = "early_afternoon (12-14h)"
    elif h < 16:
        bucket = "mid_afternoon (14-16h)"
    elif h < 18:
        bucket = "late_afternoon (16-18h)"
    else:
        bucket = "evening (18h+)"
    time_buckets[bucket]["total"] += 1
    if r.get("is_gagnant"):
        time_buckets[bucket]["wins"] += 1

for bucket in sorted(time_buckets.keys()):
    d = time_buckets[bucket]
    wr = d["wins"] / d["total"] * 100 if d["total"] > 0 else 0
    print(f"  {bucket}: {wr:.2f}% win rate (n={d['total']})")

# More granular: hourly
print("\n--- 1a-bis. Win rate by hour (granular) ---")
hour_buckets = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    h = parse_hour(r)
    if h is None:
        continue
    hh = int(h)
    hour_buckets[hh]["total"] += 1
    if r.get("is_gagnant"):
        hour_buckets[hh]["wins"] += 1

for hh in sorted(hour_buckets.keys()):
    d = hour_buckets[hh]
    wr = d["wins"] / d["total"] * 100 if d["total"] > 0 else 0
    print(f"  {hh:02d}h: {wr:.2f}% win rate (n={d['total']})")

# 1b. Jockey hot/cold streaks beyond 30 days
print("\n--- 1b. Jockey streak analysis (windows: 30/60/90/180 days) ---")
# Group by jockey, sort by date
jockey_races = defaultdict(list)
for r in reservoir:
    j = r.get("jockey_driver")
    d = r.get("date_reunion_iso")
    if j and d:
        jockey_races[j].append({
            "date": d[:10],
            "win": bool(r.get("is_gagnant")),
            "cote": safe_float(r.get("cote_finale")),
        })

# Analyze streak autocorrelation
print("  Jockeys with 3+ races in sample:", sum(1 for j, races in jockey_races.items() if len(races) >= 3))

# For each jockey, check if winning in window X predicts winning next
windows = [30, 60, 90, 180]
for w in windows:
    hot_next_win = 0
    hot_total = 0
    cold_next_win = 0
    cold_total = 0
    for j, races in jockey_races.items():
        sorted_races = sorted(races, key=lambda x: x["date"])
        for i in range(1, len(sorted_races)):
            cur_date = datetime.strptime(sorted_races[i]["date"], "%Y-%m-%d")
            window_races = [
                r for r in sorted_races[:i]
                if (cur_date - datetime.strptime(r["date"], "%Y-%m-%d")).days <= w
                and (cur_date - datetime.strptime(r["date"], "%Y-%m-%d")).days > 0
            ]
            if len(window_races) < 3:
                continue
            wr = sum(1 for r in window_races if r["win"]) / len(window_races)
            if wr > 0.15:  # "hot"
                hot_total += 1
                if sorted_races[i]["win"]:
                    hot_next_win += 1
            else:  # "cold"
                cold_total += 1
                if sorted_races[i]["win"]:
                    cold_next_win += 1
    hot_wr = hot_next_win / hot_total * 100 if hot_total > 0 else 0
    cold_wr = cold_next_win / cold_total * 100 if cold_total > 0 else 0
    print(f"  Window {w}d: HOT jockey next_win={hot_wr:.2f}% (n={hot_total}) | COLD next_win={cold_wr:.2f}% (n={cold_total}) | delta={hot_wr-cold_wr:.2f}pp")

# 1c. Return from break sweet spot
print("\n--- 1c. Return from break: optimal rest days ---")
# Parse ecart_precedent or compute from sequential data
rest_perf = defaultdict(lambda: {"wins": 0, "total": 0, "places": 0})
for r in reservoir:
    ecart = r.get("ecart_precedent")
    if ecart is None or ecart == "":
        # try jours_repos if already computed
        ecart = r.get("jours_repos")
    if ecart is None or ecart == "":
        continue
    try:
        days = int(float(ecart))
    except:
        continue
    if days <= 0 or days > 365:
        continue
    # 5-day buckets
    bucket = (days // 5) * 5
    rest_perf[bucket]["total"] += 1
    if r.get("is_gagnant"):
        rest_perf[bucket]["wins"] += 1
    if r.get("is_place"):
        rest_perf[bucket]["places"] += 1

print("  Rest days (5-day buckets) | Win% | Place% | n")
for bucket in sorted(rest_perf.keys()):
    d = rest_perf[bucket]
    if d["total"] < 5:
        continue
    wr = d["wins"] / d["total"] * 100
    pr = d["places"] / d["total"] * 100
    print(f"  {bucket:3d}-{bucket+4:3d}d: win={wr:.2f}% place={pr:.2f}% (n={d['total']})")

# Also try 7-day buckets for more granularity
print("\n  Finer: 7-day buckets with n >= 10")
rest_perf7 = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    ecart = r.get("ecart_precedent") or r.get("jours_repos")
    if ecart is None or ecart == "":
        continue
    try:
        days = int(float(ecart))
    except:
        continue
    if days <= 0 or days > 180:
        continue
    bucket = (days // 7) * 7
    rest_perf7[bucket]["total"] += 1
    if r.get("is_gagnant"):
        rest_perf7[bucket]["wins"] += 1

for bucket in sorted(rest_perf7.keys()):
    d = rest_perf7[bucket]
    if d["total"] < 10:
        continue
    wr = d["wins"] / d["total"] * 100
    print(f"  {bucket:3d}-{bucket+6:3d}d: win={wr:.2f}% (n={d['total']})")

# 1d. Trainer performance by month
print("\n--- 1d. Trainer monthly patterns ---")
month_perf = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    d = r.get("date_reunion_iso")
    if not d:
        continue
    try:
        month = int(d[5:7])
    except:
        continue
    month_perf[month]["total"] += 1
    if r.get("is_gagnant"):
        month_perf[month]["wins"] += 1

print("  Month | Win% | n")
for m in range(1, 13):
    d = month_perf[m]
    if d["total"] == 0:
        continue
    wr = d["wins"] / d["total"] * 100
    print(f"  {m:2d}: win={wr:.2f}% (n={d['total']})")

# Trainer x month interaction - top trainers
print("\n  Top-15 trainers: month with best/worst win rate")
trainer_month = defaultdict(lambda: defaultdict(lambda: {"wins": 0, "total": 0}))
trainer_total = defaultdict(int)
for r in reservoir:
    t = r.get("entraineur")
    d = r.get("date_reunion_iso")
    if not t or not d:
        continue
    try:
        month = int(d[5:7])
    except:
        continue
    trainer_month[t][month]["total"] += 1
    trainer_total[t] += 1
    if r.get("is_gagnant"):
        trainer_month[t][month]["wins"] += 1

top_trainers = sorted(trainer_total.items(), key=lambda x: -x[1])[:15]
for t, total in top_trainers:
    months = trainer_month[t]
    best_m, best_wr = None, -1
    worst_m, worst_wr = None, 999
    for m, d in months.items():
        if d["total"] < 2:
            continue
        wr = d["wins"] / d["total"]
        if wr > best_wr:
            best_wr = wr
            best_m = m
        if wr < worst_wr:
            worst_wr = wr
            worst_m = m
    if best_m:
        print(f"  {t[:30]:30s} (n={total}): best=month {best_m} ({best_wr*100:.1f}%), worst=month {worst_m} ({worst_wr*100:.1f}%)")


# =====================================================================
# 2. COMBINATION PATTERNS
# =====================================================================
print("\n" + "=" * 70)
print("2. COMBINATION PATTERNS")
print("=" * 70)

# 2a. jockey x distance x terrain triple combo
print("\n--- 2a. Jockey x Distance x Terrain triple combo ---")
def dist_cat(d):
    try:
        d = int(d)
    except:
        return None
    if d < 1400:
        return "sprint"
    elif d < 1800:
        return "mile"
    elif d < 2400:
        return "inter"
    else:
        return "staying"

jdt_combo = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    j = r.get("jockey_driver")
    dc = dist_cat(r.get("distance"))
    terrain = r.get("type_piste") or r.get("discipline")
    if not all([j, dc, terrain]):
        continue
    key = f"{j}|{dc}|{terrain}"
    jdt_combo[key]["total"] += 1
    if r.get("is_gagnant"):
        jdt_combo[key]["wins"] += 1

# Find combos with high win rates and sufficient sample
print("  Jockey x Distance x Terrain combos with 5+ races and >20% win rate:")
high_combos = []
for key, d in jdt_combo.items():
    if d["total"] >= 5:
        wr = d["wins"] / d["total"]
        if wr > 0.20:
            high_combos.append((key, wr, d["total"]))
high_combos.sort(key=lambda x: -x[1])
for key, wr, n in high_combos[:20]:
    parts = key.split("|")
    print(f"  {parts[0][:20]:20s} | {parts[1]:8s} | {parts[2]:15s} | win={wr*100:.1f}% (n={n})")
print(f"  Total combos with 5+ races: {sum(1 for k,d in jdt_combo.items() if d['total']>=5)}")

# 2b. trainer x hippodrome x discipline
print("\n--- 2b. Trainer x Hippodrome x Discipline triple combo ---")
thd_combo = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    t = r.get("entraineur")
    h = r.get("hippodrome_normalise")
    disc = r.get("discipline")
    if not all([t, h, disc]):
        continue
    key = f"{t}|{h}|{disc}"
    thd_combo[key]["total"] += 1
    if r.get("is_gagnant"):
        thd_combo[key]["wins"] += 1

print("  Trainer x Hippo x Discipline combos with 5+ races and >25% win rate:")
high_thd = []
for key, d in thd_combo.items():
    if d["total"] >= 5:
        wr = d["wins"] / d["total"]
        if wr > 0.25:
            high_thd.append((key, wr, d["total"]))
high_thd.sort(key=lambda x: -x[1])
for key, wr, n in high_thd[:20]:
    parts = key.split("|")
    print(f"  {parts[0][:20]:20s} | {parts[1]:15s} | {parts[2]:15s} | win={wr*100:.1f}% (n={n})")
print(f"  Total combos with 5+ races: {sum(1 for k,d in thd_combo.items() if d['total']>=5)}")

# 2c. age x sex x distance
print("\n--- 2c. Age x Sex x Distance category ---")
asd_combo = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    age = r.get("age")
    sex = r.get("sexe")
    dc = dist_cat(r.get("distance"))
    if age is None or not sex or not dc:
        continue
    key = f"{age}|{sex}|{dc}"
    asd_combo[key]["total"] += 1
    if r.get("is_gagnant"):
        asd_combo[key]["wins"] += 1

print("  Age x Sex x Distance combos with 20+ races (sorted by win rate):")
asd_items = [(k, d["wins"]/d["total"], d["total"]) for k, d in asd_combo.items() if d["total"] >= 20]
asd_items.sort(key=lambda x: -x[1])
for key, wr, n in asd_items[:25]:
    parts = key.split("|")
    print(f"  age={parts[0]:2s} sex={parts[1]:10s} dist={parts[2]:8s} | win={wr*100:.1f}% (n={n})")

# Overall baseline win rate
baseline_wins = sum(1 for r in reservoir if r.get("is_gagnant"))
baseline_total = len(reservoir)
print(f"\n  Baseline win rate: {baseline_wins/baseline_total*100:.2f}% (n={baseline_total})")

# 2d. Jockey-Horse specific combos
print("\n--- 2d. Jockey x Horse specific combos ---")
jh_combo = defaultdict(lambda: {"wins": 0, "total": 0, "cotes": []})
for r in reservoir:
    j = r.get("jockey_driver")
    h = r.get("nom_cheval")
    if not j or not h:
        continue
    key = f"{j}|{h}"
    jh_combo[key]["total"] += 1
    if r.get("is_gagnant"):
        jh_combo[key]["wins"] += 1
    c = safe_float(r.get("cote_finale"))
    if c:
        jh_combo[key]["cotes"].append(c)

print("  Jockey x Horse combos with 3+ races and >40% win rate:")
jh_items = []
for key, d in jh_combo.items():
    if d["total"] >= 3:
        wr = d["wins"] / d["total"]
        if wr > 0.40:
            avg_cote = sum(d["cotes"])/len(d["cotes"]) if d["cotes"] else 0
            jh_items.append((key, wr, d["total"], avg_cote))
jh_items.sort(key=lambda x: -x[1])
for key, wr, n, avg_cote in jh_items[:20]:
    parts = key.split("|")
    print(f"  {parts[0][:20]:20s} x {parts[1][:20]:20s} | win={wr*100:.1f}% avg_cote={avg_cote:.1f} (n={n})")
print(f"  Total J-H combos with 3+ races: {sum(1 for k,d in jh_combo.items() if d['total']>=3)}")


# =====================================================================
# 3. MARKET INEFFICIENCY PATTERNS
# =====================================================================
print("\n" + "=" * 70)
print("3. MARKET INEFFICIENCY PATTERNS")
print("=" * 70)

# 3a. Win rate by odds range
print("\n--- 3a. Win rate by odds range ---")
odds_buckets = defaultdict(lambda: {"wins": 0, "total": 0, "roi_sum": 0})
for r in reservoir:
    c = safe_float(r.get("cote_finale"))
    if c is None or c <= 0:
        continue
    if c < 2:
        bucket = "1.0-1.9"
    elif c < 3:
        bucket = "2.0-2.9"
    elif c < 5:
        bucket = "3.0-4.9"
    elif c < 8:
        bucket = "5.0-7.9"
    elif c < 12:
        bucket = "8.0-11.9"
    elif c < 20:
        bucket = "12.0-19.9"
    elif c < 35:
        bucket = "20.0-34.9"
    elif c < 60:
        bucket = "35.0-59.9"
    else:
        bucket = "60.0+"
    odds_buckets[bucket]["total"] += 1
    win = bool(r.get("is_gagnant"))
    if win:
        odds_buckets[bucket]["wins"] += 1
        odds_buckets[bucket]["roi_sum"] += c - 1
    else:
        odds_buckets[bucket]["roi_sum"] -= 1

print("  Odds Range  | Win%  | Expected | Diff   | ROI%    | n")
for bucket in ["1.0-1.9", "2.0-2.9", "3.0-4.9", "5.0-7.9", "8.0-11.9", "12.0-19.9", "20.0-34.9", "35.0-59.9", "60.0+"]:
    d = odds_buckets[bucket]
    if d["total"] == 0:
        continue
    actual_wr = d["wins"] / d["total"] * 100
    # Expected win rate from odds midpoint
    if bucket == "1.0-1.9":
        mid = 1.5
    elif bucket == "2.0-2.9":
        mid = 2.5
    elif bucket == "3.0-4.9":
        mid = 4.0
    elif bucket == "5.0-7.9":
        mid = 6.5
    elif bucket == "8.0-11.9":
        mid = 10.0
    elif bucket == "12.0-19.9":
        mid = 16.0
    elif bucket == "20.0-34.9":
        mid = 27.0
    elif bucket == "35.0-59.9":
        mid = 47.0
    else:
        mid = 80.0
    expected_wr = 100 / mid
    diff = actual_wr - expected_wr
    roi = d["roi_sum"] / d["total"] * 100
    print(f"  {bucket:12s} | {actual_wr:5.2f} | {expected_wr:5.2f}    | {diff:+6.2f} | {roi:+7.2f}% | n={d['total']}")

# 3b. Upset rate by hippodrome
print("\n--- 3b. Upset rate by hippodrome (fav not winning) ---")
# Group by course_uid to find favourite
course_runners = defaultdict(list)
for r in reservoir:
    cuid = r.get("course_uid")
    c = safe_float(r.get("cote_finale"))
    if cuid and c and c > 0:
        course_runners[cuid].append({
            "cote": c,
            "win": bool(r.get("is_gagnant")),
            "hippo": r.get("hippodrome_normalise"),
        })

hippo_upset = defaultdict(lambda: {"upsets": 0, "total": 0, "fav_wins": 0})
for cuid, runners in course_runners.items():
    if len(runners) < 3:
        continue
    fav = min(runners, key=lambda x: x["cote"])
    hippo = fav["hippo"]
    if not hippo:
        continue
    hippo_upset[hippo]["total"] += 1
    if fav["win"]:
        hippo_upset[hippo]["fav_wins"] += 1
    else:
        hippo_upset[hippo]["upsets"] += 1

print("  Hippodrome      | Upset%  | Fav_Win% | n_races")
hippo_items = [(h, d) for h, d in hippo_upset.items() if d["total"] >= 10]
hippo_items.sort(key=lambda x: -x[1]["upsets"]/x[1]["total"])
for h, d in hippo_items[:25]:
    upset_pct = d["upsets"] / d["total"] * 100
    fav_pct = d["fav_wins"] / d["total"] * 100
    print(f"  {h[:18]:18s} | {upset_pct:6.1f}% | {fav_pct:7.1f}% | n={d['total']}")

# Average across all hippodromes
all_upsets = sum(d["upsets"] for _, d in hippo_upset.items())
all_races = sum(d["total"] for _, d in hippo_upset.items())
if all_races > 0:
    print(f"\n  Overall favourite win rate: {(all_races-all_upsets)/all_races*100:.1f}% (upset rate: {all_upsets/all_races*100:.1f}%)")

# 3c. Odds movement analysis (smart money)
print("\n--- 3c. Smart money: odds shortening vs performance ---")
drift_perf = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    opening = safe_float(r.get("cote_reference"))
    final = safe_float(r.get("cote_finale"))
    if opening is None or final is None or opening <= 0:
        continue
    change_pct = (final - opening) / opening * 100
    if change_pct < -20:
        bucket = "big_steam (<-20%)"
    elif change_pct < -10:
        bucket = "steam (-20 to -10%)"
    elif change_pct < -3:
        bucket = "slight_steam (-10 to -3%)"
    elif change_pct <= 3:
        bucket = "stable (-3 to +3%)"
    elif change_pct <= 10:
        bucket = "slight_drift (+3 to +10%)"
    elif change_pct <= 20:
        bucket = "drift (+10 to +20%)"
    else:
        bucket = "big_drift (>+20%)"
    drift_perf[bucket]["total"] += 1
    if r.get("is_gagnant"):
        drift_perf[bucket]["wins"] += 1

print("  Odds Movement       | Win%   | n")
for bucket in ["big_steam (<-20%)", "steam (-20 to -10%)", "slight_steam (-10 to -3%)",
               "stable (-3 to +3%)", "slight_drift (+3 to +10%)", "drift (+10 to +20%)", "big_drift (>+20%)"]:
    d = drift_perf.get(bucket)
    if not d or d["total"] == 0:
        continue
    wr = d["wins"] / d["total"] * 100
    print(f"  {bucket:25s} | {wr:5.2f}% | n={d['total']}")

# 3d. Odds calibration: over/under-bet ranges
print("\n--- 3d. Market calibration: which odds ranges are least efficient? ---")
# Compute actual vs implied probability per odds decile
odds_cal = defaultdict(lambda: {"wins": 0, "total": 0, "implied_sum": 0})
for r in reservoir:
    c = safe_float(r.get("cote_finale"))
    if c is None or c <= 1:
        continue
    implied_prob = 1 / c
    # Use round odds as key
    if c < 5:
        bucket_key = round(c * 2) / 2  # 0.5 steps
    elif c < 20:
        bucket_key = round(c)
    else:
        bucket_key = round(c / 5) * 5  # 5-step
    odds_cal[bucket_key]["total"] += 1
    odds_cal[bucket_key]["implied_sum"] += implied_prob
    if r.get("is_gagnant"):
        odds_cal[bucket_key]["wins"] += 1

print("  Odds | Actual% | Implied% | Edge (actual-implied) | n")
for bucket_key in sorted(odds_cal.keys()):
    d = odds_cal[bucket_key]
    if d["total"] < 20:
        continue
    actual = d["wins"] / d["total"] * 100
    implied = d["implied_sum"] / d["total"] * 100
    edge = actual - implied
    print(f"  {bucket_key:5.1f} | {actual:6.2f} | {implied:7.2f}  | {edge:+6.2f}pp              | n={d['total']}")

# =====================================================================
# 4. ADDITIONAL UNDISCOVERED PATTERNS
# =====================================================================
print("\n" + "=" * 70)
print("4. ADDITIONAL PATTERN SIGNALS")
print("=" * 70)

# 4a. Day of week effect
print("\n--- 4a. Day of week win rate ---")
dow_perf = defaultdict(lambda: {"wins": 0, "total": 0})
dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
for r in reservoir:
    d = r.get("date_reunion_iso")
    if not d:
        continue
    try:
        dt = datetime.strptime(d[:10], "%Y-%m-%d")
        dow = dt.weekday()
    except:
        continue
    dow_perf[dow]["total"] += 1
    if r.get("is_gagnant"):
        dow_perf[dow]["wins"] += 1

for dow in range(7):
    d = dow_perf[dow]
    if d["total"] == 0:
        continue
    wr = d["wins"] / d["total"] * 100
    print(f"  {dow_names[dow]}: win={wr:.2f}% (n={d['total']})")

# 4b. Field size interaction with odds
print("\n--- 4b. Field size x Favouritism interaction ---")
field_fav = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    np = r.get("nombre_partants")
    c = safe_float(r.get("cote_finale"))
    if np is None or c is None or c <= 0:
        continue
    if np < 8:
        fs = "small (<8)"
    elif np < 12:
        fs = "medium (8-11)"
    elif np < 16:
        fs = "large (12-15)"
    else:
        fs = "xlarge (16+)"
    if c < 3:
        fav = "fav (<3)"
    elif c < 8:
        fav = "mid (3-8)"
    else:
        fav = "long (8+)"
    key = f"{fs}|{fav}"
    field_fav[key]["total"] += 1
    if r.get("is_gagnant"):
        field_fav[key]["wins"] += 1

print("  Field Size     x Odds Range  | Win%   | n")
for fs in ["small (<8)", "medium (8-11)", "large (12-15)", "xlarge (16+)"]:
    for fav in ["fav (<3)", "mid (3-8)", "long (8+)"]:
        key = f"{fs}|{fav}"
        d = field_fav.get(key)
        if not d or d["total"] == 0:
            continue
        wr = d["wins"] / d["total"] * 100
        print(f"  {fs:15s} x {fav:10s} | {wr:5.2f}% | n={d['total']}")

# 4c. Career experience sweet spot
print("\n--- 4c. Career experience (nb_courses_carriere) vs win rate ---")
exp_perf = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    nc = r.get("nb_courses_carriere")
    if nc is None:
        continue
    try:
        nc = int(nc)
    except:
        continue
    if nc < 5:
        bucket = "0-4"
    elif nc < 10:
        bucket = "5-9"
    elif nc < 20:
        bucket = "10-19"
    elif nc < 40:
        bucket = "20-39"
    elif nc < 70:
        bucket = "40-69"
    elif nc < 100:
        bucket = "70-99"
    else:
        bucket = "100+"
    exp_perf[bucket]["total"] += 1
    if r.get("is_gagnant"):
        exp_perf[bucket]["wins"] += 1

for bucket in ["0-4", "5-9", "10-19", "20-39", "40-69", "70-99", "100+"]:
    d = exp_perf.get(bucket)
    if not d or d["total"] == 0:
        continue
    wr = d["wins"] / d["total"] * 100
    print(f"  {bucket:6s} races: win={wr:.2f}% (n={d['total']})")

# 4d. Discipline x time of day
print("\n--- 4d. Discipline x Time of day ---")
disc_time = defaultdict(lambda: {"wins": 0, "total": 0})
for r in reservoir:
    disc = r.get("discipline")
    h = parse_hour(r)
    if not disc or h is None:
        continue
    if h < 14:
        tslot = "before_14h"
    elif h < 17:
        tslot = "14h-17h"
    else:
        tslot = "after_17h"
    key = f"{disc}|{tslot}"
    disc_time[key]["total"] += 1
    if r.get("is_gagnant"):
        disc_time[key]["wins"] += 1

print("  Discipline          x Time     | Win%   | n")
for key in sorted(disc_time.keys()):
    d = disc_time[key]
    if d["total"] < 20:
        continue
    wr = d["wins"] / d["total"] * 100
    parts = key.split("|")
    print(f"  {parts[0]:22s} x {parts[1]:10s} | {wr:5.2f}% | n={d['total']}")

print("\n=== ANALYSIS COMPLETE ===")
