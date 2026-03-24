#!/usr/bin/env python3
"""
Normalisation Unites Internationales -- Detect and report unit conversion needs.

Streams partants_master.jsonl with reservoir sampling (10K records),
detects values needing conversion (distances, weights, times, currencies),
and outputs a report to quality/normalisation_report.md.

Read-only: does NOT modify data files.
RAM usage kept under 2GB via streaming + fixed-size reservoir.
"""

import json
import random
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DATA_MASTER = BASE / "data_master"
PARTANTS = DATA_MASTER / "partants_master.jsonl"
QUALITY_DIR = BASE / "quality"
QUALITY_DIR.mkdir(exist_ok=True)
REPORT_PATH = QUALITY_DIR / "normalisation_report.md"

SAMPLE_SIZE = 10_000
RESERVOIR_SEED = 42

# ---------------------------------------------------------------------------
# Conversion constants
# ---------------------------------------------------------------------------
FURLONG_TO_M = 201.168
YARD_TO_M = 0.9144

LB_TO_KG = 0.4536
STONE_TO_KG = 6.35

# Approximate exchange rates to EUR (for flagging purposes)
CURRENCY_RATES_TO_EUR = {
    "GBP": 1.17,
    "USD": 0.92,
    "AUD": 0.60,
    "HKD": 0.12,
    "JPY": 0.0062,
    "KRW": 0.00069,
    "EUR": 1.0,
}

# Distance fields to inspect
DISTANCE_FIELDS = {"distance", "distance_course", "dist", "distance_raw"}

# Weight fields to inspect
WEIGHT_FIELDS = {"poids", "poids_jockey", "handicap_poids", "poids_raw", "weight"}

# Time fields to inspect
TIME_FIELDS = {"temps", "temps_course", "reduction_km", "temps_km", "time", "temps_raw"}

# Currency / monetary fields to inspect
MONEY_FIELDS = {
    "allocation", "allocation_totale", "dotation", "prix",
    "gain", "gains", "gains_carriere", "gain_annee",
    "gains_place", "gains_victoire",
}

# Currency indicator fields (where the currency code might live)
CURRENCY_INDICATOR_FIELDS = {"devise", "currency", "currency_code"}

# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------

# Pattern: number followed by 'f' or 'fur' (furlongs)
RE_FURLONG = re.compile(r"^(\d+(?:\.\d+)?)\s*(?:f|fur|furlongs?)$", re.IGNORECASE)
# Pattern: number followed by 'y' or 'yd' (yards)
RE_YARD = re.compile(r"^(\d+(?:\.\d+)?)\s*(?:y|yd|yards?)$", re.IGNORECASE)
# Pattern: number followed by 'mi' or 'miles'
RE_MILE = re.compile(r"^(\d+(?:\.\d+)?)\s*(?:mi|miles?)$", re.IGNORECASE)

# Pattern: number followed by 'lb' or 'lbs' (pounds)
RE_LB = re.compile(r"^(\d+(?:\.\d+)?)\s*(?:lbs?|pounds?)$", re.IGNORECASE)
# Pattern: number followed by 'st' (stones)
RE_STONE = re.compile(r"^(\d+(?:\.\d+)?)\s*(?:st|stones?)$", re.IGNORECASE)
# Stones-and-pounds: e.g. "9st 7lb"
RE_STONE_LB = re.compile(
    r"^(\d+)\s*(?:st|stones?)\s*(\d+)\s*(?:lbs?|pounds?)$", re.IGNORECASE
)

# Time in MM:SS.ss format
RE_TIME_MMSS = re.compile(r"^(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?$")
# Time in milliseconds (large integer, > 10000 implies ms)
THRESHOLD_MS = 10_000


def detect_distance_issue(field, value):
    """Return (issue_type, original, converted_m) or None."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = RE_FURLONG.match(s)
    if m:
        furlongs = float(m.group(1))
        return ("furlongs->m", s, round(furlongs * FURLONG_TO_M, 1))

    m = RE_YARD.match(s)
    if m:
        yards = float(m.group(1))
        return ("yards->m", s, round(yards * YARD_TO_M, 1))

    m = RE_MILE.match(s)
    if m:
        miles = float(m.group(1))
        return ("miles->m", s, round(miles * 1609.344, 1))

    # Numeric but suspiciously small (< 50) could be furlongs stored as plain number
    try:
        num = float(s)
        if 0 < num < 50:
            return ("possible_furlongs", s, round(num * FURLONG_TO_M, 1))
    except (ValueError, TypeError):
        pass

    return None


def detect_weight_issue(field, value):
    """Return (issue_type, original, converted_kg) or None."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = RE_STONE_LB.match(s)
    if m:
        stones = int(m.group(1))
        lbs = int(m.group(2))
        kg = round(stones * STONE_TO_KG + lbs * LB_TO_KG, 2)
        return ("st+lb->kg", s, kg)

    m = RE_STONE.match(s)
    if m:
        stones = float(m.group(1))
        return ("stones->kg", s, round(stones * STONE_TO_KG, 2))

    m = RE_LB.match(s)
    if m:
        lbs = float(m.group(1))
        return ("lbs->kg", s, round(lbs * LB_TO_KG, 2))

    # Numeric but > 100 could be pounds stored as plain number (horse weights rarely > 80kg)
    try:
        num = float(s)
        if num > 100:
            return ("possible_lbs", s, round(num * LB_TO_KG, 2))
    except (ValueError, TypeError):
        pass

    return None


def detect_time_issue(field, value):
    """Return (issue_type, original, converted_seconds) or None."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # MM:SS.ss format
    m = RE_TIME_MMSS.match(s)
    if m:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        frac = int(m.group(3)) if m.group(3) else 0
        # Normalize fraction: "5" -> 0.5, "50" -> 0.5, "500" -> 0.5
        if m.group(3):
            frac_val = frac / (10 ** len(m.group(3)))
        else:
            frac_val = 0.0
        total_s = minutes * 60 + seconds + frac_val
        return ("MM:SS.ss->s", s, round(total_s, 3))

    # Milliseconds (plain large number)
    try:
        num = float(s)
        if num > THRESHOLD_MS:
            return ("ms->s", s, round(num / 1000.0, 3))
    except (ValueError, TypeError):
        pass

    return None


def detect_currency_issue(rec, field, value):
    """Return (issue_type, original, converted_eur) or None."""
    if value is None:
        return None
    try:
        amount = float(value)
    except (ValueError, TypeError):
        return None

    if amount == 0:
        return None

    # Look for currency indicator in the record
    currency = None
    for cf in CURRENCY_INDICATOR_FIELDS:
        cv = rec.get(cf)
        if cv:
            currency = str(cv).strip().upper()
            break

    if currency and currency != "EUR" and currency in CURRENCY_RATES_TO_EUR:
        rate = CURRENCY_RATES_TO_EUR[currency]
        return (f"{currency}->EUR", f"{amount} {currency}", round(amount * rate, 2))

    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    partants_path = PARTANTS
    if not partants_path.exists():
        alt = BASE / "partants_master.jsonl"
        if alt.exists():
            partants_path = alt
        else:
            print(f"ERREUR: {partants_path} introuvable")
            sys.exit(1)

    print("=" * 70)
    print("NORMALISATION UNITES INTERNATIONALES -- Detection des conversions")
    print("=" * 70)
    print(f"Fichier : {partants_path}")
    print(f"Echantillon : {SAMPLE_SIZE:,} enregistrements (reservoir sampling)")
    print()

    # --- Phase 1: Reservoir sampling -------------------------------------
    reservoir = []
    random.seed(RESERVOIR_SEED)
    total_lines = 0
    t0 = time.time()
    last_report = t0

    with open(partants_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_lines += 1

            if total_lines <= SAMPLE_SIZE:
                reservoir.append(rec)
            else:
                j = random.randint(1, total_lines)
                if j <= SAMPLE_SIZE:
                    reservoir[j - 1] = rec

            now = time.time()
            if now - last_report > 60:
                elapsed = now - t0
                rate = total_lines / elapsed
                print(f"  [{int(elapsed)}s] {total_lines:,} lignes lues, {rate:,.0f} lignes/s")
                last_report = now

    elapsed = time.time() - t0
    sample_n = len(reservoir)
    print(f"Lecture terminee : {total_lines:,} lignes en {elapsed:.0f}s")
    print(f"Echantillon effectif : {sample_n:,} enregistrements")
    print()

    if sample_n == 0:
        print("ERREUR: Aucun enregistrement lu.")
        sys.exit(1)

    # --- Phase 2: Detect conversion needs --------------------------------
    # Structure: category -> issue_type -> {count, examples: [(original, converted)]}
    findings = {
        "distance": defaultdict(lambda: {"count": 0, "examples": []}),
        "weight": defaultdict(lambda: {"count": 0, "examples": []}),
        "time": defaultdict(lambda: {"count": 0, "examples": []}),
        "currency": defaultdict(lambda: {"count": 0, "examples": []}),
    }

    for rec in reservoir:
        # Distance checks
        for field in DISTANCE_FIELDS:
            val = rec.get(field)
            if val is not None:
                result = detect_distance_issue(field, val)
                if result:
                    issue_type, original, converted = result
                    findings["distance"][issue_type]["count"] += 1
                    if len(findings["distance"][issue_type]["examples"]) < 5:
                        findings["distance"][issue_type]["examples"].append(
                            (field, original, f"{converted} m")
                        )

        # Weight checks
        for field in WEIGHT_FIELDS:
            val = rec.get(field)
            if val is not None:
                result = detect_weight_issue(field, val)
                if result:
                    issue_type, original, converted = result
                    findings["weight"][issue_type]["count"] += 1
                    if len(findings["weight"][issue_type]["examples"]) < 5:
                        findings["weight"][issue_type]["examples"].append(
                            (field, original, f"{converted} kg")
                        )

        # Time checks
        for field in TIME_FIELDS:
            val = rec.get(field)
            if val is not None:
                result = detect_time_issue(field, val)
                if result:
                    issue_type, original, converted = result
                    findings["time"][issue_type]["count"] += 1
                    if len(findings["time"][issue_type]["examples"]) < 5:
                        findings["time"][issue_type]["examples"].append(
                            (field, original, f"{converted} s")
                        )

        # Currency checks
        for field in MONEY_FIELDS:
            val = rec.get(field)
            if val is not None:
                result = detect_currency_issue(rec, field, val)
                if result:
                    issue_type, original, converted = result
                    findings["currency"][issue_type]["count"] += 1
                    if len(findings["currency"][issue_type]["examples"]) < 5:
                        findings["currency"][issue_type]["examples"].append(
                            (field, original, f"{converted} EUR")
                        )

    # --- Phase 3: Console report -----------------------------------------
    categories = [
        ("distance", "Distances (cible: metres)"),
        ("weight", "Poids (cible: kg)"),
        ("time", "Temps (cible: secondes)"),
        ("currency", "Devises (cible: EUR)"),
    ]

    total_issues = 0
    for cat_key, cat_label in categories:
        cat_findings = findings[cat_key]
        if not cat_findings:
            print(f"  PASS  {cat_label} : aucune conversion detectee")
            continue
        cat_total = sum(v["count"] for v in cat_findings.values())
        total_issues += cat_total
        print(f"  WARN  {cat_label} : {cat_total:,} valeurs a convertir")
        for issue_type, info in sorted(cat_findings.items()):
            ex = info["examples"][0] if info["examples"] else ("", "", "")
            print(f"        {issue_type}: {info['count']:,}x  ex: {ex[0]}={ex[1]} -> {ex[2]}")

    print()
    print(f"Total valeurs necessitant conversion : {total_issues:,} / {sample_n:,}")

    # --- Phase 4: Markdown report ----------------------------------------
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    md_lines = [
        "# Rapport de Normalisation des Unites Internationales",
        "",
        f"> Genere le {now_str}",
        f"> Fichier source : `{partants_path.name}`",
        f"> Echantillon : {sample_n:,} / {total_lines:,} enregistrements",
        "",
        "## Resume",
        "",
        f"| Categorie | Valeurs a convertir | % echantillon |",
        f"|---|---:|---:|",
    ]

    for cat_key, cat_label in categories:
        cat_findings = findings[cat_key]
        cat_total = sum(v["count"] for v in cat_findings.values())
        pct = 100.0 * cat_total / sample_n if sample_n else 0
        md_lines.append(f"| {cat_label} | {cat_total:,} | {pct:.2f}% |")

    md_lines.append(f"| **Total** | **{total_issues:,}** | **{100.0 * total_issues / sample_n if sample_n else 0:.2f}%** |")
    md_lines.append("")

    # Detailed sections
    for cat_key, cat_label in categories:
        cat_findings = findings[cat_key]
        md_lines.append(f"## {cat_label}")
        md_lines.append("")

        if not cat_findings:
            md_lines.append("Aucune conversion necessaire.")
            md_lines.append("")
            continue

        md_lines.append("| Type | Occurrences | Champ | Exemple avant | Exemple apres |")
        md_lines.append("|---|---:|---|---|---|")

        for issue_type, info in sorted(cat_findings.items()):
            for ex in info["examples"][:3]:
                field_name, original, converted = ex
                md_lines.append(
                    f"| {issue_type} | {info['count']:,} | `{field_name}` | {original} | {converted} |"
                )

        md_lines.append("")

    # Conversion reference
    md_lines.extend([
        "## Reference des conversions",
        "",
        "| Unite source | Unite cible | Facteur |",
        "|---|---|---|",
        "| 1 furlong | metres | 201.168 |",
        "| 1 yard | metres | 0.9144 |",
        "| 1 mile | metres | 1609.344 |",
        "| 1 lb | kg | 0.4536 |",
        "| 1 stone | kg | 6.35 |",
        "| MM:SS.ss | secondes | minutes*60 + secondes |",
        "| millisecondes | secondes | /1000 |",
    ])

    for code, rate in sorted(CURRENCY_RATES_TO_EUR.items()):
        if code != "EUR":
            md_lines.append(f"| 1 {code} | EUR | {rate} |")

    md_lines.append("")

    report_text = "\n".join(md_lines)
    REPORT_PATH.write_text(report_text, encoding="utf-8")
    print(f"\nRapport ecrit : {REPORT_PATH}")

    return 0 if total_issues == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
