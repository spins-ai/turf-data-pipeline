#!/usr/bin/env python3
"""
Sanity Checks Metier -- Domain-specific sanity checks for horse racing data.

Streams partants_master.jsonl with reservoir sampling (10K records),
then reports violations per check with counts and percentages.
RAM usage kept under 2GB via streaming + fixed-size reservoir.
"""

import json
import os
import random
import sys
import time
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DATA_MASTER = BASE / "data_master"
PARTANTS = DATA_MASTER / "partants_master.jsonl"

SAMPLE_SIZE = 10_000
RESERVOIR_SEED = 42

# Known racing disciplines
KNOWN_DISCIPLINES = {
    "trot_attele",
    "trot_monte",
    "plat",
    "haie",
    "steeple",
    "cross_country",
}


# ---------------------------------------------------------------------------
# Individual checks -- each returns a short violation string or None
# ---------------------------------------------------------------------------

def check_nb_partants(rec):
    """nb_partants should be between 3 and 20 for most races."""
    val = rec.get("nb_partants")
    if val is None:
        return None
    try:
        n = int(val)
    except (ValueError, TypeError):
        return f"non-numeric nb_partants={val!r}"
    if n < 3 or n > 20:
        return f"nb_partants={n} hors [3,20]"
    return None


def check_distance(rec):
    """distance should be between 800m and 10000m."""
    val = rec.get("distance") or rec.get("distance_course") or rec.get("dist")
    if val is None:
        return None
    try:
        d = float(val)
    except (ValueError, TypeError):
        return f"non-numeric distance={val!r}"
    if d < 800 or d > 10000:
        return f"distance={d} hors [800,10000]"
    return None


def check_allocation(rec):
    """allocation_totale must be > 0 when present."""
    val = rec.get("allocation_totale") or rec.get("allocation") or rec.get("dotation")
    if val is None:
        return None
    try:
        a = float(val)
    except (ValueError, TypeError):
        return f"non-numeric allocation={val!r}"
    if a <= 0:
        return f"allocation={a} <= 0"
    return None


def check_position_vs_partants(rec):
    """position_arrivee <= nb_partants when both exist."""
    pos = rec.get("position_arrivee") or rec.get("place") or rec.get("rang")
    nb = rec.get("nb_partants")
    if pos is None or nb is None:
        return None
    try:
        p = int(pos)
        n = int(nb)
    except (ValueError, TypeError):
        return None
    if p > n:
        return f"position={p} > nb_partants={n}"
    return None


def check_cote_finale(rec):
    """cote_finale > 1.0 (odds cannot be less than evens)."""
    val = rec.get("cote_finale") or rec.get("cote") or rec.get("cote_depart")
    if val is None:
        return None
    try:
        c = float(val)
    except (ValueError, TypeError):
        return f"non-numeric cote={val!r}"
    if c < 1.0:
        return f"cote={c} < 1.0"
    return None


def check_age(rec):
    """age should be between 2 and 15 for most horses."""
    val = rec.get("age")
    if val is None:
        return None
    try:
        a = int(val)
    except (ValueError, TypeError):
        return f"non-numeric age={val!r}"
    if a < 2 or a > 15:
        return f"age={a} hors [2,15]"
    return None


def check_gains(rec):
    """gains must be positive when present."""
    for field in ("gain", "gains", "gains_carriere", "gain_annee"):
        val = rec.get(field)
        if val is None:
            continue
        try:
            g = float(val)
        except (ValueError, TypeError):
            continue
        if g < 0:
            return f"{field}={g} < 0"
    return None


def check_discipline(rec):
    """discipline should be in the known list."""
    val = rec.get("discipline")
    if val is None or val == "":
        return None
    val_lower = str(val).strip().lower()
    if val_lower not in KNOWN_DISCIPLINES:
        return f"discipline inconnue: {val!r}"
    return None


# Ordered list of (check_name, check_function)
CHECKS = [
    ("nb_partants [3,20]", check_nb_partants),
    ("distance [800,10000]", check_distance),
    ("allocation > 0", check_allocation),
    ("position <= nb_partants", check_position_vs_partants),
    ("cote >= 1.0", check_cote_finale),
    ("age [2,15]", check_age),
    ("gains >= 0", check_gains),
    ("discipline connue", check_discipline),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    partants_path = PARTANTS
    if not partants_path.exists():
        # Try fallback locations
        alt = BASE / "partants_master.jsonl"
        if alt.exists():
            partants_path = alt
        else:
            print(f"ERREUR: {partants_path} introuvable")
            sys.exit(1)

    print("=" * 70)
    print("SANITY CHECKS METIER -- Controles domaine courses hippiques")
    print("=" * 70)
    print(f"Fichier : {partants_path}")
    print(f"Echantillon : {SAMPLE_SIZE:,} enregistrements (reservoir sampling)")
    print()

    # --- Phase 1: Reservoir sampling (single pass, O(1) memory) -----------
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

            # Progress every 60s
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

    # --- Phase 2: Run checks on sampled records --------------------------
    violations = defaultdict(lambda: {"count": 0, "examples": []})

    for rec in reservoir:
        for check_name, check_fn in CHECKS:
            msg = check_fn(rec)
            if msg:
                violations[check_name]["count"] += 1
                if len(violations[check_name]["examples"]) < 5:
                    violations[check_name]["examples"].append(msg)

    # --- Phase 3: Report -------------------------------------------------
    print("-" * 70)
    print(f"{'Check':<30} {'Violations':>10} {'%':>8}  Exemples")
    print("-" * 70)

    total_violations = 0
    for check_name, _ in CHECKS:
        info = violations.get(check_name)
        if info:
            count = info["count"]
            pct = 100.0 * count / sample_n
            examples_str = "; ".join(info["examples"][:3])
            status = "WARN" if pct < 5.0 else "FAIL"
            print(f"  {status} {check_name:<27} {count:>7,} {pct:>7.2f}%  {examples_str}")
            total_violations += count
        else:
            print(f"  PASS {check_name:<27} {'0':>7} {'0.00':>7}%")

    print("-" * 70)
    print(f"Total violations : {total_violations:,} sur {sample_n:,} enregistrements")
    pct_total = 100.0 * total_violations / (sample_n * len(CHECKS))
    print(f"Taux global de conformite : {100.0 - pct_total:.2f}%")

    overall = "PASS" if total_violations == 0 else "WARN"
    print(f"\nResultat global : {overall}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
