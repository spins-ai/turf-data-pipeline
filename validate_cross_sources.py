#!/usr/bin/env python3
"""
validate_cross_sources.py — Étape 11.3
=======================================

Validation croisée entre sources pour détecter les incohérences.

Validations effectuées :
  1. PMU results = Le Trot results  (même classement pour la même course)
  2. PMU odds ≈ Exchange odds       (cotes cohérentes à ±30%)
  3. Pedigree consistent            (père/mère identiques entre sources)
  4. Dates cohérentes               (pas de course dans le futur, pas de doublons)
  5. Nombre de partants cohérent    (entre sources)

Produit un rapport détaillé des anomalies trouvées.

Usage:
    python validate_cross_sources.py
"""

import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"
OUTPUT_DIR = BASE_DIR / "output"
DOCS_DIR = BASE_DIR / "docs"

PARTANTS_PATH = DATA_MASTER / "partants_master.jsonl"
PEDIGREE_PATH = DATA_MASTER / "pedigree_master.json"
RAPPORTS_PATH = DATA_MASTER / "rapports_master.json"
MARCHE_PATH = DATA_MASTER / "marche_master.json"

REPORT_PATH = DOCS_DIR / "VALIDATION_CROISEE.md"


# -----------------------------------------------------------------------
# 1. Résultats PMU vs Le Trot
# -----------------------------------------------------------------------

def validate_results_pmu_vs_letrot() -> dict:
    """Compare les classements entre sources PMU et Le Trot."""
    print("\n--- Validation 1: Résultats PMU vs Le Trot ---")
    t0 = time.time()

    # Collecter les résultats par source
    pmu_results = {}   # {course_uid|num_pmu: position}
    letrot_results = {}

    if not PARTANTS_PATH.exists():
        return {"status": "SKIP", "reason": "partants_master.jsonl introuvable"}

    with open(PARTANTS_PATH, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            uid = rec.get("course_uid", "")
            num = rec.get("num_pmu")
            pos = rec.get("position_arrivee")
            sources = rec.get("mch__sources") or rec.get("pgr__sources") or []
            source_field = rec.get("source", "")

            if not uid or num is None or pos is None:
                continue
            if not isinstance(pos, (int, float)) or pos <= 0:
                continue

            key = f"{uid}|{num}"

            # Déterminer la source
            if source_field == "pmu" or "pmu" in str(sources).lower():
                pmu_results[key] = int(pos)
            if "letrot" in str(sources).lower() or "02b" in str(sources).lower():
                letrot_results[key] = int(pos)

    # Comparer
    common_keys = set(pmu_results.keys()) & set(letrot_results.keys())
    mismatches = []
    for key in common_keys:
        if pmu_results[key] != letrot_results[key]:
            mismatches.append({
                "key": key,
                "pmu_pos": pmu_results[key],
                "letrot_pos": letrot_results[key],
            })

    total_compared = len(common_keys)
    n_mismatches = len(mismatches)
    pct_ok = ((total_compared - n_mismatches) / total_compared * 100) if total_compared > 0 else 0

    result = {
        "status": "OK" if pct_ok >= 95 else "WARN" if pct_ok >= 80 else "FAIL",
        "pmu_count": len(pmu_results),
        "letrot_count": len(letrot_results),
        "common": total_compared,
        "mismatches": n_mismatches,
        "match_rate": round(pct_ok, 2),
        "examples": mismatches[:10],
        "elapsed": round(time.time() - t0, 1),
    }

    print(f"  PMU: {len(pmu_results):,}, Le Trot: {len(letrot_results):,}, "
          f"Communs: {total_compared:,}, Désaccords: {n_mismatches:,} "
          f"({pct_ok:.1f}% OK) [{result['elapsed']}s]")
    return result


# -----------------------------------------------------------------------
# 2. PMU odds vs Exchange odds
# -----------------------------------------------------------------------

def validate_odds_pmu_vs_exchange() -> dict:
    """Compare les cotes PMU (pari-mutuel) aux cotes exchange (Smarkets etc.)."""
    print("\n--- Validation 2: Cotes PMU vs Exchange ---")
    t0 = time.time()

    if not MARCHE_PATH.exists():
        return {"status": "SKIP", "reason": "marche_master.json introuvable"}

    try:
        with open(MARCHE_PATH, "r", encoding="utf-8", errors="replace") as f:
            marche_data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"status": "SKIP", "reason": "Erreur lecture marche_master.json"}

    pmu_odds = {}     # {course_uid|num: cote}
    exchange_odds = {}

    for rec in marche_data:
        uid = rec.get("course_uid", "")
        num = rec.get("num_pmu") or rec.get("numero")
        if not uid or num is None:
            continue
        key = f"{uid}|{num}"

        cote_pmu = rec.get("cote_pmu") or rec.get("cote_finale")
        cote_exchange = rec.get("cote_exchange") or rec.get("cote_smarkets") or rec.get("cote_betfair")

        if cote_pmu is not None:
            try:
                pmu_odds[key] = float(cote_pmu)
            except (ValueError, TypeError):
                pass

        if cote_exchange is not None:
            try:
                exchange_odds[key] = float(cote_exchange)
            except (ValueError, TypeError):
                pass

    # Comparer
    common_keys = set(pmu_odds.keys()) & set(exchange_odds.keys())
    outliers = []
    diffs = []

    for key in common_keys:
        pmu_c = pmu_odds[key]
        exch_c = exchange_odds[key]
        if pmu_c <= 0 or exch_c <= 0:
            continue

        ratio = abs(pmu_c - exch_c) / max(pmu_c, exch_c)
        diffs.append(ratio)

        if ratio > 0.30:  # >30% d'écart
            outliers.append({
                "key": key,
                "pmu": round(pmu_c, 2),
                "exchange": round(exch_c, 2),
                "ecart_pct": round(ratio * 100, 1),
            })

    avg_diff = sum(diffs) / len(diffs) * 100 if diffs else 0
    n_outliers = len(outliers)
    pct_ok = ((len(diffs) - n_outliers) / len(diffs) * 100) if diffs else 0

    result = {
        "status": "OK" if pct_ok >= 80 else "WARN" if pct_ok >= 60 else "FAIL",
        "pmu_count": len(pmu_odds),
        "exchange_count": len(exchange_odds),
        "common": len(common_keys),
        "compared": len(diffs),
        "avg_diff_pct": round(avg_diff, 2),
        "outliers_30pct": n_outliers,
        "match_rate": round(pct_ok, 2),
        "examples": sorted(outliers, key=lambda x: -x["ecart_pct"])[:10],
        "elapsed": round(time.time() - t0, 1),
    }

    print(f"  PMU: {len(pmu_odds):,}, Exchange: {len(exchange_odds):,}, "
          f"Communs: {len(common_keys):,}, Écart moyen: {avg_diff:.1f}%, "
          f"Outliers >30%: {n_outliers:,} [{result['elapsed']}s]")
    return result


# -----------------------------------------------------------------------
# 3. Pedigree consistent between sources
# -----------------------------------------------------------------------

def validate_pedigree_consistency() -> dict:
    """Vérifie que père/mère sont identiques entre pedigree_master et partants_master."""
    print("\n--- Validation 3: Cohérence pedigree ---")
    t0 = time.time()

    # Charger pedigree_master
    ped_index = {}  # {nom_upper: {pere, mere}}
    if PEDIGREE_PATH.exists():
        try:
            with open(PEDIGREE_PATH, "r", encoding="utf-8", errors="replace") as f:
                ped_data = json.load(f)
            for rec in ped_data:
                nom = (rec.get("nom") or "").strip().upper()
                if not nom:
                    continue
                pere = (rec.get("pere") or "").strip().upper()
                mere = (rec.get("mere") or "").strip().upper()
                if pere or mere:
                    ped_index[nom] = {"pere": pere, "mere": mere}
        except (json.JSONDecodeError, OSError):
            pass

    if not ped_index:
        return {"status": "SKIP", "reason": "pedigree_master vide ou introuvable"}

    # Scanner partants et comparer
    mismatches_pere = []
    mismatches_mere = []
    checked = 0

    if not PARTANTS_PATH.exists():
        return {"status": "SKIP", "reason": "partants_master.jsonl introuvable"}

    seen_chevaux = set()

    with open(PARTANTS_PATH, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            nom = (rec.get("nom_cheval") or "").strip().upper()
            if not nom or nom in seen_chevaux:
                continue
            seen_chevaux.add(nom)

            if nom not in ped_index:
                continue

            pere_partant = (rec.get("pere") or "").strip().upper()
            mere_partant = (rec.get("mere") or "").strip().upper()
            pere_ped = ped_index[nom]["pere"]
            mere_ped = ped_index[nom]["mere"]

            if not pere_partant and not mere_partant:
                continue

            checked += 1

            if pere_partant and pere_ped and pere_partant != pere_ped:
                mismatches_pere.append({
                    "cheval": nom,
                    "pere_partants": pere_partant,
                    "pere_pedigree": pere_ped,
                })

            if mere_partant and mere_ped and mere_partant != mere_ped:
                mismatches_mere.append({
                    "cheval": nom,
                    "mere_partants": mere_partant,
                    "mere_pedigree": mere_ped,
                })

    total_mismatches = len(mismatches_pere) + len(mismatches_mere)
    pct_ok = ((checked * 2 - total_mismatches) / (checked * 2) * 100) if checked > 0 else 0

    result = {
        "status": "OK" if pct_ok >= 95 else "WARN" if pct_ok >= 85 else "FAIL",
        "pedigree_count": len(ped_index),
        "checked": checked,
        "mismatches_pere": len(mismatches_pere),
        "mismatches_mere": len(mismatches_mere),
        "match_rate": round(pct_ok, 2),
        "examples_pere": mismatches_pere[:5],
        "examples_mere": mismatches_mere[:5],
        "elapsed": round(time.time() - t0, 1),
    }

    print(f"  Pedigree: {len(ped_index):,} chevaux, Comparés: {checked:,}, "
          f"Désaccords père: {len(mismatches_pere):,}, "
          f"Désaccords mère: {len(mismatches_mere):,} [{result['elapsed']}s]")
    return result


# -----------------------------------------------------------------------
# 4. Validation des dates
# -----------------------------------------------------------------------

def validate_dates() -> dict:
    """Vérifie la cohérence des dates (pas de futur, pas d'aberrations)."""
    print("\n--- Validation 4: Cohérence des dates ---")
    t0 = time.time()

    from datetime import date as dt_date

    today = dt_date.today().isoformat()
    future_dates = []
    invalid_dates = []
    date_counts = defaultdict(int)

    if not PARTANTS_PATH.exists():
        return {"status": "SKIP", "reason": "partants_master.jsonl introuvable"}

    with open(PARTANTS_PATH, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            date_iso = rec.get("date_reunion_iso", "")
            if not date_iso:
                continue

            try:
                d = dt_date.fromisoformat(date_iso)
                date_counts[date_iso] += 1

                if date_iso > today:
                    future_dates.append(date_iso)
                elif d.year < 2000:
                    invalid_dates.append(date_iso)
            except (ValueError, TypeError):
                invalid_dates.append(date_iso)

    # Détecter les jours avec un nombre anormal de partants
    avg_per_day = sum(date_counts.values()) / len(date_counts) if date_counts else 0
    abnormal_days = []
    for d, count in date_counts.items():
        if count > avg_per_day * 5:
            abnormal_days.append({"date": d, "count": count, "ratio_vs_avg": round(count / avg_per_day, 1)})

    result = {
        "status": "OK" if not future_dates and not invalid_dates else "WARN",
        "total_dates": len(date_counts),
        "future_dates": len(set(future_dates)),
        "invalid_dates": len(set(invalid_dates)),
        "abnormal_days": len(abnormal_days),
        "avg_partants_per_day": round(avg_per_day, 1),
        "examples_future": list(set(future_dates))[:5],
        "examples_invalid": list(set(invalid_dates))[:5],
        "examples_abnormal": sorted(abnormal_days, key=lambda x: -x["count"])[:5],
        "elapsed": round(time.time() - t0, 1),
    }

    print(f"  {len(date_counts):,} jours, Futures: {len(set(future_dates))}, "
          f"Invalides: {len(set(invalid_dates))}, "
          f"Anormaux: {len(abnormal_days)} [{result['elapsed']}s]")
    return result


# -----------------------------------------------------------------------
# 5. Nombre de partants cohérent
# -----------------------------------------------------------------------

def validate_nombre_partants() -> dict:
    """Vérifie que le champ nombre_partants correspond au décompte réel."""
    print("\n--- Validation 5: Nombre de partants ---")
    t0 = time.time()

    course_partant_count = defaultdict(int)
    course_nombre_declare = {}

    if not PARTANTS_PATH.exists():
        return {"status": "SKIP", "reason": "partants_master.jsonl introuvable"}

    with open(PARTANTS_PATH, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            uid = rec.get("course_uid", "")
            if not uid:
                continue

            # Ne compter que les vrais partants (pas les non-partants)
            statut = (rec.get("statut") or "").lower()
            if statut != "non_partant":
                course_partant_count[uid] += 1

            nb_declare = rec.get("nombre_partants")
            if nb_declare is not None and isinstance(nb_declare, (int, float)):
                course_nombre_declare[uid] = int(nb_declare)

    # Comparer
    mismatches = []
    checked = 0
    for uid in course_nombre_declare:
        if uid in course_partant_count:
            checked += 1
            declare = course_nombre_declare[uid]
            reel = course_partant_count[uid]
            if declare != reel:
                mismatches.append({
                    "course_uid": uid,
                    "declare": declare,
                    "reel": reel,
                    "diff": reel - declare,
                })

    pct_ok = ((checked - len(mismatches)) / checked * 100) if checked > 0 else 0

    result = {
        "status": "OK" if pct_ok >= 90 else "WARN" if pct_ok >= 70 else "FAIL",
        "courses_checked": checked,
        "mismatches": len(mismatches),
        "match_rate": round(pct_ok, 2),
        "examples": sorted(mismatches, key=lambda x: abs(x["diff"]), reverse=True)[:10],
        "elapsed": round(time.time() - t0, 1),
    }

    print(f"  Courses vérifiées: {checked:,}, "
          f"Désaccords: {len(mismatches):,} ({pct_ok:.1f}% OK) [{result['elapsed']}s]")
    return result


# -----------------------------------------------------------------------
# Génération du rapport Markdown
# -----------------------------------------------------------------------

def generate_report(results: dict) -> str:
    """Génère le rapport Markdown de validation croisée."""
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = []
    lines.append("# Rapport de Validation Croisée")
    lines.append(f"")
    lines.append(f"*Généré le {now}*")
    lines.append(f"")

    # Résumé
    lines.append("## Résumé")
    lines.append(f"")
    lines.append("| Test | Statut | Détail |")
    lines.append("|------|--------|--------|")

    test_names = {
        "results_pmu_letrot": "PMU vs Le Trot (résultats)",
        "odds_pmu_exchange": "PMU vs Exchange (cotes)",
        "pedigree": "Cohérence pedigree",
        "dates": "Cohérence dates",
        "nombre_partants": "Nombre de partants",
    }

    for key, label in test_names.items():
        r = results.get(key, {})
        status = r.get("status", "?")
        status_emoji = {"OK": "OK", "WARN": "WARN", "FAIL": "FAIL", "SKIP": "SKIP"}.get(status, "?")
        detail = ""
        if "match_rate" in r:
            detail = f"{r['match_rate']}% concordance"
        elif "reason" in r:
            detail = r["reason"]
        lines.append(f"| {label} | {status_emoji} | {detail} |")

    lines.append(f"")

    # Détails par test
    for key, label in test_names.items():
        r = results.get(key, {})
        if r.get("status") == "SKIP":
            continue

        lines.append(f"## {label}")
        lines.append(f"")

        # Stats
        for stat_key, stat_val in r.items():
            if stat_key in ("status", "examples", "examples_pere", "examples_mere",
                            "examples_future", "examples_invalid", "examples_abnormal",
                            "elapsed"):
                continue
            lines.append(f"- **{stat_key}**: {stat_val}")
        lines.append(f"- *Temps*: {r.get('elapsed', '?')}s")
        lines.append(f"")

        # Exemples de problèmes
        examples = r.get("examples") or r.get("examples_pere") or []
        if examples:
            lines.append(f"### Exemples d'anomalies (max 10)")
            lines.append(f"")
            lines.append("```json")
            for ex in examples[:10]:
                lines.append(json.dumps(ex, ensure_ascii=False))
            lines.append("```")
            lines.append(f"")

    return "\n".join(lines)


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    t0 = time.time()
    print("=" * 60)
    print("VALIDATION CROISÉE ENTRE SOURCES — Étape 11.3")
    print("=" * 60)

    results = {}

    # 1. Résultats PMU vs Le Trot
    results["results_pmu_letrot"] = validate_results_pmu_vs_letrot()

    # 2. Cotes PMU vs Exchange
    results["odds_pmu_exchange"] = validate_odds_pmu_vs_exchange()

    # 3. Pedigree
    results["pedigree"] = validate_pedigree_consistency()

    # 4. Dates
    results["dates"] = validate_dates()

    # 5. Nombre de partants
    results["nombre_partants"] = validate_nombre_partants()

    # Rapport
    md = generate_report(results)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8", errors="replace") as f:
        f.write(md)

    # Aussi sauvegarder en JSON pour traitement automatique
    json_path = DOCS_DIR / "validation_croisee.json"
    with open(json_path, "w", encoding="utf-8", errors="replace") as f:
        # Nettoyer les sets pour JSON serialization
        clean_results = json.loads(json.dumps(results, default=str))
        json.dump(clean_results, f, ensure_ascii=False, indent=2)

    elapsed_total = time.time() - t0
    print(f"\n{'='*60}")
    print(f"RAPPORT FINAL")
    print(f"{'='*60}")
    for key in results:
        r = results[key]
        print(f"  [{r.get('status', '?'):>4s}] {key}")
    print(f"\nRapport Markdown : {REPORT_PATH}")
    print(f"Rapport JSON     : {json_path}")
    print(f"Temps total      : {elapsed_total:.1f}s")
    print("Terminé.")


if __name__ == "__main__":
    main()
