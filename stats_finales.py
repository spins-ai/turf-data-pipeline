#!/usr/bin/env python3
"""
stats_finales.py — Étape 11.2
==============================

Génère les statistiques finales du pipeline et les enregistre dans docs/STATS.md.

Statistiques produites :
  - Total courses, partants, chevaux uniques, jockeys, hippodromes
  - Plage de dates couverte
  - Nombre total de features
  - Taux de remplissage par champ (partants_master)
  - Taille des fichiers features
  - Résumé des data_master

Usage:
    python stats_finales.py
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"
FEATURES_DIR = BASE_DIR / "output" / "features"
DOCS_DIR = BASE_DIR / "docs"
STATS_OUT = DOCS_DIR / "STATS.md"

PARTANTS_PATH = DATA_MASTER / "partants_master.jsonl"
COURSES_PATH = DATA_MASTER / "courses_master.jsonl"


def scan_partants() -> dict:
    """Scan partants_master.jsonl pour extraire les stats globales."""
    print("Scan partants_master.jsonl ...")
    t0 = time.time()

    stats = {
        "total_partants": 0,
        "chevaux": set(),
        "jockeys": set(),
        "hippodromes": set(),
        "entraineurs": set(),
        "courses_uids": set(),
        "dates": set(),
        "disciplines": defaultdict(int),
        "field_filled": defaultdict(int),
        "field_total": defaultdict(int),
    }

    all_keys = set()

    with open(PARTANTS_PATH, "r", encoding="utf-8", errors="replace") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            stats["total_partants"] += 1
            all_keys.update(rec.keys())

            # Entités uniques
            nom = (rec.get("nom_cheval") or "").strip().upper()
            if nom:
                stats["chevaux"].add(nom)

            jockey = (rec.get("jockey_driver") or "").strip().upper()
            if jockey:
                stats["jockeys"].add(jockey)

            hippo = (rec.get("hippodrome_normalise") or "").strip()
            if hippo:
                stats["hippodromes"].add(hippo)

            entraineur = (rec.get("entraineur") or "").strip().upper()
            if entraineur:
                stats["entraineurs"].add(entraineur)

            uid = rec.get("course_uid", "")
            if uid:
                stats["courses_uids"].add(uid)

            date_iso = rec.get("date_reunion_iso", "")
            if date_iso:
                stats["dates"].add(date_iso)

            disc = (rec.get("discipline") or "").strip()
            if disc:
                stats["disciplines"][disc] += 1

            # Taux de remplissage par champ
            for key in rec.keys():
                stats["field_total"][key] += 1
                val = rec[key]
                if val is not None and val != "" and val != 0:
                    stats["field_filled"][key] += 1

            if line_num % 500_000 == 0:
                print(f"  {line_num:>10,} lignes ...", flush=True)

    stats["all_keys"] = sorted(all_keys)
    print(f"  Scan terminé: {stats['total_partants']:,} partants en {time.time()-t0:.1f}s")
    return stats


def scan_courses() -> dict:
    """Scan courses_master.jsonl pour les stats courses."""
    print("Scan courses_master.jsonl ...")
    stats = {"total_courses": 0}

    if not COURSES_PATH.exists():
        print("  [WARN] courses_master.jsonl introuvable")
        return stats

    t0 = time.time()
    with open(COURSES_PATH, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)
                stats["total_courses"] += 1
            except json.JSONDecodeError:
                continue

    print(f"  {stats['total_courses']:,} courses en {time.time()-t0:.1f}s")
    return stats


def scan_features() -> list[dict]:
    """Scan les fichiers features pour taille et nombre de lignes."""
    print("Scan features ...")
    results = []

    if not FEATURES_DIR.exists():
        print("  [WARN] output/features introuvable")
        return results

    for fname in sorted(os.listdir(FEATURES_DIR)):
        fpath = FEATURES_DIR / fname
        if not fpath.is_file():
            continue

        size_bytes = fpath.stat().st_size
        size_gb = size_bytes / (1024**3)

        # Compter les colonnes pour les JSONL
        num_cols = 0
        num_rows = 0
        if fname.endswith(".jsonl"):
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                first_line = f.readline().strip()
                if first_line:
                    try:
                        rec = json.loads(first_line)
                        num_cols = len(rec.keys())
                    except json.JSONDecodeError:
                        pass
                # Count lines (approximation for large files via file size)
                f.seek(0)
                line_count = 0
                for _ in f:
                    line_count += 1
                num_rows = line_count

        results.append({
            "file": fname,
            "size_gb": round(size_gb, 2),
            "size_bytes": size_bytes,
            "columns": num_cols,
            "rows": num_rows,
        })

    return results


def scan_masters() -> list[dict]:
    """Stats sur les fichiers data_master."""
    print("Scan data_master ...")
    results = []

    if not DATA_MASTER.exists():
        return results

    for fname in sorted(os.listdir(DATA_MASTER)):
        fpath = DATA_MASTER / fname
        if not fpath.is_file():
            continue

        size_bytes = fpath.stat().st_size
        size_mb = size_bytes / (1024**2)

        results.append({
            "file": fname,
            "size_mb": round(size_mb, 1),
        })

    return results


def generate_markdown(partant_stats, course_stats, features_info, masters_info) -> str:
    """Génère le contenu Markdown du rapport."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    total_p = partant_stats["total_partants"]
    total_c = course_stats.get("total_courses", len(partant_stats["courses_uids"]))

    dates = sorted(partant_stats["dates"])
    date_min = dates[0] if dates else "?"
    date_max = dates[-1] if dates else "?"

    lines = []
    lines.append(f"# Statistiques Finales du Pipeline")
    lines.append(f"")
    lines.append(f"*Généré le {now}*")
    lines.append(f"")
    lines.append(f"## Résumé Global")
    lines.append(f"")
    lines.append(f"| Métrique | Valeur |")
    lines.append(f"|----------|--------|")
    lines.append(f"| Total courses | {total_c:,} |")
    lines.append(f"| Total partants | {total_p:,} |")
    lines.append(f"| Chevaux uniques | {len(partant_stats['chevaux']):,} |")
    lines.append(f"| Jockeys uniques | {len(partant_stats['jockeys']):,} |")
    lines.append(f"| Entraîneurs uniques | {len(partant_stats['entraineurs']):,} |")
    lines.append(f"| Hippodromes | {len(partant_stats['hippodromes']):,} |")
    lines.append(f"| Plage de dates | {date_min} -> {date_max} |")
    lines.append(f"| Nombre de jours | {len(dates):,} |")
    lines.append(f"| Champs partants | {len(partant_stats['all_keys'])} |")
    lines.append(f"")

    # Disciplines
    lines.append(f"## Répartition par Discipline")
    lines.append(f"")
    lines.append(f"| Discipline | Partants | % |")
    lines.append(f"|------------|----------|---|")
    for disc, count in sorted(partant_stats["disciplines"].items(), key=lambda x: -x[1]):
        pct = count / total_p * 100 if total_p > 0 else 0
        lines.append(f"| {disc} | {count:,} | {pct:.1f}% |")
    lines.append(f"")

    # Taux de remplissage
    lines.append(f"## Taux de Remplissage par Champ (partants_master)")
    lines.append(f"")
    lines.append(f"| Champ | Remplis | Total | Taux |")
    lines.append(f"|-------|---------|-------|------|")

    field_rates = []
    for key in partant_stats["all_keys"]:
        filled = partant_stats["field_filled"].get(key, 0)
        total = partant_stats["field_total"].get(key, 0)
        rate = filled / total * 100 if total > 0 else 0
        field_rates.append((key, filled, total, rate))

    # Trier par taux croissant (les plus vides en premier)
    field_rates.sort(key=lambda x: x[3])

    for key, filled, total, rate in field_rates:
        emoji = "" if rate >= 90 else " !" if rate < 50 else ""
        lines.append(f"| {key} | {filled:,} | {total:,} | {rate:.1f}%{emoji} |")
    lines.append(f"")

    # Features
    if features_info:
        lines.append(f"## Fichiers Features")
        lines.append(f"")
        total_feat_size = sum(f["size_gb"] for f in features_info)
        total_feat_rows = sum(f["rows"] for f in features_info if f["rows"] > 0)
        total_feat_cols = max((f["columns"] for f in features_info if f["columns"] > 0), default=0)
        lines.append(f"| Fichier | Taille | Lignes | Colonnes |")
        lines.append(f"|---------|--------|--------|----------|")
        for fi in features_info:
            size_str = f"{fi['size_gb']:.1f} GB" if fi["size_gb"] >= 1 else f"{fi['size_gb']*1024:.0f} MB"
            rows_str = f"{fi['rows']:,}" if fi["rows"] > 0 else "—"
            cols_str = str(fi["columns"]) if fi["columns"] > 0 else "—"
            lines.append(f"| {fi['file']} | {size_str} | {rows_str} | {cols_str} |")
        lines.append(f"| **TOTAL** | **{total_feat_size:.1f} GB** | **{total_feat_rows:,}** | — |")
        lines.append(f"")

    # Masters
    if masters_info:
        lines.append(f"## Fichiers Data Master")
        lines.append(f"")
        lines.append(f"| Fichier | Taille |")
        lines.append(f"|---------|--------|")
        total_master_mb = 0
        for mi in masters_info:
            size_str = f"{mi['size_mb']:.1f} MB" if mi["size_mb"] < 1024 else f"{mi['size_mb']/1024:.1f} GB"
            lines.append(f"| {mi['file']} | {size_str} |")
            total_master_mb += mi["size_mb"]
        total_str = f"{total_master_mb:.0f} MB" if total_master_mb < 1024 else f"{total_master_mb/1024:.1f} GB"
        lines.append(f"| **TOTAL** | **{total_str}** |")
        lines.append(f"")

    return "\n".join(lines)


def main():
    t0 = time.time()
    print("=" * 60)
    print("STATS FINALES — Étape 11.2")
    print("=" * 60)

    if not PARTANTS_PATH.exists():
        print(f"ERREUR: {PARTANTS_PATH} introuvable")
        sys.exit(1)

    # Scans
    partant_stats = scan_partants()
    course_stats = scan_courses()
    features_info = scan_features()
    masters_info = scan_masters()

    # Générer le Markdown
    md = generate_markdown(partant_stats, course_stats, features_info, masters_info)

    # Sauvegarder
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATS_OUT, "w", encoding="utf-8", errors="replace") as f:
        f.write(md)

    print(f"\nRapport enregistré : {STATS_OUT}")
    print(f"Temps total : {time.time()-t0:.1f}s")

    # Résumé console
    total_p = partant_stats["total_partants"]
    print(f"\n--- Résumé ---")
    print(f"  Partants:    {total_p:,}")
    print(f"  Courses:     {course_stats.get('total_courses', '?'):,}")
    print(f"  Chevaux:     {len(partant_stats['chevaux']):,}")
    print(f"  Jockeys:     {len(partant_stats['jockeys']):,}")
    print(f"  Hippodromes: {len(partant_stats['hippodromes']):,}")
    dates = sorted(partant_stats["dates"])
    if dates:
        print(f"  Dates:       {dates[0]} -> {dates[-1]} ({len(dates):,} jours)")

    # Champs les plus vides
    print(f"\n--- 15 champs les moins remplis ---")
    field_rates = []
    for key in partant_stats["all_keys"]:
        filled = partant_stats["field_filled"].get(key, 0)
        rate = filled / total_p * 100 if total_p > 0 else 0
        field_rates.append((key, filled, rate))
    field_rates.sort(key=lambda x: x[2])
    for key, filled, rate in field_rates[:15]:
        print(f"  {key:<40s} {filled:>10,} / {total_p:,}  ({rate:.1f}%)")

    print("\nTerminé.")


if __name__ == "__main__":
    main()
