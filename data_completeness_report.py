#!/usr/bin/env python3
"""
data_completeness_report.py
===========================
Genere un rapport detaille de completude des donnees.

Analyse le fichier partants_master (ou v2) et produit:
  - Taux de remplissage par champ
  - Couverture par annee
  - Couverture par source
  - Couverture par discipline

Sauvegarde dans docs/COMPLETENESS.md

Usage:
    python3 data_completeness_report.py
    python3 data_completeness_report.py --input data_master/partants_master_v2.jsonl
    python3 data_completeness_report.py --sample 500000
"""

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)
nBASE_DIR = os.path.dirname(os.path.abspath(__file__))

DOCS_DIR = "docs"


def parse_args():
    parser = argparse.ArgumentParser(description="Rapport de completude des donnees")
    parser.add_argument("--input", default=None,
                        help="Fichier JSONL source (defaut: auto-detect)")
    parser.add_argument("--sample", type=int, default=0,
                        help="Nombre max de lignes a lire (0 = tout)")
    return parser.parse_args()


def detect_source_file():
    """Detecte le meilleur fichier source."""
    candidates = [
        os.path.join(BASE_DIR, "data_master", "partants_master_v2.jsonl"),
        os.path.join(BASE_DIR, "data_master", "partants_master.jsonl"),
        os.path.join(BASE_DIR, "data_master", "partants_master_enrichi.jsonl"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


def extract_year(record):
    """Extrait l'annee d'un record."""
    for field in ["date_reunion_iso", "date_course", "date"]:
        val = record.get(field, "")
        if val and isinstance(val, str) and len(val) >= 4:
            try:
                year = int(val[:4])
                if 1990 <= year <= 2030:
                    return year
            except ValueError:
                pass
    return None


def extract_discipline(record):
    """Extrait la discipline d'un record."""
    disc = record.get("discipline", "")
    if not disc:
        disc = record.get("type_course", "")
    if isinstance(disc, str):
        disc = disc.strip().upper()
    if not disc:
        return "INCONNU"
    # Normaliser
    if "PLAT" in disc:
        return "PLAT"
    elif "TROT" in disc and "ATTELE" in disc:
        return "TROT ATTELE"
    elif "TROT" in disc and "MONTE" in disc:
        return "TROT MONTE"
    elif "TROT" in disc:
        return "TROT"
    elif "OBSTACLE" in disc or "HAIE" in disc or "STEEPLE" in disc or "CROSS" in disc:
        return "OBSTACLE"
    return disc[:20]


def extract_source_prefix(field_name):
    """Extrait le prefixe source d'un champ."""
    # Champs avec prefixe connu
    prefixes = {
        "enr_": "40_enrichissement",
        "seq_": "41_sequences",
        "rp_": "42_racing_post",
        "met_": "43_meteo",
        "ped_": "44_pedigree",
        "gnn_": "45_graphe",
        "spd_": "46_track_speed",
        "mkt_": "49_ecart_cotes",
        "eqp_": "equipements",
        "rap_": "rapports",
        "mto_": "meteo",
        "mch_": "marche",
        "cnd_": "48_conditions",
        "pgr_": "pedigree",
        "hst_": "horse_stats",
        "ext_": "stats_externes",
        "hist_": "05_historique",
        "jockey_": "06_jockeys",
        "cotes_": "07_cotes",
        "equip_": "09_equipements",
        "poids_": "10_poids",
        "sect_": "11_sectionals",
        "sire_": "17_sire_ifce",
        "enrich_": "40_enrichissement",
        "reunion_": "39_reunions",
    }
    for prefix, source in prefixes.items():
        if field_name.startswith(prefix):
            return source
    return "base"


def main():
    args = parse_args()

    input_file = args.input or detect_source_file()
    if not input_file:
        log.error("Aucun fichier source trouve. Utilisez --input")
        sys.exit(1)

    if not os.path.exists(input_file):
        log.error("Fichier introuvable: %s", input_file)
        sys.exit(1)

    sample_limit = args.sample

    file_size = os.path.getsize(input_file)
    log.info("=" * 70)
    log.info("RAPPORT DE COMPLETUDE DES DONNEES")
    log.info("  Source: %s (%.2f GB)", input_file, file_size / (1024 ** 3))
    if sample_limit > 0:
        log.info("  Echantillon: %d lignes max", sample_limit)
    log.info("=" * 70)

    t0 = time.time()

    # Compteurs
    total = 0
    field_fill = defaultdict(int)      # champ -> nb de records remplis
    field_examples = {}                 # champ -> exemple de valeur
    year_counts = defaultdict(int)      # annee -> nb records
    year_field_fill = defaultdict(lambda: defaultdict(int))  # annee -> champ -> nb
    discipline_counts = defaultdict(int)  # discipline -> nb records
    discipline_field_fill = defaultdict(lambda: defaultdict(int))
    source_field_count = defaultdict(int)  # source -> nb champs remplis total
    all_fields = set()

    # Streaming
    with open(input_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            if not isinstance(rec, dict):
                continue

            total += 1

            year = extract_year(rec)
            discipline = extract_discipline(rec)

            if year:
                year_counts[year] += 1
            discipline_counts[discipline] += 1

            for field, value in rec.items():
                all_fields.add(field)

                is_filled = (value is not None and value != "" and value != []
                             and value != {} and value != 0)
                if is_filled:
                    field_fill[field] += 1
                    source = extract_source_prefix(field)
                    source_field_count[source] += 1

                    if field not in field_examples:
                        # Garder un exemple court
                        example = str(value)[:80]
                        field_examples[field] = example

                    if year:
                        year_field_fill[year][field] += 1
                    discipline_field_fill[discipline][field] += 1

            if total % 200000 == 0:
                elapsed = time.time() - t0
                log.info("  %d lignes lues... (%.0fs)", total, elapsed)

            if sample_limit > 0 and total >= sample_limit:
                log.info("  Limite d'echantillon atteinte (%d)", sample_limit)
                break

    elapsed = time.time() - t0
    log.info("")
    log.info("Lecture terminee: %d records, %d champs, %.1fs", total, len(all_fields), elapsed)

    # ================================================================
    # Generer le rapport Markdown
    # ================================================================

    lines = []
    lines.append("# Rapport de completude des donnees")
    lines.append("")
    lines.append("Date: %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    lines.append("")
    lines.append("Source: `%s`" % input_file)
    lines.append("")
    lines.append("Total records: **%s**" % "{:,}".format(total))
    lines.append("")
    lines.append("Total champs uniques: **%d**" % len(all_fields))
    lines.append("")

    # ---- 1. Taux de remplissage par champ ----
    lines.append("## 1. Taux de remplissage par champ")
    lines.append("")
    lines.append("| Champ | Remplis | Taux | Exemple |")
    lines.append("|-------|---------|------|---------|")

    sorted_fields = sorted(all_fields, key=lambda f: -field_fill.get(f, 0))
    for field in sorted_fields:
        count = field_fill.get(field, 0)
        rate = 100.0 * count / max(total, 1)
        example = field_examples.get(field, "")
        # Tronquer et echapper les pipes
        example = example.replace("|", "/")[:50]
        lines.append("| %s | %s | %.1f%% | %s |" % (
            field, "{:,}".format(count), rate, example
        ))

    lines.append("")

    # Resume par tranche de remplissage
    lines.append("### Resume par tranche")
    lines.append("")
    bins = [(90, 100), (70, 90), (50, 70), (30, 50), (10, 30), (0, 10)]
    lines.append("| Tranche | Nb champs |")
    lines.append("|---------|-----------|")
    for lo, hi in bins:
        count = sum(1 for f in all_fields
                    if lo <= 100.0 * field_fill.get(f, 0) / max(total, 1) < hi)
        if hi == 100:
            count += sum(1 for f in all_fields
                         if 100.0 * field_fill.get(f, 0) / max(total, 1) == 100)
        lines.append("| %d%%-%d%% | %d |" % (lo, hi, count))
    lines.append("")

    # ---- 2. Couverture par annee ----
    lines.append("## 2. Couverture par annee")
    lines.append("")
    lines.append("| Annee | Records | % du total |")
    lines.append("|-------|---------|------------|")

    for year in sorted(year_counts.keys()):
        count = year_counts[year]
        rate = 100.0 * count / max(total, 1)
        lines.append("| %d | %s | %.1f%% |" % (year, "{:,}".format(count), rate))
    lines.append("")

    # Champs cles par annee
    key_fields = ["cote_finale", "poids_porte_kg", "discipline",
                  "distance_metres", "terrain", "jockey_driver"]
    # Filtrer les champs qui existent
    key_fields = [f for f in key_fields if f in all_fields]

    if key_fields and year_counts:
        lines.append("### Remplissage des champs cles par annee")
        lines.append("")
        header = "| Annee | " + " | ".join(key_fields) + " |"
        lines.append(header)
        lines.append("|" + "-------|" * (len(key_fields) + 1))

        for year in sorted(year_counts.keys()):
            yr_total = year_counts[year]
            cells = [str(year)]
            for f in key_fields:
                fill = year_field_fill[year].get(f, 0)
                rate = 100.0 * fill / max(yr_total, 1)
                cells.append("%.0f%%" % rate)
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    # ---- 3. Couverture par source ----
    lines.append("## 3. Couverture par source (prefixe)")
    lines.append("")

    # Grouper les champs par source
    source_fields = defaultdict(list)
    for field in all_fields:
        source = extract_source_prefix(field)
        source_fields[source].append(field)

    lines.append("| Source | Nb champs | Remplissage moyen | Champs > 50%% |")
    lines.append("|--------|-----------|-------------------|--------------|")

    for source in sorted(source_fields.keys()):
        fields = source_fields[source]
        nb_fields = len(fields)
        avg_rate = sum(100.0 * field_fill.get(f, 0) / max(total, 1) for f in fields) / max(nb_fields, 1)
        above_50 = sum(1 for f in fields if 100.0 * field_fill.get(f, 0) / max(total, 1) >= 50)
        lines.append("| %s | %d | %.1f%% | %d |" % (source, nb_fields, avg_rate, above_50))
    lines.append("")

    # ---- 4. Couverture par discipline ----
    lines.append("## 4. Couverture par discipline")
    lines.append("")
    lines.append("| Discipline | Records | % du total |")
    lines.append("|------------|---------|------------|")

    for disc in sorted(discipline_counts.keys(), key=lambda d: -discipline_counts[d]):
        count = discipline_counts[disc]
        rate = 100.0 * count / max(total, 1)
        lines.append("| %s | %s | %.1f%% |" % (disc, "{:,}".format(count), rate))
    lines.append("")

    # Remplissage par discipline pour champs cles
    if key_fields and discipline_counts:
        lines.append("### Remplissage des champs cles par discipline")
        lines.append("")
        header = "| Discipline | " + " | ".join(key_fields) + " |"
        lines.append(header)
        lines.append("|" + "------------|" + "-------|" * len(key_fields))

        for disc in sorted(discipline_counts.keys(), key=lambda d: -discipline_counts[d]):
            disc_total = discipline_counts[disc]
            cells = [disc]
            for f in key_fields:
                fill = discipline_field_fill[disc].get(f, 0)
                rate = 100.0 * fill / max(disc_total, 1)
                cells.append("%.0f%%" % rate)
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    # ---- 5. Champs critiques manquants ----
    lines.append("## 5. Champs les moins remplis (< 10%%)")
    lines.append("")
    sparse_fields = [(f, field_fill.get(f, 0)) for f in all_fields
                     if 100.0 * field_fill.get(f, 0) / max(total, 1) < 10]
    sparse_fields.sort(key=lambda x: x[1])

    if sparse_fields:
        lines.append("| Champ | Remplis | Taux |")
        lines.append("|-------|---------|------|")
        for f, count in sparse_fields[:50]:
            rate = 100.0 * count / max(total, 1)
            lines.append("| %s | %s | %.2f%% |" % (f, "{:,}".format(count), rate))
    else:
        lines.append("Aucun champ en dessous de 10%% de remplissage.")
    lines.append("")

    # ---- 6. Champs 100% remplis ----
    lines.append("## 6. Champs 100%% remplis")
    lines.append("")
    full_fields = [f for f in all_fields if field_fill.get(f, 0) == total and total > 0]
    full_fields.sort()
    if full_fields:
        lines.append("| Champ |")
        lines.append("|-------|")
        for f in full_fields:
            lines.append("| %s |" % f)
    else:
        lines.append("Aucun champ n'est rempli a 100%%.")
    lines.append("")

    # Sauvegarder
    os.makedirs(DOCS_DIR, exist_ok=True)
    report_path = os.path.join(DOCS_DIR, "COMPLETENESS.md")
    report_text = "\n".join(lines)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    log.info("")
    log.info("Rapport sauvegarde: %s", report_path)

    # Aussi sauver un JSON resume
    summary = {
        "source": input_file,
        "total_records": total,
        "total_fields": len(all_fields),
        "field_fill_rates": {
            f: round(100.0 * field_fill.get(f, 0) / max(total, 1), 2)
            for f in sorted_fields
        },
        "year_coverage": dict(year_counts),
        "discipline_coverage": dict(discipline_counts),
        "source_coverage": {s: len(fs) for s, fs in source_fields.items()},
    }
    json_path = os.path.join(DOCS_DIR, "completeness_summary.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    log.info("JSON resume: %s", json_path)

    # Stats console
    log.info("")
    log.info("=" * 70)
    log.info("RESUME RAPIDE")
    log.info("=" * 70)
    log.info("  Records: %s", "{:,}".format(total))
    log.info("  Champs: %d", len(all_fields))
    log.info("  Champs > 90%%: %d", sum(1 for f in all_fields
             if 100.0 * field_fill.get(f, 0) / max(total, 1) >= 90))
    log.info("  Champs > 50%%: %d", sum(1 for f in all_fields
             if 100.0 * field_fill.get(f, 0) / max(total, 1) >= 50))
    log.info("  Champs < 10%%: %d", len(sparse_fields))
    log.info("  Annees couvertes: %s",
             "%d-%d" % (min(year_counts.keys()), max(year_counts.keys())) if year_counts else "N/A")
    log.info("  Disciplines: %s", ", ".join(sorted(discipline_counts.keys())))
    log.info("=" * 70)
    log.info("TERMINE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
