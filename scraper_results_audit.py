#!/usr/bin/env python3
"""
scraper_results_audit.py
========================
Audit de ce que chaque scraper a reellement collecte.

Scanne tous les dossiers output/XX_*/ et pour chacun:
  - Compte le nombre de fichiers et leur taille
  - Compte le nombre de records (JSON/JSONL)
  - Identifie les plages de dates
  - Determine le statut (succes, vide, erreur)

Genere un rapport scraper_audit.md dans docs/.

Usage:
    python3 scraper_results_audit.py
"""

import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
DOCS_DIR = "docs"


def human_size(size_bytes):
    """Convertit des bytes en format lisible."""
    if size_bytes < 1024:
        return "%d B" % size_bytes
    elif size_bytes < 1024 * 1024:
        return "%.1f KB" % (size_bytes / 1024)
    elif size_bytes < 1024 * 1024 * 1024:
        return "%.1f MB" % (size_bytes / (1024 * 1024))
    else:
        return "%.2f GB" % (size_bytes / (1024 * 1024 * 1024))


def count_json_records(filepath):
    """Compte le nombre de records dans un fichier JSON ou JSONL."""
    if not os.path.exists(filepath):
        return 0

    try:
        if filepath.endswith(".jsonl"):
            count = 0
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        count += 1
            return count
        elif filepath.endswith(".json"):
            size = os.path.getsize(filepath)
            if size > 500 * 1024 * 1024:
                # Trop gros, estimer le nombre de lignes
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    # Lire le debut pour estimer
                    head = f.read(1024 * 100)
                    if head.strip().startswith("["):
                        # Compter les '{' au premier niveau
                        bracket_count = head.count('"partant_uid"')
                        if bracket_count == 0:
                            bracket_count = head.count('"course_uid"')
                        if bracket_count == 0:
                            bracket_count = head.count('"nom"')
                        if bracket_count > 0:
                            ratio = size / (1024 * 100)
                            return int(bracket_count * ratio)
                return -1  # Trop gros pour compter
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, list):
                return len(data)
            elif isinstance(data, dict):
                return len(data)
            return 1
    except (json.JSONDecodeError, MemoryError, UnicodeDecodeError):
        return -1
    except Exception:
        return -1


def extract_dates_from_file(filepath, max_records=5000):
    """Extrait les dates trouvees dans un fichier JSON/JSONL."""
    dates = set()
    date_fields = ["date_reunion_iso", "date_course", "date", "date_reunion",
                   "date_collecte", "timestamp_collecte"]
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    try:
        if filepath.endswith(".jsonl"):
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= max_records:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    for field in date_fields:
                        val = rec.get(field, "")
                        if val:
                            m = date_pattern.search(str(val))
                            if m:
                                dates.add(m.group(1))
        elif filepath.endswith(".json"):
            size = os.path.getsize(filepath)
            if size > 200 * 1024 * 1024:
                # Trop gros, lire juste le debut
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    head = f.read(1024 * 500)
                for m in date_pattern.finditer(head):
                    d = m.group(1)
                    if d.startswith("20") or d.startswith("19"):
                        dates.add(d)
                return dates

            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, list):
                for rec in data[:max_records]:
                    if isinstance(rec, dict):
                        for field in date_fields:
                            val = rec.get(field, "")
                            if val:
                                m = date_pattern.search(str(val))
                                if m:
                                    dates.add(m.group(1))
            del data
    except Exception:
        pass

    return dates


def scan_directory(dir_path):
    """Scanne un dossier et retourne ses statistiques."""
    stats = {
        "name": os.path.basename(dir_path),
        "path": dir_path,
        "total_files": 0,
        "total_size": 0,
        "json_files": 0,
        "jsonl_files": 0,
        "csv_files": 0,
        "parquet_files": 0,
        "other_files": 0,
        "total_records": 0,
        "min_date": None,
        "max_date": None,
        "status": "inconnu",
        "files_detail": [],
    }

    if not os.path.isdir(dir_path):
        stats["status"] = "absent"
        return stats

    all_dates = set()

    for root, dirs, files in os.walk(dir_path):
        for f in files:
            fp = os.path.join(root, f)
            try:
                fsize = os.path.getsize(fp)
            except OSError:
                fsize = 0

            stats["total_files"] += 1
            stats["total_size"] += fsize

            rel_path = os.path.relpath(fp, dir_path)

            if f.endswith(".json"):
                stats["json_files"] += 1
                records = count_json_records(fp)
                if records > 0:
                    stats["total_records"] += records
                dates = extract_dates_from_file(fp)
                all_dates.update(dates)
                stats["files_detail"].append({
                    "name": rel_path, "size": fsize, "records": records
                })
            elif f.endswith(".jsonl"):
                stats["jsonl_files"] += 1
                records = count_json_records(fp)
                if records > 0:
                    stats["total_records"] += records
                dates = extract_dates_from_file(fp)
                all_dates.update(dates)
                stats["files_detail"].append({
                    "name": rel_path, "size": fsize, "records": records
                })
            elif f.endswith(".csv"):
                stats["csv_files"] += 1
                # Compter les lignes
                try:
                    with open(fp, "r", encoding="utf-8", errors="replace") as fh:
                        line_count = sum(1 for _ in fh) - 1  # -1 for header
                    if line_count > 0:
                        stats["total_records"] += line_count
                except Exception:
                    pass
                stats["files_detail"].append({
                    "name": rel_path, "size": fsize, "records": line_count if 'line_count' in dir() else 0
                })
            elif f.endswith(".parquet"):
                stats["parquet_files"] += 1
                stats["files_detail"].append({
                    "name": rel_path, "size": fsize, "records": -1
                })
            else:
                stats["other_files"] += 1

    # Plage de dates
    valid_dates = sorted([d for d in all_dates
                          if d >= "2000-01-01" and d <= "2030-12-31"])
    if valid_dates:
        stats["min_date"] = valid_dates[0]
        stats["max_date"] = valid_dates[-1]

    # Statut
    if stats["total_files"] == 0:
        stats["status"] = "vide"
    elif stats["total_records"] > 0:
        stats["status"] = "OK"
    elif stats["total_size"] > 0:
        stats["status"] = "donnees_brutes"
    else:
        stats["status"] = "inconnu"

    return stats


def generate_markdown_report(all_stats):
    """Genere le rapport markdown."""
    lines = []
    lines.append("# Audit des scrapers - Rapport")
    lines.append("")
    lines.append("Date: %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    lines.append("")

    # Resume global
    total_dirs = len(all_stats)
    ok_count = sum(1 for s in all_stats if s["status"] == "OK")
    vide_count = sum(1 for s in all_stats if s["status"] == "vide")
    brut_count = sum(1 for s in all_stats if s["status"] == "donnees_brutes")
    total_records = sum(s["total_records"] for s in all_stats)
    total_size = sum(s["total_size"] for s in all_stats)

    lines.append("## Resume global")
    lines.append("")
    lines.append("| Metrique | Valeur |")
    lines.append("|----------|--------|")
    lines.append("| Dossiers scannes | %d |" % total_dirs)
    lines.append("| Scrapers OK | %d |" % ok_count)
    lines.append("| Scrapers vides | %d |" % vide_count)
    lines.append("| Donnees brutes | %d |" % brut_count)
    lines.append("| Total records | %s |" % "{:,}".format(total_records))
    lines.append("| Taille totale | %s |" % human_size(total_size))
    lines.append("")

    # Tableau detaille
    lines.append("## Detail par scraper")
    lines.append("")
    lines.append("| Scraper | Statut | Fichiers | Taille | Records | Date min | Date max |")
    lines.append("|---------|--------|----------|--------|---------|----------|----------|")

    for s in sorted(all_stats, key=lambda x: x["name"]):
        status_icon = {
            "OK": "OK",
            "vide": "VIDE",
            "donnees_brutes": "BRUT",
            "inconnu": "?",
            "absent": "ABSENT",
        }.get(s["status"], "?")

        records_str = "{:,}".format(s["total_records"]) if s["total_records"] > 0 else "-"
        date_min = s["min_date"] or "-"
        date_max = s["max_date"] or "-"

        lines.append("| %s | %s | %d | %s | %s | %s | %s |" % (
            s["name"], status_icon, s["total_files"],
            human_size(s["total_size"]), records_str,
            date_min, date_max
        ))

    lines.append("")

    # Detail des fichiers par scraper
    lines.append("## Detail des fichiers")
    lines.append("")

    for s in sorted(all_stats, key=lambda x: x["name"]):
        if not s["files_detail"]:
            continue
        lines.append("### %s" % s["name"])
        lines.append("")
        lines.append("| Fichier | Taille | Records |")
        lines.append("|---------|--------|---------|")
        for fd in sorted(s["files_detail"], key=lambda x: -x["size"]):
            rec_str = "{:,}".format(fd["records"]) if fd["records"] >= 0 else "N/A"
            lines.append("| %s | %s | %s |" % (fd["name"], human_size(fd["size"]), rec_str))
        lines.append("")

    # Scrapers vides ou problematiques
    problem_scrapers = [s for s in all_stats if s["status"] in ("vide", "inconnu")]
    if problem_scrapers:
        lines.append("## Scrapers problematiques")
        lines.append("")
        for s in problem_scrapers:
            lines.append("- **%s**: %s" % (s["name"], s["status"]))
        lines.append("")

    return "\n".join(lines)


def main():
    log.info("=" * 70)
    log.info("AUDIT DES SCRAPERS")
    log.info("=" * 70)

    if not os.path.exists(OUTPUT_DIR):
        log.error("Le dossier %s n'existe pas", OUTPUT_DIR)
        sys.exit(1)

    # Lister tous les sous-dossiers de output/
    subdirs = []
    for entry in sorted(os.listdir(OUTPUT_DIR)):
        full_path = os.path.join(OUTPUT_DIR, entry)
        if os.path.isdir(full_path):
            subdirs.append(full_path)

    log.info("Dossiers trouves: %d", len(subdirs))

    # Scanner chaque dossier
    all_stats = []
    for i, subdir in enumerate(subdirs):
        name = os.path.basename(subdir)
        log.info("  [%d/%d] Scan %s...", i + 1, len(subdirs), name)
        t0 = time.time()
        stats = scan_directory(subdir)
        elapsed = time.time() - t0
        log.info("    -> %d fichiers, %s, %d records (%.1fs)",
                 stats["total_files"], human_size(stats["total_size"]),
                 stats["total_records"], elapsed)
        all_stats.append(stats)

    # Resume console
    log.info("")
    log.info("=" * 70)
    log.info("RESUME")
    log.info("=" * 70)

    ok_count = 0
    for s in sorted(all_stats, key=lambda x: x["name"]):
        status = s["status"]
        if status == "OK":
            ok_count += 1
        marker = "[OK]  " if status == "OK" else "[%s]" % status.upper()
        log.info("  %s %s: %d fichiers, %s, %d records",
                 marker, s["name"], s["total_files"],
                 human_size(s["total_size"]), s["total_records"])
        if s["min_date"]:
            log.info("       Dates: %s -> %s", s["min_date"], s["max_date"])

    log.info("")
    log.info("Scrapers OK: %d / %d", ok_count, len(all_stats))

    # Generer le rapport markdown
    os.makedirs(DOCS_DIR, exist_ok=True)
    report_path = os.path.join(DOCS_DIR, "scraper_audit.md")
    report = generate_markdown_report(all_stats)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    log.info("Rapport genere: %s", report_path)

    # Aussi sauver un JSON machine-readable
    json_path = os.path.join(DOCS_DIR, "scraper_audit.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2, default=str)
    log.info("JSON genere: %s", json_path)

    log.info("=" * 70)
    log.info("TERMINE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
