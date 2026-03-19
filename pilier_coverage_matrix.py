#!/usr/bin/env python3
"""
pilier_coverage_matrix.py -- Pilier Qualite : Matrice de couverture
===================================================================

Genere une matrice de couverture source x annee montrant le nombre
d'enregistrements par source de donnees et par annee.

Fonctionnalites :
  1. Pour chaque source de donnees, compte les enregistrements par annee
  2. Genere une table markdown montrant les trous de couverture
  3. Identifie les annees manquantes par source
  4. Export JSON + markdown

Usage:
    python pilier_coverage_matrix.py
    python pilier_coverage_matrix.py --output logs/coverage_matrix.md
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DATA_MASTER = BASE_DIR / "data_master"
LOGS_DIR = BASE_DIR / "logs"
REPORT_JSON = LOGS_DIR / "coverage_matrix.json"
REPORT_MD = LOGS_DIR / "coverage_matrix.md"


def extract_year(record: dict) -> str:
    """Extrait l'annee d'un enregistrement."""
    for key in ("date", "date_course", "date_reunion", "jour", "date_debut", "annee", "year"):
        val = record.get(key)
        if val is None:
            continue
        if isinstance(val, (int, float)) and 1990 <= val <= 2030:
            return str(int(val))
        if isinstance(val, str):
            # YYYY-MM-DD ou YYYY/MM/DD
            if len(val) >= 4 and val[:4].isdigit():
                y = int(val[:4])
                if 1990 <= y <= 2030:
                    return val[:4]
            # DD/MM/YYYY
            if len(val) >= 10 and val[2] in ("-", "/"):
                try:
                    parts = val.split(val[2])
                    if len(parts) == 3:
                        y = int(parts[2][:4])
                        if 1990 <= y <= 2030:
                            return str(y)
                except (ValueError, IndexError):
                    pass
    return None


def load_jsonl(filepath: Path) -> list[dict]:
    """Charge un fichier JSONL."""
    records = []
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def load_json(filepath: Path) -> list[dict]:
    """Charge un fichier JSON."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def scan_output_sources(output_dir: Path) -> dict:
    """Scanne les sous-dossiers output/ pour compter les records par annee."""
    sources = {}

    if not output_dir.exists():
        return sources

    for subdir in sorted(output_dir.iterdir()):
        if not subdir.is_dir():
            continue

        source_name = subdir.name
        year_counts = Counter()
        total = 0
        errors = 0

        for f in subdir.rglob("*"):
            if f.suffix not in (".json", ".jsonl"):
                continue
            try:
                if f.suffix == ".jsonl":
                    records = load_jsonl(f)
                else:
                    records = load_json(f)

                for rec in records:
                    total += 1
                    year = extract_year(rec)
                    if year:
                        year_counts[year] += 1
                    else:
                        year_counts["N/A"] += 1
            except Exception:
                errors += 1
                continue

        if total > 0 or errors > 0:
            sources[source_name] = {
                "year_counts": dict(year_counts),
                "total": total,
                "errors": errors,
            }

    return sources


def scan_master_sources(master_dir: Path) -> dict:
    """Scanne les fichiers data_master/."""
    sources = {}

    if not master_dir.exists():
        return sources

    for f in sorted(master_dir.iterdir()):
        if f.suffix not in (".json", ".jsonl") or f.name.endswith(".tmp"):
            continue

        source_name = f.stem
        year_counts = Counter()
        total = 0

        try:
            if f.suffix == ".jsonl":
                records = load_jsonl(f)
            else:
                records = load_json(f)

            for rec in records:
                total += 1
                year = extract_year(rec)
                if year:
                    year_counts[year] += 1
                else:
                    year_counts["N/A"] += 1
        except Exception:
            sources[source_name] = {
                "year_counts": {},
                "total": 0,
                "errors": 1,
            }
            continue

        sources[source_name] = {
            "year_counts": dict(year_counts),
            "total": total,
            "errors": 0,
        }

    return sources


def build_matrix(sources: dict) -> dict:
    """Construit la matrice de couverture."""
    # Collecter toutes les annees
    all_years = set()
    for info in sources.values():
        for y in info.get("year_counts", {}):
            if y != "N/A":
                all_years.add(y)

    years_sorted = sorted(all_years)
    if not years_sorted:
        return {"years": [], "sources": {}, "gaps": {}}

    # Matrice
    matrix = {}
    gaps = {}
    for source, info in sorted(sources.items()):
        yc = info.get("year_counts", {})
        row = {}
        source_gaps = []
        for y in years_sorted:
            count = yc.get(y, 0)
            row[y] = count
            if count == 0:
                source_gaps.append(y)

        matrix[source] = row
        if source_gaps:
            gaps[source] = source_gaps

    return {
        "years": years_sorted,
        "sources": matrix,
        "gaps": gaps,
        "totals": {s: info.get("total", 0) for s, info in sorted(sources.items())},
    }


def generate_markdown(matrix_data: dict) -> str:
    """Genere une table markdown de la matrice."""
    years = matrix_data["years"]
    sources = matrix_data["sources"]
    gaps = matrix_data["gaps"]

    lines = []
    lines.append("# Matrice de couverture : Source x Annee")
    lines.append("")
    lines.append(f"Genere le {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    lines.append("")

    if not years:
        lines.append("Aucune donnee avec annee trouvee.")
        return "\n".join(lines)

    # Table header
    header = "| Source |"
    separator = "|--------|"
    for y in years:
        header += f" {y} |"
        separator += "------:|"
    header += " Total |"
    separator += "------:|"

    lines.append(header)
    lines.append(separator)

    # Rows
    for source in sorted(sources.keys()):
        row = sources[source]
        total = matrix_data["totals"].get(source, 0)
        line = f"| {source[:30]} |"
        for y in years:
            count = row.get(y, 0)
            if count == 0:
                line += " - |"
            else:
                line += f" {count:,} |".replace(",", " ")
            pass
        line += f" {total:,} |".replace(",", " ")
        lines.append(line)

    lines.append("")

    # Gaps summary
    if gaps:
        lines.append("## Trous de couverture")
        lines.append("")
        for source, missing_years in sorted(gaps.items()):
            if len(missing_years) <= 10:
                lines.append(f"- **{source}**: manque {', '.join(missing_years)}")
            else:
                lines.append(f"- **{source}**: manque {len(missing_years)} annees"
                             f" ({missing_years[0]}..{missing_years[-1]})")
    else:
        lines.append("## Aucun trou de couverture detecte")

    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Matrice de couverture source x annee")
    parser.add_argument("--output", "-o", help="Fichier markdown de sortie")
    parser.add_argument("--json-output", help="Fichier JSON de sortie")
    parser.add_argument("--master-only", action="store_true",
                        help="Analyser uniquement data_master/")
    args = parser.parse_args()

    print("=" * 60)
    print("PILIER COVERAGE MATRIX")
    print("=" * 60)

    all_sources = {}

    # Scanner data_master
    print("Scan data_master/ ...")
    master_sources = scan_master_sources(DATA_MASTER)
    print(f"  {len(master_sources)} fichiers master trouves")
    for name, info in master_sources.items():
        all_sources[f"master/{name}"] = info

    # Scanner output
    if not args.master_only:
        print("Scan output/ ...")
        output_sources = scan_output_sources(OUTPUT_DIR)
        print(f"  {len(output_sources)} sources output trouvees")
        for name, info in output_sources.items():
            all_sources[f"output/{name}"] = info

    if not all_sources:
        print("Aucune source trouvee.")
        sys.exit(1)

    print(f"Total sources: {len(all_sources)}")
    print("-" * 60)

    # Construire la matrice
    matrix_data = build_matrix(all_sources)

    # Generer le markdown
    md_content = generate_markdown(matrix_data)

    # Sauvegarder markdown
    md_path = Path(args.output) if args.output else REPORT_MD
    md_path.parent.mkdir(parents=True, exist_ok=True)
    with open(md_path, "w", encoding="utf-8", errors="replace") as f:
        f.write(md_content)
    print(f"Markdown: {md_path}")

    # Sauvegarder JSON
    json_path = Path(args.json_output) if args.json_output else REPORT_JSON
    json_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "n_sources": len(all_sources),
        "years": matrix_data["years"],
        "matrix": matrix_data["sources"],
        "totals": matrix_data["totals"],
        "gaps": matrix_data["gaps"],
    }
    with open(json_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"JSON: {json_path}")

    # Resume gaps
    n_gaps = len(matrix_data["gaps"])
    print("-" * 60)
    if n_gaps:
        print(f"Sources avec trous: {n_gaps}")
        for source, missing in sorted(matrix_data["gaps"].items())[:10]:
            print(f"  {source}: {len(missing)} annee(s) manquante(s)")
    else:
        print("Aucun trou de couverture detecte")
    print("=" * 60)


if __name__ == "__main__":
    main()
