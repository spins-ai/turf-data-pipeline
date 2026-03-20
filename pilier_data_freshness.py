#!/usr/bin/env python3
"""
pilier_data_freshness.py -- Pilier Qualite : Fraicheur des donnees
===================================================================

Verifie la fraicheur des donnees du pipeline.

Fonctionnalites :
  1. Pour chaque fichier output, verifie la date de derniere modification
  2. Pour chaque source, verifie la date du record le plus recent
  3. Flag les donnees obsoletes (>30 jours)
  4. Genere un dashboard de fraicheur (JSON + console)

Usage:
    python pilier_data_freshness.py
    python pilier_data_freshness.py --stale-days 14
    python pilier_data_freshness.py --output logs/freshness.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DATA_MASTER = BASE_DIR / "data_master"
LOGS_DIR = BASE_DIR / "logs"
REPORT_FILE = LOGS_DIR / "freshness_dashboard.json"

DEFAULT_STALE_DAYS = 30


# -----------------------------------------------------------------------
# Extraction de dates depuis les records
# -----------------------------------------------------------------------

def parse_date(val) -> datetime:
    """Tente de parser une valeur en datetime."""
    if not val or not isinstance(val, str):
        raise ValueError("not a string")

    val = val.strip()

    # YYYY-MM-DD...
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(val[:len(fmt.replace("%", "X").replace("X", "0"))], fmt)
        except (ValueError, IndexError):
            continue

    # Essai generique
    if len(val) >= 10:
        # YYYY-MM-DD
        if val[4] in ("-", "/"):
            try:
                return datetime(int(val[:4]), int(val[5:7]), int(val[8:10]))
            except (ValueError, IndexError):
                pass
        # DD/MM/YYYY
        if val[2] in ("-", "/"):
            try:
                return datetime(int(val[6:10]), int(val[3:5]), int(val[:2]))
            except (ValueError, IndexError):
                pass

    raise ValueError(f"Cannot parse date: {val}")


def find_most_recent_date(filepath: Path, sample_size: int = 500) -> dict:
    """Trouve la date la plus recente dans un fichier de donnees."""
    date_fields = ("date", "date_course", "date_reunion", "jour", "date_debut")
    most_recent = None
    most_recent_str = ""
    dates_found = 0

    date_fields = date_fields + ("date_reunion_iso",)

    try:
        file_size = filepath.stat().st_size

        if filepath.suffix == ".jsonl":
            # Streaming: lire les N premieres lignes
            with open(filepath, "r", encoding="utf-8", errors="replace", buffering=1048576) as f:
                count = 0
                line = f.readline()
                while line and count < sample_size:
                    stripped = line.strip()
                    if stripped:
                        try:
                            rec = json.loads(stripped)
                            for key in date_fields:
                                val = rec.get(key)
                                if val:
                                    try:
                                        dt = parse_date(val)
                                        dates_found += 1
                                        if most_recent is None or dt > most_recent:
                                            most_recent = dt
                                            most_recent_str = str(val)
                                    except (ValueError, TypeError):
                                        continue
                        except json.JSONDecodeError:
                            pass
                        count += 1
                    line = f.readline()

            # Lire les dernieres lignes en streaming inverse (tail)
            # Pour les gros fichiers: lire les derniers 2 MB
            tail_size = min(file_size, 2 * 1024 * 1024)
            if tail_size > 0 and file_size > tail_size:
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(file_size - tail_size)
                    f.readline()  # skip partial line
                    tail_count = 0
                    for tline in f:
                        stripped = tline.strip()
                        if stripped and tail_count < sample_size:
                            try:
                                rec = json.loads(stripped)
                                for key in date_fields:
                                    val = rec.get(key)
                                    if val:
                                        try:
                                            dt = parse_date(val)
                                            dates_found += 1
                                            if most_recent is None or dt > most_recent:
                                                most_recent = dt
                                                most_recent_str = str(val)
                                        except (ValueError, TypeError):
                                            continue
                            except json.JSONDecodeError:
                                pass
                            tail_count += 1

        elif filepath.suffix == ".json":
            if file_size > 500_000_000:
                pass  # Skip gros JSON
            else:
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    data = [data]
                if not isinstance(data, list):
                    data = []

                sample = data[:sample_size] + data[-sample_size:]
                for rec in sample:
                    if not isinstance(rec, dict):
                        continue
                    for key in date_fields:
                        val = rec.get(key)
                        if val:
                            try:
                                dt = parse_date(val)
                                dates_found += 1
                                if most_recent is None or dt > most_recent:
                                    most_recent = dt
                                    most_recent_str = str(val)
                            except (ValueError, TypeError):
                                continue

    except Exception:
        pass

    return {
        "most_recent_date": most_recent.isoformat() if most_recent else None,
        "most_recent_raw": most_recent_str,
        "dates_sampled": dates_found,
    }


# -----------------------------------------------------------------------
# Scan des fichiers
# -----------------------------------------------------------------------

def scan_file_freshness(filepath: Path, stale_days: int) -> dict:
    """Analyse la fraicheur d'un fichier."""
    now = datetime.utcnow()
    stat = filepath.stat()

    mod_time = datetime.utcfromtimestamp(stat.st_mtime)
    age_days = (now - mod_time).days
    size_mb = stat.st_size / (1024 * 1024)

    result = {
        "path": str(filepath.relative_to(BASE_DIR)),
        "size_mb": round(size_mb, 2),
        "last_modified": mod_time.isoformat(),
        "age_days": age_days,
        "file_stale": age_days > stale_days,
    }

    # Chercher la date la plus recente dans les donnees
    if filepath.suffix in (".json", ".jsonl"):
        date_info = find_most_recent_date(filepath)
        result.update(date_info)

        if date_info["most_recent_date"]:
            try:
                data_date = datetime.fromisoformat(date_info["most_recent_date"])
                data_age = (now - data_date).days
                result["data_age_days"] = data_age
                result["data_stale"] = data_age > stale_days
            except (ValueError, TypeError):
                result["data_age_days"] = None
                result["data_stale"] = None

    return result


def scan_directory(directory: Path, stale_days: int, prefix: str = "") -> list[dict]:
    """Scanne un repertoire pour la fraicheur. Utilise os.walk pour eviter cache_corrupted."""
    results = []

    if not directory.exists():
        return results

    extensions = {".json", ".jsonl", ".csv", ".parquet"}
    skip_dirs = {"cache_corrupted", "cache", ".git", "__pycache__", "node_modules"}

    for root, dirs, filenames in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for fname in sorted(filenames):
            f = Path(root) / fname
            if f.suffix in extensions and not f.name.endswith((".tmp", ".bak")):
                try:
                    info = scan_file_freshness(f, stale_days)
                    info["source"] = prefix
                    results.append(info)
                except Exception as e:
                    results.append({
                        "path": str(f.relative_to(BASE_DIR)),
                        "error": str(e),
                        "source": prefix,
                    })

    return results


# -----------------------------------------------------------------------
# Dashboard
# -----------------------------------------------------------------------

def generate_dashboard(results: list[dict], stale_days: int) -> dict:
    """Genere le dashboard de fraicheur."""
    now = datetime.utcnow()

    total_files = len(results)
    stale_files = [r for r in results if r.get("file_stale")]
    stale_data = [r for r in results if r.get("data_stale")]
    fresh_files = [r for r in results if not r.get("file_stale") and "file_stale" in r]
    errors = [r for r in results if "error" in r]

    # Grouper par source
    by_source = {}
    for r in results:
        src = r.get("source", "unknown")
        if src not in by_source:
            by_source[src] = {"total": 0, "stale": 0, "fresh": 0, "files": []}
        by_source[src]["total"] += 1
        if r.get("file_stale"):
            by_source[src]["stale"] += 1
        else:
            by_source[src]["fresh"] += 1
        by_source[src]["files"].append(r.get("path", ""))

    dashboard = {
        "generated_at": now.isoformat() + "Z",
        "stale_threshold_days": stale_days,
        "summary": {
            "total_files": total_files,
            "fresh_files": len(fresh_files),
            "stale_files": len(stale_files),
            "stale_data": len(stale_data),
            "errors": len(errors),
            "health_pct": round(
                len(fresh_files) / total_files * 100, 1
            ) if total_files > 0 else 0,
        },
        "by_source": by_source,
        "stale_files": sorted(
            [
                {
                    "path": r.get("path"),
                    "age_days": r.get("age_days"),
                    "last_modified": r.get("last_modified"),
                }
                for r in stale_files
            ],
            key=lambda x: x.get("age_days", 0),
            reverse=True,
        ),
        "stale_data": sorted(
            [
                {
                    "path": r.get("path"),
                    "data_age_days": r.get("data_age_days"),
                    "most_recent_date": r.get("most_recent_date"),
                }
                for r in stale_data
            ],
            key=lambda x: x.get("data_age_days", 0),
            reverse=True,
        ),
        "all_files": results,
    }

    return dashboard


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Verification fraicheur des donnees")
    parser.add_argument("--stale-days", "-s", type=int, default=DEFAULT_STALE_DAYS,
                        help=f"Seuil en jours (defaut: {DEFAULT_STALE_DAYS})")
    parser.add_argument("--output", "-o", help="Fichier rapport de sortie")
    parser.add_argument("--master-only", action="store_true",
                        help="Analyser uniquement data_master/")
    args = parser.parse_args()

    print("=" * 60)
    print("PILIER DATA FRESHNESS")
    print(f"Seuil de peremption: {args.stale_days} jours")
    print("=" * 60)

    all_results = []

    # Scanner data_master
    print("Scan data_master/ ...")
    master_results = scan_directory(DATA_MASTER, args.stale_days, "data_master")
    all_results.extend(master_results)
    print(f"  {len(master_results)} fichiers")

    # Scanner output
    if not args.master_only:
        print("Scan output/ ...")
        output_results = scan_directory(OUTPUT_DIR, args.stale_days, "output")
        all_results.extend(output_results)
        print(f"  {len(output_results)} fichiers")

    # Scanner logs
    print("Scan logs/ ...")
    log_results = scan_directory(LOGS_DIR, args.stale_days, "logs")
    all_results.extend(log_results)
    print(f"  {len(log_results)} fichiers")

    print("-" * 60)

    # Generer le dashboard
    dashboard = generate_dashboard(all_results, args.stale_days)
    summary = dashboard["summary"]

    print(f"Total fichiers scannes: {summary['total_files']}")
    print(f"Fichiers frais:         {summary['fresh_files']}")
    print(f"Fichiers obsoletes:     {summary['stale_files']}")
    print(f"Donnees obsoletes:      {summary['stale_data']}")
    print(f"Sante globale:          {summary['health_pct']}%")

    # Afficher les fichiers obsoletes
    if dashboard["stale_files"]:
        print("-" * 60)
        print("Fichiers obsoletes (par anciennete) :")
        for item in dashboard["stale_files"][:15]:
            print(f"  [{item.get('age_days', '?')}j] {item.get('path', '?')}")

    if dashboard["stale_data"]:
        print("-" * 60)
        print("Donnees obsoletes (date la plus recente dans le fichier) :")
        for item in dashboard["stale_data"][:15]:
            print(f"  [{item.get('data_age_days', '?')}j] {item.get('path', '?')}"
                  f"  (dernier: {item.get('most_recent_date', '?')[:10]})")

    # Sauvegarder
    out_path = Path(args.output) if args.output else REPORT_FILE
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False, default=str)

    print("-" * 60)
    print(f"Dashboard sauvegarde: {out_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
