#!/usr/bin/env python3
"""
validate_new_source.py -- Valide la sortie d'un nouveau scraper.

Etape 8 : validation avant integration dans le pipeline.

Verifications effectuees :
  1. Format JSONL valide (chaque ligne est du JSON)
  2. Champs requis presents (date, hippodrome ou equivalent)
  3. Plage de dates raisonnable
  4. Detection de doublons
  5. Suggestion de mappings vers le schema standard

Usage :
    python scripts/validate_new_source.py \\
        --source-dir output/117_jockey_planet \\
        [--required-fields date hippodrome] \\
        [--date-field date] \\
        [--max-records 100000]

Exit code 0 = toutes les verifications passent, 1 = au moins un probleme.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# ---------------------------------------------------------------------------
# Standard schema fields for mapping suggestions
# ---------------------------------------------------------------------------
STANDARD_FIELDS = [
    "date", "hippodrome", "reunion", "course", "nom_cheval", "numero",
    "jockey", "entraineur", "proprietaire", "poids", "corde",
    "cote_probable", "resultat", "gains", "distance", "discipline",
    "terrain", "allocation", "uid_course", "uid_partant",
]

# Common aliases per standard field
FIELD_ALIASES: dict[str, list[str]] = {
    "date": ["date", "race_date", "jour", "event_date", "date_course",
             "dateReunion", "dateCourse"],
    "hippodrome": ["hippodrome", "track", "racecourse", "venue",
                   "course_name", "track_name", "lieu", "nomHippodrome"],
    "reunion": ["reunion", "meeting", "num_reunion", "r", "numReunion"],
    "course": ["course", "race", "race_number", "num_course", "c", "numCourse"],
    "nom_cheval": ["nom_cheval", "cheval", "horse", "horse_name", "runner",
                   "runner_name", "nom", "nomCheval"],
    "numero": ["numero", "number", "num", "cloth", "saddle_number", "numPmu"],
    "jockey": ["jockey", "jockey_name", "rider", "driver", "nomJockey"],
    "entraineur": ["entraineur", "trainer", "trainer_name", "nomEntraineur"],
    "poids": ["poids", "weight", "poids_monte", "poidsConditionMonte"],
    "corde": ["corde", "draw", "barrier", "stall", "placeCorde"],
    "cote_probable": ["cote_probable", "cote", "odds", "sp", "starting_price",
                      "coteDirect"],
    "resultat": ["resultat", "result", "finish", "position", "place",
                 "finishing_position", "ordreArrivee"],
    "distance": ["distance", "dist", "race_distance"],
    "discipline": ["discipline", "type", "race_type", "specialite"],
    "terrain": ["terrain", "going", "ground", "track_condition", "etatTerrain"],
}


class ValidationResult:
    """Collects validation results."""

    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.info: list[str] = []
        self.stats: dict[str, object] = {}

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_info(self, msg: str) -> None:
        self.info.append(msg)

    def print_report(self) -> None:
        print("\n" + "=" * 70)
        print("RAPPORT DE VALIDATION")
        print("=" * 70)

        if self.info:
            print("\n--- Informations ---")
            for msg in self.info:
                print(f"  [INFO] {msg}")

        if self.warnings:
            print("\n--- Avertissements ---")
            for msg in self.warnings:
                print(f"  [WARN] {msg}")

        if self.errors:
            print("\n--- Erreurs ---")
            for msg in self.errors:
                print(f"  [FAIL] {msg}")

        if self.stats:
            print("\n--- Statistiques ---")
            for key, val in self.stats.items():
                print(f"  {key}: {val}")

        print("\n" + "-" * 70)
        if self.passed:
            print("RESULTAT: PASS (aucune erreur)")
        else:
            print(f"RESULTAT: FAIL ({len(self.errors)} erreur(s))")
        print("=" * 70)


def discover_jsonl_files(source_dir: Path) -> list[Path]:
    """Find all JSONL files in source directory."""
    jsonl_files = sorted(source_dir.glob("*.jsonl"))
    if not jsonl_files:
        jsonl_files = sorted(source_dir.glob("*.json"))
    return jsonl_files


def check_jsonl_format(jsonl_path: Path, max_records: int,
                       result: ValidationResult) -> list[dict]:
    """Check 1: Validate JSONL format. Returns parsed records."""
    records: list[dict] = []
    parse_errors = 0
    empty_lines = 0
    total_lines = 0

    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                total_lines += 1
                if total_lines > max_records:
                    break

                line = line.strip()
                if not line:
                    empty_lines += 1
                    continue

                try:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        parse_errors += 1
                        if parse_errors <= 3:
                            result.add_warning(
                                f"Ligne {i+1}: JSON valide mais pas un objet "
                                f"(type: {type(record).__name__})"
                            )
                    else:
                        records.append(record)
                except json.JSONDecodeError as e:
                    parse_errors += 1
                    if parse_errors <= 3:
                        result.add_error(
                            f"Ligne {i+1}: JSON invalide: {e}"
                        )
    except UnicodeDecodeError as e:
        result.add_error(f"Erreur d'encodage: {e}")
        return records

    result.stats["total_lignes"] = total_lines
    result.stats["lignes_vides"] = empty_lines
    result.stats["records_valides"] = len(records)
    result.stats["erreurs_parse"] = parse_errors

    if parse_errors == 0:
        result.add_info(f"Format JSONL valide ({len(records)} records)")
    else:
        error_rate = parse_errors / max(total_lines, 1) * 100
        result.add_error(
            f"{parse_errors} erreurs de parsing sur {total_lines} lignes "
            f"({error_rate:.1f}%)"
        )

    if empty_lines > total_lines * 0.1:
        result.add_warning(
            f"{empty_lines} lignes vides ({empty_lines/max(total_lines,1)*100:.0f}%)"
        )

    return records


def check_required_fields(records: list[dict], required_fields: list[str],
                          result: ValidationResult) -> None:
    """Check 2: Verify required fields exist."""
    if not records:
        result.add_error("Aucun record a verifier")
        return

    # Get all fields and their coverage
    field_coverage: dict[str, int] = {}
    for rec in records:
        for key in rec:
            field_coverage[key] = field_coverage.get(key, 0) + 1

    total = len(records)
    result.stats["champs_detectes"] = len(field_coverage)

    # Check each required field
    for field in required_fields:
        count = field_coverage.get(field, 0)
        pct = count / total * 100

        if count == 0:
            # Check if an alias exists
            found_alias = None
            for std_field, aliases in FIELD_ALIASES.items():
                if field == std_field:
                    for alias in aliases:
                        if alias in field_coverage and alias != field:
                            found_alias = alias
                            break
                    break

            if found_alias:
                alias_count = field_coverage[found_alias]
                result.add_warning(
                    f"Champ requis '{field}' absent, mais alias '{found_alias}' "
                    f"present ({alias_count}/{total} records, "
                    f"{alias_count/total*100:.0f}%)"
                )
            else:
                result.add_error(
                    f"Champ requis '{field}' absent (0/{total} records)"
                )
        elif pct < 90:
            result.add_warning(
                f"Champ requis '{field}' partiellement present "
                f"({count}/{total}, {pct:.0f}%)"
            )
        else:
            result.add_info(
                f"Champ requis '{field}' present ({count}/{total}, {pct:.0f}%)"
            )


def check_date_range(records: list[dict], date_field: str,
                     result: ValidationResult) -> None:
    """Check 3: Verify date range is reasonable."""
    dates: list[str] = []
    for rec in records:
        val = rec.get(date_field)
        if val and isinstance(val, str):
            dates.append(val)

    if not dates:
        result.add_warning(
            f"Aucune date trouvee dans le champ '{date_field}'. "
            f"Verifier le nom du champ date."
        )
        return

    # Try to parse dates (support common formats)
    parsed_dates: list[datetime] = []
    formats_to_try = [
        "%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%Y%m%d",
    ]

    for d in dates[:1000]:  # Sample
        for fmt in formats_to_try:
            try:
                parsed_dates.append(datetime.strptime(d[:19], fmt))
                break
            except ValueError:
                continue

    if not parsed_dates:
        result.add_warning(
            f"Impossible de parser les dates (echantillon: {dates[:3]})"
        )
        return

    min_date = min(parsed_dates)
    max_date = max(parsed_dates)
    now = datetime.now()

    result.stats["date_min"] = min_date.strftime("%Y-%m-%d")
    result.stats["date_max"] = max_date.strftime("%Y-%m-%d")
    result.stats["plage_jours"] = (max_date - min_date).days

    result.add_info(
        f"Plage de dates: {min_date:%Y-%m-%d} -> {max_date:%Y-%m-%d} "
        f"({(max_date - min_date).days} jours)"
    )

    # Sanity checks
    if min_date.year < 2000:
        result.add_warning(
            f"Dates anterieures a 2000 detectees (min: {min_date:%Y-%m-%d})"
        )
    if max_date > now:
        result.add_warning(
            f"Dates futures detectees (max: {max_date:%Y-%m-%d})"
        )
    if (max_date - min_date).days < 1:
        result.add_warning("Toutes les dates sont le meme jour")


def check_duplicates(records: list[dict], result: ValidationResult) -> None:
    """Check 4: Detect duplicate records."""
    if not records:
        return

    # Strategy: hash entire record
    hashes: Counter[str] = Counter()
    for rec in records:
        h = hashlib.md5(
            json.dumps(rec, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        hashes[h] += 1

    unique = len(hashes)
    total = len(records)
    dupes = total - unique
    dupe_pct = dupes / max(total, 1) * 100

    result.stats["records_uniques"] = unique
    result.stats["doublons_exacts"] = dupes

    if dupes == 0:
        result.add_info(f"Aucun doublon exact ({total} records uniques)")
    elif dupe_pct < 5:
        result.add_warning(
            f"{dupes} doublons exacts ({dupe_pct:.1f}%) sur {total} records"
        )
    else:
        result.add_error(
            f"{dupes} doublons exacts ({dupe_pct:.1f}%) sur {total} records"
        )

    # Show most duplicated records
    most_common = hashes.most_common(3)
    for h, count in most_common:
        if count > 1:
            result.add_info(f"  Hash {h[:12]}... : {count} occurrences")


def suggest_field_mappings(records: list[dict],
                           result: ValidationResult) -> None:
    """Check 5: Suggest field mappings to standard schema."""
    if not records:
        return

    # Get all source fields
    all_fields: set[str] = set()
    for rec in records:
        all_fields.update(rec.keys())

    suggestions: dict[str, str] = {}
    unmapped: list[str] = []

    for src_field in sorted(all_fields):
        src_lower = src_field.lower().strip()
        matched = False
        for std_field, aliases in FIELD_ALIASES.items():
            if src_lower in [a.lower() for a in aliases]:
                suggestions[src_field] = std_field
                matched = True
                break
        if not matched:
            unmapped.append(src_field)

    if suggestions:
        result.add_info(f"Mappings suggeres ({len(suggestions)} champs):")
        for src, std in sorted(suggestions.items()):
            result.add_info(f"  {src:40s} -> {std}")

    if unmapped:
        result.add_info(f"Champs non mappes ({len(unmapped)}):")
        for field in unmapped[:20]:
            # Show a sample value
            sample_val = None
            for rec in records[:10]:
                if field in rec and rec[field] is not None:
                    sample_val = rec[field]
                    break
            val_repr = repr(sample_val)[:50] if sample_val is not None else "null"
            result.add_info(f"  {field:40s} (ex: {val_repr})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Valide la sortie d'un nouveau scraper."
    )
    parser.add_argument(
        "--source-dir", required=True,
        help="Repertoire de sortie du scraper (ex: output/117_jockey_planet)"
    )
    parser.add_argument(
        "--required-fields", nargs="*",
        default=["date", "hippodrome"],
        help="Champs requis a verifier (default: date hippodrome)"
    )
    parser.add_argument(
        "--date-field", default="date",
        help="Nom du champ date (default: date)"
    )
    parser.add_argument(
        "--max-records", type=int, default=100000,
        help="Nombre max de records a lire (default: 100000)"
    )
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    if not source_dir.is_absolute():
        source_dir = PROJECT_ROOT / source_dir

    print(f"=== Validation de la source dans {source_dir} ===\n")

    result = ValidationResult()

    # Discover files
    jsonl_files = discover_jsonl_files(source_dir)
    if not jsonl_files:
        result.add_error(f"Aucun fichier JSONL/JSON dans {source_dir}")
        result.print_report()
        sys.exit(1)

    result.add_info(f"{len(jsonl_files)} fichier(s) trouves")
    for f in jsonl_files[:5]:
        result.add_info(f"  - {f.name} ({f.stat().st_size / 1024:.0f} Ko)")

    # Run all checks on each file
    all_records: list[dict] = []
    for jsonl_path in jsonl_files:
        result.add_info(f"\n--- Validation de {jsonl_path.name} ---")

        # Check 1: JSONL format
        records = check_jsonl_format(jsonl_path, args.max_records, result)
        all_records.extend(records)

    if all_records:
        # Check 2: Required fields
        check_required_fields(all_records, args.required_fields, result)

        # Check 3: Date range
        check_date_range(all_records, args.date_field, result)

        # Check 4: Duplicates
        check_duplicates(all_records, result)

        # Check 5: Field mapping suggestions
        suggest_field_mappings(all_records, result)

    result.print_report()
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
