#!/usr/bin/env python3
"""
pilier_auto_repair.py -- Pilier Qualite : Reparation automatique
=================================================================

Repare automatiquement les problemes courants dans les fichiers de donnees.

Fonctionnalites :
  1. Repare les fichiers JSON/JSONL tronques
  2. Re-encode les fichiers avec mauvais encodage
  3. Supprime les lignes dupliquees dans les JSONL
  4. Repare les checkpoints casses

Usage:
    python pilier_auto_repair.py
    python pilier_auto_repair.py --file data_master/partants_master.jsonl
    python pilier_auto_repair.py --dry-run
    python pilier_auto_repair.py --fix-encoding
    python pilier_auto_repair.py --fix-truncated
    python pilier_auto_repair.py --fix-duplicates
    python pilier_auto_repair.py --fix-checkpoints
"""

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
REPAIR_LOG = LOGS_DIR / "auto_repair.json"

# Encodages a tester
ENCODINGS_TO_TRY = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1", "ascii"]


# -----------------------------------------------------------------------
# 1. Reparation des fichiers JSON/JSONL tronques
# -----------------------------------------------------------------------

def repair_truncated_json(filepath: Path, dry_run: bool = False) -> dict:
    """Repare un fichier JSON tronque."""
    result = {
        "file": str(filepath.relative_to(BASE_DIR)),
        "type": "truncated_json",
        "repaired": False,
        "details": "",
    }

    # Skip les gros fichiers JSON (> 500 MB) pour eviter de saturer la RAM
    file_size = filepath.stat().st_size
    if file_size > 500_000_000:
        result["details"] = f"Skip (fichier trop gros: {file_size // 1_000_000} MB)"
        return result

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as e:
        result["details"] = f"Erreur lecture: {e}"
        return result

    if not content.strip():
        result["details"] = "Fichier vide"
        return result

    # Tester si le JSON est valide
    try:
        json.loads(content)
        result["details"] = "JSON valide, pas de reparation necessaire"
        return result
    except json.JSONDecodeError as e:
        result["details"] = f"JSON invalide: {e}"

    content_stripped = content.strip()

    # Strategies de reparation
    repaired_content = None

    # 1. JSON tronque: manque le ] ou } final
    if content_stripped.startswith("["):
        # Essayer d'ajouter ]
        # Trouver le dernier element valide
        try:
            # Retirer la virgule trainante et fermer
            trimmed = content_stripped.rstrip().rstrip(",").rstrip()
            if not trimmed.endswith("]"):
                trimmed += "\n]"
            json.loads(trimmed)
            repaired_content = trimmed
            result["details"] = "Ajout du ] final"
        except json.JSONDecodeError:
            # Essayer de trouver le dernier } valide
            last_brace = content_stripped.rfind("}")
            if last_brace > 0:
                trimmed = content_stripped[:last_brace + 1].rstrip().rstrip(",") + "\n]"
                try:
                    json.loads(trimmed)
                    repaired_content = trimmed
                    result["details"] = "Tronque au dernier objet valide + ]"
                except json.JSONDecodeError:
                    pass

    elif content_stripped.startswith("{"):
        # Objet JSON tronque
        try:
            # Compter les accolades
            open_count = content_stripped.count("{")
            close_count = content_stripped.count("}")
            missing = open_count - close_count
            if missing > 0:
                # Retirer la virgule trainante
                trimmed = content_stripped.rstrip().rstrip(",")
                trimmed += "}" * missing
                json.loads(trimmed)
                repaired_content = trimmed
                result["details"] = f"Ajout de {missing} accolade(s) fermante(s)"
        except json.JSONDecodeError:
            pass

    if repaired_content and not dry_run:
        # Backup
        backup = filepath.with_suffix(filepath.suffix + ".bak")
        shutil.copy2(filepath, backup)

        with open(filepath, "w", encoding="utf-8", errors="replace") as f:
            f.write(repaired_content)
        result["repaired"] = True
        result["backup"] = str(backup.relative_to(BASE_DIR))
    elif repaired_content and dry_run:
        result["repaired"] = False
        result["would_repair"] = True
    else:
        result["details"] = "Reparation automatique impossible"

    return result


def repair_truncated_jsonl(filepath: Path, dry_run: bool = False) -> dict:
    """Repare un fichier JSONL avec des lignes invalides. Streaming pour gros fichiers."""
    result = {
        "file": str(filepath.relative_to(BASE_DIR)),
        "type": "truncated_jsonl",
        "repaired": False,
        "lines_total": 0,
        "lines_valid": 0,
        "lines_removed": 0,
    }

    file_size = filepath.stat().st_size
    big_file = file_size > 500_000_000  # > 500 MB

    if big_file:
        # STREAMING: 2 passes pour eviter de charger en RAM
        # Pass 1: compter les invalides
        invalid_count = 0
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace", buffering=1048576) as f:
                line = f.readline()
                while line:
                    stripped = line.strip()
                    if stripped:
                        result["lines_total"] += 1
                        try:
                            json.loads(stripped)
                            result["lines_valid"] += 1
                        except json.JSONDecodeError:
                            invalid_count += 1
                    line = f.readline()
        except Exception as e:
            result["details"] = f"Erreur lecture: {e}"
            return result

        result["lines_removed"] = invalid_count
        if invalid_count == 0:
            result["details"] = "Toutes les lignes sont valides"
            return result

        result["details"] = f"{invalid_count} ligne(s) invalide(s) (fichier {file_size // 1_000_000} MB)"

        if not dry_run:
            backup = filepath.with_suffix(filepath.suffix + ".bak")
            shutil.copy2(filepath, backup)
            tmp = filepath.with_suffix(".tmp")
            with open(filepath, "r", encoding="utf-8", errors="replace", buffering=1048576) as fin, \
                 open(tmp, "w", encoding="utf-8", errors="replace", buffering=1048576) as fout:
                line = fin.readline()
                while line:
                    stripped = line.strip()
                    if stripped:
                        try:
                            json.loads(stripped)
                            fout.write(stripped + "\n")
                        except json.JSONDecodeError:
                            pass
                    line = fin.readline()
            tmp.replace(filepath)
            result["repaired"] = True
            result["backup"] = str(backup.relative_to(BASE_DIR))
        else:
            result["would_repair"] = True
        return result

    # Petits fichiers: methode originale en memoire
    valid_lines = []
    invalid_count = 0

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                result["lines_total"] += 1
                try:
                    json.loads(line)
                    valid_lines.append(line)
                    result["lines_valid"] += 1
                except json.JSONDecodeError:
                    invalid_count += 1
    except Exception as e:
        result["details"] = f"Erreur lecture: {e}"
        return result

    result["lines_removed"] = invalid_count

    if invalid_count == 0:
        result["details"] = "Toutes les lignes sont valides"
        return result

    result["details"] = f"{invalid_count} ligne(s) invalide(s) retiree(s)"

    if not dry_run:
        backup = filepath.with_suffix(filepath.suffix + ".bak")
        shutil.copy2(filepath, backup)

        with open(filepath, "w", encoding="utf-8", errors="replace") as f:
            for line in valid_lines:
                f.write(line + "\n")
        result["repaired"] = True
        result["backup"] = str(backup.relative_to(BASE_DIR))
    else:
        result["would_repair"] = True

    return result


# -----------------------------------------------------------------------
# 2. Re-encodage des fichiers
# -----------------------------------------------------------------------

def detect_encoding(filepath: Path) -> str:
    """Detecte l'encodage d'un fichier."""
    # Lire les premiers octets
    with open(filepath, "rb") as f:
        raw = f.read(8192)

    # BOM UTF-8
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    # BOM UTF-16
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return "utf-16"

    # Tester chaque encodage
    for enc in ENCODINGS_TO_TRY:
        try:
            raw.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue

    return "unknown"


def fix_encoding(filepath: Path, dry_run: bool = False) -> dict:
    """Re-encode un fichier en UTF-8."""
    result = {
        "file": str(filepath.relative_to(BASE_DIR)),
        "type": "encoding",
        "repaired": False,
    }

    # Skip gros fichiers (> 500 MB) - deja en utf-8 avec errors=replace
    if filepath.stat().st_size > 500_000_000:
        result["details"] = f"Skip (fichier trop gros: {filepath.stat().st_size // 1_000_000} MB)"
        return result

    detected = detect_encoding(filepath)
    result["detected_encoding"] = detected

    if detected in ("utf-8", "ascii"):
        result["details"] = "Deja en UTF-8"
        return result

    if detected == "unknown":
        result["details"] = "Encodage non detecte"
        return result

    # Lire avec l'encodage detecte
    try:
        with open(filepath, "r", encoding=detected, errors="replace") as f:
            content = f.read()
    except Exception as e:
        result["details"] = f"Erreur lecture: {e}"
        return result

    result["details"] = f"Re-encode de {detected} vers utf-8"

    if not dry_run:
        backup = filepath.with_suffix(filepath.suffix + ".bak")
        shutil.copy2(filepath, backup)

        with open(filepath, "w", encoding="utf-8", errors="replace") as f:
            f.write(content)
        result["repaired"] = True
        result["backup"] = str(backup.relative_to(BASE_DIR))
    else:
        result["would_repair"] = True

    return result


# -----------------------------------------------------------------------
# 3. Deduplication JSONL
# -----------------------------------------------------------------------

def remove_duplicates_jsonl(filepath: Path, dry_run: bool = False) -> dict:
    """Supprime les lignes dupliquees dans un fichier JSONL. Streaming pour gros fichiers."""
    result = {
        "file": str(filepath.relative_to(BASE_DIR)),
        "type": "duplicates",
        "repaired": False,
        "lines_total": 0,
        "lines_unique": 0,
        "lines_removed": 0,
    }

    file_size = filepath.stat().st_size

    # Pour les gros fichiers: streaming avec hash set uniquement (pas de stockage des lignes)
    big_file = file_size > 500_000_000  # > 500 MB

    if big_file:
        # Pass 1: compter les doublons avec hash set
        seen_hashes = set()
        dup_count = 0
        total = 0
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace", buffering=1048576) as f:
                line = f.readline()
                while line:
                    stripped = line.strip()
                    if stripped:
                        total += 1
                        try:
                            obj = json.loads(stripped)
                            normalized = json.dumps(obj, sort_keys=True, ensure_ascii=False)
                            h = hashlib.md5(normalized.encode("utf-8", errors="replace")).hexdigest()
                        except json.JSONDecodeError:
                            h = hashlib.md5(stripped.encode("utf-8", errors="replace")).hexdigest()
                        if h in seen_hashes:
                            dup_count += 1
                        else:
                            seen_hashes.add(h)
                    line = f.readline()
        except Exception as e:
            result["details"] = f"Erreur lecture: {e}"
            return result

        result["lines_total"] = total
        result["lines_unique"] = total - dup_count
        result["lines_removed"] = dup_count

        if dup_count == 0:
            result["details"] = f"Aucun doublon ({file_size // 1_000_000} MB)"
            return result

        result["details"] = f"{dup_count} doublon(s) ({file_size // 1_000_000} MB)"

        if not dry_run:
            backup = filepath.with_suffix(filepath.suffix + ".bak")
            shutil.copy2(filepath, backup)
            # Pass 2: ecrire en streaming sans doublons
            seen_hashes2 = set()
            tmp = filepath.with_suffix(".tmp")
            with open(filepath, "r", encoding="utf-8", errors="replace", buffering=1048576) as fin, \
                 open(tmp, "w", encoding="utf-8", errors="replace", buffering=1048576) as fout:
                line = fin.readline()
                while line:
                    stripped = line.strip()
                    if stripped:
                        try:
                            obj = json.loads(stripped)
                            normalized = json.dumps(obj, sort_keys=True, ensure_ascii=False)
                            h = hashlib.md5(normalized.encode("utf-8", errors="replace")).hexdigest()
                        except json.JSONDecodeError:
                            h = hashlib.md5(stripped.encode("utf-8", errors="replace")).hexdigest()
                        if h not in seen_hashes2:
                            seen_hashes2.add(h)
                            fout.write(stripped + "\n")
                    line = fin.readline()
            tmp.replace(filepath)
            result["repaired"] = True
            result["backup"] = str(backup.relative_to(BASE_DIR))
        else:
            result["would_repair"] = True
        return result

    # Petits fichiers: methode originale en memoire
    seen_hashes = set()
    unique_lines = []

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                result["lines_total"] += 1
                try:
                    obj = json.loads(line)
                    normalized = json.dumps(obj, sort_keys=True, ensure_ascii=False)
                    h = hashlib.md5(normalized.encode("utf-8", errors="replace")).hexdigest()
                except json.JSONDecodeError:
                    h = hashlib.md5(line.encode("utf-8", errors="replace")).hexdigest()
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    unique_lines.append(line)
    except Exception as e:
        result["details"] = f"Erreur lecture: {e}"
        return result

    result["lines_unique"] = len(unique_lines)
    result["lines_removed"] = result["lines_total"] - len(unique_lines)

    if result["lines_removed"] == 0:
        result["details"] = "Aucun doublon"
        return result

    result["details"] = f"{result['lines_removed']} doublon(s) retire(s)"

    if not dry_run:
        backup = filepath.with_suffix(filepath.suffix + ".bak")
        shutil.copy2(filepath, backup)
        with open(filepath, "w", encoding="utf-8", errors="replace") as f:
            for line in unique_lines:
                f.write(line + "\n")
        result["repaired"] = True
        result["backup"] = str(backup.relative_to(BASE_DIR))
    else:
        result["would_repair"] = True

    return result


# -----------------------------------------------------------------------
# 4. Reparation des checkpoints
# -----------------------------------------------------------------------

def repair_checkpoints(dry_run: bool = False) -> list[dict]:
    """Repare les fichiers checkpoint casses."""
    results = []

    # Chercher les checkpoints
    checkpoint_patterns = [
        BASE_DIR / "output" / "**" / "checkpoint*.json",
        BASE_DIR / "output" / "**" / "*.checkpoint",
        BASE_DIR / "logs" / "*.checkpoint.json",
        BASE_DIR / "**" / "progress_*.json",
    ]

    checkpoint_files = []
    for pattern in checkpoint_patterns:
        parent = pattern.parent
        glob_part = pattern.name
        if parent.exists():
            checkpoint_files.extend(parent.rglob(glob_part))

    # Aussi chercher les fichiers .tmp orphelins
    for d in [DATA_MASTER, OUTPUT_DIR]:
        if d.exists():
            for f in d.rglob("*.tmp"):
                checkpoint_files.append(f)

    for cp_file in checkpoint_files:
        result = {
            "file": str(cp_file.relative_to(BASE_DIR)),
            "type": "checkpoint",
            "repaired": False,
        }

        # Fichiers .tmp : verifier si le fichier final existe
        if cp_file.suffix == ".tmp":
            final_name = cp_file.stem  # sans le .tmp
            final_path = cp_file.parent / final_name

            if final_path.exists():
                # Le fichier final existe, le .tmp est orphelin
                if cp_file.stat().st_size > final_path.stat().st_size:
                    # Le .tmp est plus gros -> probablement plus recent
                    result["details"] = (
                        f".tmp plus recent ({cp_file.stat().st_size} > {final_path.stat().st_size})"
                    )
                    if not dry_run:
                        backup = final_path.with_suffix(final_path.suffix + ".bak")
                        shutil.copy2(final_path, backup)
                        shutil.move(str(cp_file), str(final_path))
                        result["repaired"] = True
                        result["action"] = "tmp_promoted"
                    else:
                        result["would_repair"] = True
                        result["action"] = "would_promote_tmp"
                else:
                    result["details"] = ".tmp orphelin (final existe et est plus gros)"
                    if not dry_run:
                        cp_file.unlink()
                        result["repaired"] = True
                        result["action"] = "tmp_removed"
                    else:
                        result["would_repair"] = True
                        result["action"] = "would_remove_tmp"
            else:
                # Pas de fichier final, renommer le .tmp
                result["details"] = ".tmp sans fichier final"
                if cp_file.suffix == ".tmp" and cp_file.stem.endswith(
                    (".json", ".jsonl", ".csv")
                ):
                    if not dry_run:
                        shutil.move(str(cp_file), str(final_path))
                        result["repaired"] = True
                        result["action"] = "tmp_renamed"
                    else:
                        result["would_repair"] = True
            results.append(result)
            continue

        # Fichiers checkpoint JSON : verifier la validite
        try:
            with open(cp_file, "r", encoding="utf-8", errors="replace") as f:
                content = f.read().strip()

            if not content:
                result["details"] = "Checkpoint vide"
                if not dry_run:
                    cp_file.unlink()
                    result["repaired"] = True
                    result["action"] = "empty_removed"
                else:
                    result["would_repair"] = True
                results.append(result)
                continue

            try:
                json.loads(content)
                result["details"] = "Checkpoint valide"
            except json.JSONDecodeError:
                result["details"] = "Checkpoint JSON invalide"
                # Tenter reparation
                repair_result = repair_truncated_json(cp_file, dry_run)
                result["repaired"] = repair_result.get("repaired", False)
                result["repair_details"] = repair_result.get("details", "")

        except Exception as e:
            result["details"] = f"Erreur: {e}"

        results.append(result)

    return results


# -----------------------------------------------------------------------
# Orchestration
# -----------------------------------------------------------------------

def find_data_files(specific_file: str = None) -> list[Path]:
    """Trouve tous les fichiers de donnees."""
    if specific_file:
        target = BASE_DIR / specific_file
        if target.exists():
            return [target]
        print(f"ERREUR: Fichier introuvable: {specific_file}")
        return []

    files = []
    # Dossiers a ignorer (corrompus sur disque, WinError 1392)
    # Ignorer les caches (centaines de milliers de petits fichiers)
    skip_dirs = {"cache_corrupted", "cache", ".git", "__pycache__", "node_modules"}
    for d in [DATA_MASTER, OUTPUT_DIR]:
        if d.exists():
            for root, dirs, filenames in os.walk(d):
                dirs[:] = [dn for dn in dirs if dn not in skip_dirs]
                for fname in filenames:
                    fpath = Path(root) / fname
                    if fpath.suffix in (".json", ".jsonl") and not fname.endswith(".bak"):
                        files.append(fpath)
    return sorted(files)


def run_all_repairs(
    files: list[Path],
    dry_run: bool = False,
    fix_truncated: bool = True,
    fix_enc: bool = True,
    fix_dupes: bool = True,
    fix_cp: bool = True,
) -> dict:
    """Execute toutes les reparations."""
    report = {
        "generated_at": datetime.now().isoformat() + "Z",
        "dry_run": dry_run,
        "repairs": [],
        "summary": {
            "files_scanned": 0,
            "repairs_done": 0,
            "repairs_failed": 0,
        },
    }

    # 1. Fichiers tronques
    if fix_truncated:
        print("[1/4] Reparation des fichiers tronques ...")
        for f in files:
            report["summary"]["files_scanned"] += 1
            if f.suffix == ".jsonl":
                result = repair_truncated_jsonl(f, dry_run)
            elif f.suffix == ".json":
                result = repair_truncated_json(f, dry_run)
            else:
                continue

            if result.get("repaired") or result.get("would_repair"):
                report["repairs"].append(result)
                if result.get("repaired"):
                    report["summary"]["repairs_done"] += 1
                    print(f"  REPARE: {result['file']} -- {result.get('details', '')}")
                else:
                    print(f"  [DRY] {result['file']} -- {result.get('details', '')}")
    else:
        print("[1/4] Reparation tronques: IGNORE")

    # 2. Encodage
    if fix_enc:
        print("[2/4] Verification des encodages ...")
        for f in files:
            result = fix_encoding(f, dry_run)
            if result.get("repaired") or result.get("would_repair"):
                report["repairs"].append(result)
                if result.get("repaired"):
                    report["summary"]["repairs_done"] += 1
                    print(f"  REPARE: {result['file']} -- {result.get('details', '')}")
                else:
                    print(f"  [DRY] {result['file']} -- {result.get('details', '')}")
    else:
        print("[2/4] Verification encodage: IGNORE")

    # 3. Doublons JSONL
    if fix_dupes:
        print("[3/4] Deduplication JSONL ...")
        for f in files:
            if f.suffix != ".jsonl":
                continue
            result = remove_duplicates_jsonl(f, dry_run)
            if result.get("repaired") or result.get("would_repair"):
                report["repairs"].append(result)
                if result.get("repaired"):
                    report["summary"]["repairs_done"] += 1
                    print(f"  REPARE: {result['file']} -- {result.get('details', '')}")
                else:
                    print(f"  [DRY] {result['file']} -- {result.get('details', '')}")
    else:
        print("[3/4] Deduplication: IGNORE")

    # 4. Checkpoints
    if fix_cp:
        print("[4/4] Reparation des checkpoints ...")
        cp_results = repair_checkpoints(dry_run)
        for result in cp_results:
            if result.get("repaired") or result.get("would_repair"):
                report["repairs"].append(result)
                if result.get("repaired"):
                    report["summary"]["repairs_done"] += 1
                    print(f"  REPARE: {result['file']} -- {result.get('details', '')}")
                else:
                    print(f"  [DRY] {result['file']} -- {result.get('details', '')}")
    else:
        print("[4/4] Checkpoints: IGNORE")

    return report


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Reparation automatique des donnees")
    parser.add_argument("--file", "-f", help="Fichier specifique a reparer")
    parser.add_argument("--dry-run", "-n", action="store_true",
                        help="Mode simulation (pas de modification)")
    parser.add_argument("--fix-truncated", action="store_true",
                        help="Reparer uniquement les fichiers tronques")
    parser.add_argument("--fix-encoding", action="store_true",
                        help="Reparer uniquement les encodages")
    parser.add_argument("--fix-duplicates", action="store_true",
                        help="Supprimer uniquement les doublons")
    parser.add_argument("--fix-checkpoints", action="store_true",
                        help="Reparer uniquement les checkpoints")
    parser.add_argument("--output", "-o", help="Fichier rapport de sortie")
    args = parser.parse_args()

    print("=" * 60)
    print("PILIER AUTO REPAIR")
    if args.dry_run:
        print("  MODE: Simulation (aucune modification)")
    else:
        print("  MODE: Reparation (avec backup)")
    print("=" * 60)

    # Determiner quoi reparer
    specific_fixes = any([
        args.fix_truncated, args.fix_encoding,
        args.fix_duplicates, args.fix_checkpoints,
    ])

    fix_truncated = args.fix_truncated if specific_fixes else True
    fix_enc = args.fix_encoding if specific_fixes else True
    fix_dupes = args.fix_duplicates if specific_fixes else True
    fix_cp = args.fix_checkpoints if specific_fixes else True

    # Trouver les fichiers
    files = find_data_files(args.file)
    print(f"Fichiers a scanner: {len(files)}")
    print("-" * 60)

    # Lancer les reparations
    report = run_all_repairs(
        files,
        dry_run=args.dry_run,
        fix_truncated=fix_truncated,
        fix_enc=fix_enc,
        fix_dupes=fix_dupes,
        fix_cp=fix_cp,
    )

    # Sauvegarder le rapport
    out_path = Path(args.output) if args.output else REPAIR_LOG
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("-" * 60)
    summary = report["summary"]
    print(f"Fichiers scannes: {summary['files_scanned']}")
    print(f"Reparations effectuees: {summary['repairs_done']}")
    print(f"Rapport: {out_path}")

    if args.dry_run:
        would_repair = sum(1 for r in report["repairs"] if r.get("would_repair"))
        print(f"Reparations possibles (dry-run): {would_repair}")

    print("=" * 60)


if __name__ == "__main__":
    main()
