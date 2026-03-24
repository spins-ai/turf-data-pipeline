#!/usr/bin/env python3
"""
backup_checksums.py — Pilier 2 : Securite - Checksums d'integrite
=================================================================
Genere des checksums SHA-256 pour tous les fichiers importants du projet
et les sauvegarde dans checksums.json.

Peut ensuite verifier l'integrite en comparant les checksums actuels
avec ceux enregistres.

Fichiers couverts :
  - data_master/*.jsonl
  - output/**/*.jsonl
  - *.py (scripts racine)
  - feature_builders/*.py
  - pipeline/**/*.py
  - quality/*.py

Usage :
    python security/backup_checksums.py                  # Generer les checksums
    python security/backup_checksums.py --verify         # Verifier l'integrite
    python security/backup_checksums.py --diff           # Montrer les differences
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
CHECKSUMS_FILE = BASE_DIR / "security" / "checksums.json"
LOG_DIR = BASE_DIR / "logs"

# Patterns de fichiers a inclure
FILE_PATTERNS = [
    ("data_master", "*.jsonl"),
    ("output", "**/*.jsonl"),
    (".", "*.py"),
    ("feature_builders", "*.py"),
    ("pipeline", "**/*.py"),
    ("quality", "*.py"),
    ("security", "*.py"),
    ("betting", "*.py"),
    ("labels", "*.py"),
    ("models", "*.py"),
    ("post_course", "*.py"),
]

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from utils.logging_setup import setup_logging
log = setup_logging("backup_checksums")


# ---------------------------------------------------------------------------
# SHA-256
# ---------------------------------------------------------------------------

def sha256_file(filepath: Path, block_size: int = 65536) -> str:
    """Calcule le SHA-256 d'un fichier en streaming."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            block = f.read(block_size)
            if not block:
                break
            hasher.update(block)
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# Collecte des fichiers
# ---------------------------------------------------------------------------

def collect_files() -> List[Path]:
    """Collecte tous les fichiers importants selon les patterns definis."""
    files: List[Path] = []
    seen: set = set()

    for rel_dir, pattern in FILE_PATTERNS:
        search_dir = BASE_DIR / rel_dir
        if not search_dir.exists():
            continue

        for filepath in search_dir.glob(pattern):
            if filepath.is_file() and filepath.resolve() not in seen:
                seen.add(filepath.resolve())
                files.append(filepath)

    return sorted(files)


def format_size(size_bytes: int) -> str:
    """Formate une taille en octets."""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    size = float(size_bytes)
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.1f} {units[idx]}"


# ---------------------------------------------------------------------------
# Generation des checksums
# ---------------------------------------------------------------------------

def generate_checksums() -> Dict[str, Any]:
    """
    Genere les checksums SHA-256 pour tous les fichiers importants.
    Retourne le dict complet pret a etre sauvegarde.
    """
    files = collect_files()
    log.info(f"Fichiers a hasher : {len(files)}")

    checksums: Dict[str, Dict[str, Any]] = {}
    total_size = 0

    for filepath in files:
        try:
            rel_path = filepath.relative_to(BASE_DIR)
        except ValueError:
            rel_path = filepath

        # Utiliser des slashes forward pour la portabilite
        key = str(rel_path).replace("\\", "/")

        try:
            size = filepath.stat().st_size
            sha = sha256_file(filepath)
            mtime = datetime.fromtimestamp(filepath.stat().st_mtime).isoformat()

            checksums[key] = {
                "sha256": sha,
                "size": size,
                "size_human": format_size(size),
                "modified": mtime,
            }
            total_size += size

        except (IOError, OSError) as e:
            log.warning(f"  Erreur sur {key} : {e}")
            checksums[key] = {"error": str(e)}

    result = {
        "generated_at": datetime.now().isoformat(),
        "base_dir": str(BASE_DIR),
        "total_files": len(checksums),
        "total_size": total_size,
        "total_size_human": format_size(total_size),
        "files": checksums,
    }

    return result


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_checksums() -> Tuple[int, int, int, List[str]]:
    """
    Verifie les checksums actuels contre ceux sauvegardes.
    Retourne (ok, changed, missing, messages).
    """
    if not CHECKSUMS_FILE.exists():
        return 0, 0, 0, ["checksums.json introuvable. Lancez d'abord sans --verify."]

    with open(CHECKSUMS_FILE, "r", encoding="utf-8") as f:
        saved = json.load(f)

    saved_files = saved.get("files", {})
    ok = 0
    changed = 0
    missing = 0
    messages: List[str] = []

    for key, info in saved_files.items():
        filepath = BASE_DIR / key

        if not filepath.exists():
            missing += 1
            messages.append(f"MISSING  {key}")
            continue

        if "error" in info:
            messages.append(f"SKIP     {key} (erreur originale)")
            continue

        saved_sha = info.get("sha256", "")
        current_sha = sha256_file(filepath)

        if current_sha == saved_sha:
            ok += 1
        else:
            changed += 1
            messages.append(f"CHANGED  {key}")
            messages.append(f"  avant : {saved_sha[:16]}...")
            messages.append(f"  apres : {current_sha[:16]}...")

    # Verifier les nouveaux fichiers
    current_files = collect_files()
    current_keys = set()
    for fp in current_files:
        try:
            rel = fp.relative_to(BASE_DIR)
        except ValueError:
            rel = fp
        current_keys.add(str(rel).replace("\\", "/"))

    new_files = current_keys - set(saved_files.keys())
    if new_files:
        messages.append(f"\nNOUVEAUX FICHIERS ({len(new_files)}) :")
        for nf in sorted(new_files):
            messages.append(f"  NEW    {nf}")

    return ok, changed, missing, messages


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------

def diff_checksums() -> List[str]:
    """Montre les differences entre checksums sauvegardes et actuels."""
    _, _, _, messages = verify_checksums()
    return messages


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Gestion des checksums SHA-256 pour l'integrite des donnees"
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Verifier l'integrite des fichiers vs checksums sauvegardes",
    )
    parser.add_argument(
        "--diff", action="store_true",
        help="Afficher les differences uniquement",
    )
    args = parser.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(CHECKSUMS_FILE.parent, exist_ok=True)

    if args.verify or args.diff:
        log.info("=" * 70)
        log.info("VERIFICATION D'INTEGRITE (SHA-256)")
        log.info(f"  Fichier checksums : {CHECKSUMS_FILE}")
        log.info("=" * 70)

        ok, changed, missing, messages = verify_checksums()

        for msg in messages:
            if msg.startswith("CHANGED") or msg.startswith("MISSING"):
                log.warning(msg)
            elif msg.startswith("NEW"):
                log.info(msg)
            else:
                log.info(msg)

        log.info("\n" + "=" * 70)
        log.info("RESUME VERIFICATION")
        log.info(f"  OK       : {ok}")
        log.info(f"  Modifies : {changed}")
        log.info(f"  Manquants: {missing}")

        status = "INTEGRITE OK" if changed == 0 and missing == 0 else "DIFFERENCES DETECTEES"
        log.info(f"  Statut   : {status}")
        log.info("=" * 70)

        sys.exit(0 if changed == 0 and missing == 0 else 1)

    else:
        log.info("=" * 70)
        log.info("GENERATION DES CHECKSUMS SHA-256")
        log.info(f"  Base dir : {BASE_DIR}")
        log.info("=" * 70)

        result = generate_checksums()

        with open(CHECKSUMS_FILE, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        log.info(f"\nChecksums sauvegardes : {CHECKSUMS_FILE}")
        log.info(f"  Fichiers hashes : {result['total_files']}")
        log.info(f"  Taille totale   : {result['total_size_human']}")
        log.info("=" * 70)


if __name__ == "__main__":
    main()
