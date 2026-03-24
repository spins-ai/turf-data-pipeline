#!/usr/bin/env python3
"""
organize_project.py
===================
Reorganise le projet turf-data-pipeline en une arborescence propre.

Etat actuel : tous les scripts sont en vrac a la racine (00-55, feat_*, merge_*, etc.)

Structure cible :
    scripts/
      collection/     # 00-40 + 51-60 (scrapers)
      calcul/         # 41-49 (scripts de calcul / croisement)
      merge/          # merge_*.py, mega_merge_*.py
      pipeline/       # audit_*, nettoyage_*, deduplication.py, comblage_*.py, entity_resolution.py
      utils/          # hippodromes_db.py, test_endpoints.py, patch_*.py, parse_*.py,
                      # fill_*.py, fix_*.py, renormaliser.py, postprocess_*.py,
                      # fetch_openmeteo_missing.py, enrichissement_meteo_nasa.py,
                      # organize_model_data.py, organize_pipeline.py
    feature_builders/ # Deja existant — on ne touche pas
    features/         # feat_*.py + feature_engineering.py + master_feature_builder.py
    quality/          # Deja existant — on ne touche pas
    docs/             # Deja existant — on ne touche pas

Modes :
    python organize_project.py --dry-run      # Montre ce qui serait fait
    python organize_project.py --execute      # Execute le deplacement
    python organize_project.py --undo         # Annule via migration_log.json

Produit migration_log.json pour tracer et permettre l'annulation.
"""
from __future__ import annotations

import argparse
import hashlib
import json

import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent

MIGRATION_LOG = ROOT / "migration_log.json"

# Directories that already exist and should NOT be touched
KEEP_AS_IS = {"feature_builders", "quality", "docs", "betting", "labels",
              "models", "output", "post_course", "turf", "pipeline", ".claude",
              "__pycache__", ".git", "data_master", "logs", "data"}

from utils.logging_setup import setup_logging
log = setup_logging("organize")


# ---------------------------------------------------------------------------
# Classification rules
# ---------------------------------------------------------------------------

def classify_file(filename: str) -> Optional[str]:
    """Return the target subdirectory for a root-level .py file, or None to skip."""

    base = filename  # e.g. "04_resultats.py"

    # --- feat_*.py  +  feature_engineering.py  +  master_feature_builder.py ---
    if base.startswith("feat_"):
        return "features"
    if base in ("feature_engineering.py", "master_feature_builder.py"):
        return "features"

    # --- merge / mega_merge ---
    if base.startswith("merge_") or base.startswith("mega_merge_"):
        return "scripts/merge"

    # --- pipeline: audit, nettoyage, deduplication, comblage, entity_resolution ---
    if base.startswith("audit_"):
        return "scripts/pipeline"
    if base.startswith("nettoyage_"):
        return "scripts/pipeline"
    if base.startswith("comblage_"):
        return "scripts/pipeline"
    if base in ("deduplication.py", "entity_resolution.py"):
        return "scripts/pipeline"

    # --- utils: patch, parse, fill, fix, postprocess, renormaliser, hippodromes_db,
    #            test_endpoints, fetch_openmeteo_missing, enrichissement_meteo_nasa,
    #            organize_model_data, organize_pipeline ---
    if base.startswith("patch_"):
        return "scripts/utils"
    if base.startswith("parse_"):
        return "scripts/utils"
    if base.startswith("fill_"):
        return "scripts/utils"
    if base.startswith("fix_"):
        return "scripts/utils"
    if base.startswith("postprocess_"):
        return "scripts/utils"
    if base in ("hippodromes_db.py", "test_endpoints.py", "renormaliser.py",
                "fetch_openmeteo_missing.py", "enrichissement_meteo_nasa.py",
                "organize_model_data.py", "organize_pipeline.py"):
        return "scripts/utils"

    # --- numbered scripts: 41-49 => calcul ---
    m = re.match(r"^(\d+)", base)
    if m:
        num = int(m.group(1))
        if 41 <= num <= 49:
            return "scripts/calcul"
        # 00-40 and 51-60 => collection
        if (0 <= num <= 40) or (51 <= num <= 60):
            return "scripts/collection"

    # Not classified — skip
    return None


# ---------------------------------------------------------------------------
# Path / import rewriting helpers
# ---------------------------------------------------------------------------

# Patterns we look for inside scripts to update file references
# These match common Python patterns for specifying output/input paths

# Regex: from <module> import ... | import <module>
RE_IMPORT_FROM = re.compile(r'^(\s*from\s+)(\w[\w.]*)(\s+import\s+.*)$', re.MULTILINE)
RE_IMPORT_PLAIN = re.compile(r'^(\s*import\s+)(\w[\w.]*(?:\s*,\s*\w[\w.]*)*)(.*)$', re.MULTILINE)

# Common path patterns in scripts (quoted strings that look like relative paths to output/ or data dirs)
RE_PATH_STRINGS = re.compile(
    r'''(["'])((?:output/|data_master/|logs/|data/)[^"']*)\1'''
)

# os.makedirs("output/...", ...) or similar
RE_MAKEDIRS = re.compile(
    r'''(os\.makedirs\s*\(\s*["'])((?:output|data_master|logs|data)(?:/[^"']*)?)(["'])'''
)


def compute_relative_prefix(dest_subdir: str) -> str:
    """Compute how many '../' we need to get back to project root from dest_subdir.

    E.g. 'scripts/collection' => '../../'
         'features'           => '../'
    """
    depth = dest_subdir.count("/") + 1
    return "../" * depth


def rewrite_file_content(content: str, dest_subdir: str, filename: str) -> Tuple[str, List[str]]:
    """Rewrite path references inside a script so they still resolve from the new location.

    Returns (new_content, list_of_changes_made).
    """
    changes: List[str] = []
    prefix = compute_relative_prefix(dest_subdir)

    new_content = content

    # --- Rewrite quoted path strings that reference output/, data_master/, logs/, data/ ---
    def rewrite_path_string(m):
        quote = m.group(1)
        path_val = m.group(2)
        # Don't touch if it already has ../ prefix
        if path_val.startswith("../"):
            return m.group(0)
        new_path = prefix + path_val
        changes.append(f"  path: {quote}{path_val}{quote} -> {quote}{new_path}{quote}")
        return f"{quote}{new_path}{quote}"

    new_content = RE_PATH_STRINGS.sub(rewrite_path_string, new_content)

    # --- Rewrite os.makedirs with those directories ---
    def rewrite_makedirs(m):
        pre = m.group(1)
        path_val = m.group(2)
        post = m.group(3)
        if path_val.startswith("../"):
            return m.group(0)
        new_path = prefix + path_val
        changes.append(f"  makedirs: {path_val} -> {new_path}")
        return f"{pre}{new_path}{post}"

    new_content = RE_MAKEDIRS.sub(rewrite_makedirs, new_content)

    # --- Rewrite bare directory strings used standalone (common pattern) ---
    # E.g.  OUTPUT_DIR = "output/04_resultats"
    for dirname in ("output", "data_master", "logs", "data"):
        pattern = re.compile(
            rf'''(["'])({dirname})(\1)'''
        )
        def rewrite_bare(m, dirname=dirname):
            quote = m.group(1)
            val = m.group(2)
            new_val = prefix.rstrip("/") + "/" + val if prefix else val
            # Only if it's actually a bare dir reference
            if val == dirname:
                changes.append(f"  bare dir: {quote}{val}{quote} -> {quote}{new_val}{quote}")
                return f"{quote}{new_val}{quote}"
            return m.group(0)
        new_content = pattern.sub(rewrite_bare, new_content)

    # --- Rewrite local imports: from hippodromes_db import ... ---
    # We need to know where other files are going to fix cross-imports.
    # For now, we add a sys.path fixup at the top of moved files.

    return new_content, changes


def add_sys_path_fixup(content: str, dest_subdir: str) -> str:
    """Insert a sys.path fixup near the top of the file so that imports from the
    project root still work after the file is moved.

    Adds:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    right after the docstring / initial comments.
    """
    prefix = compute_relative_prefix(dest_subdir)
    depth = dest_subdir.count("/") + 1
    parents = ", ".join(["'..'" for _ in range(depth)])

    fixup_line = (
        f"import sys as _sys, os as _os  # auto-added by organize_project.py\n"
        f"_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), {parents}))  # project root\n"
    )

    # Check if fixup already present
    if "organize_project.py" in content:
        return content

    # Insert after shebang + docstring
    lines = content.split("\n")
    insert_idx = 0

    # Skip shebang
    if lines and lines[0].startswith("#!"):
        insert_idx = 1
    # Skip encoding declaration
    if insert_idx < len(lines) and lines[insert_idx].startswith("# -*-"):
        insert_idx += 1
    # Skip docstring
    if insert_idx < len(lines):
        stripped = lines[insert_idx].strip()
        if stripped.startswith('"""') or stripped.startswith("'''"):
            quote = stripped[:3]
            if stripped.count(quote) >= 2 and len(stripped) > 6:
                # Single-line docstring
                insert_idx += 1
            else:
                # Multi-line docstring: find closing
                insert_idx += 1
                while insert_idx < len(lines):
                    if quote in lines[insert_idx]:
                        insert_idx += 1
                        break
                    insert_idx += 1

    # Skip any blank lines after docstring
    while insert_idx < len(lines) and lines[insert_idx].strip() == "":
        insert_idx += 1

    lines.insert(insert_idx, fixup_line)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class MoveRecord:
    original: str        # relative path from ROOT
    destination: str     # relative path from ROOT
    sha256_before: str
    sha256_after: str
    path_changes: List[str] = field(default_factory=list)
    timestamp: str = ""


def build_move_plan() -> List[Tuple[str, str]]:
    """Scan root-level .py files and return list of (filename, dest_subdir)."""
    plan = []
    for entry in sorted(ROOT.iterdir()):
        if entry.is_file() and entry.suffix == ".py":
            dest = classify_file(entry.name)
            if dest is not None:
                plan.append((entry.name, dest))
    return plan


def dry_run(plan: List[Tuple[str, str]]) -> None:
    """Print what would happen without doing anything."""
    if not plan:
        log.info("Nothing to move — all files are already organized or unclassified.")
        return

    # Group by destination
    by_dest: Dict[str, List[str]] = {}
    for fname, dest in plan:
        by_dest.setdefault(dest, []).append(fname)

    log.info("=" * 70)
    log.info("DRY RUN — %d files would be moved", len(plan))
    log.info("=" * 70)

    for dest in sorted(by_dest):
        files = by_dest[dest]
        log.info("")
        log.info("  %s/ (%d files)", dest, len(files))
        for f in files:
            log.info("    <- %s", f)

    log.info("")
    log.info("Directories to create:")
    for d in sorted(set(dest for _, dest in plan)):
        target = ROOT / d
        if not target.exists():
            log.info("    mkdir %s/", d)
        else:
            log.info("    (exists) %s/", d)

    log.info("")
    log.info("Use --execute to perform the migration.")


def execute(plan: List[Tuple[str, str]]) -> None:
    """Execute the migration: copy, rewrite, verify, delete original."""
    if not plan:
        log.info("Nothing to move.")
        return

    records: List[MoveRecord] = []
    errors: List[str] = []

    # 1. Create all target directories
    dirs_needed = sorted(set(dest for _, dest in plan))
    for d in dirs_needed:
        target_dir = ROOT / d
        target_dir.mkdir(parents=True, exist_ok=True)
        # Add __init__.py so imports work
        init_file = target_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text("# Auto-generated by organize_project.py\n", encoding="utf-8")
            log.info("Created %s/__init__.py", d)

    log.info("=" * 70)
    log.info("EXECUTING MIGRATION — %d files", len(plan))
    log.info("=" * 70)

    for fname, dest in plan:
        src = ROOT / fname
        dst = ROOT / dest / fname
        log.info("")
        log.info("Moving %s -> %s/%s", fname, dest, fname)

        if dst.exists():
            log.warning("  SKIP: destination already exists: %s", dst.relative_to(ROOT))
            continue

        try:
            # Read original
            content = src.read_text(encoding="utf-8")
            sha_before = sha256_file(src)

            # Rewrite paths
            new_content, path_changes = rewrite_file_content(content, dest, fname)

            # Add sys.path fixup
            new_content = add_sys_path_fixup(new_content, dest)

            if path_changes:
                for c in path_changes:
                    log.info(c)

            # Write to destination
            dst.write_text(new_content, encoding="utf-8")

            # Verify the destination was written correctly by checking it's non-empty
            # and the file exists
            if not dst.exists() or dst.stat().st_size == 0:
                raise RuntimeError(f"Destination file is empty or missing: {dst}")

            sha_after = sha256_file(dst)

            # Record
            rec = MoveRecord(
                original=fname,
                destination=str(Path(dest) / fname),
                sha256_before=sha_before,
                sha256_after=sha_after,
                path_changes=path_changes,
                timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
            records.append(rec)

            # Delete original
            src.unlink()
            log.info("  OK (sha256_src=%s...)", sha_before[:12])

        except Exception as e:
            msg = f"ERROR moving {fname}: {e}"
            log.error(msg)
            errors.append(msg)
            # If dest was partially written, clean up
            if dst.exists():
                dst.unlink()

    # Write migration log
    log_data = {
        "version": 1,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "root": str(ROOT),
        "total_files": len(plan),
        "moved": len(records),
        "errors": len(errors),
        "records": [asdict(r) for r in records],
        "error_details": errors,
    }
    MIGRATION_LOG.write_text(json.dumps(log_data, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info("")
    log.info("=" * 70)
    log.info("DONE: %d/%d files moved, %d errors", len(records), len(plan), len(errors))
    log.info("Migration log: %s", MIGRATION_LOG.relative_to(ROOT))
    log.info("=" * 70)

    if errors:
        log.warning("Errors encountered:")
        for e in errors:
            log.warning("  %s", e)


def undo() -> None:
    """Reverse the migration using migration_log.json."""
    if not MIGRATION_LOG.exists():
        log.error("No migration_log.json found — cannot undo.")
        sys.exit(1)

    log_data = json.loads(MIGRATION_LOG.read_text(encoding="utf-8"))
    records = log_data.get("records", [])

    if not records:
        log.info("Migration log is empty — nothing to undo.")
        return

    log.info("=" * 70)
    log.info("UNDOING MIGRATION — %d files to restore", len(records))
    log.info("=" * 70)

    restored = 0
    errors = []

    for rec in records:
        original = rec["original"]
        destination = rec["destination"]
        sha_before = rec["sha256_before"]

        src_path = ROOT / destination  # current location (moved file)
        dst_path = ROOT / original     # original location

        log.info("Restoring %s <- %s", original, destination)

        if not src_path.exists():
            msg = f"  SKIP: moved file not found: {destination}"
            log.warning(msg)
            errors.append(msg)
            continue

        if dst_path.exists():
            msg = f"  SKIP: original location already has a file: {original}"
            log.warning(msg)
            errors.append(msg)
            continue

        try:
            # We need to reverse the path rewriting. The simplest safe approach:
            # read the moved file, undo the sys.path fixup and path prefix changes.
            content = src_path.read_text(encoding="utf-8")

            # Remove the sys.path fixup lines
            lines = content.split("\n")
            new_lines = []
            for line in lines:
                if "auto-added by organize_project.py" in line:
                    continue
                if "project root" in line and "_sys.path.insert" in line:
                    continue
                new_lines.append(line)
            content = "\n".join(new_lines)

            # Reverse path prefix: remove the ../../ or ../ prefix from paths
            prefix = compute_relative_prefix(str(Path(destination).parent))
            if prefix:
                # Escape for regex
                escaped = re.escape(prefix)
                content = re.sub(escaped, "", content)

            # Write back to original location
            dst_path.write_text(content, encoding="utf-8")

            # Verify
            if not dst_path.exists() or dst_path.stat().st_size == 0:
                raise RuntimeError(f"Restored file is empty or missing: {dst_path}")

            # Remove moved file
            src_path.unlink()
            restored += 1
            log.info("  OK")

        except Exception as e:
            msg = f"ERROR restoring {original}: {e}"
            log.error(msg)
            errors.append(msg)

    # Clean up empty directories
    for rec in records:
        dest_dir = (ROOT / rec["destination"]).parent
        try:
            # Remove __init__.py if we created it
            init_file = dest_dir / "__init__.py"
            if init_file.exists():
                init_content = init_file.read_text(encoding="utf-8")
                if "organize_project.py" in init_content:
                    init_file.unlink()
            # Remove dir if empty
            if dest_dir.exists() and not any(dest_dir.iterdir()):
                dest_dir.rmdir()
                log.info("Removed empty directory: %s", dest_dir.relative_to(ROOT))
                # Try parent too (e.g. scripts/ if all subdirs removed)
                parent = dest_dir.parent
                if parent != ROOT and parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                    log.info("Removed empty directory: %s", parent.relative_to(ROOT))
        except Exception as e:
            log.debug("Error removing empty directory: %s", e)

    # Update migration log
    undo_log = {
        "version": 1,
        "action": "undo",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "restored": restored,
        "errors": len(errors),
        "error_details": errors,
    }
    undo_log_path = ROOT / "migration_undo_log.json"
    undo_log_path.write_text(json.dumps(undo_log, indent=2, ensure_ascii=False), encoding="utf-8")

    # Rename migration log so it is not accidentally reused
    if restored > 0:
        backup = ROOT / "migration_log.done.json"
        MIGRATION_LOG.rename(backup)
        log.info("Migration log archived to %s", backup.name)

    log.info("")
    log.info("=" * 70)
    log.info("UNDO COMPLETE: %d/%d restored, %d errors", restored, len(records), len(errors))
    log.info("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Reorganise le projet turf-data-pipeline en arborescence propre.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python organize_project.py --dry-run     Montre ce qui serait fait
  python organize_project.py --execute     Execute la migration
  python organize_project.py --undo        Annule via migration_log.json
        """,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Affiche le plan sans rien deplacer")
    group.add_argument("--execute", action="store_true",
                       help="Execute la reorganisation")
    group.add_argument("--undo", action="store_true",
                       help="Annule la migration en utilisant migration_log.json")

    args = parser.parse_args()

    plan = build_move_plan()

    if args.dry_run:
        dry_run(plan)
    elif args.execute:
        # Safety check
        if MIGRATION_LOG.exists():
            log.error("migration_log.json already exists. Undo first or delete it.")
            sys.exit(1)
        execute(plan)
    elif args.undo:
        undo()


if __name__ == "__main__":
    main()
