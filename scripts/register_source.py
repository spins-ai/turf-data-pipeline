#!/usr/bin/env python3
"""
register_source.py -- Enregistre une nouvelle source dans le pipeline.

Etape 8 : registration de nouvelles sources.

Actions effectuees :
  1. Ajoute une entree dans docs/SOURCES.md
  2. Cree le repertoire output/<id>_<name>/ avec .gitkeep
  3. Ajoute les dependances pip a requirements.txt si besoin
  4. Met a jour config/pipeline_config.yaml avec le nouveau script

Usage :
    python scripts/register_source.py \\
        --source-id 118 \\
        --source-name stable_performance \\
        --category "H. Autres scrapers" \\
        --url "https://example.com" \\
        --description "Stable performance data" \\
        [--pip-deps playwright cloudscraper] \\
        [--dry-run]
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

SOURCES_MD = PROJECT_ROOT / "docs" / "SOURCES.md"
REQUIREMENTS_TXT = PROJECT_ROOT / "requirements.txt"
PIPELINE_CONFIG = PROJECT_ROOT / "config" / "pipeline_config.yaml"
OUTPUT_DIR = PROJECT_ROOT / "output"


def add_to_sources_md(source_id: int, source_name: str, category: str,
                      url: str, description: str, dry_run: bool) -> None:
    """Add a new entry to docs/SOURCES.md."""
    if not SOURCES_MD.exists():
        print(f"[WARN] {SOURCES_MD} introuvable, creation impossible.")
        return

    content = SOURCES_MD.read_text(encoding="utf-8")

    # Build the new row
    today = datetime.now().strftime("%Y-%m-%d")
    new_row = (
        f"| {source_id} | {description} | `{url}` "
        f"| 0 | 0 | {today} | \U0001f195 New |"
    )

    # Find the category section
    # Categories in SOURCES.md look like: ## X. Category Name
    category_pattern = re.compile(
        rf"^(## {re.escape(category)})\s*$", re.MULTILINE
    )
    cat_match = category_pattern.search(content)

    if cat_match:
        # Find the end of the table in this category (next ## or end of file)
        section_start = cat_match.end()
        next_section = re.search(r"\n## ", content[section_start:])
        if next_section:
            insert_pos = section_start + next_section.start()
        else:
            insert_pos = len(content)

        # Find the last table row before insert_pos
        # Walk backwards to find a line starting with |
        before_section = content[section_start:insert_pos]
        table_lines = [
            i for i, line in enumerate(before_section.split("\n"))
            if line.strip().startswith("|") and "---" not in line
        ]
        if table_lines:
            # Insert after the last table row
            lines = before_section.split("\n")
            last_table_idx = table_lines[-1]
            lines.insert(last_table_idx + 1, new_row)
            new_section = "\n".join(lines)
            content = content[:section_start] + new_section + content[insert_pos:]
        else:
            # No table rows found, insert after header + separator
            content = content[:insert_pos] + "\n" + new_row + "\n" + content[insert_pos:]
    else:
        # Category not found: append new category at end
        new_category = (
            f"\n---\n\n## {category}\n\n"
            f"| # | Source | URL | Records | Size | Last Update | Status |\n"
            f"|---|--------|-----|---------|------|-------------|--------|\n"
            f"{new_row}\n"
        )
        content += new_category

    if dry_run:
        print(f"[DRY-RUN] Ajouterait source {source_id} dans SOURCES.md (categorie: {category})")
        print(f"  Ligne: {new_row}")
    else:
        SOURCES_MD.write_text(content, encoding="utf-8")
        print(f"[OK] Source {source_id} ajoutee a {SOURCES_MD}")


def create_output_directory(source_id: int, source_name: str,
                            dry_run: bool) -> Path:
    """Create output/<id>_<name>/ with .gitkeep."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_name)
    dir_name = f"{source_id:02d}_{safe_name}"
    dir_path = OUTPUT_DIR / dir_name

    if dry_run:
        print(f"[DRY-RUN] Creerait {dir_path}/ avec .gitkeep")
        return dir_path

    dir_path.mkdir(parents=True, exist_ok=True)
    gitkeep = dir_path / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.touch()
    print(f"[OK] Repertoire cree: {dir_path}/")
    return dir_path


def add_pip_dependencies(deps: list[str], dry_run: bool) -> None:
    """Add pip dependencies to requirements.txt if not already present."""
    if not deps:
        return

    if not REQUIREMENTS_TXT.exists():
        print(f"[WARN] {REQUIREMENTS_TXT} introuvable.")
        return

    content = REQUIREMENTS_TXT.read_text(encoding="utf-8")
    existing = {
        line.strip().lower().split("==")[0].split(">=")[0].split("<=")[0]
        for line in content.splitlines()
        if line.strip() and not line.strip().startswith("#")
    }

    new_deps = [d for d in deps if d.lower() not in existing]

    if not new_deps:
        print("[INFO] Toutes les dependances sont deja dans requirements.txt")
        return

    if dry_run:
        print(f"[DRY-RUN] Ajouterait a requirements.txt: {', '.join(new_deps)}")
        return

    # Append new deps at the end
    if not content.endswith("\n"):
        content += "\n"
    content += "\n# Added for source integration\n"
    for dep in new_deps:
        content += f"{dep}\n"

    REQUIREMENTS_TXT.write_text(content, encoding="utf-8")
    print(f"[OK] Dependances ajoutees a requirements.txt: {', '.join(new_deps)}")


def update_pipeline_config(source_id: int, source_name: str,
                           dry_run: bool) -> None:
    """Add a new scraper entry to config/pipeline_config.yaml."""
    if not PIPELINE_CONFIG.exists():
        print(f"[WARN] {PIPELINE_CONFIG} introuvable.")
        return

    content = PIPELINE_CONFIG.read_text(encoding="utf-8")
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_name)
    merge_name = f"merge_{safe_name}_master"
    script_name = f"merge_{safe_name}_master.py"

    # Check if already registered
    if merge_name in content:
        print(f"[INFO] {merge_name} deja present dans pipeline_config.yaml")
        return

    # Build new entry for Phase 5 (Merges)
    new_entry = (
        f"\n      - name: {merge_name}\n"
        f"        script: {script_name}\n"
        f"        timeout_seconds: 1800\n"
        f"        ram_budget_mb: 1024\n"
        f"        supports_resume: false\n"
        f"        depends_on: [comblage]\n"
    )

    if dry_run:
        print(f"[DRY-RUN] Ajouterait dans pipeline_config.yaml (Phase 5):")
        print(new_entry)
        return

    # Find the Phase 5 section and insert at the end of its scripts list
    # Look for Phase 6 header as insertion boundary
    phase6_pattern = re.compile(r"(  # =+\n  # Phase 6)", re.MULTILINE)
    match = phase6_pattern.search(content)
    if match:
        insert_pos = match.start()
        content = content[:insert_pos] + new_entry + "\n" + content[insert_pos:]
    else:
        # Fallback: look for "phase: 6"
        phase6_alt = content.find("phase: 6")
        if phase6_alt > 0:
            # Go back to the previous line
            insert_pos = content.rfind("\n", 0, phase6_alt)
            content = content[:insert_pos] + new_entry + content[insert_pos:]
        else:
            print(f"[WARN] Phase 6 non trouvee dans pipeline_config.yaml. "
                  f"Ajouter manuellement:")
            print(new_entry)
            return

    PIPELINE_CONFIG.write_text(content, encoding="utf-8")
    print(f"[OK] {merge_name} ajoute a pipeline_config.yaml (Phase 5)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enregistre une nouvelle source dans le pipeline."
    )
    parser.add_argument(
        "--source-id", required=True, type=int,
        help="Numero de la source (ex: 118)"
    )
    parser.add_argument(
        "--source-name", required=True,
        help="Nom court de la source en snake_case (ex: stable_performance)"
    )
    parser.add_argument(
        "--category", default="H. Autres scrapers",
        help="Categorie dans SOURCES.md (ex: 'H. Autres scrapers')"
    )
    parser.add_argument(
        "--url", default="N/A",
        help="URL de la source"
    )
    parser.add_argument(
        "--description", default="",
        help="Description courte de la source"
    )
    parser.add_argument(
        "--pip-deps", nargs="*", default=[],
        help="Dependances pip supplementaires (ex: playwright cloudscraper)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Afficher les actions sans modifier de fichiers"
    )
    args = parser.parse_args()

    if not args.description:
        args.description = args.source_name.replace("_", " ").title()

    print(f"=== Enregistrement de la source {args.source_id} ({args.source_name}) ===\n")

    # Step 1: Add to SOURCES.md
    add_to_sources_md(
        args.source_id, args.source_name, args.category,
        args.url, args.description, args.dry_run,
    )

    # Step 2: Create output directory
    create_output_directory(args.source_id, args.source_name, args.dry_run)

    # Step 3: Add pip dependencies
    add_pip_dependencies(args.pip_deps, args.dry_run)

    # Step 4: Update pipeline_config.yaml
    update_pipeline_config(args.source_id, args.source_name, args.dry_run)

    print(f"\n=== Enregistrement termine pour {args.source_name} ===")
    print("Prochaines etapes :")
    print(f"  1. python scripts/integrate_new_source.py --source-dir output/{args.source_id:02d}_{args.source_name} ...")
    print(f"  2. python scripts/validate_new_source.py --source-dir output/{args.source_id:02d}_{args.source_name}")


if __name__ == "__main__":
    main()
