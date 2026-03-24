#!/usr/bin/env python3
"""
scripts/create_release_tag.py
==============================
Etape 16 — Validation : lance le pre-model checklist et, si READY, cree
un tag Git ``data-v1.0-ready`` avec les statistiques dans le message du tag.

Workflow :
  1. Execute scripts/pre_model_checklist.py
  2. Si READY (exit 0), lit quality/pre_model_checklist.json pour les stats
  3. Cree le tag git annote ``data-v1.0-ready``
  4. Affiche les prochaines etapes (entrainement modele)

Usage :
    python scripts/create_release_tag.py
    python scripts/create_release_tag.py --tag data-v2.0-ready
    python scripts/create_release_tag.py --min-records 1000000
    python scripts/create_release_tag.py --force   # re-tag meme si existe
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Project bootstrap
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

DEFAULT_TAG = "data-v1.0-ready"
CHECKLIST_SCRIPT = _PROJECT_ROOT / "scripts" / "pre_model_checklist.py"
CHECKLIST_JSON = _PROJECT_ROOT / "quality" / "pre_model_checklist.json"

# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------
import os

if os.environ.get("NO_COLOR") or not sys.stdout.isatty():
    GREEN = RED = CYAN = BOLD = RESET = ""
else:
    GREEN = "\033[92m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


# ===================================================================
# HELPERS
# ===================================================================

def _run_checklist(min_records: int) -> bool:
    """Run pre_model_checklist.py. Returns True if READY."""
    print(f"\n{BOLD}{CYAN}=== Etape 1/3 : Pre-Model Checklist ==={RESET}\n")
    args = [sys.executable, str(CHECKLIST_SCRIPT)]
    if min_records:
        args.extend(["--min-records", str(min_records)])
    result = subprocess.run(args, cwd=str(_PROJECT_ROOT))
    return result.returncode == 0


def _load_stats() -> dict:
    """Load checklist stats from JSON."""
    if CHECKLIST_JSON.exists():
        with open(CHECKLIST_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _tag_exists(tag: str) -> bool:
    """Check if a git tag already exists."""
    result = subprocess.run(
        ["git", "tag", "-l", tag],
        cwd=str(_PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    return tag in result.stdout.strip().splitlines()


def _create_tag(tag: str, message: str, force: bool = False) -> bool:
    """Create an annotated git tag."""
    cmd = ["git", "tag", "-a", tag, "-m", message]
    if force:
        cmd.append("-f")
    result = subprocess.run(cmd, cwd=str(_PROJECT_ROOT), capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  {RED}Erreur git tag : {result.stderr.strip()}{RESET}")
        return False
    return True


def _build_tag_message(stats: dict) -> str:
    """Build a descriptive tag message from checklist stats."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pm = stats.get("partants_master.jsonl_records", "?")
    fm = stats.get("features_matrix.jsonl_records", "?")
    tl = stats.get("training_labels.jsonl_records", "?")
    feat = stats.get("unique_fields_in_sample", "?")
    overlap = stats.get("label_overlap_pct", "?")
    date_range = stats.get("date_range", "?")
    checks_pass = stats.get("pass", "?")
    checks_fail = stats.get("fail", "?")
    checks_warn = stats.get("warn", "?")

    return textwrap.dedent(f"""\
        Data pipeline ready for ML training

        Date: {now}
        Pipeline stats:
          partants_master.jsonl : {pm:,} records
          features_matrix.jsonl : {fm:,} records
          training_labels.jsonl : {tl:,} records
          Unique features       : {feat}
          Label overlap         : {overlap}%
          Date range            : {date_range}

        Checklist: {checks_pass} PASS, {checks_fail} FAIL, {checks_warn} WARN
    """).strip() if isinstance(pm, int) else textwrap.dedent(f"""\
        Data pipeline ready for ML training

        Date: {now}
        Pipeline stats:
          partants_master records : {pm}
          features_matrix records : {fm}
          training_labels records : {tl}
          Unique features         : {feat}
          Label overlap           : {overlap}%
          Date range              : {date_range}

        Checklist: {checks_pass} PASS, {checks_fail} FAIL, {checks_warn} WARN
    """).strip()


def _print_next_steps(tag: str) -> None:
    """Print instructions for next steps."""
    print(f"\n{BOLD}{CYAN}=== Prochaines etapes ==={RESET}")
    print(textwrap.dedent(f"""\
        Le tag {GREEN}{tag}{RESET} a ete cree. Pour continuer :

        1. {BOLD}Pousser le tag vers le depot distant :{RESET}
           git push origin {tag}

        2. {BOLD}Lancer l'entrainement des modeles :{RESET}
           python models/phase_01_infrastructure/historical_dataset_builder.py
           python models/train_xgboost.py
           python models/train_lightgbm.py
           python models/train_catboost.py

        3. {BOLD}Evaluer les modeles :{RESET}
           python models/evaluate_models.py

        4. {BOLD}(Optionnel) Generer les predictions :{RESET}
           python models/predict.py --date today

        Le dataset est verrouille a cette version via le tag Git.
        Pour regenerer, relancez le pipeline et creez un nouveau tag.
    """))


# ===================================================================
# MAIN
# ===================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Etape 16 : validation et creation du tag de release."
    )
    parser.add_argument(
        "--tag", default=DEFAULT_TAG,
        help=f"Nom du tag Git (defaut: {DEFAULT_TAG})",
    )
    parser.add_argument(
        "--min-records", type=int, default=0,
        help="Nombre minimum de records (transmis au checklist, 0 = defaut checklist)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Forcer la recreation du tag s'il existe deja",
    )
    parser.add_argument(
        "--skip-checklist", action="store_true",
        help="Sauter le checklist (utiliser les stats existantes)",
    )
    args = parser.parse_args()

    t0 = time.monotonic()

    # Step 1: Run checklist
    if not args.skip_checklist:
        ready = _run_checklist(args.min_records)
        if not ready:
            print(f"\n  {RED}{BOLD}Checklist NOT READY — tag non cree.{RESET}")
            print(f"  Corrigez les erreurs ci-dessus et relancez.\n")
            sys.exit(1)
    else:
        print(f"\n{BOLD}{CYAN}=== Checklist saute (--skip-checklist) ==={RESET}")

    # Step 2: Load stats
    stats = _load_stats()

    # Step 3: Create tag
    print(f"\n{BOLD}{CYAN}=== Etape 2/3 : Creation du tag Git ==={RESET}\n")

    if _tag_exists(args.tag) and not args.force:
        print(f"  {RED}Le tag '{args.tag}' existe deja.{RESET}")
        print(f"  Utilisez --force pour le recreer ou choisissez un autre nom avec --tag.")
        sys.exit(1)

    message = _build_tag_message(stats)
    if _create_tag(args.tag, message, force=args.force):
        print(f"  {GREEN}{BOLD}Tag '{args.tag}' cree avec succes.{RESET}")
    else:
        print(f"  {RED}{BOLD}Echec de la creation du tag.{RESET}")
        sys.exit(1)

    # Step 4: Next steps
    _print_next_steps(args.tag)

    elapsed = time.monotonic() - t0
    print(f"  Temps total : {elapsed:.1f}s\n")


if __name__ == "__main__":
    main()
