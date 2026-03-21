#!/usr/bin/env python3
"""
run_pipeline.py - Orchestrateur principal du pipeline turf-data.

Execute l'ensemble du pipeline de bout en bout :
  Phase 1: Audit
  Phase 2: Nettoyage
  Phase 3: Deduplication
  Phase 4: Comblage
  Phase 5: Merges (parallele)
  Phase 6: Mega merge
  Phase 7: Features (parallele)
  Phase 8: Master features
  Phase 9: Quality

Fonctionnalites :
  - DAG avec dependances explicites
  - Execution parallele des etapes independantes (ThreadPoolExecutor)
  - Checkpoint JSON pour reprise apres interruption
  - Logging complet dans pipeline.log
  - Mode --restart pour repartir de zero
  - Mode --from STEP pour reprendre a partir d'une etape
  - Mode --only STEP pour executer une seule etape
  - Mode --dry-run pour afficher le plan sans executer
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PYTHON = os.environ.get("PYTHON_EXE", sys.executable)
BASE_DIR = Path(__file__).resolve().parent
CHECKPOINT_FILE = BASE_DIR / "pipeline_checkpoint.json"
MAX_WORKERS = 4  # parallelisme pour les phases paralleles


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dataclass
class Step:
    """A single pipeline step."""
    name: str
    script: str  # relative path from BASE_DIR
    depends_on: List[str] = field(default_factory=list)
    phase: int = 0


def build_dag() -> Dict[str, Step]:
    """Return the full pipeline DAG as {name: Step}."""
    steps: Dict[str, Step] = {}

    def add(name: str, script: str, depends_on: Optional[List[str]] = None, phase: int = 0):
        steps[name] = Step(
            name=name,
            script=script,
            depends_on=depends_on or [],
            phase=phase,
        )

    # --- Phase 1 : Audit ---------------------------------------------------
    add("audit", "audit_data_integrity.py", phase=1)

    # --- Phase 2 : Nettoyage ------------------------------------------------
    add("nettoyage", "nettoyage_global.py", depends_on=["audit"], phase=2)

    # --- Phase 3 : Deduplication --------------------------------------------
    add("dedup", "deduplication.py", depends_on=["nettoyage"], phase=3)

    # --- Phase 4 : Comblage -------------------------------------------------
    add("comblage", "comblage_trous.py", depends_on=["dedup"], phase=4)

    # --- Phase 5 : Merges (parallele) ---------------------------------------
    merge_deps = ["comblage"]
    add("merge_courses_master", "merge_02_02b_courses_master.py", depends_on=merge_deps, phase=5)
    add("merge_pedigree_master", "merge_pedigree_master.py", depends_on=merge_deps, phase=5)
    add("merge_rapports_21_38", "merge_rapports_21_38.py", depends_on=merge_deps, phase=5)
    add("merge_rapports_master", "merge_rapports_master.py", depends_on=["merge_rapports_21_38"], phase=5)
    add("merge_meteo", "merge_meteo.py", depends_on=merge_deps, phase=5)
    add("merge_meteo_master", "merge_meteo_master.py", depends_on=["merge_meteo"], phase=5)
    add("merge_equipements_master", "merge_equipements_master.py", depends_on=merge_deps, phase=5)
    add("merge_marche_master", "merge_marche_master.py", depends_on=merge_deps, phase=5)
    add("merge_performances_master", "merge_performances_master.py", depends_on=merge_deps, phase=5)
    add("merge_stats_externes_master", "merge_stats_externes_master.py", depends_on=merge_deps, phase=5)

    # --- Phase 6 : Mega merge ----------------------------------------------
    all_merges = [
        "merge_courses_master", "merge_pedigree_master",
        "merge_rapports_master", "merge_meteo_master",
        "merge_equipements_master", "merge_marche_master",
        "merge_performances_master", "merge_stats_externes_master",
    ]
    add("mega_merge", "mega_merge_partants_master.py", depends_on=all_merges, phase=6)

    # --- Phase 7 : Features (parallele) ------------------------------------
    feat_deps = ["mega_merge"]

    # feature_builders/*.py (skip __init__.py and master_feature_builder.py)
    feature_builder_scripts = [
        "feature_builders/cheval_features.py",
        "feature_builders/course_features.py",
        "feature_builders/field_strength_builder.py",
        "feature_builders/jockey_features.py",
        "feature_builders/marche_features.py",
        "feature_builders/pace_profile_builder.py",
        "feature_builders/pedigree_features.py",
        "feature_builders/track_bias_detector.py",
        "feature_builders/perf_detaillees_builder.py",
        "feature_builders/smarkets_builder.py",
        "feature_builders/racing_post_builder.py",
        "feature_builders/reunions_builder.py",
        "feature_builders/enrichissement_builder.py",
        "feature_builders/pedigree_advanced_builder.py",
        "feature_builders/canalturf_builder.py",
        "feature_builders/turfostats_builder.py",
        "feature_builders/geny_builder.py",
        "feature_builders/musique_features.py",
        "feature_builders/temps_features.py",
        "feature_builders/profil_cheval_features.py",
        "feature_builders/equipement_features.py",
        "feature_builders/poids_features.py",
        "feature_builders/meteo_features.py",
        "feature_builders/combo_features.py",
        "feature_builders/class_change_features.py",
        "feature_builders/interaction_features.py",
        "feature_builders/precomputed_partant_joiner.py",
        "feature_builders/precomputed_entity_joiner.py",
    ]
    for script in feature_builder_scripts:
        name = "fb_" + Path(script).stem
        add(name, script, depends_on=feat_deps, phase=7)

    # feat_*.py
    feat_scripts = [
        "feat_croisements.py",
        "feat_historique.py",
        "feat_interactions.py",
        "feat_jockey.py",
        "feat_pedigree.py",
        "feat_sequences.py",
        "feat_temporel.py",
        "feat_cheval_jockey_affinity.py",
        "feat_cheval_hippodrome_affinity.py",
        "feat_cheval_distance_affinity.py",
        "feat_cheval_terrain_affinity.py",
        "feat_jockey_entraineur_combo.py",
        "feat_entraineur_hippodrome.py",
        "feat_value_betting.py",
        "feat_meteo_terrain_interaction.py",
        "feat_pedigree_discipline_match.py",
        "feat_field_strength.py",
    ]
    for script in feat_scripts:
        name = Path(script).stem
        add(name, script, depends_on=feat_deps, phase=7)

    # 41-49 calculation scripts
    calc_scripts = [
        "41_sequences_performances.py",
        "42_croisement_racing_post_pmu.py",
        "43_croisement_meteo_courses.py",
        "44_croisement_pedigree_partants.py",
        "45_graphe_relations_gnn.py",
        "46_track_bias_speed_class.py",
        "48_parse_conditions_texte.py",
        "49_ecart_cotes_internet_national.py",
    ]
    for script in calc_scripts:
        name = "calc_" + Path(script).stem
        add(name, script, depends_on=feat_deps, phase=7)

    # --- Phase 8 : Master features -----------------------------------------
    all_features = [n for n, s in steps.items() if s.phase == 7]
    add("master_features", "master_feature_builder.py", depends_on=all_features, phase=8)

    # --- Phase 9 : Quality --------------------------------------------------
    add("quality", "quality/run_all_tests.py", depends_on=["master_features"], phase=9)

    return steps


# ---------------------------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------------------------

# NOTE: Not migrated to utils.scraping.load_checkpoint/save_checkpoint because
# these use a hardcoded CHECKPOINT_FILE, return a pipeline-specific default dict
# (completed, failed, timings, started_at), and merge defaults via setdefault.
def load_checkpoint() -> Dict:
    """Load checkpoint from disk. Returns dict with 'completed', 'failed', 'timings'."""
    default = {"completed": [], "failed": {}, "timings": {}, "started_at": None}
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Ensure all keys exist
            for k, v in default.items():
                data.setdefault(k, v)
            return data
        except (json.JSONDecodeError, IOError):
            pass
    return default


def save_checkpoint(ckpt: Dict):
    """Persist checkpoint to disk."""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(ckpt, f, indent=2, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Execution engine
# ---------------------------------------------------------------------------

def run_step(step: Step, logger: logging.Logger) -> tuple:
    """
    Run a single pipeline step via subprocess.
    Returns (step_name, success: bool, duration_seconds: float, error_msg: str|None).
    """
    script_path = BASE_DIR / step.script
    if not script_path.exists():
        msg = f"Script introuvable : {script_path}"
        logger.error(f"[{step.name}] {msg}")
        return (step.name, False, 0.0, msg)

    logger.info(f"[{step.name}] Demarrage -> {step.script}")
    t0 = time.time()

    try:
        result = subprocess.run(
            [PYTHON, str(script_path)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=3600,  # 1h max par etape
        )
        duration = time.time() - t0

        if result.stdout:
            for line in result.stdout.strip().splitlines():
                logger.info(f"[{step.name}:stdout] {line}")

        if result.returncode != 0:
            err = result.stderr.strip() if result.stderr else f"exit code {result.returncode}"
            for line in err.splitlines():
                logger.error(f"[{step.name}:stderr] {line}")
            logger.error(f"[{step.name}] ECHEC en {duration:.1f}s (code {result.returncode})")
            return (step.name, False, duration, err[:2000])

        logger.info(f"[{step.name}] OK en {duration:.1f}s")
        return (step.name, True, duration, None)

    except subprocess.TimeoutExpired:
        duration = time.time() - t0
        msg = f"Timeout apres {duration:.0f}s"
        logger.error(f"[{step.name}] {msg}")
        return (step.name, False, duration, msg)
    except Exception as e:
        duration = time.time() - t0
        msg = f"Exception: {e}"
        logger.error(f"[{step.name}] {msg}")
        return (step.name, False, duration, msg)


def resolve_execution_order(dag: Dict[str, Step]) -> List[List[str]]:
    """
    Topological sort of the DAG into execution waves.
    Each wave is a list of step names that can run in parallel.
    """
    completed: Set[str] = set()
    waves: List[List[str]] = []
    remaining = set(dag.keys())

    while remaining:
        # Find all steps whose deps are satisfied
        ready = []
        for name in remaining:
            step = dag[name]
            if all(d in completed for d in step.depends_on):
                ready.append(name)

        if not ready:
            unsatisfied = {n: [d for d in dag[n].depends_on if d not in completed] for n in remaining}
            raise RuntimeError(f"Cycle ou dependance manquante detecte : {unsatisfied}")

        # Sort by phase then name for deterministic ordering
        ready.sort(key=lambda n: (dag[n].phase, n))
        waves.append(ready)
        completed.update(ready)
        remaining -= set(ready)

    return waves


def execute_pipeline(
    dag: Dict[str, Step],
    checkpoint: Dict,
    logger: logging.Logger,
    dry_run: bool = False,
    stop_on_failure: bool = True,
    from_step: Optional[str] = None,
    only_step: Optional[str] = None,
):
    """Execute the full pipeline respecting the DAG ordering."""
    waves = resolve_execution_order(dag)
    completed_set: Set[str] = set(checkpoint["completed"])
    failed_steps: Dict[str, str] = dict(checkpoint.get("failed", {}))

    # --only mode : execute a single step (skip dep check)
    if only_step:
        if only_step not in dag:
            logger.error(f"Etape inconnue : {only_step}")
            return False
        step = dag[only_step]
        logger.info(f"Mode --only : execution de '{only_step}' uniquement")
        if dry_run:
            logger.info(f"  [DRY-RUN] {only_step} -> {step.script}")
            return True
        name, ok, dur, err = run_step(step, logger)
        checkpoint["timings"][name] = dur
        if ok:
            if name not in checkpoint["completed"]:
                checkpoint["completed"].append(name)
            failed_steps.pop(name, None)
        else:
            failed_steps[name] = err or "unknown"
        checkpoint["failed"] = failed_steps
        save_checkpoint(checkpoint)
        return ok

    # --from mode : mark everything before from_step as completed
    if from_step:
        if from_step not in dag:
            logger.error(f"Etape inconnue : {from_step}")
            return False
        logger.info(f"Mode --from : reprise a partir de '{from_step}'")
        # Find all ancestors of from_step and mark them as completed
        ancestors: Set[str] = set()

        def collect_ancestors(step_name: str):
            for dep in dag[step_name].depends_on:
                if dep not in ancestors:
                    ancestors.add(dep)
                    collect_ancestors(dep)

        collect_ancestors(from_step)
        completed_set.update(ancestors)
        # Remove from_step and descendants from completed so they re-run
        descendants: Set[str] = set()

        def collect_descendants(step_name: str):
            descendants.add(step_name)
            for n, s in dag.items():
                if step_name in s.depends_on and n not in descendants:
                    collect_descendants(n)

        collect_descendants(from_step)
        completed_set -= descendants
        checkpoint["completed"] = list(completed_set)
        save_checkpoint(checkpoint)

    total_steps = len(dag)
    skipped = 0
    executed = 0
    failed_count = 0

    logger.info("=" * 70)
    logger.info(f"Pipeline demarre - {total_steps} etapes au total")
    logger.info(f"Deja completees (checkpoint) : {len(completed_set)}")
    logger.info("=" * 70)

    if dry_run:
        logger.info("MODE DRY-RUN : aucune execution reelle")
        for wave_idx, wave in enumerate(waves):
            to_run = [n for n in wave if n not in completed_set]
            to_skip = [n for n in wave if n in completed_set]
            if to_skip:
                logger.info(f"  Vague {wave_idx + 1} SKIP : {', '.join(to_skip)}")
            if to_run:
                logger.info(f"  Vague {wave_idx + 1} RUN  : {', '.join(to_run)}")
        return True

    pipeline_ok = True

    for wave_idx, wave in enumerate(waves):
        # Filter out already completed steps
        to_run = [name for name in wave if name not in completed_set]
        to_skip = [name for name in wave if name in completed_set]

        if to_skip:
            skipped += len(to_skip)
            for name in to_skip:
                logger.info(f"[SKIP] {name} (deja complete)")

        if not to_run:
            continue

        # Check that no dependency has failed
        blocked = []
        for name in to_run:
            failed_deps = [d for d in dag[name].depends_on if d in failed_steps]
            if failed_deps:
                blocked.append((name, failed_deps))

        if blocked:
            for name, deps in blocked:
                msg = f"Bloque par dependances en echec : {', '.join(deps)}"
                logger.warning(f"[BLOCKED] {name} - {msg}")
                failed_steps[name] = msg
                failed_count += 1
            to_run = [n for n in to_run if n not in {b[0] for b in blocked}]

        if not to_run:
            continue

        phase = dag[to_run[0]].phase
        logger.info("-" * 50)
        logger.info(f"Vague {wave_idx + 1} (Phase {phase}) : {len(to_run)} etape(s) -> {', '.join(to_run)}")

        # Run wave steps in parallel (if >1) or sequentially
        if len(to_run) == 1:
            name, ok, dur, err = run_step(dag[to_run[0]], logger)
            checkpoint["timings"][name] = dur
            if ok:
                completed_set.add(name)
                checkpoint["completed"] = list(completed_set)
                executed += 1
            else:
                failed_steps[name] = err or "unknown"
                failed_count += 1
                pipeline_ok = False
            checkpoint["failed"] = failed_steps
            save_checkpoint(checkpoint)

            if not ok and stop_on_failure:
                logger.error(f"Arret du pipeline apres echec de '{name}'")
                break
        else:
            workers = min(MAX_WORKERS, len(to_run))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(run_step, dag[name], logger): name
                    for name in to_run
                }
                wave_failed = False
                for future in as_completed(futures):
                    name, ok, dur, err = future.result()
                    checkpoint["timings"][name] = dur
                    if ok:
                        completed_set.add(name)
                        checkpoint["completed"] = list(completed_set)
                        executed += 1
                    else:
                        failed_steps[name] = err or "unknown"
                        failed_count += 1
                        wave_failed = True
                        pipeline_ok = False
                    checkpoint["failed"] = failed_steps
                    save_checkpoint(checkpoint)

                if wave_failed and stop_on_failure:
                    logger.error("Arret du pipeline apres echec(s) dans cette vague")
                    break

    # --- Summary -----------------------------------------------------------
    logger.info("=" * 70)
    logger.info("RESUME DU PIPELINE")
    logger.info(f"  Etapes executees avec succes : {executed}")
    logger.info(f"  Etapes sautees (checkpoint)  : {skipped}")
    logger.info(f"  Etapes en echec              : {failed_count}")
    logger.info(f"  Total etapes                 : {total_steps}")

    if checkpoint.get("timings"):
        total_time = sum(checkpoint["timings"].values())
        logger.info(f"  Temps total d'execution      : {timedelta(seconds=int(total_time))}")
        # Top 5 slowest
        sorted_timings = sorted(checkpoint["timings"].items(), key=lambda x: x[1], reverse=True)
        logger.info("  Top 5 etapes les plus lentes :")
        for name, dur in sorted_timings[:5]:
            logger.info(f"    {name:40s} {dur:8.1f}s")

    if failed_steps:
        logger.info("  Etapes en echec :")
        for name, err in failed_steps.items():
            short_err = err.splitlines()[-1] if err else "?"
            logger.info(f"    {name}: {short_err[:120]}")

    status = "SUCCES" if pipeline_ok and failed_count == 0 else "ECHEC"
    logger.info(f"  Statut final : {status}")
    logger.info("=" * 70)

    return pipeline_ok


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

from utils.logging_setup import setup_logging


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Orchestrateur du pipeline turf-data (execution bout-en-bout)",
    )
    parser.add_argument(
        "--restart", action="store_true",
        help="Effacer le checkpoint et repartir de zero",
    )
    parser.add_argument(
        "--from", dest="from_step", metavar="STEP",
        help="Reprendre a partir de l'etape STEP (les dependances sont marquees OK)",
    )
    parser.add_argument(
        "--only", dest="only_step", metavar="STEP",
        help="Executer uniquement l'etape STEP (ignore les dependances)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Afficher le plan d'execution sans rien lancer",
    )
    parser.add_argument(
        "--no-stop-on-failure", action="store_true",
        help="Continuer meme si une etape echoue (par defaut : arret)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Lister toutes les etapes du pipeline et quitter",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Afficher l'etat du checkpoint et quitter",
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_WORKERS,
        help=f"Nombre de workers paralleles (defaut: {MAX_WORKERS})",
    )
    return parser.parse_args()


def cmd_list(dag: Dict[str, Step]):
    """Print all steps grouped by phase."""
    by_phase = defaultdict(list)
    for name, step in dag.items():
        by_phase[step.phase].append(step)

    phase_names = {
        1: "Audit", 2: "Nettoyage", 3: "Deduplication", 4: "Comblage",
        5: "Merges", 6: "Mega merge", 7: "Features", 8: "Master features",
        9: "Quality",
    }

    for phase_num in sorted(by_phase.keys()):
        steps = sorted(by_phase[phase_num], key=lambda s: s.name)
        label = phase_names.get(phase_num, f"Phase {phase_num}")
        print(f"\n--- Phase {phase_num} : {label} ({len(steps)} etapes) ---")
        for step in steps:
            deps = ", ".join(step.depends_on) if step.depends_on else "(aucune)"
            print(f"  {step.name:40s} {step.script:50s} deps=[{deps}]")

    print(f"\nTotal : {len(dag)} etapes")


def cmd_status(dag: Dict[str, Step]):
    """Print current checkpoint status."""
    ckpt = load_checkpoint()
    completed = set(ckpt.get("completed", []))
    failed = ckpt.get("failed", {})
    timings = ckpt.get("timings", {})

    print(f"Checkpoint : {CHECKPOINT_FILE}")
    print(f"Completees : {len(completed)} / {len(dag)}")
    print(f"En echec   : {len(failed)}")

    if timings:
        total = sum(timings.values())
        print(f"Temps total: {timedelta(seconds=int(total))}")

    remaining = set(dag.keys()) - completed - set(failed.keys())
    if remaining:
        print(f"\nRestantes ({len(remaining)}) :")
        for name in sorted(remaining):
            print(f"  {name}")

    if failed:
        print(f"\nEn echec ({len(failed)}) :")
        for name, err in sorted(failed.items()):
            short = err.splitlines()[-1][:100] if err else "?"
            print(f"  {name}: {short}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global MAX_WORKERS
    args = parse_args()
    dag = build_dag()

    # Info-only commands
    if args.list:
        cmd_list(dag)
        return

    if args.status:
        cmd_status(dag)
        return

    MAX_WORKERS = args.workers
    logger = setup_logging("pipeline")

    logger.info(f"Python    : {PYTHON}")
    logger.info(f"Base dir  : {BASE_DIR}")
    logger.info(f"Workers   : {MAX_WORKERS}")
    logger.info(f"Checkpoint: {CHECKPOINT_FILE}")

    # Handle --restart
    if args.restart:
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint efface (--restart)")

    checkpoint = load_checkpoint()
    if not checkpoint.get("started_at"):
        checkpoint["started_at"] = datetime.now().isoformat()
        save_checkpoint(checkpoint)

    success = execute_pipeline(
        dag=dag,
        checkpoint=checkpoint,
        logger=logger,
        dry_run=args.dry_run,
        stop_on_failure=not args.no_stop_on_failure,
        from_step=args.from_step,
        only_step=args.only_step,
    )

    checkpoint["finished_at"] = datetime.now().isoformat()
    save_checkpoint(checkpoint)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
