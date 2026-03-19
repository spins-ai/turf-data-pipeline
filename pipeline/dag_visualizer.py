#!/usr/bin/env python3
"""
dag_visualizer.py — Pilier 8 : Orchestration - Visualisation du DAG
====================================================================
Lit la definition du DAG depuis run_pipeline.py (via import de build_dag)
et genere un diagramme Mermaid.

Le diagramme est sauvegarde dans docs/DAG.md.

Usage :
    python pipeline/dag_visualizer.py
    python pipeline/dag_visualizer.py --output docs/DAG.md
    python pipeline/dag_visualizer.py --format mermaid  (defaut)
"""

import argparse
import importlib.util
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
RUN_PIPELINE_PATH = BASE_DIR / "run_pipeline.py"
DOCS_DIR = BASE_DIR / "docs"
DEFAULT_OUTPUT = DOCS_DIR / "DAG.md"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger(__name__)

# Noms lisibles des phases
PHASE_NAMES = {
    1: "Audit",
    2: "Nettoyage",
    3: "Deduplication",
    4: "Comblage",
    5: "Merges",
    6: "Mega merge",
    7: "Features",
    8: "Master features",
    9: "Quality",
}

# Couleurs Mermaid par phase
PHASE_STYLES = {
    1: "fill:#e1f5fe,stroke:#0288d1",
    2: "fill:#f3e5f5,stroke:#7b1fa2",
    3: "fill:#e8f5e9,stroke:#388e3c",
    4: "fill:#fff3e0,stroke:#f57c00",
    5: "fill:#fce4ec,stroke:#c62828",
    6: "fill:#e8eaf6,stroke:#283593",
    7: "fill:#f1f8e9,stroke:#558b2f",
    8: "fill:#fff8e1,stroke:#ff8f00",
    9: "fill:#efebe9,stroke:#4e342e",
}


# ---------------------------------------------------------------------------
# Import dynamique de build_dag
# ---------------------------------------------------------------------------

def load_build_dag():
    """
    Importe dynamiquement build_dag() depuis run_pipeline.py.
    Retourne le DAG dict {name: Step}.
    """
    if not RUN_PIPELINE_PATH.exists():
        log.error(f"run_pipeline.py introuvable : {RUN_PIPELINE_PATH}")
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("run_pipeline", str(RUN_PIPELINE_PATH))
    module = importlib.util.module_from_spec(spec)

    # Eviter l'execution de main() lors de l'import
    sys.modules["run_pipeline"] = module
    spec.loader.exec_module(module)

    if not hasattr(module, "build_dag"):
        log.error("Fonction build_dag() introuvable dans run_pipeline.py")
        sys.exit(1)

    return module.build_dag()


# ---------------------------------------------------------------------------
# Sanitize pour Mermaid
# ---------------------------------------------------------------------------

def sanitize_id(name: str) -> str:
    """Transforme un nom en identifiant Mermaid valide."""
    return name.replace("-", "_").replace(".", "_").replace("/", "_")


def short_label(name: str) -> str:
    """Label court et lisible pour un noeud."""
    # Supprimer les prefixes communs
    for prefix in ("fb_", "calc_", "merge_", "feat_"):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


# ---------------------------------------------------------------------------
# Generation Mermaid
# ---------------------------------------------------------------------------

def generate_mermaid(dag: Dict[str, Any]) -> str:
    """Genere un diagramme Mermaid a partir du DAG."""
    lines: List[str] = []
    lines.append("```mermaid")
    lines.append("graph TD")
    lines.append("")

    # Grouper par phase
    by_phase: Dict[int, List] = defaultdict(list)
    for name, step in dag.items():
        by_phase[step.phase].append(step)

    # Generer les sous-graphes par phase
    for phase_num in sorted(by_phase.keys()):
        steps = sorted(by_phase[phase_num], key=lambda s: s.name)
        phase_label = PHASE_NAMES.get(phase_num, f"Phase {phase_num}")

        lines.append(f"    subgraph Phase{phase_num}[\"{phase_label}\"]")

        for step in steps:
            sid = sanitize_id(step.name)
            label = short_label(step.name)
            lines.append(f"        {sid}[\"{label}\"]")

        lines.append("    end")
        lines.append("")

    # Generer les liens (dependances)
    lines.append("    %% Dependances")
    for name, step in sorted(dag.items()):
        sid = sanitize_id(name)
        for dep in step.depends_on:
            dep_id = sanitize_id(dep)
            lines.append(f"    {dep_id} --> {sid}")

    lines.append("")

    # Generer les styles par phase
    lines.append("    %% Styles par phase")
    for phase_num, steps in by_phase.items():
        style = PHASE_STYLES.get(phase_num, "fill:#ffffff,stroke:#000000")
        for step in steps:
            sid = sanitize_id(step.name)
            lines.append(f"    style {sid} {style}")

    lines.append("```")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Generation du document Markdown
# ---------------------------------------------------------------------------

def generate_markdown(dag: Dict[str, Any]) -> str:
    """Genere le document Markdown complet avec le diagramme et les statistiques."""
    sections: List[str] = []

    # Header
    sections.append("# DAG du Pipeline Turf-Data")
    sections.append("")
    sections.append("Diagramme genere automatiquement depuis `run_pipeline.py`.")
    sections.append("")

    # Statistiques
    by_phase = defaultdict(list)
    for name, step in dag.items():
        by_phase[step.phase].append(step)

    total_steps = len(dag)
    total_edges = sum(len(s.depends_on) for s in dag.values())

    sections.append("## Statistiques")
    sections.append("")
    sections.append(f"| Metrique | Valeur |")
    sections.append(f"|----------|--------|")
    sections.append(f"| Etapes totales | {total_steps} |")
    sections.append(f"| Dependances totales | {total_edges} |")
    sections.append(f"| Phases | {len(by_phase)} |")
    sections.append("")

    # Detail par phase
    sections.append("## Detail par phase")
    sections.append("")
    sections.append("| Phase | Nom | Etapes |")
    sections.append("|-------|-----|--------|")
    for phase_num in sorted(by_phase.keys()):
        label = PHASE_NAMES.get(phase_num, f"Phase {phase_num}")
        count = len(by_phase[phase_num])
        sections.append(f"| {phase_num} | {label} | {count} |")
    sections.append("")

    # Diagramme Mermaid
    sections.append("## Diagramme")
    sections.append("")
    sections.append(generate_mermaid(dag))
    sections.append("")

    # Liste detaillee
    sections.append("## Liste des etapes")
    sections.append("")

    for phase_num in sorted(by_phase.keys()):
        label = PHASE_NAMES.get(phase_num, f"Phase {phase_num}")
        steps = sorted(by_phase[phase_num], key=lambda s: s.name)

        sections.append(f"### Phase {phase_num} : {label}")
        sections.append("")
        sections.append("| Etape | Script | Dependances |")
        sections.append("|-------|--------|-------------|")

        for step in steps:
            deps = ", ".join(step.depends_on) if step.depends_on else "-"
            sections.append(f"| {step.name} | `{step.script}` | {deps} |")

        sections.append("")

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Genere un diagramme Mermaid du DAG pipeline"
    )
    parser.add_argument(
        "--output", type=str, default=str(DEFAULT_OUTPUT),
        help=f"Fichier de sortie (defaut: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--format", choices=["mermaid", "full"], default="full",
        help="Format de sortie : 'mermaid' (diagramme seul) ou 'full' (markdown complet)",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    os.makedirs(output_path.parent, exist_ok=True)

    log.info("=" * 70)
    log.info("PILIER 8 : Visualisation du DAG Pipeline")
    log.info(f"  Source  : {RUN_PIPELINE_PATH}")
    log.info(f"  Output  : {output_path}")
    log.info(f"  Format  : {args.format}")
    log.info("=" * 70)

    # Charger le DAG
    log.info("Chargement du DAG depuis run_pipeline.py ...")
    dag = load_build_dag()
    log.info(f"  Etapes chargees : {len(dag)}")

    # Statistiques rapides
    by_phase = defaultdict(list)
    for name, step in dag.items():
        by_phase[step.phase].append(step)

    for phase_num in sorted(by_phase.keys()):
        label = PHASE_NAMES.get(phase_num, f"Phase {phase_num}")
        log.info(f"  Phase {phase_num} ({label:20s}) : {len(by_phase[phase_num]):3d} etapes")

    # Generer le contenu
    if args.format == "mermaid":
        content = generate_mermaid(dag)
    else:
        content = generate_markdown(dag)

    # Ecrire le fichier
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    log.info(f"\nDiagramme sauvegarde : {output_path}")
    log.info(f"  Taille : {len(content):,} caracteres")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
