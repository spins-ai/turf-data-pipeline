#!/usr/bin/env python3
"""
quality/data_lineage_tracker.py
================================
Traceur de lignee des donnees (data lineage).

Enregistre chaque etape de traitement (source -> transformation -> sortie)
avec metadata (timestamp, script, fichiers, comptages de lignes).
Stocke dans un fichier JSON de lignee appendable.

Aucun appel API : traitement 100% local.

Usage :
    # Dans un script de pipeline :
    from quality.data_lineage_tracker import LineageTracker
    tracker = LineageTracker()
    tracker.log_step(
        step_name="normalisation_partants",
        script="02_liste_courses.py",
        inputs=["output/02_liste_courses/partants_brut.json"],
        outputs=["output/02_liste_courses/partants_normalises.json"],
        input_rows=2600000,
        output_rows=2600000,
        params={"version": "v5"},
    )

    # Query lineage :
    python3 quality/data_lineage_tracker.py --query output/labels/labels.parquet
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# ===========================================================================
# CONFIG
# ===========================================================================

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
DEFAULT_LINEAGE_FILE = Path(__file__).resolve().parent.parent / "output" / "quality" / "data_lineage.json"


# ===========================================================================
# LOGGING
# ===========================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("data_lineage_tracker")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_DIR / "data_lineage_tracker.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ===========================================================================
# CORE
# ===========================================================================

class LineageTracker:
    """Traceur de lignee des donnees.

    Enregistre les etapes de transformation dans un fichier JSON.
    Chaque entree contient : nom de l'etape, script, fichiers d'entree/sortie,
    comptages de lignes, parametres, et timestamp.

    Parameters
    ----------
    lineage_file : Path or str
        Chemin vers le fichier de lignee JSON.
    """

    def __init__(self, lineage_file: Path | str = DEFAULT_LINEAGE_FILE):
        self.lineage_file = Path(lineage_file)
        self.logger = setup_logging()
        self._entries: list[dict] = self._load()

    def _load(self) -> list[dict]:
        """Charge le fichier de lignee existant."""
        if self.lineage_file.exists():
            try:
                with open(self.lineage_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
            except (json.JSONDecodeError, OSError) as e:
                self.logger.warning("Fichier lineage corrompu, reinitialisation: %s", e)
        return []

    def _save(self) -> None:
        """Sauvegarde le fichier de lignee."""
        self.lineage_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.lineage_file.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._entries, f, ensure_ascii=False, indent=2, default=str)
        tmp.replace(self.lineage_file)

    def log_step(
        self,
        step_name: str,
        script: str,
        inputs: list[str],
        outputs: list[str],
        input_rows: Optional[int] = None,
        output_rows: Optional[int] = None,
        params: Optional[dict[str, Any]] = None,
        notes: Optional[str] = None,
    ) -> dict:
        """Enregistre une etape de traitement.

        Parameters
        ----------
        step_name : str
            Nom de l'etape (ex: "normalisation_partants").
        script : str
            Nom du script executant l'etape.
        inputs : list[str]
            Liste des fichiers d'entree.
        outputs : list[str]
            Liste des fichiers de sortie.
        input_rows : int, optional
            Nombre de lignes en entree.
        output_rows : int, optional
            Nombre de lignes en sortie.
        params : dict, optional
            Parametres de la transformation.
        notes : str, optional
            Notes supplementaires.

        Returns
        -------
        dict
            L'entree de lignee creee.
        """
        entry = {
            "timestamp": datetime.now().isoformat(),
            "step_name": step_name,
            "script": script,
            "inputs": inputs,
            "outputs": outputs,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "params": params or {},
            "notes": notes,
        }

        self._entries.append(entry)
        self._save()

        self.logger.info(
            "Lignee: %s (%s) — %s -> %s [%s -> %s lignes]",
            step_name, script,
            ", ".join(inputs), ", ".join(outputs),
            input_rows or "?", output_rows or "?",
        )

        return entry

    def query_output(self, output_file: str) -> list[dict]:
        """Retrouve toutes les etapes ayant produit un fichier de sortie.

        Parameters
        ----------
        output_file : str
            Chemin du fichier de sortie a tracer.

        Returns
        -------
        list[dict]
            Etapes de lignee ayant produit ce fichier.
        """
        matches = []
        for entry in self._entries:
            for out in entry.get("outputs", []):
                if output_file in out or out in output_file:
                    matches.append(entry)
                    break
        return matches

    def trace_full_lineage(self, output_file: str) -> list[dict]:
        """Trace recursif : remonte de la sortie jusqu'aux sources originales.

        Parameters
        ----------
        output_file : str
            Fichier de sortie initial.

        Returns
        -------
        list[dict]
            Chaine complete de lignee, de la source a la sortie.
        """
        visited: set[str] = set()
        chain: list[dict] = []

        def _trace(target: str) -> None:
            if target in visited:
                return
            visited.add(target)

            steps = self.query_output(target)
            for step in steps:
                chain.append(step)
                for inp in step.get("inputs", []):
                    _trace(inp)

        _trace(output_file)

        # Inverser pour avoir source -> sortie
        chain.reverse()
        return chain

    def get_all_entries(self) -> list[dict]:
        """Retourne toutes les entrees de lignee."""
        return list(self._entries)

    def get_summary(self) -> dict:
        """Resume de la lignee : nombre d'etapes, fichiers, scripts.

        Returns
        -------
        dict
            Resume statistique.
        """
        all_scripts = set()
        all_inputs = set()
        all_outputs = set()

        for entry in self._entries:
            all_scripts.add(entry.get("script", ""))
            for inp in entry.get("inputs", []):
                all_inputs.add(inp)
            for out in entry.get("outputs", []):
                all_outputs.add(out)

        return {
            "n_steps": len(self._entries),
            "n_scripts": len(all_scripts),
            "n_input_files": len(all_inputs),
            "n_output_files": len(all_outputs),
            "scripts": sorted(all_scripts),
            "first_timestamp": self._entries[0]["timestamp"] if self._entries else None,
            "last_timestamp": self._entries[-1]["timestamp"] if self._entries else None,
        }

    def clear(self) -> None:
        """Reinitialise le fichier de lignee."""
        self._entries = []
        self._save()
        self.logger.info("Fichier de lignee reinitialise")


def format_lineage_chain(chain: list[dict]) -> str:
    """Formate une chaine de lignee en texte lisible.

    Parameters
    ----------
    chain : list[dict]
        Chaine de lignee (resultat de trace_full_lineage).

    Returns
    -------
    str
        Texte formate.
    """
    lines = []
    for i, entry in enumerate(chain):
        prefix = "  " * i + ("-> " if i > 0 else "")
        lines.append(f"{prefix}{entry['step_name']} ({entry['script']})")
        lines.append(f"{'  ' * (i + 1)}inputs:  {', '.join(entry.get('inputs', []))}")
        lines.append(f"{'  ' * (i + 1)}outputs: {', '.join(entry.get('outputs', []))}")
        if entry.get("input_rows") or entry.get("output_rows"):
            lines.append(f"{'  ' * (i + 1)}rows:    {entry.get('input_rows', '?')} -> {entry.get('output_rows', '?')}")
        lines.append(f"{'  ' * (i + 1)}date:    {entry.get('timestamp', '?')}")
    return "\n".join(lines)


# ===========================================================================
# MAIN (CLI)
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Traceur de lignee des donnees"
    )
    parser.add_argument(
        "--query", type=str, default=None,
        help="Fichier de sortie a tracer (query lineage)"
    )
    parser.add_argument(
        "--lineage-file", type=str, default=str(DEFAULT_LINEAGE_FILE),
        help="Chemin vers le fichier de lignee JSON"
    )
    parser.add_argument(
        "--summary", action="store_true",
        help="Afficher le resume de la lignee"
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 70)
    logger.info("data_lineage_tracker.py — Lignee des donnees")
    logger.info("=" * 70)

    tracker = LineageTracker(lineage_file=args.lineage_file)

    if args.summary:
        summary = tracker.get_summary()
        print(f"\n{'='*70}")
        print("RESUME DE LA LIGNEE")
        print(f"{'='*70}")
        print(f"Etapes         : {summary['n_steps']}")
        print(f"Scripts        : {summary['n_scripts']}")
        print(f"Fichiers entree: {summary['n_input_files']}")
        print(f"Fichiers sortie: {summary['n_output_files']}")
        if summary["first_timestamp"]:
            print(f"Premiere etape : {summary['first_timestamp']}")
            print(f"Derniere etape : {summary['last_timestamp']}")
        return

    if args.query:
        chain = tracker.trace_full_lineage(args.query)
        if not chain:
            print(f"Aucune lignee trouvee pour : {args.query}")
            sys.exit(1)

        print(f"\n{'='*70}")
        print(f"LIGNEE POUR : {args.query}")
        print(f"{'='*70}")
        print(format_lineage_chain(chain))
        return

    # Sans argument, afficher toutes les entrees
    entries = tracker.get_all_entries()
    if not entries:
        print("Aucune entree de lignee.")
        return

    print(f"\n{'='*70}")
    print(f"TOUTES LES ENTREES DE LIGNEE ({len(entries)})")
    print(f"{'='*70}")
    for entry in entries:
        print(f"\n[{entry['timestamp']}] {entry['step_name']} ({entry['script']})")
        print(f"  inputs:  {', '.join(entry.get('inputs', []))}")
        print(f"  outputs: {', '.join(entry.get('outputs', []))}")


if __name__ == "__main__":
    main()
