#!/usr/bin/env python3
"""
pilier_audit_trail.py — Pilier 5 : Traçabilité complète
========================================================

Système d'audit trail pour le pipeline de données hippiques.
Chaque transformation est loguée avec timestamp, script source,
fichiers en entrée/sortie, et métriques.

Fonctionnalités :
  1. Logger — enregistre chaque exécution de script
  2. Lineage — trace la provenance de chaque fichier
  3. Query  — interroge l'historique d'un fichier ou d'un champ
  4. Report — génère un rapport de traçabilité complet
  5. Décorateur — @audit_step pour instrumenter automatiquement les scripts

Le journal est stocké dans logs/audit_trail.jsonl (append-only).

Usage:
    # En tant que module (dans un autre script):
    from pilier_audit_trail import AuditTrail
    audit = AuditTrail()
    audit.log_step("merge_pedigree", inputs=["08_pedigree/*.json"], outputs=["pedigree_master.json"])

    # En ligne de commande (query):
    python pilier_audit_trail.py lineage pedigree_master.json
    python pilier_audit_trail.py history merge_pedigree
    python pilier_audit_trail.py report
    python pilier_audit_trail.py stats
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
LOGS_DIR = BASE_DIR / "logs"
AUDIT_FILE = LOGS_DIR / "audit_trail.jsonl"


class AuditTrail:
    """Système d'audit trail pour le pipeline."""

    def __init__(self, audit_file: Path = AUDIT_FILE):
        self.audit_file = audit_file
        self.audit_file.parent.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------
    # 1. Logger — enregistrer une étape
    # -------------------------------------------------------------------

    def log_step(
        self,
        script_name: str,
        step_name: str = "",
        inputs: list[str] = None,
        outputs: list[str] = None,
        metrics: dict = None,
        status: str = "success",
        error: str = "",
        duration_s: float = 0,
        details: dict = None,
    ):
        """Enregistre une étape de transformation dans le journal."""
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "script": script_name,
            "step": step_name or script_name,
            "inputs": inputs or [],
            "outputs": outputs or [],
            "metrics": metrics or {},
            "status": status,
            "error": error,
            "duration_s": round(duration_s, 2),
            "details": details or {},
        }

        # Ajouter les tailles de fichiers en sortie
        for out_path in (outputs or []):
            full_path = BASE_DIR / out_path
            if full_path.exists():
                size_mb = full_path.stat().st_size / (1024 * 1024)
                entry["metrics"][f"size_mb_{Path(out_path).name}"] = round(size_mb, 1)

        with open(self.audit_file, "a", encoding="utf-8", errors="replace") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        return entry

    def log_start(self, script_name: str, step_name: str = "", inputs: list[str] = None):
        """Log le début d'une étape et retourne un contexte pour log_end."""
        return {
            "script": script_name,
            "step": step_name or script_name,
            "inputs": inputs or [],
            "t0": time.time(),
        }

    def log_end(
        self,
        context: dict,
        outputs: list[str] = None,
        metrics: dict = None,
        status: str = "success",
        error: str = "",
    ):
        """Log la fin d'une étape à partir du contexte de log_start."""
        duration = time.time() - context["t0"]
        return self.log_step(
            script_name=context["script"],
            step_name=context["step"],
            inputs=context["inputs"],
            outputs=outputs or [],
            metrics=metrics or {},
            status=status,
            error=error,
            duration_s=duration,
        )

    # -------------------------------------------------------------------
    # 2. Lineage — tracer la provenance d'un fichier
    # -------------------------------------------------------------------

    def get_lineage(self, filename: str) -> list[dict]:
        """Retourne toute la chaîne de provenance d'un fichier.

        Remonte récursivement : qui a produit ce fichier → quels étaient ses inputs
        → qui a produit ces inputs → etc.
        """
        entries = self._load_entries()

        # Trouver les étapes qui ont produit ce fichier
        producers = []
        for entry in entries:
            for out in entry.get("outputs", []):
                if filename in out or Path(out).name == filename:
                    producers.append(entry)

        if not producers:
            return [{"file": filename, "status": "AUCUN PRODUCTEUR TROUVÉ"}]

        lineage = []
        visited = set()

        def trace(fname, depth=0):
            if fname in visited or depth > 10:
                return
            visited.add(fname)

            for entry in entries:
                for out in entry.get("outputs", []):
                    if fname in out or Path(out).name == fname:
                        node = {
                            "depth": depth,
                            "file": fname,
                            "produced_by": entry["script"],
                            "step": entry.get("step", ""),
                            "timestamp": entry["timestamp"],
                            "inputs": entry.get("inputs", []),
                            "metrics": entry.get("metrics", {}),
                        }
                        lineage.append(node)

                        # Remonter les inputs
                        for inp in entry.get("inputs", []):
                            trace(Path(inp).name, depth + 1)

        trace(filename)
        return lineage

    # -------------------------------------------------------------------
    # 3. Query — historique d'un script ou recherche
    # -------------------------------------------------------------------

    def get_history(self, script_name: str = None, last_n: int = 50) -> list[dict]:
        """Retourne l'historique d'exécution d'un script (ou de tous)."""
        entries = self._load_entries()

        if script_name:
            entries = [e for e in entries if script_name in e.get("script", "")]

        return entries[-last_n:]

    def search(self, query: str) -> list[dict]:
        """Recherche dans tout le journal d'audit."""
        entries = self._load_entries()
        query_lower = query.lower()

        results = []
        for entry in entries:
            entry_str = json.dumps(entry, ensure_ascii=False).lower()
            if query_lower in entry_str:
                results.append(entry)

        return results

    # -------------------------------------------------------------------
    # 4. Report — rapport de traçabilité
    # -------------------------------------------------------------------

    def generate_report(self) -> str:
        """Génère un rapport complet de traçabilité."""
        entries = self._load_entries()

        if not entries:
            return "Aucune entrée dans le journal d'audit."

        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        # Stats globales
        scripts = set()
        total_duration = 0
        statuses = {"success": 0, "error": 0, "warning": 0}
        all_outputs = set()
        all_inputs = set()

        for entry in entries:
            scripts.add(entry.get("script", ""))
            total_duration += entry.get("duration_s", 0)
            status = entry.get("status", "success")
            statuses[status] = statuses.get(status, 0) + 1
            for o in entry.get("outputs", []):
                all_outputs.add(o)
            for i in entry.get("inputs", []):
                all_inputs.add(i)

        lines = []
        lines.append("# Rapport d'Audit Trail")
        lines.append(f"")
        lines.append(f"*Généré le {now}*")
        lines.append(f"")
        lines.append(f"## Résumé")
        lines.append(f"")
        lines.append(f"| Métrique | Valeur |")
        lines.append(f"|----------|--------|")
        lines.append(f"| Entrées journal | {len(entries)} |")
        lines.append(f"| Scripts distincts | {len(scripts)} |")
        lines.append(f"| Fichiers produits | {len(all_outputs)} |")
        lines.append(f"| Fichiers consommés | {len(all_inputs)} |")
        lines.append(f"| Durée cumulée | {total_duration:.0f}s ({total_duration/3600:.1f}h) |")
        lines.append(f"| Succès | {statuses.get('success', 0)} |")
        lines.append(f"| Erreurs | {statuses.get('error', 0)} |")
        lines.append(f"")

        # Timeline
        lines.append(f"## Timeline des exécutions")
        lines.append(f"")
        lines.append(f"| Timestamp | Script | Step | Durée | Status |")
        lines.append(f"|-----------|--------|------|-------|--------|")
        for entry in entries[-50:]:
            ts = entry.get("timestamp", "?")[:19]
            script = entry.get("script", "?")
            step = entry.get("step", "")
            dur = entry.get("duration_s", 0)
            status = entry.get("status", "?")
            lines.append(f"| {ts} | {script} | {step} | {dur:.1f}s | {status} |")
        lines.append(f"")

        # Graphe de dépendances
        lines.append(f"## Graphe de dépendances (fichiers)")
        lines.append(f"")
        lines.append("```")
        for entry in entries:
            script = entry.get("script", "?")
            inputs = entry.get("inputs", [])
            outputs = entry.get("outputs", [])
            if inputs or outputs:
                inp_str = ", ".join(Path(i).name for i in inputs[:3])
                out_str = ", ".join(Path(o).name for o in outputs[:3])
                lines.append(f"  [{inp_str}] --({script})--> [{out_str}]")
        lines.append("```")
        lines.append(f"")

        return "\n".join(lines)

    def print_stats(self):
        """Affiche les statistiques du journal d'audit."""
        entries = self._load_entries()
        print(f"\n--- Audit Trail Stats ---")
        print(f"  Entrées totales : {len(entries)}")

        if not entries:
            return

        scripts = {}
        for e in entries:
            s = e.get("script", "?")
            scripts[s] = scripts.get(s, 0) + 1

        print(f"  Scripts distincts : {len(scripts)}")
        print(f"\n  Exécutions par script :")
        for s, count in sorted(scripts.items(), key=lambda x: -x[1]):
            print(f"    {s:<40s} : {count}")

        # Dernière entrée
        last = entries[-1]
        print(f"\n  Dernière entrée :")
        print(f"    Timestamp : {last.get('timestamp', '?')}")
        print(f"    Script    : {last.get('script', '?')}")
        print(f"    Status    : {last.get('status', '?')}")

    # -------------------------------------------------------------------
    # 5. Décorateur — @audit_step
    # -------------------------------------------------------------------

    def audit_step(self, script_name: str, inputs: list[str] = None, outputs: list[str] = None):
        """Décorateur pour instrumenter automatiquement une fonction."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                ctx = self.log_start(script_name, step_name=func.__name__, inputs=inputs or [])
                try:
                    result = func(*args, **kwargs)
                    metrics = result if isinstance(result, dict) else {}
                    self.log_end(ctx, outputs=outputs or [], metrics=metrics, status="success")
                    return result
                except Exception as exc:
                    self.log_end(ctx, outputs=outputs or [], status="error", error=str(exc))
                    raise
            wrapper.__name__ = func.__name__
            wrapper.__doc__ = func.__doc__
            return wrapper
        return decorator

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    def _load_entries(self) -> list[dict]:
        """Charge toutes les entrées du journal."""
        if not self.audit_file.exists():
            return []

        entries = []
        with open(self.audit_file, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return entries


# -----------------------------------------------------------------------
# Fonction utilitaire pour les autres scripts
# -----------------------------------------------------------------------

_default_audit = None


def get_audit() -> AuditTrail:
    """Retourne l'instance globale d'AuditTrail."""
    global _default_audit
    if _default_audit is None:
        _default_audit = AuditTrail()
    return _default_audit


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Pilier 5 — Audit Trail du pipeline hippique",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python pilier_audit_trail.py lineage pedigree_master.json
  python pilier_audit_trail.py history merge_pedigree
  python pilier_audit_trail.py search "enrichissement"
  python pilier_audit_trail.py report
  python pilier_audit_trail.py stats
  python pilier_audit_trail.py log --script merge_test --inputs a.json b.json --outputs c.json
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commande")

    # lineage
    p_lineage = subparsers.add_parser("lineage", help="Tracer la provenance d'un fichier")
    p_lineage.add_argument("filename", help="Nom du fichier à tracer")

    # history
    p_history = subparsers.add_parser("history", help="Historique d'un script")
    p_history.add_argument("script", nargs="?", default=None, help="Nom du script (optionnel)")
    p_history.add_argument("--last", type=int, default=20, help="Nombre d'entrées (défaut: 20)")

    # search
    p_search = subparsers.add_parser("search", help="Rechercher dans le journal")
    p_search.add_argument("query", help="Terme de recherche")

    # report
    subparsers.add_parser("report", help="Générer le rapport de traçabilité")

    # stats
    subparsers.add_parser("stats", help="Statistiques du journal")

    # log (ajout manuel)
    p_log = subparsers.add_parser("log", help="Ajouter une entrée manuellement")
    p_log.add_argument("--script", required=True, help="Nom du script")
    p_log.add_argument("--step", default="", help="Nom de l'étape")
    p_log.add_argument("--inputs", nargs="*", default=[], help="Fichiers en entrée")
    p_log.add_argument("--outputs", nargs="*", default=[], help="Fichiers en sortie")
    p_log.add_argument("--status", default="success", help="Statut (success/error)")
    p_log.add_argument("--duration", type=float, default=0, help="Durée en secondes")

    # backfill — scanner les fichiers existants pour reconstituer l'historique
    subparsers.add_parser("backfill", help="Reconstituer l'audit trail depuis les fichiers existants")

    args = parser.parse_args()
    audit = AuditTrail()

    if args.command == "lineage":
        lineage = audit.get_lineage(args.filename)
        if not lineage:
            print(f"Aucune entrée trouvée pour '{args.filename}'")
        else:
            print(f"\nLineage de '{args.filename}' :")
            for node in lineage:
                indent = "  " * node.get("depth", 0)
                print(f"{indent}← {node.get('produced_by', '?')} "
                      f"({node.get('timestamp', '?')[:19]})")
                for inp in node.get("inputs", []):
                    print(f"{indent}   ← source: {inp}")

    elif args.command == "history":
        entries = audit.get_history(args.script, last_n=args.last)
        if not entries:
            print("Aucune entrée trouvée.")
        else:
            print(f"\nHistorique ({len(entries)} entrées) :")
            for e in entries:
                ts = e.get("timestamp", "?")[:19]
                status = e.get("status", "?")
                dur = e.get("duration_s", 0)
                script = e.get("script", "?")
                step = e.get("step", "")
                print(f"  [{ts}] {script} / {step} — {status} ({dur:.1f}s)")

    elif args.command == "search":
        results = audit.search(args.query)
        print(f"\n{len(results)} résultat(s) pour '{args.query}' :")
        for r in results:
            ts = r.get("timestamp", "?")[:19]
            script = r.get("script", "?")
            print(f"  [{ts}] {script} — {r.get('step', '')}")

    elif args.command == "report":
        md = audit.generate_report()
        report_path = LOGS_DIR / "AUDIT_REPORT.md"
        with open(report_path, "w", encoding="utf-8", errors="replace") as f:
            f.write(md)
        print(f"Rapport généré : {report_path}")
        print(md[:2000])

    elif args.command == "stats":
        audit.print_stats()

    elif args.command == "log":
        entry = audit.log_step(
            script_name=args.script,
            step_name=args.step,
            inputs=args.inputs,
            outputs=args.outputs,
            status=args.status,
            duration_s=args.duration,
        )
        print(f"Entrée ajoutée : {json.dumps(entry, ensure_ascii=False)}")

    elif args.command == "backfill":
        backfill_from_filesystem(audit)

    else:
        parser.print_help()


def backfill_from_filesystem(audit: AuditTrail):
    """Reconstitue l'audit trail en scannant les fichiers existants."""
    print("Backfill — Reconstitution de l'audit trail depuis le filesystem")
    print("=" * 60)

    # Scanner les scripts et leurs outputs probables
    script_output_map = {
        "merge_pedigree_master.py": [
            "data_master/pedigree_master.json",
            "data_master/pedigree_master.csv",
            "data_master/pedigree_master.parquet",
        ],
        "merge_equipements_master.py": [
            "data_master/equipements_master.json",
            "data_master/equipements_master.parquet",
        ],
        "merge_meteo_master.py": [
            "data_master/meteo_master.json",
            "data_master/meteo_master.csv",
            "data_master/meteo_master.parquet",
        ],
        "merge_rapports_master.py": [
            "data_master/rapports_master.json",
            "data_master/rapports_master.parquet",
        ],
        "merge_marche_master.py": [
            "data_master/marche_master.json",
            "data_master/marche_master.parquet",
        ],
        "merge_performances_master.py": [
            "data_master/performances_master.json",
        ],
        "mega_merge_courses.py": [
            "data_master/courses_master.jsonl",
            "data_master/partants_master.jsonl",
        ],
        "postprocess_meteo.py": ["data_master/meteo_master.json"],
        "postprocess_rapports.py": ["data_master/rapports_master.json"],
        "postprocess_marche.py": ["data_master/marche_master.json"],
        "postprocess_equipements.py": ["data_master/equipements_master.json"],
        "postprocess_horse_stats.py": ["data_master/horse_stats_master.json"],
        "entity_resolution.py": [
            "data_master/partants_master.jsonl",
            "data_master/courses_master.jsonl",
        ],
        "fill_empty_fields.py": [
            "output/02_filled/courses_normalisees.json",
            "output/02_filled/partants_normalises.json",
        ],
        "enrichissement_champs.py": [
            "data_master/partants_master_enrichi.jsonl",
        ],
    }

    # Scanner les feat_* scripts
    for fname in sorted(os.listdir(BASE_DIR)):
        if fname.startswith("feat_") and fname.endswith(".py"):
            feat_name = fname.replace(".py", "").replace("feat_", "")
            output_file = f"output/features/{feat_name}_features.jsonl"
            if (BASE_DIR / output_file).exists():
                script_output_map[fname] = [output_file]

    count = 0
    for script, outputs in script_output_map.items():
        script_path = BASE_DIR / script
        if not script_path.exists():
            continue

        existing_outputs = [o for o in outputs if (BASE_DIR / o).exists()]
        if not existing_outputs:
            continue

        # Utiliser la date de modification du fichier comme timestamp approximatif
        latest_mtime = max(
            (BASE_DIR / o).stat().st_mtime
            for o in existing_outputs
        )
        ts = datetime.fromtimestamp(latest_mtime).isoformat() + "Z"

        entry = {
            "timestamp": ts,
            "script": script,
            "step": "backfill",
            "inputs": [],
            "outputs": existing_outputs,
            "metrics": {},
            "status": "success",
            "error": "",
            "duration_s": 0,
            "details": {"backfill": True},
        }

        # Ajouter tailles
        for out in existing_outputs:
            full = BASE_DIR / out
            if full.exists():
                size_mb = full.stat().st_size / (1024 * 1024)
                entry["metrics"][f"size_mb_{Path(out).name}"] = round(size_mb, 1)

        with open(audit.audit_file, "a", encoding="utf-8", errors="replace") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        count += 1
        print(f"  + {script:<40s} → {', '.join(Path(o).name for o in existing_outputs)}")

    print(f"\n{count} entrées ajoutées au journal d'audit.")
    print(f"Journal : {audit.audit_file}")


if __name__ == "__main__":
    main()
