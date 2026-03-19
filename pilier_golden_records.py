#!/usr/bin/env python3
"""
pilier_golden_records.py -- Pilier Qualite : Reconciliation Golden Records
==========================================================================

Pour chaque entite (cheval, jockey, entraineur), fusionne les informations
de toutes les sources, score la confiance par champ selon l'accord inter-sources,
et signale les conflits.

Fonctionnalites :
  1. Merge des infos de toutes les sources pour chaque entite
  2. Score de confiance par champ (0-1) base sur l'accord entre sources
  3. Flag des conflits entre sources
  4. Export du rapport de reconciliation

Usage:
    python pilier_golden_records.py
    python pilier_golden_records.py --entity cheval
    python pilier_golden_records.py --limit 1000
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
REPORT_FILE = LOGS_DIR / "golden_records_report.json"


# -----------------------------------------------------------------------
# Chargement donnees
# -----------------------------------------------------------------------

def load_jsonl(filepath: Path) -> list[dict]:
    records = []
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def load_json(filepath: Path) -> list[dict]:
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def load_data(filepath: Path) -> list[dict]:
    suffix = filepath.suffix.lower()
    if suffix == ".jsonl":
        return load_jsonl(filepath)
    elif suffix == ".json":
        return load_json(filepath)
    return []


# -----------------------------------------------------------------------
# Normalisation des noms d'entite
# -----------------------------------------------------------------------

def normalize_name(name) -> str:
    """Normalise un nom pour le matching."""
    if not name or not isinstance(name, str):
        return ""
    name = name.strip().upper()
    # Supprimer les accents communs
    replacements = {
        "E": "E", "E": "E", "A": "A", "U": "U",
        "I": "I", "O": "O", "C": "C",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    # Supprimer ponctuation
    name = "".join(c if c.isalnum() or c == " " else " " for c in name)
    # Normaliser espaces
    name = " ".join(name.split())
    return name


# -----------------------------------------------------------------------
# Extraction des entites par type
# -----------------------------------------------------------------------

# Champs cle pour identifier les entites
ENTITY_KEYS = {
    "cheval": ["nom_cheval", "cheval", "horse_name", "nom"],
    "jockey": ["nom_jockey", "jockey", "jockey_name", "driver"],
    "entraineur": ["nom_entraineur", "entraineur", "trainer", "trainer_name"],
}

# Champs d'interet par type d'entite
ENTITY_FIELDS = {
    "cheval": [
        "nom_cheval", "cheval", "horse_name", "nom",
        "sexe", "age", "race", "robe", "pere", "mere",
        "naisseur", "proprietaire", "entraineur",
        "gains", "nb_courses", "nb_victoires",
        "musique", "derniere_perf", "allocation",
    ],
    "jockey": [
        "nom_jockey", "jockey", "jockey_name", "driver",
        "nb_victoires", "nb_courses", "taux_reussite",
        "gains", "specialite",
    ],
    "entraineur": [
        "nom_entraineur", "entraineur", "trainer", "trainer_name",
        "nb_victoires", "nb_courses", "taux_reussite",
        "gains", "ecurie",
    ],
}


def extract_entity_key(record: dict, entity_type: str) -> str:
    """Extrait la cle d'identification d'une entite."""
    for key in ENTITY_KEYS.get(entity_type, []):
        val = record.get(key)
        if val and isinstance(val, str) and len(val.strip()) > 1:
            return normalize_name(val)
    return ""


def extract_entity_fields(record: dict, entity_type: str, source: str) -> dict:
    """Extrait les champs pertinents d'un enregistrement."""
    fields = {}
    relevant = ENTITY_FIELDS.get(entity_type, [])

    for key, val in record.items():
        if val is None or val == "":
            continue
        if key in relevant or key.startswith(("nb_", "taux_", "gain")):
            fields[key] = {"value": val, "source": source}

    return fields


# -----------------------------------------------------------------------
# Golden record builder
# -----------------------------------------------------------------------

class GoldenRecordBuilder:
    """Construit les golden records par reconciliation multi-sources."""

    def __init__(self):
        # {entity_key: {field_name: [{value, source}, ...]}}
        self.entities = defaultdict(lambda: defaultdict(list))
        self.source_counts = Counter()

    def ingest(self, records: list[dict], source: str, entity_type: str):
        """Ingere des enregistrements d'une source."""
        for rec in records:
            key = extract_entity_key(rec, entity_type)
            if not key:
                continue

            fields = extract_entity_fields(rec, entity_type, source)
            for field_name, field_info in fields.items():
                self.entities[key][field_name].append(field_info)

            self.source_counts[source] += 1

    def build_golden_records(self) -> dict:
        """Construit les golden records avec scores de confiance."""
        golden = {}

        for entity_key, fields in self.entities.items():
            record = {
                "entity_key": entity_key,
                "fields": {},
                "conflicts": [],
                "n_sources": 0,
                "confidence_avg": 0.0,
            }

            sources_seen = set()
            confidence_sum = 0.0
            n_scored = 0

            for field_name, observations in fields.items():
                if not observations:
                    continue

                # Collecter les sources
                for obs in observations:
                    sources_seen.add(obs["source"])

                # Grouper les valeurs
                value_groups = defaultdict(list)
                for obs in observations:
                    val_str = str(obs["value"]).strip()
                    if val_str:
                        value_groups[val_str].append(obs["source"])

                if not value_groups:
                    continue

                # Vote majoritaire
                sorted_vals = sorted(
                    value_groups.items(),
                    key=lambda x: len(x[1]),
                    reverse=True,
                )

                best_val, best_sources = sorted_vals[0]
                total_votes = sum(len(s) for s in value_groups.values())

                # Score de confiance
                confidence = len(best_sources) / total_votes if total_votes > 0 else 0.0

                record["fields"][field_name] = {
                    "golden_value": best_val,
                    "confidence": round(confidence, 3),
                    "n_sources_agree": len(best_sources),
                    "n_observations": total_votes,
                    "sources": best_sources,
                }

                confidence_sum += confidence
                n_scored += 1

                # Detecter les conflits
                if len(sorted_vals) > 1:
                    conflict = {
                        "field": field_name,
                        "values": {},
                    }
                    for val, srcs in sorted_vals[:5]:
                        conflict["values"][val] = srcs

                    record["conflicts"].append(conflict)

            record["n_sources"] = len(sources_seen)
            record["n_conflicts"] = len(record["conflicts"])
            record["confidence_avg"] = (
                round(confidence_sum / n_scored, 3) if n_scored > 0 else 0.0
            )

            golden[entity_key] = record

        return golden


# -----------------------------------------------------------------------
# Scan des sources
# -----------------------------------------------------------------------

def find_data_files() -> list[tuple]:
    """Trouve les fichiers de donnees avec leur nom de source."""
    files = []

    # data_master/
    if DATA_MASTER.exists():
        for f in DATA_MASTER.iterdir():
            if f.suffix in (".json", ".jsonl") and not f.name.endswith(".tmp"):
                files.append((f, f"master/{f.stem}"))

    # output/ subdirs
    if OUTPUT_DIR.exists():
        for subdir in OUTPUT_DIR.iterdir():
            if not subdir.is_dir():
                continue
            for f in subdir.rglob("*"):
                if f.suffix in (".json", ".jsonl"):
                    files.append((f, f"output/{subdir.name}"))

    return files


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Golden records - reconciliation multi-sources")
    parser.add_argument("--entity", "-e", choices=["cheval", "jockey", "entraineur"],
                        default="cheval", help="Type d'entite (defaut: cheval)")
    parser.add_argument("--limit", "-l", type=int, default=0,
                        help="Limite de records par fichier (0=illimite)")
    parser.add_argument("--output", "-o", help="Fichier rapport de sortie")
    parser.add_argument("--top-conflicts", type=int, default=50,
                        help="Nombre de top conflits a afficher")
    args = parser.parse_args()

    print("=" * 60)
    print(f"PILIER GOLDEN RECORDS - Entite: {args.entity}")
    print("=" * 60)

    builder = GoldenRecordBuilder()

    # Trouver les fichiers
    data_files = find_data_files()
    print(f"Fichiers trouves: {len(data_files)}")
    print("-" * 60)

    # Ingerer
    for filepath, source_name in data_files:
        try:
            records = load_data(filepath)
            if args.limit > 0:
                records = records[:args.limit]

            if not records:
                continue

            # Verifier si ce fichier contient des entites de ce type
            sample = records[:10]
            has_entity = any(
                extract_entity_key(r, args.entity)
                for r in sample
            )
            if not has_entity:
                continue

            builder.ingest(records, source_name, args.entity)
            n = builder.source_counts[source_name]
            print(f"  {source_name}: {n} entites ingerees")

        except Exception as e:
            print(f"  {source_name}: ERREUR - {e}")
            continue

    print("-" * 60)
    print(f"Total entites uniques: {len(builder.entities)}")
    print(f"Total sources actives: {len(builder.source_counts)}")

    # Construire golden records
    print("Construction des golden records ...")
    golden = builder.build_golden_records()

    # Statistiques
    n_with_conflicts = sum(1 for g in golden.values() if g["n_conflicts"] > 0)
    avg_confidence = (
        sum(g["confidence_avg"] for g in golden.values()) / len(golden)
        if golden else 0
    )

    print(f"Golden records: {len(golden)}")
    print(f"Avec conflits: {n_with_conflicts}")
    print(f"Confiance moyenne: {avg_confidence:.3f}")

    # Top conflits
    top_conflicts = sorted(
        [(k, v) for k, v in golden.items() if v["n_conflicts"] > 0],
        key=lambda x: x[1]["n_conflicts"],
        reverse=True,
    )[:args.top_conflicts]

    if top_conflicts:
        print("-" * 60)
        print(f"Top {min(len(top_conflicts), args.top_conflicts)} entites les plus conflictuelles:")
        for key, info in top_conflicts[:10]:
            print(f"  {key}: {info['n_conflicts']} conflits, "
                  f"confiance={info['confidence_avg']:.2f}, "
                  f"sources={info['n_sources']}")

    # Sauvegarder le rapport
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "entity_type": args.entity,
        "summary": {
            "total_golden_records": len(golden),
            "with_conflicts": n_with_conflicts,
            "without_conflicts": len(golden) - n_with_conflicts,
            "avg_confidence": round(avg_confidence, 4),
            "sources_used": dict(builder.source_counts),
        },
        "top_conflicts": [
            {
                "entity": key,
                "n_conflicts": info["n_conflicts"],
                "confidence_avg": info["confidence_avg"],
                "conflicts": info["conflicts"][:10],
            }
            for key, info in top_conflicts
        ],
        # Sauvegarder un echantillon de golden records (pas tout pour eviter
        # un fichier trop gros)
        "sample_golden_records": {
            k: v for k, v in list(golden.items())[:200]
        },
    }

    out_path = Path(args.output) if args.output else REPORT_FILE
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("-" * 60)
    print(f"Rapport sauvegarde: {out_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
