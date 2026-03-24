#!/usr/bin/env python3
"""
integrate_new_source.py -- Outil generique pour integrer une nouvelle source de scraper.

Etape 8 : integration de nouvelles sources dans le pipeline.

Ce script prend le repertoire de sortie d'un scraper et genere :
  1. Un squelette de merge script (merge_<source>_master.py)
  2. Un squelette de feature builder (feature_builders/<source>_builder.py)
  3. Met a jour run_pipeline.py avec les nouvelles entrees DAG
  4. Mappe les champs du JSONL vers le schema standard via un fichier de mapping

Usage :
    python scripts/integrate_new_source.py \\
        --source-dir output/117_jockey_planet \\
        --source-name jockey_planet \\
        --source-id 117 \\
        [--mapping-config config/mappings/117_jockey_planet.yaml] \\
        [--dry-run]

Le mapping config YAML a la forme :
    source_field: standard_field
    ex:
      course_name: nom_course
      race_date: date
      track: hippodrome
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import textwrap
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# ---------------------------------------------------------------------------
# Standard schema fields (champs attendus dans partants_master.jsonl)
# ---------------------------------------------------------------------------
STANDARD_FIELDS = [
    "date",
    "hippodrome",
    "reunion",
    "course",
    "nom_cheval",
    "numero",
    "jockey",
    "entraineur",
    "proprietaire",
    "poids",
    "corde",
    "cote_probable",
    "resultat",
    "gains",
    "distance",
    "discipline",
    "terrain",
    "allocation",
    "uid_course",
    "uid_partant",
]


def discover_jsonl_files(source_dir: Path) -> list[Path]:
    """Find all JSONL files in source directory."""
    jsonl_files = sorted(source_dir.glob("*.jsonl"))
    if not jsonl_files:
        # Also check for .json files (some scrapers output JSON)
        jsonl_files = sorted(source_dir.glob("*.json"))
    return jsonl_files


def read_sample_records(jsonl_path: Path, max_records: int = 50) -> list[dict]:
    """Read a sample of records from a JSONL file."""
    records: list[dict] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= max_records:
                break
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def extract_fields(records: list[dict]) -> dict[str, int]:
    """Extract field names and their frequency across sample records."""
    field_counts: dict[str, int] = {}
    for rec in records:
        for key in rec:
            field_counts[key] = field_counts.get(key, 0) + 1
    return dict(sorted(field_counts.items(), key=lambda x: -x[1]))


def load_mapping_config(mapping_path: Path) -> dict[str, str]:
    """Load field mapping from YAML config file."""
    try:
        import yaml
    except ImportError:
        print("[WARN] pyyaml non installe, lecture du mapping en JSON fallback.")
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)

    with open(mapping_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if isinstance(data, dict):
        return data
    return {}


def suggest_mappings(source_fields: list[str]) -> dict[str, str]:
    """Suggest automatic mappings from source fields to standard schema."""
    # Heuristic mapping based on common field name patterns
    patterns: dict[str, list[str]] = {
        "date": ["date", "race_date", "jour", "event_date", "date_course"],
        "hippodrome": ["hippodrome", "track", "racecourse", "venue", "course_name",
                       "track_name", "lieu"],
        "reunion": ["reunion", "meeting", "num_reunion", "r"],
        "course": ["course", "race", "race_number", "num_course", "c"],
        "nom_cheval": ["nom_cheval", "cheval", "horse", "horse_name", "runner",
                       "runner_name", "nom"],
        "numero": ["numero", "number", "num", "cloth", "saddle_number", "numPmu"],
        "jockey": ["jockey", "jockey_name", "rider", "driver"],
        "entraineur": ["entraineur", "trainer", "trainer_name", "coach"],
        "poids": ["poids", "weight", "poids_monte"],
        "corde": ["corde", "draw", "barrier", "stall"],
        "cote_probable": ["cote_probable", "cote", "odds", "sp", "starting_price"],
        "resultat": ["resultat", "result", "finish", "position", "place",
                      "finishing_position", "ordreArrivee"],
        "distance": ["distance", "dist", "race_distance"],
        "discipline": ["discipline", "type", "race_type", "specialite"],
        "terrain": ["terrain", "going", "ground", "track_condition"],
    }

    suggestions: dict[str, str] = {}
    for src_field in source_fields:
        src_lower = src_field.lower().strip()
        for std_field, aliases in patterns.items():
            if src_lower in aliases:
                suggestions[src_field] = std_field
                break
    return suggestions


def generate_merge_script(source_name: str, source_id: int,
                          source_fields: list[str],
                          mapping: dict[str, str],
                          output_path: Path) -> Path:
    """Generate a merge script skeleton for the new source."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_name)
    script_name = f"merge_{safe_name}_master.py"
    script_path = output_path / script_name

    # Build mapping lines
    mapping_lines = ""
    for src, dst in sorted(mapping.items()):
        mapping_lines += f'    "{src}": "{dst}",\n'

    content = textwrap.dedent(f"""\
        #!/usr/bin/env python3
        \"\"\"
        {script_name} -- Merge script pour la source {source_id} ({source_name}).

        Genere automatiquement par integrate_new_source.py le {datetime.now():%Y-%m-%d}.
        A adapter manuellement pour les besoins specifiques de la source.

        Usage :
            python {script_name}
        \"\"\"

        from __future__ import annotations

        import json
        import sys
        from pathlib import Path

        PROJECT_ROOT = Path(__file__).resolve().parent
        sys.path.insert(0, str(PROJECT_ROOT))

        from config import OUTPUT_DIR, DATA_MASTER_DIR  # noqa: E402

        # ---------------------------------------------------------------------------
        # Configuration
        # ---------------------------------------------------------------------------

        SOURCE_DIR = OUTPUT_DIR / "{source_id:02d}_{safe_name}"
        OUTPUT_FILE = DATA_MASTER_DIR / "{safe_name}_master.jsonl"

        FIELD_MAPPING = {{
        {mapping_lines}}}


        def map_record(raw: dict) -> dict:
            \"\"\"Map raw record fields to standard schema.\"\"\"
            mapped = {{}}
            for src_field, std_field in FIELD_MAPPING.items():
                if src_field in raw:
                    mapped[std_field] = raw[src_field]
            # Keep unmapped fields under 'extra' namespace
            mapped_src_fields = set(FIELD_MAPPING.keys())
            extras = {{k: v for k, v in raw.items() if k not in mapped_src_fields}}
            if extras:
                mapped["extra_{safe_name}"] = extras
            return mapped


        def main() -> None:
            \"\"\"Main merge logic.\"\"\"
            source_files = sorted(SOURCE_DIR.glob("*.jsonl"))
            if not source_files:
                print(f"[WARN] Aucun fichier JSONL dans {{SOURCE_DIR}}")
                return

            count = 0
            OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
                for fpath in source_files:
                    with open(fpath, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                raw = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            mapped = map_record(raw)
                            out.write(json.dumps(mapped, ensure_ascii=False) + "\\n")
                            count += 1

            print(f"[OK] {{count}} records ecrits dans {{OUTPUT_FILE}}")


        if __name__ == "__main__":
            main()
    """)

    script_path.write_text(content, encoding="utf-8")
    return script_path


def generate_feature_builder(source_name: str, source_id: int,
                             source_fields: list[str],
                             output_path: Path) -> Path:
    """Generate a feature builder skeleton for the new source."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_name)
    script_name = f"{safe_name}_builder.py"
    fb_dir = output_path / "feature_builders"
    fb_dir.mkdir(parents=True, exist_ok=True)
    script_path = fb_dir / script_name

    # Suggest feature columns based on detected numeric-looking fields
    feature_candidates = [f for f in source_fields if f not in STANDARD_FIELDS]

    content = textwrap.dedent(f"""\
        #!/usr/bin/env python3
        \"\"\"
        {script_name} -- Feature builder pour la source {source_id} ({source_name}).

        Genere automatiquement par integrate_new_source.py le {datetime.now():%Y-%m-%d}.
        A adapter manuellement pour definir les features a extraire.

        Usage :
            python feature_builders/{script_name}
        \"\"\"

        from __future__ import annotations

        import json
        import sys
        from pathlib import Path

        PROJECT_ROOT = Path(__file__).resolve().parent.parent
        sys.path.insert(0, str(PROJECT_ROOT))

        from config import DATA_MASTER_DIR, FEATURES_DIR  # noqa: E402

        # ---------------------------------------------------------------------------
        # Configuration
        # ---------------------------------------------------------------------------

        SOURCE_MASTER = DATA_MASTER_DIR / "{safe_name}_master.jsonl"
        OUTPUT_FILE = FEATURES_DIR / "feat_{safe_name}.jsonl"

        # Champs candidats pour features (a filtrer manuellement) :
        # {', '.join(feature_candidates[:15])}


        def build_features(record: dict) -> dict | None:
            \"\"\"Extract features from a single record.

            Retourne un dict avec uid_partant + colonnes de features,
            ou None si le record est inutilisable.
            \"\"\"
            uid = record.get("uid_partant")
            if not uid:
                return None

            features: dict = {{"uid_partant": uid}}

            # TODO: Ajouter les features specifiques a cette source.
            # Exemples :
            #   features["feat_{safe_name}_xxx"] = record.get("xxx")
            #   features["feat_{safe_name}_yyy"] = record.get("yyy")

            return features


        def main() -> None:
            \"\"\"Build features from {safe_name}_master.jsonl.\"\"\"
            if not SOURCE_MASTER.exists():
                print(f"[WARN] {{SOURCE_MASTER}} introuvable. Lancer le merge d'abord.")
                return

            OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
            count = 0
            with open(SOURCE_MASTER, "r", encoding="utf-8") as fin, \\
                 open(OUTPUT_FILE, "w", encoding="utf-8") as fout:
                for line in fin:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    features = build_features(record)
                    if features and len(features) > 1:
                        fout.write(json.dumps(features, ensure_ascii=False) + "\\n")
                        count += 1

            print(f"[OK] {{count}} feature records ecrits dans {{OUTPUT_FILE}}")


        if __name__ == "__main__":
            main()
    """)

    script_path.write_text(content, encoding="utf-8")
    return script_path


def update_run_pipeline(source_name: str, merge_script_name: str,
                        feature_builder_name: str, dry_run: bool) -> None:
    """Show instructions (or patch) to update run_pipeline.py DAG."""
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_name)

    merge_step = f'merge_{safe_name}_master'
    feature_step = f'feat_{safe_name}'

    dag_merge_line = (
        f'    add("{merge_step}", "{merge_script_name}", '
        f'depends_on=merge_deps, phase=5)'
    )
    dag_feature_line = (
        f'    add("{feature_step}", "feature_builders/{feature_builder_name}", '
        f'depends_on=feat_deps, phase=7)'
    )

    run_pipeline_path = PROJECT_ROOT / "run_pipeline.py"

    if dry_run:
        print("\n[DRY-RUN] Lignes a ajouter dans run_pipeline.py :")
        print(f"  Phase 5 (merges) :  {dag_merge_line}")
        print(f"  Phase 7 (features): {dag_feature_line}")
        print(f"  Phase 6 all_merges: ajouter \"{merge_step}\"")
        return

    if not run_pipeline_path.exists():
        print(f"[WARN] {run_pipeline_path} introuvable, voici les lignes a ajouter:")
        print(f"  Phase 5: {dag_merge_line}")
        print(f"  Phase 7: {dag_feature_line}")
        return

    content = run_pipeline_path.read_text(encoding="utf-8")

    # Insert merge step after the last Phase 5 add() call
    phase5_pattern = r'(add\("[^"]+",\s*"[^"]+",\s*depends_on=merge_deps,\s*phase=5\))'
    phase5_matches = list(re.finditer(phase5_pattern, content))
    if phase5_matches:
        last_match = phase5_matches[-1]
        insert_pos = last_match.end()
        content = content[:insert_pos] + "\n" + dag_merge_line + content[insert_pos:]
        print(f"[OK] Merge step '{merge_step}' insere dans run_pipeline.py (Phase 5)")
    else:
        print(f"[WARN] Pattern Phase 5 non trouve. Ajouter manuellement: {dag_merge_line}")

    # Add to all_merges list
    all_merges_pattern = r'(all_merges\s*=\s*\[.*?\])'
    match = re.search(all_merges_pattern, content, re.DOTALL)
    if match:
        old_list = match.group(1)
        # Insert before the closing bracket
        new_list = old_list.rstrip("]").rstrip() + f',\n        "{merge_step}",\n    ]'
        content = content.replace(old_list, new_list)
        print(f"[OK] '{merge_step}' ajoute a all_merges")

    # Insert feature builder after last Phase 7 add() call
    phase7_pattern = r'(add\("[^"]+",\s*"feature_builders/[^"]+",\s*depends_on=feat_deps,\s*phase=7\))'
    phase7_matches = list(re.finditer(phase7_pattern, content))
    if phase7_matches:
        last_match = phase7_matches[-1]
        insert_pos = last_match.end()
        content = content[:insert_pos] + "\n" + dag_feature_line + content[insert_pos:]
        print(f"[OK] Feature step '{feature_step}' insere dans run_pipeline.py (Phase 7)")
    else:
        print(f"[WARN] Pattern Phase 7 non trouve. Ajouter manuellement: {dag_feature_line}")

    run_pipeline_path.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Integre une nouvelle source de scraper dans le pipeline."
    )
    parser.add_argument(
        "--source-dir", required=True,
        help="Repertoire de sortie du scraper (ex: output/117_jockey_planet)"
    )
    parser.add_argument(
        "--source-name", required=True,
        help="Nom court de la source (ex: jockey_planet)"
    )
    parser.add_argument(
        "--source-id", required=True, type=int,
        help="Numero de la source (ex: 117)"
    )
    parser.add_argument(
        "--mapping-config",
        help="Fichier YAML/JSON de mapping champs source -> standard"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Afficher les actions sans modifier de fichiers"
    )
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    if not source_dir.is_absolute():
        source_dir = PROJECT_ROOT / source_dir

    print(f"=== Integration de la source {args.source_id} ({args.source_name}) ===\n")

    # Step 1: Discover JSONL files
    jsonl_files = discover_jsonl_files(source_dir)
    if not jsonl_files:
        print(f"[ERREUR] Aucun fichier JSONL/JSON dans {source_dir}")
        sys.exit(1)

    print(f"[INFO] {len(jsonl_files)} fichier(s) JSONL trouves dans {source_dir}")
    for f in jsonl_files[:5]:
        print(f"  - {f.name}")

    # Step 2: Read sample and extract fields
    sample = read_sample_records(jsonl_files[0])
    if not sample:
        print(f"[ERREUR] Impossible de lire des records depuis {jsonl_files[0]}")
        sys.exit(1)

    fields = extract_fields(sample)
    print(f"\n[INFO] {len(fields)} champs detectes (sur {len(sample)} records):")
    for fname, fcount in list(fields.items())[:20]:
        print(f"  {fname:40s} ({fcount}/{len(sample)} records)")

    # Step 3: Build field mapping
    if args.mapping_config:
        mapping = load_mapping_config(Path(args.mapping_config))
        print(f"\n[INFO] Mapping charge depuis {args.mapping_config} ({len(mapping)} champs)")
    else:
        mapping = suggest_mappings(list(fields.keys()))
        print(f"\n[INFO] Mapping auto-suggere ({len(mapping)} champs):")
        for src, dst in mapping.items():
            print(f"  {src:40s} -> {dst}")

    if args.dry_run:
        print("\n[DRY-RUN] Aucun fichier genere.")
        update_run_pipeline(args.source_name, "", "", dry_run=True)
        return

    # Step 4: Generate merge script
    merge_path = generate_merge_script(
        args.source_name, args.source_id,
        list(fields.keys()), mapping, PROJECT_ROOT,
    )
    print(f"\n[OK] Merge script genere: {merge_path}")

    # Step 5: Generate feature builder
    fb_path = generate_feature_builder(
        args.source_name, args.source_id,
        list(fields.keys()), PROJECT_ROOT,
    )
    print(f"[OK] Feature builder genere: {fb_path}")

    # Step 6: Update run_pipeline.py
    update_run_pipeline(
        args.source_name,
        merge_path.name,
        fb_path.name,
        dry_run=False,
    )

    print(f"\n=== Integration terminee pour {args.source_name} ===")
    print("Prochaines etapes :")
    print(f"  1. Verifier/ajuster le mapping dans {merge_path.name}")
    print(f"  2. Implementer les features dans {fb_path.name}")
    print(f"  3. Lancer: python scripts/validate_new_source.py --source-dir {args.source_dir}")


if __name__ == "__main__":
    main()
