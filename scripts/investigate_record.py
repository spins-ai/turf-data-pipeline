#!/usr/bin/env python3
"""
investigate_record.py <partant_uid>

Affiche TOUTES les donnees brutes + transformees + features avec source de chaque valeur
pour un partant_uid donne.

Recherche dans:
  - data_master/partants_master.jsonl  (donnees brutes + enrichies)
  - data_master/partants_master_enrichi.jsonl (merge complet)
  - pipeline/.../features_matrix.json (si present)
  - pipeline/.../labels.json (si present)

Streaming search, max 2GB RAM.
"""
import argparse
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve project root (two levels up from scripts/)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DATA_MASTER = PROJECT_ROOT / "data_master"

# Files to search (order matters: first found wins for each source)
MASTER_FILE = DATA_MASTER / "partants_master.jsonl"
ENRICHED_FILE = DATA_MASTER / "partants_master_enrichi.jsonl"

# Max lines to scan in secondary files (keep it fast)
MAX_LINES_SECONDARY = 100_000


# ---------------------------------------------------------------------------
# Field categorisation by prefix
# ---------------------------------------------------------------------------
CATEGORIES = {
    "base": {
        "label": "DONNEES DE BASE (partant)",
        "description": "Identifiants et infos de base du partant",
        "prefixes": [],  # fallback category
        "exact": {
            "partant_uid", "course_uid", "reunion_uid", "cle_partant",
            "source", "date_reunion_iso", "hippodrome_normalise",
            "numero_reunion", "numero_course", "distance", "discipline",
            "horse_id", "nom_cheval", "num_pmu", "age", "sexe", "race",
            "robe", "musique", "nb_courses_carriere", "nb_victoires_carriere",
            "nb_places_carriere", "nb_places_2eme", "nb_places_3eme",
            "gains_carriere_euros", "gains_annee_euros", "is_inedit",
            "jockey_driver", "jockey_driver_change", "entraineur",
            "proprietaire", "eleveur", "oeilleres", "deferre", "statut",
            "engagement", "supplement_euros", "handicap_distance_m",
            "poids_porte_kg", "poids_base_kg", "surcharge_decharge_kg",
            "handicap_valeur", "poids_monte_change", "taux_reclamation_euros",
            "place_corde", "allure", "pays_cheval", "pays_entrainement",
            "incident", "ecart_precedent", "commentaire_apres_course",
            "avis_entraineur", "jument_pleine", "type_piste", "corde",
            "nombre_partants", "timestamp_collecte",
        },
    },
    "resultat": {
        "label": "RESULTAT DE COURSE",
        "description": "Position, temps, cotes",
        "prefixes": [],
        "exact": {
            "position_arrivee", "temps_ms", "reduction_km_ms",
            "is_gagnant", "is_place", "is_disqualifie",
            "cote_finale", "cote_reference", "proba_implicite",
        },
    },
    "pedigree": {
        "label": "PEDIGREE (pgr_ / ped_)",
        "description": "Donnees genealogiques et pedigree",
        "prefixes": ["pgr_", "ped_"],
        "exact": {"pere", "mere", "pere_mere"},
    },
    "rapports": {
        "label": "RAPPORTS DE PARIS (rap_)",
        "description": "Rapports PMU (simple, couple, tierce, etc.)",
        "prefixes": ["rap_"],
        "exact": set(),
    },
    "marche": {
        "label": "MARCHE / COTES (mch_)",
        "description": "Donnees de marche des paris",
        "prefixes": ["mch_"],
        "exact": set(),
    },
    "meteo": {
        "label": "METEO (met_)",
        "description": "Impact meteo sur la course",
        "prefixes": ["met_"],
        "exact": set(),
    },
    "sequences": {
        "label": "SEQUENCES / HISTORIQUE (seq_)",
        "description": "Series de resultats recents",
        "prefixes": ["seq_"],
        "exact": set(),
    },
    "conditions": {
        "label": "CONDITIONS DE COURSE (cnd_)",
        "description": "Conditions extraites du texte",
        "prefixes": ["cnd_"],
        "exact": set(),
    },
    "speed": {
        "label": "SPEED / CLASS FIGURES (spd_)",
        "description": "Speed figures et class ratings",
        "prefixes": ["spd_"],
        "exact": set(),
    },
    "graphe": {
        "label": "GRAPHE / RESEAU (gnn_)",
        "description": "Features de graphe (jockey-cheval-entraineur)",
        "prefixes": ["gnn_"],
        "exact": set(),
    },
    "labels": {
        "label": "LABELS D'ENTRAINEMENT",
        "description": "Variables cibles pour le ML",
        "prefixes": ["lbl_", "label_", "target_"],
        "exact": set(),
    },
    "features": {
        "label": "FEATURES AVANCEES (feat_ / f_)",
        "description": "Features calculees pour le ML",
        "prefixes": ["feat_", "f_"],
        "exact": set(),
    },
    "sources_meta": {
        "label": "METADONNEES DE SOURCES",
        "description": "Nombre de sources et liste des sources",
        "prefixes": [],
        "exact": set(),
    },
}

# Fields ending with __sources or __nb_sources go to sources_meta
SOURCE_META_SUFFIXES = ("__sources", "__nb_sources")


def categorise_field(field_name: str) -> str:
    """Return the category key for a given field name."""
    # Check source metadata suffixes first
    for suffix in SOURCE_META_SUFFIXES:
        if field_name.endswith(suffix):
            return "sources_meta"

    # Check exact matches in each category
    for cat_key, cat_def in CATEGORIES.items():
        if field_name in cat_def.get("exact", set()):
            return cat_key

    # Check prefixes
    for cat_key, cat_def in CATEGORIES.items():
        for prefix in cat_def.get("prefixes", []):
            if field_name.startswith(prefix):
                return cat_key

    # Fallback: if it looks like a known prefix pattern we missed
    return "base"


def search_jsonl(filepath: Path, uid: str, max_lines: int = 0) -> dict | None:
    """Stream-search a JSONL file for a record with given partant_uid.

    Args:
        filepath: path to the .jsonl file
        uid: the partant_uid to find
        max_lines: max lines to scan (0 = unlimited)

    Returns:
        The parsed dict if found, else None
    """
    if not filepath.exists():
        return None

    count = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            count += 1
            if max_lines and count > max_lines:
                break
            # Quick string check before parsing JSON (fast filter)
            if uid not in line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("partant_uid") == uid:
                return record
    return None


def format_value(value) -> str:
    """Format a value for display."""
    if value is None:
        return "(null)"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if value == int(value):
            return str(int(value))
        return f"{value:.6g}"
    if isinstance(value, str):
        if len(value) > 120:
            return value[:120] + "..."
        return value
    if isinstance(value, (list, dict)):
        s = json.dumps(value, ensure_ascii=False)
        if len(s) > 120:
            return s[:120] + "..."
        return s
    return str(value)


def print_separator(char: str = "=", width: int = 80):
    print(char * width)


def print_category(cat_key: str, fields: dict):
    """Print a category section."""
    cat_def = CATEGORIES.get(cat_key, {"label": cat_key.upper(), "description": ""})
    print()
    print_separator("=")
    print(f"  {cat_def['label']}")
    if cat_def.get("description"):
        print(f"  {cat_def['description']}")
    print_separator("-")

    if not fields:
        print("  (aucune donnee)")
        return

    # Align field names
    max_key_len = max(len(k) for k in fields)
    for key in sorted(fields.keys()):
        val = format_value(fields[key])
        print(f"  {key:<{max_key_len}}  {val}")


def main():
    parser = argparse.ArgumentParser(
        description="Investigate a partant record: show all raw + transformed + features data"
    )
    parser.add_argument(
        "partant_uid",
        help="The partant_uid to investigate"
    )
    parser.add_argument(
        "--enriched", "-e",
        action="store_true",
        help="Also search partants_master_enrichi.jsonl (slower, 17GB)"
    )
    args = parser.parse_args()

    uid = args.partant_uid

    print_separator("=")
    print(f"  INVESTIGATION: partant_uid = {uid}")
    print_separator("=")

    # -----------------------------------------------------------------------
    # 1. Search partants_master.jsonl (main source, full scan)
    # -----------------------------------------------------------------------
    print(f"\n[1/3] Searching {MASTER_FILE.name} (full scan)...")
    master_record = search_jsonl(MASTER_FILE, uid)

    if master_record:
        print(f"      FOUND in {MASTER_FILE.name} ({len(master_record)} fields)")
    else:
        print(f"      NOT FOUND in {MASTER_FILE.name}")

    # -----------------------------------------------------------------------
    # 2. Search enriched file (optional, very large)
    # -----------------------------------------------------------------------
    enriched_record = None
    if args.enriched and ENRICHED_FILE.exists():
        print(f"\n[2/3] Searching {ENRICHED_FILE.name} (full scan, may be slow)...")
        enriched_record = search_jsonl(ENRICHED_FILE, uid)
        if enriched_record:
            print(f"      FOUND in {ENRICHED_FILE.name} ({len(enriched_record)} fields)")
        else:
            print(f"      NOT FOUND in {ENRICHED_FILE.name}")
    else:
        print(f"\n[2/3] Skipping {ENRICHED_FILE.name} (use --enriched to include)")

    # -----------------------------------------------------------------------
    # 3. Search pipeline features_matrix / labels files (first 100K lines)
    # -----------------------------------------------------------------------
    features_record = None
    labels_record = None

    # Look for any features_matrix.jsonl or training_labels.jsonl in pipeline
    pipeline_dir = PROJECT_ROOT / "pipeline"
    if pipeline_dir.exists():
        print(f"\n[3/3] Searching pipeline/ for features_matrix.jsonl and training_labels.jsonl...")
        found_features = False
        found_labels = False
        for jsonl_path in sorted(pipeline_dir.rglob("features_matrix.jsonl")):
            result = search_jsonl(jsonl_path, uid, max_lines=MAX_LINES_SECONDARY)
            if result:
                features_record = result
                found_features = True
                print(f"      FOUND features in {jsonl_path.relative_to(PROJECT_ROOT)}")
                break
        if not found_features:
            print("      No features_matrix.jsonl found or UID not in first 100K lines")

        for jsonl_path in sorted(pipeline_dir.rglob("training_labels.jsonl")):
            result = search_jsonl(jsonl_path, uid, max_lines=MAX_LINES_SECONDARY)
            if result:
                labels_record = result
                found_labels = True
                print(f"      FOUND labels in {jsonl_path.relative_to(PROJECT_ROOT)}")
                break
        if not found_labels:
            print("      No training_labels.jsonl found or UID not in first 100K lines")
    else:
        print(f"\n[3/3] No pipeline/ directory found")

    # -----------------------------------------------------------------------
    # Merge all records (master is base, enriched overrides, features/labels add)
    # -----------------------------------------------------------------------
    if not master_record and not enriched_record and not features_record and not labels_record:
        print("\n")
        print_separator("!")
        print("  AUCUN ENREGISTREMENT TROUVE pour ce partant_uid")
        print_separator("!")
        sys.exit(1)

    merged = {}
    source_map = {}  # field -> source file

    # Layer 1: master
    if master_record:
        for k, v in master_record.items():
            merged[k] = v
            source_map[k] = MASTER_FILE.name

    # Layer 2: enriched (overrides)
    if enriched_record:
        for k, v in enriched_record.items():
            if k not in merged or merged[k] != v:
                source_map[k] = ENRICHED_FILE.name
            merged[k] = v

    # Layer 3: features
    if features_record:
        for k, v in features_record.items():
            if k not in merged:
                source_map[k] = "features_matrix.jsonl"
            merged[k] = v

    # Layer 4: labels
    if labels_record:
        for k, v in labels_record.items():
            if k not in merged:
                source_map[k] = "training_labels.jsonl"
            merged[k] = v

    # -----------------------------------------------------------------------
    # Group fields by category
    # -----------------------------------------------------------------------
    categorised: dict[str, dict] = {cat: {} for cat in CATEGORIES}
    uncategorised = {}

    for field, value in merged.items():
        cat = categorise_field(field)
        if cat in categorised:
            categorised[cat][field] = value
        else:
            uncategorised[field] = value

    # -----------------------------------------------------------------------
    # Print report
    # -----------------------------------------------------------------------
    print("\n")
    print_separator("=")
    print(f"  RAPPORT COMPLET: {uid}")
    print(f"  Total fields: {len(merged)}")
    print(f"  Sources: {', '.join(sorted(set(source_map.values())))}")
    print_separator("=")

    # Print categories in logical order
    cat_order = [
        "base", "resultat", "pedigree", "rapports", "marche",
        "meteo", "sequences", "conditions", "speed", "graphe",
        "features", "labels", "sources_meta",
    ]

    for cat_key in cat_order:
        fields = categorised.get(cat_key, {})
        if fields:
            print_category(cat_key, fields)

    if uncategorised:
        print()
        print_separator("=")
        print("  AUTRES CHAMPS (non categorises)")
        print_separator("-")
        max_key_len = max(len(k) for k in uncategorised) if uncategorised else 0
        for key in sorted(uncategorised.keys()):
            val = format_value(uncategorised[key])
            print(f"  {key:<{max_key_len}}  {val}")

    # -----------------------------------------------------------------------
    # Source provenance table
    # -----------------------------------------------------------------------
    print()
    print_separator("=")
    print("  PROVENANCE DES DONNEES")
    print_separator("-")
    by_source: dict[str, list] = {}
    for field, src in sorted(source_map.items()):
        by_source.setdefault(src, []).append(field)

    for src, fields in sorted(by_source.items()):
        print(f"\n  [{src}] ({len(fields)} fields)")
        # Show field names in columns
        cols = 3
        for i in range(0, len(fields), cols):
            chunk = fields[i:i + cols]
            line = "    " + "  ".join(f"{f:<30}" for f in chunk)
            print(line)

    print()
    print_separator("=")
    print("  FIN DU RAPPORT")
    print_separator("=")


if __name__ == "__main__":
    main()
