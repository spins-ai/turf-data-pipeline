#!/usr/bin/env python3
"""
rebuild_mega_merge.py
=====================
Re-run mega merge using partants_master_enrichi.jsonl (au lieu de l'original).
Meme logique que mega_merge_partants_master.py mais lit le fichier enrichi
et produit partants_master_v2.jsonl dans data_master/.

Streaming ligne par ligne, faible consommation RAM.

Usage:
    python3 rebuild_mega_merge.py
"""

import json

import os
import sys

OUTPUT_DIR = "data_master"
os.makedirs(OUTPUT_DIR, exist_ok=True)

from utils.logging_setup import setup_logging
log = setup_logging("rebuild_mega_merge")
nBASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ================================================================
# CHARGEMENT DES INDEX
# ================================================================

def load_json_index(path, key_field, description, keep_fields=None):
    """Charge un fichier JSON/JSONL et indexe par key_field."""
    index = {}
    if not os.path.exists(path):
        log.info("  [ABSENT] %s: %s", description, path)
        return index

    log.info("  Chargement %s: %s", description, path)
    size = os.path.getsize(path)

    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = r.get(key_field, "")
                if key:
                    if keep_fields:
                        index[key] = {k: r[k] for k in keep_fields if k in r}
                    else:
                        index[key] = r
    elif size > 2000 * 1024 * 1024:
        log.warning("  [TROP GROS] %s (%d MB) -- skip", path, size // 1024 // 1024)
        return index
    else:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, list):
                for r in data:
                    key = r.get(key_field, "")
                    if key:
                        if keep_fields:
                            index[key] = {k: r[k] for k in keep_fields if k in r}
                        else:
                            index[key] = r
            del data
        except (json.JSONDecodeError, MemoryError) as e:
            log.warning("  Erreur: %s", e)

    log.info("    -> %d entrees", len(index))
    return index


def load_horse_index(path, description):
    """Charge un index par nom de cheval (UPPERCASE)."""
    index = {}
    if not os.path.exists(path):
        log.info("  [ABSENT] %s: %s", description, path)
        return index

    log.info("  Chargement %s: %s", description, path)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
        if isinstance(data, list):
            for r in data:
                nom = (r.get("nom_cheval") or r.get("nom") or "").upper().strip()
                if nom:
                    index[nom] = r
        del data
    except (json.JSONDecodeError, MemoryError) as e:
        log.warning("  Erreur: %s", e)

    log.info("    -> %d entrees", len(index))
    return index


# ================================================================
# MERGE
# ================================================================

def merge_dict(target, source, prefix=""):
    """Fusionne source dans target sans ecraser les valeurs existantes."""
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        if key in ("course_uid", "partant_uid", "reunion_uid", "nom_cheval",
                    "date_reunion_iso", "hippodrome_normalise", "source",
                    "timestamp_collecte", "url_source", "cle_partant", "cle_course"):
            continue

        target_key = "{}{}".format(prefix, key) if prefix else key

        existing = target.get(target_key)
        if existing is not None and existing != "" and existing != []:
            continue

        if value is not None and value != "" and value != []:
            target[target_key] = value


# ================================================================
# MAIN
# ================================================================

def main():
    log.info("=" * 70)
    log.info("REBUILD MEGA-MERGE -> partants_master_v2.jsonl")
    log.info("  Source: partants_master_enrichi.jsonl")
    log.info("=" * 70)

    # Source: fichier enrichi
    source_path = os.path.join(OUTPUT_DIR, "partants_master_enrichi.jsonl")
    if not os.path.exists(source_path):
        log.error("Fichier source introuvable: %s", source_path)
        sys.exit(1)

    log.info("Source partants: %s", source_path)
    src_size = os.path.getsize(source_path)
    log.info("  Taille: %.2f GB", src_size / (1024 ** 3))

    # Charger TOUS les index
    log.info("")
    log.info("Chargement des index de jointure...")

    # Par partant_uid
    idx_enrichissement = load_json_index(
        os.path.join(BASE_DIR, "output", "40_enrichissement_partants", "enrichissement.json"),
        "partant_uid", "Enrichissement 40")

    # Par course_uid
    idx_rapports = load_json_index(
        os.path.join(BASE_DIR, "data_master", "rapports_master.json"),
        "course_uid", "Rapports master")

    idx_meteo = load_json_index(
        os.path.join(BASE_DIR, "data_master", "meteo_master.json"),
        "course_uid", "Meteo master",
        keep_fields={"temperature", "precipitation_mm", "wind_force", "terrain_category",
                     "penetrometre_numeric", "meteo_score", "is_psf"})

    # Par nom cheval
    idx_pedigree = load_horse_index(
        os.path.join(BASE_DIR, "data_master", "pedigree_master.json"), "Pedigree master")

    idx_horse_stats = load_horse_index(
        os.path.join(BASE_DIR, "data_master", "horse_stats_master.json"), "Horse stats master")

    # Calculs 41-49
    idx_41 = load_json_index(
        os.path.join(BASE_DIR, "output", "41_sequences", "sequences_performances.jsonl"),
        "partant_uid", "41 Sequences")

    idx_42 = load_json_index(
        os.path.join(BASE_DIR, "output", "42_croisement_rp", "croisement_rp_pmu.jsonl"),
        "partant_uid", "42 Racing Post")

    idx_43 = load_json_index(
        os.path.join(BASE_DIR, "output", "43_croisement_meteo", "croisement_meteo_courses.jsonl"),
        "partant_uid", "43 Meteo")

    idx_44 = load_json_index(
        os.path.join(BASE_DIR, "output", "44_croisement_pedigree", "croisement_pedigree_partants.jsonl"),
        "partant_uid", "44 Pedigree")

    idx_45 = load_json_index(
        os.path.join(BASE_DIR, "output", "45_graphe_gnn", "graph_features_partants.jsonl"),
        "partant_uid", "45 Graphe GNN")

    idx_46 = load_json_index(
        os.path.join(BASE_DIR, "output", "46_track_bias_speed", "track_bias_speed_class.jsonl"),
        "partant_uid", "46 Track/Speed/Class")

    idx_48 = load_json_index(
        os.path.join(BASE_DIR, "output", "48_conditions_texte", "conditions_parsees.jsonl"),
        "course_uid", "48 Conditions texte")

    idx_49 = load_json_index(
        os.path.join(BASE_DIR, "output", "49_ecart_cotes", "ecart_cotes_market.jsonl"),
        "partant_uid", "49 Ecart cotes")

    # Marche
    idx_marche = load_json_index(
        os.path.join(BASE_DIR, "data_master", "marche_master.json"),
        "course_uid", "Marche master")

    # Equipements
    idx_equipements = load_json_index(
        os.path.join(BASE_DIR, "data_master", "equipements_master.json"),
        "partant_uid", "Equipements master")

    # Stats externes
    idx_stats_ext = load_json_index(
        os.path.join(BASE_DIR, "data_master", "stats_externes_master.json"),
        "nom_cheval", "Stats externes master")

    log.info("")
    log.info("=" * 50)
    log.info("Merge en cours (streaming)...")

    output_file = os.path.join(OUTPUT_DIR, "partants_master_v2.jsonl")
    total = 0
    total_enriched = 0
    total_fields_added = 0

    with open(output_file, "w", encoding="utf-8") as fout:
        with open(source_path, "r", encoding="utf-8", errors="replace") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if not isinstance(record, dict):
                    continue

                total += 1
                fields_before = len(record)

                puid = record.get("partant_uid", "")
                cuid = record.get("course_uid", "")
                nom = (record.get("nom_cheval") or "").upper().strip()

                # === Jointures par partant_uid ===
                if puid:
                    merge_dict(record, idx_enrichissement.get(puid, {}), "enr_")
                    merge_dict(record, idx_41.get(puid, {}), "seq_")
                    merge_dict(record, idx_42.get(puid, {}), "rp_")
                    merge_dict(record, idx_43.get(puid, {}), "met_")
                    merge_dict(record, idx_44.get(puid, {}), "ped_")
                    merge_dict(record, idx_45.get(puid, {}), "gnn_")
                    merge_dict(record, idx_46.get(puid, {}), "spd_")
                    merge_dict(record, idx_49.get(puid, {}), "mkt_")
                    merge_dict(record, idx_equipements.get(puid, {}), "eqp_")

                # === Jointures par course_uid ===
                if cuid:
                    merge_dict(record, idx_rapports.get(cuid, {}), "rap_")
                    merge_dict(record, idx_meteo.get(cuid, {}), "mto_")
                    merge_dict(record, idx_marche.get(cuid, {}), "mch_")
                    merge_dict(record, idx_48.get(cuid, {}), "cnd_")

                # === Jointures par nom cheval ===
                if nom:
                    merge_dict(record, idx_pedigree.get(nom, {}), "pgr_")
                    merge_dict(record, idx_horse_stats.get(nom, {}), "hst_")
                    merge_dict(record, idx_stats_ext.get(nom, {}), "ext_")

                fields_after = len(record)
                fields_added = fields_after - fields_before
                if fields_added > 0:
                    total_enriched += 1
                    total_fields_added += fields_added

                fout.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

                if total % 200000 == 0:
                    log.info("  %d traites, %d enrichis, +%d champs ajoutes",
                             total, total_enriched, total_fields_added)

    avg_fields = total_fields_added / max(total_enriched, 1)
    log.info("")
    log.info("REBUILD MEGA-MERGE TERMINE:")
    log.info("  Total partants: %d", total)
    log.info("  Enrichis: %d (%.1f%%)", total_enriched, 100 * total_enriched / max(total, 1))
    log.info("  Champs ajoutes: %d (moy: %.1f/partant)", total_fields_added, avg_fields)
    log.info("  Output: %s", output_file)

    # Compter les colonnes du dernier record
    if total > 0:
        with open(output_file, "rb") as f:
            f.seek(max(0, f.seek(0, 2) - 10000))
            last_lines = f.read().decode("utf-8", errors="replace").strip().split("\n")
            if last_lines:
                try:
                    last = json.loads(last_lines[-1])
                    log.info("  Colonnes (dernier record): %d", len(last))
                except json.JSONDecodeError:
                    pass

    # Taille du fichier
    out_size = os.path.getsize(output_file)
    log.info("  Taille output: %.2f GB", out_size / (1024 ** 3))

    # Rapport
    rapport = {
        "source": source_path,
        "total": total,
        "enriched": total_enriched,
        "fields_added": total_fields_added,
        "avg_fields_per_partant": round(avg_fields, 1),
        "output": output_file,
        "output_size_gb": round(out_size / (1024 ** 3), 2),
    }
    rapport_path = os.path.join(OUTPUT_DIR, "mega_merge_v2_rapport.json")
    with open(rapport_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)
    log.info("  Rapport: %s", rapport_path)

    log.info("=" * 70)
    log.info("TERMINE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
