#!/usr/bin/env python3
"""
mega_merge_partants_master.py — Étape 5 du TODO
==================================================
Fusion de TOUTES les sources en un fichier maître unique.

Prend les partants nettoyés/comblés et joint toutes les données :
  - historique cheval (05)
  - historique jockey (06)
  - cotes marché (07)
  - pedigree_master
  - équipements (09)
  - poids/handicaps (10)
  - sectionals (11)
  - meteo_master
  - SIRE/IFCE (17)
  - performances détaillées (22)
  - rapports_master
  - pronostics (23)
  - stats_externes_master
  - marche_master
  - Racing Post (37)
  - réunions enrichies (39)
  - enrichissement partants (40)
  - Smarkets exchange (30)
  - hippodromes_db
  - Calculs 41-49

Output : data_master/partants_master.jsonl (LE fichier maître)

Usage :
    python3 mega_merge_partants_master.py
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import os
import sys
from collections import defaultdict

OUTPUT_DIR = "../../data_master"
os.makedirs(OUTPUT_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("mega_merge_partants_master")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ================================================================
# CHARGEMENT DES INDEX
# ================================================================

def load_json_index(path, key_field, description, keep_fields=None):
    """Charge un fichier JSON/JSONL et indexe par key_field."""
    index = {}
    if not os.path.exists(path):
        log.info(f"  [ABSENT] {description}: {path}")
        return index

    log.info(f"  Chargement {description}: {path}")
    size = os.path.getsize(path)

    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
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
        log.warning(f"  [TROP GROS] {path} ({size // 1024 // 1024} MB) — skip")
        return index
    else:
        try:
            with open(path, "r", encoding="utf-8") as f:
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
        except UnicodeDecodeError:
            log.warning(f"  Retry avec encoding latin-1...")
            with open(path, "r", encoding="latin-1") as f:
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
            log.warning(f"  Erreur: {e}")

    log.info(f"    → {len(index)} entrées")
    return index


def load_horse_index(path, description):
    """Charge un index par nom de cheval (UPPERCASE)."""
    index = {}
    if not os.path.exists(path):
        log.info(f"  [ABSENT] {description}: {path}")
        return index

    log.info(f"  Chargement {description}: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for r in data:
                nom = (r.get("nom_cheval") or r.get("nom") or "").upper().strip()
                if nom:
                    index[nom] = r
        del data
    except UnicodeDecodeError:
        log.warning(f"  Retry avec encoding latin-1...")
        with open(path, "r", encoding="latin-1") as f:
            data = json.load(f)
        if isinstance(data, list):
            for r in data:
                nom = (r.get("nom_cheval") or r.get("nom") or "").upper().strip()
                if nom:
                    index[nom] = r
        del data
    except (json.JSONDecodeError, MemoryError) as e:
        log.warning(f"  Erreur: {e}")

    log.info(f"    → {len(index)} entrées")
    return index


# ================================================================
# MERGE
# ================================================================

def merge_dict(target, source, prefix=""):
    """Fusionne source dans target sans écraser les valeurs existantes."""
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        # Skip les champs meta/techniques
        if key in ("course_uid", "partant_uid", "reunion_uid", "nom_cheval",
                    "date_reunion_iso", "hippodrome_normalise", "source",
                    "timestamp_collecte", "url_source", "cle_partant", "cle_course"):
            continue

        target_key = f"{prefix}{key}" if prefix else key

        # Ne pas écraser une valeur existante non-null
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
    log.info("MEGA-MERGE → partants_master.jsonl — Étape 5")
    log.info("=" * 70)

    # Source partants
    source_path = None
    for path in [os.path.join(BASE_DIR, "../../output", "comblage/partants_combles.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "nettoyage/partants_nettoyes.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "02_liste_courses/partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "02_liste_courses/partants_normalises.json")]:
        if os.path.exists(path):
            source_path = path
            break

    if not source_path:
        log.error("Aucun fichier partants trouvé")
        sys.exit(1)

    log.info(f"Source partants: {source_path}")

    # Charger TOUS les index
    log.info("")
    log.info("Chargement des index de jointure...")

    # Par partant_uid
    idx_enrichissement = load_json_index(
        os.path.join(BASE_DIR, "../../output", "40_enrichissement_partants/enrichissement.json"),
        "partant_uid", "Enrichissement 40")

    # Par course_uid
    idx_rapports = load_json_index(
        os.path.join(BASE_DIR, "../../data_master", "rapports_master.json"),
        "course_uid", "Rapports master")

    idx_meteo = load_json_index(
        os.path.join(BASE_DIR, "../../data_master", "meteo_master.json"),
        "course_uid", "Météo master",
        keep_fields={"temperature", "precipitation_mm", "wind_force", "terrain_category",
                     "penetrometre_numeric", "meteo_score", "is_psf"})

    # Par nom cheval
    idx_pedigree = load_horse_index(
        os.path.join(BASE_DIR, "../../data_master", "pedigree_master.json"), "Pedigree master")

    idx_horse_stats = load_horse_index(
        os.path.join(BASE_DIR, "../../data_master", "horse_stats_master.json"), "Horse stats master")

    # Calculs 41-49
    idx_41 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "41_sequences/sequences_performances.jsonl"),
        "partant_uid", "41 Séquences")

    idx_42 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "42_croisement_rp/croisement_rp_pmu.jsonl"),
        "partant_uid", "42 Racing Post")

    idx_43 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "43_croisement_meteo/croisement_meteo_courses.jsonl"),
        "partant_uid", "43 Météo")

    idx_44 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "44_croisement_pedigree/croisement_pedigree_partants.jsonl"),
        "partant_uid", "44 Pedigree")

    idx_45 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "45_graphe_gnn/graph_features_partants.jsonl"),
        "partant_uid", "45 Graphe GNN")

    idx_46 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "46_track_bias_speed/track_bias_speed_class.jsonl"),
        "partant_uid", "46 Track/Speed/Class")

    idx_48 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "48_conditions_texte/conditions_parsees.jsonl"),
        "course_uid", "48 Conditions texte")

    idx_49 = load_json_index(
        os.path.join(BASE_DIR, "../../output", "49_ecart_cotes/ecart_cotes_market.jsonl"),
        "partant_uid", "49 Écart cotes")

    # Marché
    idx_marche = load_json_index(
        os.path.join(BASE_DIR, "../../data_master", "marche_master.json"),
        "course_uid", "Marché master")

    # Équipements
    idx_equipements = load_json_index(
        os.path.join(BASE_DIR, "../../data_master", "equipements_master.json"),
        "partant_uid", "Équipements master")

    # Stats externes
    idx_stats_ext = load_json_index(
        os.path.join(BASE_DIR, "../../data_master", "stats_externes_master.json"),
        "nom_cheval", "Stats externes master")

    log.info("")
    log.info("=" * 50)
    log.info("Merge en cours...")

    output_file = os.path.join(OUTPUT_DIR, "partants_master.jsonl")
    total = 0
    total_enriched = 0
    total_fields_added = 0

    import contextlib
    with contextlib.ExitStack() as stack:
        fout = stack.enter_context(open(output_file, "w", encoding="utf-8"))
        if source_path.endswith(".jsonl"):
            opener = stack.enter_context(open(source_path, "r", encoding="utf-8"))
            records_iter = (json.loads(line.strip()) for line in opener if line.strip())
        else:
            # JSON → convertir en itérateur
            with open(source_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            records_iter = iter(data)

        for record in records_iter:
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
                log.info(f"  {total} traités, {total_enriched} enrichis, "
                         f"+{total_fields_added} champs ajoutés")

        if '../../data' in dir():
            del data

    avg_fields = total_fields_added / max(total_enriched, 1)
    log.info("")
    log.info(f"MEGA-MERGE TERMINÉ:")
    log.info(f"  Total partants: {total}")
    log.info(f"  Enrichis: {total_enriched} ({100*total_enriched/max(total,1):.1f}%)")
    log.info(f"  Champs ajoutés: {total_fields_added} (moy: {avg_fields:.1f}/partant)")
    log.info(f"  Output: {output_file}")

    # Compter les colonnes du dernier record
    if total > 0:
        # Relire le dernier record pour compter
        with open(output_file, "rb") as f:
            f.seek(max(0, f.seek(0, 2) - 10000))
            last_lines = f.read().decode("utf-8", errors="replace").strip().split("\n")
            if last_lines:
                try:
                    last = json.loads(last_lines[-1])
                    log.info(f"  Colonnes (dernier record): {len(last)}")
                except json.JSONDecodeError:
                    pass

    # Rapport
    rapport = {
        "total": total,
        "enriched": total_enriched,
        "fields_added": total_fields_added,
        "avg_fields_per_partant": round(avg_fields, 1),
        "../../output": output_file,
    }
    with open(os.path.join(OUTPUT_DIR, "mega_merge_rapport.json"), "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    log.info("=" * 70)
    log.info("TERMINÉ")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
