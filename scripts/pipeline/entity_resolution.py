#!/usr/bin/env python3
"""
Entity Resolution — Relie TOUS les masters en une seule grande table.

Pour chaque PARTANT dans une COURSE, on colle :
  - Ses infos équipements (œillères, poids, déferré)
  - La météo de la course
  - Les rapports de la course (arrivée, rapports)
  - Le marché (cote, probabilité, popularité)
  - Son pedigree (père, mère, race, robe)
  - Ses stats globales (victoires, gains, forme)
  - Ses performances passées (quand performances_master sera prêt)

Résultat : un fichier partants_complets.json / .parquet
  = 1 ligne par partant, avec TOUTES les infos collées

⚠️ NE SUPPRIME RIEN — fusionne et enrichit
⚠️ Ce script a besoin de beaucoup de RAM → lancer sur le PC 64 GB

Usage : python3 entity_resolution.py
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json, os, time, unicodedata, re

from utils.normalize import normalize_name
from utils.logging_setup import setup_logging

log = setup_logging("entity_resolution")

DATA_DIR = "../../data_master"
OUTPUT = os.path.join(DATA_DIR, "partants_complets.json")


def normalize_hippodrome(name):
    """Normalise un nom d'hippodrome"""
    if not name:
        return None
    name = str(name).lower().strip()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.replace("-", " ").replace("'", " ").replace("'", " ")
    name = re.sub(r'[^a-z ]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name if name else None


# ════════════════════════════════════════════════════
#  CHARGEMENT DES INDEX
# ════════════════════════════════════════════════════

def load_json(filename):
    """Charge un master JSON"""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        log.warning(f"  ⚠️ {filename} non trouvé — ignoré")
        return []
    log.info(f"  Chargement {filename}...")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"    → {len(data)} records")
    return data


def build_course_index(data, key="course_uid"):
    """Construit un index course_uid → record"""
    index = {}
    for r in data:
        uid = r.get(key)
        if uid:
            index[uid] = r
    return index


def build_partant_index(data, key="partant_uid"):
    """Construit un index partant_uid → record"""
    index = {}
    for r in data:
        uid = r.get(key)
        if uid:
            index[uid] = r
    return index


def build_cheval_index(data, name_key="nom_cheval"):
    """Construit un index nom_cheval_normalisé → record"""
    index = {}
    for r in data:
        name = normalize_name(r.get(name_key) or r.get("nom") or r.get("nom_cheval_norm"))
        if name:
            if name not in index:
                index[name] = r
    return index


# ════════════════════════════════════════════════════
#  FUSION — COLLER TOUTES LES INFOS SUR CHAQUE PARTANT
# ════════════════════════════════════════════════════

def merge_course_data(partant, course_index, prefix, fields):
    """Colle les données de course sur un partant"""
    course_uid = partant.get("course_uid")
    if not course_uid or course_uid not in course_index:
        return
    course = course_index[course_uid]
    for field in fields:
        val = course.get(field)
        if val is not None:
            partant[f"{prefix}_{field}"] = val


def merge_partant_data(partant, partant_index, prefix, fields):
    """Colle les données partant sur un partant"""
    uid = partant.get("partant_uid")
    if not uid or uid not in partant_index:
        return
    other = partant_index[uid]
    for field in fields:
        val = other.get(field)
        if val is not None:
            partant[f"{prefix}_{field}"] = val


def merge_cheval_data(partant, cheval_index, prefix, fields):
    """Colle les données cheval sur un partant via le nom normalisé"""
    name = normalize_name(partant.get("nom_cheval") or partant.get("nom"))
    if not name or name not in cheval_index:
        return
    cheval = cheval_index[name]
    for field in fields:
        val = cheval.get(field)
        if val is not None:
            partant[f"{prefix}_{field}"] = val


# ════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════

def main():
    start = time.time()
    log.info("=" * 60)
    log.info("ENTITY RESOLUTION — Fusion de tous les masters")
    log.info("=" * 60)

    # ── 1. Charger la base de partants (équipements = plus de partants) ──
    log.info("\n📦 Chargement des masters...")
    equipements = load_json("equipements_master.json")
    meteo = load_json("meteo_master.json")
    rapports = load_json("rapports_master.json")
    marche = load_json("marche_master.json")
    pedigree = load_json("pedigree_master.json")
    horse_stats = load_json("horse_stats_master.json")

    # performances_master sera ajouté quand il sera prêt
    performances_path = os.path.join(DATA_DIR, "performances_master.json")
    has_performances = os.path.exists(performances_path) and os.path.getsize(performances_path) > 100
    if has_performances:
        performances = load_json("performances_master.json")
    else:
        log.info("  ⚠️ performances_master pas encore prêt — on continue sans")
        performances = []

    # ── 2. Construire les index ──
    log.info("\n🔗 Construction des index...")

    meteo_index = build_course_index(meteo, "course_uid")
    log.info(f"  meteo_index: {len(meteo_index)} courses")

    rapports_index = build_course_index(rapports, "course_uid")
    log.info(f"  rapports_index: {len(rapports_index)} courses")

    marche_index = build_partant_index(marche, "partant_uid")
    log.info(f"  marche_index: {len(marche_index)} partants")

    pedigree_index = build_cheval_index(pedigree)
    log.info(f"  pedigree_index: {len(pedigree_index)} chevaux")

    stats_index = build_cheval_index(horse_stats, "nom_cheval")
    log.info(f"  stats_index: {len(stats_index)} chevaux")

    if has_performances:
        perf_index = build_partant_index(performances, "partant_uid")
        log.info(f"  perf_index: {len(perf_index)} partants")
    else:
        perf_index = {}

    # ── 3. Fusionner tout sur chaque partant ──
    log.info(f"\n🔄 Fusion sur {len(equipements)} partants...")

    # Champs à coller depuis chaque master
    METEO_FIELDS = [
        "temperature_c", "temp_max_c", "temp_min_c",
        "vent_kmh", "humidite_pct", "precipitation_mm",
        "type_piste", "penetrometre",
        # Post-processed
        "terrain_category", "penetrometre_numeric", "is_psf",
        "meteo_score", "temp_category", "is_cold", "is_hot",
    ]

    RAPPORTS_FIELDS = [
        "rapport_simple_gagnant", "rapport_simple_place_1",
        "rapport_simple_place_2", "rapport_simple_place_3",
        "rapport_couple_gagnant", "rapport_tierce_ordre",
        "combinaison_gagnant", "combinaison",
        "distance", "discipline", "nb_partants_arrivee",
        # Post-processed
        "jour_semaine", "jour_semaine_label", "mois", "saison",
        "is_quinte", "is_quarte", "is_tierce",
        "discipline_norm", "distance_category", "distance_m",
        "is_surprise", "is_favori_gagne",
        "rapport_gagnant_euros",
    ]

    MARCHE_FIELDS = [
        "cote_finale", "cote_reference", "cote_mediane_course",
        "cote_moyenne_course", "ecart_cote_moyenne",
        "proba_implicite", "rang_cote", "is_favori",
        "pct_masse", "rang_combinaison",
        # Post-processed
        "cote_category", "value_ratio", "value_indicator",
        "proba_category", "taille_champ", "popularite",
        "is_top3_cote", "is_top5_cote",
    ]

    PEDIGREE_FIELDS = [
        "pere", "mere", "pere_mere",
        "grand_pere_paternel", "grand_pere_maternel",
        "race", "robe", "sexe",
        "annee_naissance", "pays_naissance",
        "consommation", "vivant",
    ]

    STATS_FIELDS = [
        "nb_courses_total", "nb_victoires_total", "nb_places_total",
        "taux_victoire", "taux_place",
        "gains_total_euros", "forme_5", "forme_10", "forme_20",
        "jours_moyen_entre_courses",
        # Post-processed
        "class_category", "gains_par_course", "performance_category",
        "specialiste_discipline", "distance_pref_category",
        "experience_category", "is_en_forme", "is_en_baisse",
        "distance_moyenne", "career_length_years",
        "courses_par_mois", "nb_hippodromes",
    ]

    # Stats de fusion
    stats = {"meteo": 0, "rapports": 0, "marche": 0, "pedigree": 0, "stats": 0, "perf": 0}

    for i, partant in enumerate(equipements):
        # Météo (via course_uid)
        if partant.get("course_uid") in meteo_index:
            merge_course_data(partant, meteo_index, "meteo", METEO_FIELDS)
            stats["meteo"] += 1

        # Rapports (via course_uid)
        if partant.get("course_uid") in rapports_index:
            merge_course_data(partant, rapports_index, "rapport", RAPPORTS_FIELDS)
            stats["rapports"] += 1

        # Marché (via partant_uid)
        if partant.get("partant_uid") in marche_index:
            merge_partant_data(partant, marche_index, "marche", MARCHE_FIELDS)
            stats["marche"] += 1

        # Pedigree (via nom_cheval)
        merge_cheval_data(partant, pedigree_index, "ped", PEDIGREE_FIELDS)
        if partant.get("ped_race"):
            stats["pedigree"] += 1

        # Stats cheval (via nom_cheval)
        merge_cheval_data(partant, stats_index, "stats", STATS_FIELDS)
        if partant.get("stats_nb_courses_total"):
            stats["stats"] += 1

        # Performances (via partant_uid) — quand disponible
        if perf_index and partant.get("partant_uid") in perf_index:
            stats["perf"] += 1
            # On pourra ajouter les champs performances ici

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(equipements)} partants traités...")

    # ── 4. Stats de fusion ──
    total = len(equipements)
    log.info(f"\n📊 Résultats de la fusion ({total} partants):")
    for source, count in stats.items():
        log.info(f"  {source}: {count} matchés ({count*100/total:.1f}%)")

    # Compter le nombre total de champs par partant
    sample = equipements[:1000]
    avg_fields = sum(len(r) for r in sample) / len(sample)
    log.info(f"  Champs moyens par partant: {avg_fields:.0f}")

    # ── 5. Sauvegarder ──
    log.info(f"\n💾 Sauvegarde {OUTPUT}...")
    tmp = OUTPUT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(equipements, f, ensure_ascii=False)
    os.replace(tmp, OUTPUT)
    size = os.path.getsize(OUTPUT) / 1024 / 1024
    log.info(f"  → {size:.0f} MB")

    # Parquet si possible
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        log.info("  Export Parquet...")
        table = pa.Table.from_pylist(equipements)
        pq_path = OUTPUT.replace(".json", ".parquet")
        pq.write_table(table, pq_path, compression="zstd")
        pq_size = os.path.getsize(pq_path) / 1024 / 1024
        log.info(f"  → {pq_path}: {pq_size:.0f} MB")
    except Exception as e:
        log.warning(f"  Parquet échoué: {e}")

    elapsed = time.time() - start
    log.info(f"\n✅ ENTITY RESOLUTION TERMINÉE en {elapsed:.0f}s")
    log.info(f"   {total} partants × {avg_fields:.0f} champs = table prête pour Feature Engineering")


if __name__ == "__main__":
    main()
