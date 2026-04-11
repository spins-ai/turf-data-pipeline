#!/usr/bin/env python3
"""
run_new_builders.py - Calcule les features manquantes a partir de partants_master.parquet
==========================================================================================
Lit partants_master.parquet en streaming (row group par row group),
calcule les nouvelles features, et sauvegarde en JSONL dans builder_outputs/.

Builders inclus:
  C1. meteo_impact     - 6 colonnes meteo inexploitees
  C2. handicap_deep    - interactions poids/handicap vs peloton
  C3. marche_enjeux    - detection argent intelligent
  C4. ecart_repos      - jours depuis derniere course, repos optimal
  C5. poids_impact     - poids x distance x terrain x age
  C6. conditions_deep  - conditions de course (age, groupe, distance)
  C7. rapports_hist    - rapports historiques par hippodrome (PAS la course actuelle!)
  C8. speed_bias       - biais corde, force du peloton

Sortie: D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/<builder_name>/
Max RAM: ~4 Go (streaming row groups)
"""

import sys
import time
import json
import math
import numpy as np
from pathlib import Path
from collections import defaultdict

import pyarrow.parquet as pq

# Paths
PARQUET = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.parquet")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs")

def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None

def safe_div(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


# =====================================================================
# C1. METEO IMPACT BUILDER
# =====================================================================
def build_meteo_impact(row):
    """Exploite les 6 colonnes meteo ignorees."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None

    features["partant_uid"] = uid

    # Colonnes meteo brutes -> features
    nb_courses_terrain = safe_float(row.get("met_cheval_nb_courses_terrain"))
    specialist = safe_float(row.get("met_cheval_specialist_terrain"))
    taux_place = safe_float(row.get("met_cheval_taux_place_terrain"))
    taux_vic_pluie = safe_float(row.get("met_cheval_taux_vic_pluie"))
    taux_vic_terrain = safe_float(row.get("met_cheval_taux_vic_terrain"))
    impact_score = safe_float(row.get("met_impact_meteo_score"))
    precipitation = safe_float(row.get("mto_precipitation_mm"))

    # Feature 1: Experience sur ce terrain (log)
    features["meteo_x__terrain_experience"] = math.log1p(nb_courses_terrain) if nb_courses_terrain else None

    # Feature 2: Specialist (deja un score)
    features["meteo_x__is_terrain_specialist"] = specialist

    # Feature 3: Taux place sur ce terrain
    features["meteo_x__terrain_place_rate"] = taux_place

    # Feature 4: Avantage pluie (taux victoire sous la pluie vs global)
    features["meteo_x__rain_advantage"] = taux_vic_pluie

    # Feature 5: Avantage terrain (taux victoire sur ce terrain)
    features["meteo_x__terrain_win_rate"] = taux_vic_terrain

    # Feature 6: Score impact meteo composite
    features["meteo_x__impact_score"] = impact_score

    # Feature 7: Interaction pluie x specialist
    if precipitation is not None and specialist is not None:
        features["meteo_x__rain_x_specialist"] = precipitation * specialist
    else:
        features["meteo_x__rain_x_specialist"] = None

    # Feature 8: Cheval adapte? (specialist + experience > seuil)
    if specialist is not None and nb_courses_terrain is not None:
        features["meteo_x__terrain_adapted"] = 1.0 if (specialist > 0.5 and nb_courses_terrain >= 3) else 0.0
    else:
        features["meteo_x__terrain_adapted"] = None

    return features


# =====================================================================
# C2. HANDICAP DEEP BUILDER
# =====================================================================
def build_handicap_deep(row, course_stats):
    """Interactions poids/handicap vs peloton."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid
    course_uid = row.get("course_uid")

    handicap_val = safe_float(row.get("handicap_valeur"))
    handicap_dist = safe_float(row.get("handicap_distance_m"))
    distance = safe_float(row.get("distance"))

    # Feature 1: Handicap value (brut)
    features["handicap_x__valeur"] = handicap_val

    # Feature 2: Handicap vs moyenne du peloton
    if course_uid and course_uid in course_stats and handicap_val is not None:
        avg_h = course_stats[course_uid].get("avg_handicap")
        if avg_h and avg_h > 0:
            features["handicap_x__vs_field_avg"] = handicap_val - avg_h
            features["handicap_x__ratio_field"] = handicap_val / avg_h
        else:
            features["handicap_x__vs_field_avg"] = None
            features["handicap_x__ratio_field"] = None
    else:
        features["handicap_x__vs_field_avg"] = None
        features["handicap_x__ratio_field"] = None

    # Feature 3: Handicap par km (charge par distance)
    features["handicap_x__per_km"] = safe_div(handicap_val, distance / 1000 if distance else None)

    # Feature 4: Ecart distance handicap vs distance course
    if handicap_dist and distance:
        features["handicap_x__dist_ecart"] = handicap_dist - distance
    else:
        features["handicap_x__dist_ecart"] = None

    return features


# =====================================================================
# C3. MARCHE ENJEUX BUILDER
# =====================================================================
def build_marche_enjeux(row):
    """Detection argent intelligent via enjeux."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid

    enjeu_combo = safe_float(row.get("mch_enjeu_combinaison"))
    pct_masse = safe_float(row.get("mch_pct_masse"))
    total_enjeu = safe_float(row.get("mch_total_enjeu_pari"))
    cote = safe_float(row.get("cote_finale"))
    proba = safe_float(row.get("mch_proba_implicite"))

    # Feature 1: Pourcentage de la masse d'enjeux
    features["enjeux_x__pct_masse"] = pct_masse

    # Feature 2: Enjeu de la combinaison (argent sur ce cheval)
    features["enjeux_x__enjeu_combo_log"] = math.log1p(enjeu_combo) if enjeu_combo else None

    # Feature 3: Total enjeu de la course (liquidite)
    features["enjeux_x__total_course_log"] = math.log1p(total_enjeu) if total_enjeu else None

    # Feature 4: Ratio enjeu/cote (argent intelligent = gros enjeu sur petite cote relative)
    if enjeu_combo and cote and cote > 0:
        features["enjeux_x__smart_money_ratio"] = enjeu_combo / cote
    else:
        features["enjeux_x__smart_money_ratio"] = None

    # Feature 5: Ecart proba implicite vs pct masse (divergence = signal)
    if proba and pct_masse:
        features["enjeux_x__proba_vs_masse_ecart"] = proba - pct_masse
    else:
        features["enjeux_x__proba_vs_masse_ecart"] = None

    return features


# =====================================================================
# C4. ECART REPOS BUILDER
# =====================================================================
def build_ecart_repos(row, horse_last_date):
    """Jours depuis derniere course, repos optimal."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid

    horse_id = row.get("horse_id") or row.get("nom_cheval", "")
    date_str = row.get("date_reunion_iso", "")

    ecart = safe_float(row.get("ecart_precedent"))

    # Feature 1: Ecart brut (jours)
    features["repos_x__ecart_jours"] = ecart

    # Feature 2: Log de l'ecart (compression des grandes valeurs)
    features["repos_x__ecart_log"] = math.log1p(ecart) if ecart and ecart > 0 else None

    # Feature 3: Bins de repos (non-lineaire)
    if ecart is not None:
        if ecart <= 10:
            features["repos_x__bin"] = 0  # trop frais
        elif ecart <= 20:
            features["repos_x__bin"] = 1  # optimal court
        elif ecart <= 35:
            features["repos_x__bin"] = 2  # optimal
        elif ecart <= 60:
            features["repos_x__bin"] = 3  # ok
        elif ecart <= 120:
            features["repos_x__bin"] = 4  # long repos
        else:
            features["repos_x__bin"] = 5  # tres long / retour
    else:
        features["repos_x__bin"] = None

    # Feature 4: Is repos optimal (14-35 jours)
    if ecart is not None:
        features["repos_x__is_optimal"] = 1.0 if 14 <= ecart <= 35 else 0.0
    else:
        features["repos_x__is_optimal"] = None

    # Feature 5: Is inedit (premiere course)
    is_inedit = row.get("is_inedit")
    features["repos_x__is_first_start"] = safe_float(is_inedit)

    # Feature 6: Nb courses carriere (experience)
    nb = safe_float(row.get("nb_courses_carriere"))
    features["repos_x__experience_log"] = math.log1p(nb) if nb and nb > 0 else None

    return features


# =====================================================================
# C5. POIDS IMPACT DEEP BUILDER
# =====================================================================
def build_poids_impact(row, course_stats):
    """Interactions poids x distance x terrain x age."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid
    course_uid = row.get("course_uid")

    poids = safe_float(row.get("handicap_valeur"))
    distance = safe_float(row.get("distance"))
    age = safe_float(row.get("age"))
    terrain_raw = row.get("met_terrain_predit") or row.get("cnd_cond_type_terrain") or ""
    terrain = str(terrain_raw) if terrain_raw is not None else ""

    # Feature 1: Poids par km
    features["poids_x__per_km"] = safe_div(poids, distance / 1000 if distance else None)

    # Feature 2: Poids vs peloton
    if course_uid and course_uid in course_stats and poids is not None:
        avg_p = course_stats[course_uid].get("avg_poids")
        if avg_p and avg_p > 0:
            features["poids_x__vs_field"] = poids - avg_p
            features["poids_x__ratio_field"] = poids / avg_p
        else:
            features["poids_x__vs_field"] = None
            features["poids_x__ratio_field"] = None
    else:
        features["poids_x__vs_field"] = None
        features["poids_x__ratio_field"] = None

    # Feature 3: Poids x age interaction
    if poids and age:
        features["poids_x__x_age"] = poids * age
    else:
        features["poids_x__x_age"] = None

    # Feature 4: Is terrain lourd (poids plus impactant sur terrain lourd)
    is_lourd = 1.0 if ("lourd" in terrain.lower() or "heavy" in terrain.lower()) else 0.0
    features["poids_x__terrain_lourd"] = is_lourd

    # Feature 5: Poids x terrain lourd
    if poids:
        features["poids_x__x_terrain_lourd"] = poids * is_lourd
    else:
        features["poids_x__x_terrain_lourd"] = None

    return features


# =====================================================================
# C6. CONDITIONS COURSE DEEP BUILDER
# =====================================================================
def build_conditions_deep(row):
    """Exploite les colonnes cnd_ inexploitees."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid

    age = safe_float(row.get("age"))
    age_min = safe_float(row.get("cnd_cond_age_min"))
    age_max = safe_float(row.get("cnd_cond_age_max"))
    dist_cond = safe_float(row.get("cnd_cond_distance_m"))
    distance = safe_float(row.get("distance"))
    groupe_raw = row.get("cnd_cond_groupe")
    groupe = str(groupe_raw) if groupe_raw is not None and not (isinstance(groupe_raw, float) and math.isnan(groupe_raw)) else ""
    nb_vic_max = safe_float(row.get("cnd_cond_nb_victoires_max"))
    prix = safe_float(row.get("cnd_cond_prix_euros"))
    is_quinte = safe_float(row.get("cnd_cond_is_quinte"))
    is_tierce = safe_float(row.get("cnd_cond_is_tierce"))

    # Feature 1: Age vs plage conditions (0=jeune, 0.5=milieu, 1=vieux)
    if age and age_min and age_max and age_max > age_min:
        features["cond_x__age_position"] = (age - age_min) / (age_max - age_min)
    else:
        features["cond_x__age_position"] = None

    # Feature 2: Ecart distance conditions vs distance reelle
    if dist_cond and distance:
        features["cond_x__dist_ecart"] = distance - dist_cond
    else:
        features["cond_x__dist_ecart"] = None

    # Feature 3: Groupe (encoded numeriquement)
    groupe_map = {"": 0, "1": 5, "2": 4, "3": 3, "liste": 2, "handicap": 1}
    features["cond_x__groupe_level"] = safe_float(groupe_map.get(groupe.lower(), 0))

    # Feature 4: Nb victoires max (niveau de la course)
    features["cond_x__nb_vic_max"] = nb_vic_max

    # Feature 5: Prix (allocation)
    features["cond_x__prix_log"] = math.log1p(prix) if prix and prix > 0 else None

    # Feature 6: Is quinte (course populaire = plus de donnees marche)
    features["cond_x__is_quinte"] = is_quinte

    # Feature 7: Is tierce
    features["cond_x__is_tierce"] = is_tierce

    return features


# =====================================================================
# C8. SPEED BIAS BUILDER
# =====================================================================
def build_speed_bias(row):
    """Exploite colonnes spd_ inexploitees."""
    features = {}
    uid = row.get("partant_uid")
    if not uid:
        return None
    features["partant_uid"] = uid

    bias_corde = safe_float(row.get("spd_bias_corde_gagnant_moy"))
    bias_int = safe_float(row.get("spd_bias_interieur"))
    field_avg = safe_float(row.get("spd_field_strength_avg"))
    field_max = safe_float(row.get("spd_field_strength_max"))
    field_std = safe_float(row.get("spd_field_strength_std"))
    corde = safe_float(row.get("corde"))

    # Feature 1: Biais corde (avantage position depart)
    features["spdbias_x__corde_advantage"] = bias_corde

    # Feature 2: Biais interieur
    features["spdbias_x__interieur"] = bias_int

    # Feature 3: Force du peloton moyenne
    features["spdbias_x__field_avg"] = field_avg

    # Feature 4: Force du peloton max (le meilleur adversaire)
    features["spdbias_x__field_max"] = field_max

    # Feature 5: Homogeneite du peloton (std faible = course serree)
    features["spdbias_x__field_std"] = field_std

    # Feature 6: Corde x biais (le cheval profite-t-il du biais?)
    if corde and bias_corde:
        # Corde basse (1-4) + biais interieur fort = avantage
        features["spdbias_x__corde_x_bias"] = (1.0 / max(corde, 1)) * bias_corde
    else:
        features["spdbias_x__corde_x_bias"] = None

    return features


# =====================================================================
# MAIN: Execute tous les builders en streaming
# =====================================================================
def main():
    start = time.time()
    print("=" * 70)
    print("  EXECUTION DES NOUVEAUX BUILDERS (C1-C6, C8)")
    print("  Source: partants_master.parquet")
    print("=" * 70)

    pf = pq.ParquetFile(str(PARQUET))
    n_rg = pf.metadata.num_row_groups
    n_rows = pf.metadata.num_rows
    print(f"  {n_rows:,} rows, {n_rg} row groups")
    print()

    # Phase 1: Calculer les stats par course (moyenne handicap, poids)
    print("Phase 1: Calcul des moyennes par course...")
    course_stats = defaultdict(lambda: {"handicaps": [], "poids": []})

    cols_phase1 = ["course_uid", "handicap_valeur"]
    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=cols_phase1)
        df = table.to_pandas()
        for _, r in df.iterrows():
            cuid = r.get("course_uid")
            if cuid:
                h = safe_float(r.get("handicap_valeur"))
                if h:
                    course_stats[cuid]["handicaps"].append(h)
                    course_stats[cuid]["poids"].append(h)
        del table, df
        if (rg_idx + 1) % 10 == 0:
            print(f"  RG {rg_idx+1}/{n_rg}")

    # Finaliser les moyennes
    for cuid, stats in course_stats.items():
        hs = stats["handicaps"]
        ps = stats["poids"]
        stats["avg_handicap"] = sum(hs) / len(hs) if hs else None
        stats["avg_poids"] = sum(ps) / len(ps) if ps else None
        del stats["handicaps"]
        del stats["poids"]

    print(f"  {len(course_stats):,} courses avec stats handicap")
    print()

    # Phase 2: Calculer toutes les features
    print("Phase 2: Calcul des features (streaming)...")

    # Colonnes necessaires
    needed_cols = [
        "partant_uid", "course_uid", "horse_id", "nom_cheval",
        "date_reunion_iso", "distance", "age", "corde",
        "handicap_valeur", "handicap_distance_m",
        "ecart_precedent", "is_inedit", "nb_courses_carriere",
        "cote_finale", "mch_proba_implicite",
        "met_cheval_nb_courses_terrain", "met_cheval_specialist_terrain",
        "met_cheval_taux_place_terrain", "met_cheval_taux_vic_pluie",
        "met_cheval_taux_vic_terrain", "met_impact_meteo_score",
        "mto_precipitation_mm", "met_terrain_predit",
        "mch_enjeu_combinaison", "mch_pct_masse", "mch_total_enjeu_pari",
        "cnd_cond_age_min", "cnd_cond_age_max", "cnd_cond_distance_m",
        "cnd_cond_groupe", "cnd_cond_nb_victoires_max", "cnd_cond_prix_euros",
        "cnd_cond_is_quinte", "cnd_cond_is_tierce", "cnd_cond_type_terrain",
        "spd_bias_corde_gagnant_moy", "spd_bias_interieur",
        "spd_field_strength_avg", "spd_field_strength_max", "spd_field_strength_std",
    ]
    # Filter to only existing columns
    schema_names = set(pf.schema_arrow.names)
    needed_cols = [c for c in needed_cols if c in schema_names]
    print(f"  Colonnes chargees: {len(needed_cols)}")

    # Output files
    builders = {
        "meteo_x": [],
        "handicap_x": [],
        "enjeux_x": [],
        "repos_x": [],
        "poids_x": [],
        "cond_x": [],
        "spdbias_x": [],
    }

    horse_last_date = {}
    total = 0

    for rg_idx in range(n_rg):
        table = pf.read_row_group(rg_idx, columns=needed_cols)
        df = table.to_pandas()
        del table

        for _, r in df.iterrows():
            row = r.to_dict()
            total += 1

            f1 = build_meteo_impact(row)
            if f1:
                builders["meteo_x"].append(f1)

            f2 = build_handicap_deep(row, course_stats)
            if f2:
                builders["handicap_x"].append(f2)

            f3 = build_marche_enjeux(row)
            if f3:
                builders["enjeux_x"].append(f3)

            f4 = build_ecart_repos(row, horse_last_date)
            if f4:
                builders["repos_x"].append(f4)

            f5 = build_poids_impact(row, course_stats)
            if f5:
                builders["poids_x"].append(f5)

            f6 = build_conditions_deep(row)
            if f6:
                builders["cond_x"].append(f6)

            f8 = build_speed_bias(row)
            if f8:
                builders["spdbias_x"].append(f8)

        del df

        # Flush to disk every 5 row groups to save RAM
        if (rg_idx + 1) % 5 == 0 or rg_idx == n_rg - 1:
            for name, records in builders.items():
                if records:
                    out_dir = OUTPUT_DIR / name
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f"{name}_features.jsonl"
                    mode = "a" if rg_idx >= 5 else "w"
                    with open(out_file, mode, encoding="utf-8", newline="\n") as f:
                        for rec in records:
                            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    records.clear()

            elapsed = time.time() - start
            print(f"  RG {rg_idx+1}/{n_rg} | {total:,} rows | {elapsed:.0f}s")

    # Summary
    elapsed = time.time() - start
    print()
    print("=" * 70)
    print(f"  TERMINE en {elapsed:.0f}s")
    print(f"  {total:,} lignes traitees")
    print()
    for name in builders:
        out_file = OUTPUT_DIR / name / f"{name}_features.jsonl"
        if out_file.exists():
            size_mb = out_file.stat().st_size / 1024 / 1024
            # Count lines
            with open(out_file, "r") as f:
                n = sum(1 for _ in f)
            print(f"  {name}: {n:,} records, {size_mb:.0f} Mo")
    print("=" * 70)


if __name__ == "__main__":
    main()
