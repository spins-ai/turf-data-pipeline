#!/usr/bin/env python3
"""
Post-processing horse_stats — Enrichit horse_stats_master.json avec :
  - class_category (top / bon / moyen / faible) basé sur les gains
  - specialiste_discipline (discipline la plus fréquente)
  - specialiste_distance (distance moyenne arrondie)
  - distance_range (min-max de distances courues)
  - nb_hippodromes (diversité)
  - career_length_days (durée de carrière)
  - regularity (régularité : nb courses / durée carrière)

⚠️ NE SUPPRIME RIEN — enrichit le fichier existant
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json, os, time
from datetime import datetime

from utils.logging_setup import setup_logging
log = setup_logging("postprocess_horse_stats")
nBASE_DIR = os.path.dirname(os.path.abspath(__file__))


def enrich_horse_stats(record):
    """Ajoute les champs calculés à un record horse_stats"""

    # ── Classe du cheval basée sur les gains ──
    gains = record.get("gains_total_euros")
    if gains is not None:
        try:
            g = float(gains)
            record["class_category"] = (
                "elite" if g > 500000 else
                "top" if g > 100000 else
                "bon" if g > 30000 else
                "moyen" if g > 5000 else
                "faible"
            )
            record["gains_par_course"] = round(g / max(record.get("nb_courses_total", 1), 1), 2)
        except (ValueError, TypeError):
            pass

    # ── Taux de réussite catégorisé ──
    tv = record.get("taux_victoire")
    if tv is not None:
        try:
            t = float(tv)
            record["performance_category"] = (
                "crack" if t > 0.30 else
                "regulier" if t > 0.15 else
                "moyen" if t > 0.05 else
                "faible"
            )
        except (ValueError, TypeError):
            pass

    # ── Spécialiste discipline ──
    disciplines = record.get("disciplines")
    if isinstance(disciplines, list) and len(disciplines) > 0:
        record["specialiste_discipline"] = disciplines[0]  # la plus fréquente
        record["nb_disciplines"] = len(set(disciplines))
        record["is_polyvalent"] = len(set(disciplines)) > 1

    # ── Spécialiste distance ──
    distances = record.get("distances_courues")
    if isinstance(distances, list) and len(distances) > 0:
        try:
            dists_num = [int(d) for d in distances if d is not None]
            if dists_num:
                avg = sum(dists_num) / len(dists_num)
                record["distance_moyenne"] = round(avg)
                record["distance_min"] = min(dists_num)
                record["distance_max"] = max(dists_num)
                record["distance_range"] = max(dists_num) - min(dists_num)
                record["distance_pref_category"] = (
                    "sprinter" if avg < 1400 else
                    "miler" if avg < 1800 else
                    "middle" if avg < 2200 else
                    "classique" if avg < 2800 else
                    "stayer" if avg < 3500 else
                    "marathon"
                )
        except (ValueError, TypeError):
            pass

    # ── Nombre d'hippodromes (diversité) ──
    hippos = record.get("hippodromes")
    if isinstance(hippos, list):
        record["nb_hippodromes"] = len(set(hippos))

    # ── Durée de carrière ──
    debut = record.get("premiere_course_date")
    fin = record.get("derniere_course_date")
    if debut and fin:
        try:
            d1 = datetime.strptime(str(debut)[:10], "%Y-%m-%d")
            d2 = datetime.strptime(str(fin)[:10], "%Y-%m-%d")
            days = (d2 - d1).days
            record["career_length_days"] = days
            record["career_length_years"] = round(days / 365.25, 1)
            # Régularité : courses par mois
            months = max(days / 30.0, 1)
            nb_courses = record.get("nb_courses_total", 1) or 1
            record["courses_par_mois"] = round(nb_courses / months, 2)
        except (ValueError, TypeError):
            pass

    # ── Expérience ──
    nb = record.get("nb_courses_total")
    if nb is not None:
        try:
            n = int(nb)
            record["experience_category"] = (
                "debutant" if n <= 3 else
                "novice" if n <= 10 else
                "confirme" if n <= 30 else
                "routinier" if n <= 60 else
                "veteran"
            )
        except (ValueError, TypeError):
            pass

    # ── Forme récente vs globale ──
    forme5 = record.get("forme_5")
    forme20 = record.get("forme_20")
    tv_global = record.get("taux_victoire")
    if forme5 is not None and tv_global is not None:
        try:
            f5 = float(forme5)
            tvg = float(tv_global)
            if tvg > 0:
                record["forme_vs_global"] = round(f5 / tvg, 2)  # > 1 = en forme montante
                record["is_en_forme"] = f5 > tvg * 1.5
                record["is_en_baisse"] = f5 < tvg * 0.5
        except (ValueError, TypeError):
            pass

    return record


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("POST-PROCESSING HORSE STATS")
    log.info("=" * 60)

    path = os.path.join(BASE_DIR, "../../data_master", "horse_stats_master.json")
    log.info(f"Chargement {path}...")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"  → {len(data)} records")

    # Enrichir
    log.info("Enrichissement...")
    for r in data:
        enrich_horse_stats(r)

    # Stats
    total = len(data)
    new_fields = [
        'class_category', 'gains_par_course', 'performance_category',
        'specialiste_discipline', 'nb_disciplines', 'is_polyvalent',
        'distance_moyenne', 'distance_pref_category', 'distance_range',
        'nb_hippodromes', 'career_length_days', 'career_length_years',
        'courses_par_mois', 'experience_category',
        'forme_vs_global', 'is_en_forme', 'is_en_baisse',
    ]
    for field in new_fields:
        count = sum(1 for r in data if r.get(field) is not None)
        log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Distributions
    for cat_field in ['class_category', 'performance_category', 'distance_pref_category',
                      'experience_category']:
        vals = {}
        for r in data:
            v = r.get(cat_field)
            if v:
                vals[v] = vals.get(v, 0) + 1
        log.info(f"  {cat_field}: {dict(sorted(vals.items(), key=lambda x: -x[1]))}")

    # En forme
    en_forme = sum(1 for r in data if r.get("is_en_forme"))
    en_baisse = sum(1 for r in data if r.get("is_en_baisse"))
    log.info(f"  En forme: {en_forme}, En baisse: {en_baisse}")

    # Sauvegarder
    log.info("Sauvegarde horse_stats_master.json enrichi...")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)
    log.info(f"  → {os.path.getsize(path)/1024/1024:.1f} MB")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s")

if __name__ == "__main__":
    main()
