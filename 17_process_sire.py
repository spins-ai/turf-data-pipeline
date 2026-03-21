#!/usr/bin/env python3
"""
Script 17 — Processing fichier SIRE/IFCE (4M chevaux)
Transforme le CSV brut en JSON normalisé utilisable par les feature builders
"""

import csv
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "output", "17_sire_ifce", "donnees-equides.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "17_sire_ifce")

def parse_date(d):
    """Parse date JJ/MM/AAAA -> YYYY-MM-DD"""
    if not d or d.strip() == "":
        return None
    try:
        return datetime.strptime(d.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return None

def normalize_name(name):
    """Normaliser le nom du cheval pour jointure"""
    if not name:
        return ""
    return name.strip().upper().replace("'", "'").replace("  ", " ")

def main():
    print("=" * 60)
    print("SCRIPT 17 — Processing SIRE/IFCE")
    print("=" * 60)

    if not os.path.exists(INPUT_FILE):
        print(f"ERREUR: {INPUT_FILE} non trouvé")
        return

    # Races pertinentes pour les courses
    RACES_COURSES = {
        "PUR-SANG", "PUR SANG", "THOROUGHBRED",
        "TROTTEUR FRANCAIS", "TROTTEUR FR", "TF",
        "AQPS", "AUTRE QUE PUR SANG",
        "SELLE FRANCAIS", "SF",
        "ANGLO-ARABE", "AA",
    }

    # Lecture et parsing
    print(f"\n[1/3] Lecture de {INPUT_FILE}...")
    all_horses = []
    race_horses = []  # Chevaux de course uniquement
    race_counts = {}
    sex_counts = {}
    country_counts = {}

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader):
            horse = {
                "nom": normalize_name(row.get("NOM", "")),
                "race": row.get("RACE", "").strip(),
                "sexe": row.get("SEXE", "").strip(),
                "robe": row.get("ROBE", "").strip(),
                "date_naissance": parse_date(row.get("DATE_DE_NAISSANCE", "")),
                "pays_naissance": row.get("PAYS_DE_NAISSANCE", "").strip(),
                "consommation": row.get("CHE_COCONSO", "").strip(),
                "date_deces": parse_date(row.get("DATE_DE_DECES", "")),
            }

            # Calculer l'âge si date de naissance connue
            if horse["date_naissance"]:
                try:
                    birth = datetime.strptime(horse["date_naissance"], "%Y-%m-%d")
                    today = datetime.now()
                    horse["age_ans"] = (today - birth).days / 365.25
                    horse["annee_naissance"] = birth.year
                except:
                    horse["age_ans"] = None
                    horse["annee_naissance"] = None
            else:
                horse["age_ans"] = None
                horse["annee_naissance"] = None

            # Vivant ou mort
            horse["vivant"] = horse["date_deces"] is None

            all_horses.append(horse)

            # Stats
            race_counts[horse["race"]] = race_counts.get(horse["race"], 0) + 1
            sex_counts[horse["sexe"]] = sex_counts.get(horse["sexe"], 0) + 1
            country_counts[horse["pays_naissance"]] = country_counts.get(horse["pays_naissance"], 0) + 1

            # Filtrer les chevaux de course
            race_upper = horse["race"].upper()
            is_racing = any(r in race_upper for r in RACES_COURSES)
            if is_racing:
                race_horses.append(horse)

            if (i + 1) % 500000 == 0:
                print(f"  {i+1:,} lignes lues, {len(race_horses):,} chevaux de course...")

    print(f"\n  Total: {len(all_horses):,} chevaux")
    print(f"  Chevaux de course: {len(race_horses):,}")

    # [2/3] Statistiques
    print(f"\n[2/3] Statistiques...")

    stats = {
        "total_chevaux": len(all_horses),
        "chevaux_course": len(race_horses),
        "top_races": dict(sorted(race_counts.items(), key=lambda x: -x[1])[:30]),
        "sexes": sex_counts,
        "top_pays": dict(sorted(country_counts.items(), key=lambda x: -x[1])[:20]),
        "vivants": sum(1 for h in all_horses if h["vivant"]),
        "morts": sum(1 for h in all_horses if not h["vivant"]),
    }

    print(f"  Top races: {list(stats['top_races'].items())[:10]}")
    print(f"  Sexes: {sex_counts}")
    print(f"  Top pays: {list(stats['top_pays'].items())[:5]}")

    # [3/3] Sauvegarde
    print(f"\n[3/3] Sauvegarde...")

    # Créer un index par nom pour jointure rapide
    index_par_nom = {}
    for h in race_horses:
        nom = h["nom"]
        if nom:
            if nom not in index_par_nom:
                index_par_nom[nom] = h
            else:
                # Garder le plus récent (date naissance la plus récente)
                existing = index_par_nom[nom]
                if h.get("annee_naissance") and existing.get("annee_naissance"):
                    if h["annee_naissance"] > existing["annee_naissance"]:
                        index_par_nom[nom] = h

    # Sauvegarder les chevaux de course (utilisable par les features)
    output_courses = os.path.join(OUTPUT_DIR, "chevaux_course.json")
    with open(output_courses, "w", encoding="utf-8") as f:
        json.dump(race_horses, f, ensure_ascii=False)
    print(f"  ✓ chevaux_course.json ({len(race_horses):,} chevaux)")

    # Index par nom (pour jointure rapide)
    output_index = os.path.join(OUTPUT_DIR, "index_par_nom.json")
    with open(output_index, "w", encoding="utf-8") as f:
        json.dump(index_par_nom, f, ensure_ascii=False)
    print(f"  ✓ index_par_nom.json ({len(index_par_nom):,} noms uniques)")

    # Stats
    output_stats = os.path.join(OUTPUT_DIR, "sire_stats.json")
    with open(output_stats, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"  ✓ sire_stats.json")

    # CSV des chevaux de course
    output_csv = os.path.join(OUTPUT_DIR, "chevaux_course.csv")
    if race_horses:
        keys = race_horses[0].keys()
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(race_horses)
    print(f"  ✓ chevaux_course.csv")

    print(f"\n{'='*60}")
    print(f"TERMINÉ: {len(race_horses):,} chevaux de course extraits")
    print(f"  Index: {len(index_par_nom):,} noms uniques pour jointure")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
