#!/usr/bin/env python3
"""
Script 48 — Parse conditions texte (regex → features structurées)
Calcul local, 0 requête API.

Input :
  - output/02_liste_courses/courses_normalisees.jsonl (ou .json)

Output : output/48_conditions_texte/
  - conditions_parsees.jsonl

Features extraites du champ conditions_texte par regex :
  - cond_age_min : âge minimum autorisé (2, 3, 4...)
  - cond_age_max : âge maximum autorisé (None si pas de max)
  - cond_sexe : restriction sexe (males, femelles, hongres, tous)
  - cond_poids_min_kg : poids minimum
  - cond_poids_max_kg : poids maximum
  - cond_gains_min : gains minimum pour participer
  - cond_gains_max : gains maximum (réclamer)
  - cond_reclamation : True si course à réclamer
  - cond_groupe : rang du groupe (1, 2, 3 ou None)
  - cond_listed : True si course listée
  - cond_handicap : True si handicap
  - cond_apprentis : True si réservé aux apprentis
  - cond_amateurs : True si réservé aux amateurs
  - cond_distance_min : distance minimum extraite
  - cond_prix_euros : montant du prix en euros
  - cond_nb_victoires_max : max victoires pour participer
  - cond_nb_courses_min : min courses courues
  - cond_type_terrain : restriction terrain (gazon, psf, etc.)
  - cond_depart : type de départ (autostart, volte, etc.)
"""

import json
import logging
import os
import re
import sys

OUTPUT_DIR = "output/48_conditions_texte"
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)


def parse_conditions(texte):
    """Parse le texte des conditions de course et extrait les features structurées."""
    if texte is None:
        texte = ""
    if not texte:
        return {}

    t = texte.strip()
    t_lower = t.lower()
    result = {}

    # === ÂGE ===
    # "3 ans", "4 ans et plus", "3 ans et au-dessus", "2 ans révolus"
    age_match = re.search(r'(\d)\s*ans?\s*(et\s*(plus|au[- ]dessus))?', t_lower)
    if age_match:
        result["cond_age_min"] = int(age_match.group(1))
        if age_match.group(2):
            result["cond_age_max"] = None  # pas de max
        else:
            result["cond_age_max"] = int(age_match.group(1))

    # "de 3 à 5 ans"
    age_range = re.search(r'de\s*(\d)\s*[àa]\s*(\d)\s*ans', t_lower)
    if age_range:
        result["cond_age_min"] = int(age_range.group(1))
        result["cond_age_max"] = int(age_range.group(2))

    # === SEXE ===
    if re.search(r'\b(femelles?|juments?|pouliches?)\b', t_lower):
        result["cond_sexe"] = "femelles"
    elif re.search(r'\b(m[âa]les?|hongres?|entiers?)\b', t_lower):
        result["cond_sexe"] = "males"
    elif re.search(r'\btous?\s*sexes?\b', t_lower):
        result["cond_sexe"] = "tous"

    # === POIDS ===
    # "53 kg", "de 54 à 62 kg"
    poids_match = re.search(r'(\d{2})\s*(?:à|a)\s*(\d{2})\s*kg', t_lower)
    if poids_match:
        result["cond_poids_min_kg"] = int(poids_match.group(1))
        result["cond_poids_max_kg"] = int(poids_match.group(2))
    else:
        poids_single = re.search(r'(\d{2})\s*kg', t_lower)
        if poids_single:
            result["cond_poids_base_kg"] = int(poids_single.group(1))

    # === GAINS ===
    # "n'ayant pas gagné 25.000 euros" → gains_max
    gains_max_match = re.search(r"n'ayant\s*pas\s*gagn[ée]\s*([\d.,]+)\s*(?:€|euros?)", t_lower)
    if gains_max_match:
        gains_str = gains_max_match.group(1).replace(".", "").replace(",", ".")
        try:
            result["cond_gains_max"] = float(gains_str)
        except ValueError:
            pass

    # "ayant gagné au moins 15.000 euros" → gains_min
    gains_min_match = re.search(r"ayant\s*gagn[ée]\s*(?:au\s*moins\s*)?([\d.,]+)\s*(?:€|euros?)", t_lower)
    if gains_min_match and "pas" not in gains_min_match.group(0):
        gains_str = gains_min_match.group(1).replace(".", "").replace(",", ".")
        try:
            result["cond_gains_min"] = float(gains_str)
        except ValueError:
            pass

    # === PRIX / ALLOCATION ===
    prix_match = re.search(r'prix\s*[:de]*\s*([\d.,]+)\s*(?:€|euros?)', t_lower)
    if prix_match:
        prix_str = prix_match.group(1).replace(".", "").replace(",", ".")
        try:
            result["cond_prix_euros"] = float(prix_str)
        except ValueError:
            pass

    # Montant dans le titre
    montant_match = re.search(r'([\d.]+)\s*(?:€|euros?)', t_lower)
    if montant_match and "cond_prix_euros" not in result:
        prix_str = montant_match.group(1).replace(".", "")
        try:
            val = float(prix_str)
            if val >= 1000:  # Ignorer les petits montants (probablement des poids)
                result["cond_prix_euros"] = val
        except ValueError:
            pass

    # === TYPE DE COURSE ===
    # Groupe
    groupe_match = re.search(r'groupe?\s*([123I]+)', t_lower)
    if groupe_match:
        g = groupe_match.group(1)
        if g in ("1", "I"):
            result["cond_groupe"] = 1
        elif g in ("2", "II"):
            result["cond_groupe"] = 2
        elif g in ("3", "III"):
            result["cond_groupe"] = 3

    # Listed
    if re.search(r'\b(list[ée]e?|listed)\b', t_lower):
        result["cond_listed"] = True

    # Handicap
    if re.search(r'\b(handicap|hand\.?|hcp)\b', t_lower):
        result["cond_handicap"] = True

    # Réclamer
    if re.search(r'\b(r[ée]clamer|claiming|claimer)\b', t_lower):
        result["cond_reclamation"] = True

    # Conditions / à conditions
    if re.search(r'\b(conditions?|course\s*[àa]\s*conditions?)\b', t_lower):
        result["cond_a_conditions"] = True

    # === RESTRICTIONS CAVALIER ===
    if re.search(r'\b(apprenti|jeunes?\s*jockeys?)\b', t_lower):
        result["cond_apprentis"] = True

    if re.search(r'\b(amateur|gentleman|lady)\b', t_lower):
        result["cond_amateurs"] = True

    # === NB VICTOIRES MAX ===
    vic_match = re.search(r"n'ayant\s*pas\s*(?:remport[ée]|gagn[ée])\s*(?:plus\s*de\s*)?(\d+)\s*(?:victoire|course)", t_lower)
    if vic_match:
        result["cond_nb_victoires_max"] = int(vic_match.group(1))

    # === DÉPART ===
    if re.search(r'\b(autostart|auto-start|départ\s*auto)\b', t_lower):
        result["cond_depart"] = "autostart"
    elif re.search(r'\b(volte)\b', t_lower):
        result["cond_depart"] = "volte"
    elif re.search(r'\b(stall|stalles?|boîtes?)\b', t_lower):
        result["cond_depart"] = "stall"

    # === TERRAIN ===
    if re.search(r'\b(piste\s*en\s*sable|psf|polytrack|fibresand)\b', t_lower):
        result["cond_type_terrain"] = "psf"
    elif re.search(r'\b(gazon|herbe|turf)\b', t_lower):
        result["cond_type_terrain"] = "gazon"

    # === DISTANCE ===
    dist_match = re.search(r'(\d{3,5})\s*(?:m[eè]tres?|m\b)', t_lower)
    if dist_match:
        result["cond_distance_m"] = int(dist_match.group(1))

    # === QUALIFICATIFS ===
    result["cond_is_quinte"] = bool(re.search(r'\b(quint[ée]|quinté\+?)\b', t_lower))
    result["cond_is_tierce"] = bool(re.search(r'\b(tierc[ée])\b', t_lower))
    result["cond_is_international"] = bool(re.search(r'\b(international|intern\.?)\b', t_lower))

    # Nombre de features extraites
    result["cond_nb_features_extraites"] = len(result)

    return result


def main():
    log.info("=" * 60)
    log.info("SCRIPT 48 — Parse conditions texte")
    log.info("=" * 60)

    # Charger les courses
    courses = []
    for path in ["output/02_liste_courses/courses_normalisees.jsonl",
                 "output/02_liste_courses/courses_normalisees.json"]:
        if not os.path.exists(path):
            continue
        log.info(f"Chargement: {path}")
        KEEP = {"course_uid", "date_reunion_iso", "hippodrome_normalise",
                "conditions_texte", "discipline", "distance", "numero_reunion", "numero_course",
                "categorie", "condition_age", "condition_sexe", "libelle"}
        if path.endswith(".jsonl"):
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        c = json.loads(line)
                        courses.append({k: c[k] for k in KEEP if k in c})
                    except json.JSONDecodeError:
                        continue
        else:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for c in data:
                courses.append({k: c[k] for k in KEEP if k in c})
            del data
        break

    if not courses:
        log.error("Aucune course trouvée")
        sys.exit(1)

    log.info(f"  {len(courses)} courses chargées")

    # Parser les conditions
    output_file = os.path.join(OUTPUT_DIR, "conditions_parsees.jsonl")
    total_enriched = 0
    total_features = 0

    with open(output_file, "w", encoding="utf-8") as fout:
        for i, c in enumerate(courses):
            texte = c.get("conditions_texte") or ""
            libelle = c.get("libelle") or ""

            # Parser les deux textes et fusionner
            parsed = parse_conditions(texte)
            parsed_libelle = parse_conditions(libelle)

            # Fusionner (conditions_texte prioritaire)
            for k, v in parsed_libelle.items():
                if k not in parsed:
                    parsed[k] = v

            # Ajouter les identifiants
            parsed["course_uid"] = c.get("course_uid", "")
            parsed["date_reunion_iso"] = c.get("date_reunion_iso", "")
            parsed["hippodrome_normalise"] = c.get("hippodrome_normalise", "")
            parsed["conditions_texte_original"] = (texte or "")[:200]  # tronqué pour debug

            if len(parsed) > 5:  # au moins quelques features extraites
                total_enriched += 1

            total_features += parsed.get("cond_nb_features_extraites", 0)

            fout.write(json.dumps(parsed, ensure_ascii=False) + "\n")

            if (i + 1) % 50000 == 0:
                log.info(f"  {i+1}/{len(courses)} parsées, {total_enriched} enrichies")

    avg_features = total_features / len(courses) if courses else 0
    log.info(f"Terminé: {total_enriched}/{len(courses)} enrichies ({100*total_enriched/len(courses):.1f}%)")
    log.info(f"  Moyenne features par course: {avg_features:.1f}")
    log.info(f"  Output: {output_file}")


if __name__ == "__main__":
    main()
