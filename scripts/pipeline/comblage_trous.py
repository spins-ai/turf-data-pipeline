#!/usr/bin/env python3
"""
comblage_trous.py — Étape 4 du TODO
=====================================
Comblage des trous dans les données.

Stratégie : pour chaque champ avec taux de remplissage < 100%,
chercher la valeur dans les autres sources.

Opérations :
  1. penetrometre (56% vide) → croiser avec réunions enrichies (39) + météo
  2. condition_age (51% vide) → regex depuis conditions_texte
  3. pays_cheval → croiser avec SIRE/IFCE (17)
  4. eleveur → croiser avec SIRE/IFCE (17)
  5. type_piste manquant → croiser avec hippodromes_db.py
  6. corde manquante → croiser avec hippodromes_db.py
  7. sexe_cheval manquant → croiser avec SIRE/IFCE (17)
  8. nombre_partants si manquant → compter depuis partants
  9. allocation si manquant → croiser avec rapports (21/38)

Fonctionne en streaming JSONL.

Input : output/nettoyage/partants_nettoyes.jsonl (ou partants_normalises)
Output : output/comblage/partants_combles.jsonl + rapport

Usage :
    python3 comblage_trous.py
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import os
import re
import sys
from collections import Counter, defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "../../output", "comblage")
os.makedirs(OUTPUT_DIR, exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("comblage_trous")


# ================================================================
# CHARGEMENT DES SOURCES DE COMBLAGE
# ================================================================

def load_reunions_meteo():
    """Index météo/terrain par (date, hippodrome) depuis réunions enrichies."""
    index = {}
    for path in [os.path.join(BASE_DIR, "../../output", "39_reunions_enrichies", "reunions_enrichies.jsonl"),
                 os.path.join(BASE_DIR, "../../data_master", "meteo_master.json")]:
        if not os.path.exists(path):
            continue
        log.info(f"  Chargement météo: {path}")
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
                    date_iso = (r.get("date_reunion_iso") or "")[:10]
                    hippo = (r.get("hippodrome_normalise") or "").lower().strip()
                    if date_iso and hippo:
                        key = f"{date_iso}|{hippo}"
                        index[key] = {
                            "penetrometre": r.get("penetrometre") or r.get("penetrometre_numeric"),
                            "type_piste": r.get("type_piste") or r.get("typePiste"),
                            "corde": r.get("corde"),
                            "terrain": r.get("terrain_category") or r.get("terrain"),
                        }
        else:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for r in data:
                    date_iso = (r.get("date_reunion_iso") or r.get("date") or "")[:10]
                    hippo = (r.get("hippodrome_normalise") or r.get("hippodrome") or "").lower().strip()
                    if date_iso and hippo:
                        key = f"{date_iso}|{hippo}"
                        if key not in index:
                            index[key] = {
                                "penetrometre": r.get("penetrometre") or r.get("penetrometre_numeric"),
                                "type_piste": r.get("type_piste"),
                                "corde": r.get("corde"),
                                "terrain": r.get("terrain_category"),
                            }
            del data

    log.info(f"  → {len(index)} entrées météo/terrain")
    return index


def load_hippodromes_db():
    """Charge la base hippodromes pour type_piste et corde."""
    db = {}
    path = "hippodromes_db.py"
    if not os.path.exists(path):
        return db

    # Parser le dict HIPPODROMES_DB depuis le fichier Python
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        # Exécuter pour récupérer le dict
        namespace = {}
        exec(content, namespace)
        raw = namespace.get("HIPPODROMES_DB", {})
        for hippo_name, info in raw.items():
            db[hippo_name.lower().strip()] = {
                "type_piste": info.get("type_piste", ""),
                "corde": info.get("corde", ""),
                "altitude": info.get("altitude"),
                "latitude": info.get("latitude"),
                "longitude": info.get("longitude"),
            }
        log.info(f"  → {len(db)} hippodromes dans la base")
    except Exception as e:
        log.warning(f"  Erreur chargement hippodromes_db: {e}")

    return db


def load_sire_ifce():
    """Charge les données SIRE/IFCE pour pays, éleveur, sexe."""
    index = {}
    path = os.path.join(BASE_DIR, "../../output", "17_sire_ifce", "sire_ifce.json")
    if not os.path.exists(path):
        return index

    log.info(f"  Chargement SIRE/IFCE: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for r in data:
                nom = (r.get("nom") or r.get("nom_cheval") or "").upper().strip()
                if nom:
                    index[nom] = {
                        "pays": r.get("pays") or r.get("pays_naissance"),
                        "eleveur": r.get("eleveur"),
                        "sexe": r.get("sexe"),
                        "race": r.get("race"),
                        "date_naissance": r.get("date_naissance"),
                    }
        del data
        log.info(f"  → {len(index)} chevaux SIRE/IFCE")
    except Exception as e:
        log.warning(f"  Erreur: {e}")

    return index


def parse_condition_age(conditions_texte):
    """Extrait l'âge depuis le texte des conditions."""
    if not conditions_texte:
        return None

    t = conditions_texte.lower()

    # "3 ans"
    m = re.search(r'(\d)\s*ans?\s*(et\s*(plus|au))?', t)
    if m:
        age = int(m.group(1))
        if m.group(2):
            return f"{age} ans et plus"
        return f"{age} ans"

    # "de 3 à 5 ans"
    m = re.search(r'de\s*(\d)\s*[àa]\s*(\d)\s*ans', t)
    if m:
        return f"{m.group(1)} à {m.group(2)} ans"

    return None


def count_partants_per_course(source_path):
    """Compte le nombre de partants par course."""
    counts = Counter()
    log.info("  Comptage partants par course...")

    if source_path.endswith(".jsonl"):
        with open(source_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    cuid = r.get("course_uid", "")
                    if cuid and r.get("statut") != "non_partant":
                        counts[cuid] += 1
                except json.JSONDecodeError:
                    continue
    else:
        with open(source_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for r in data:
            cuid = r.get("course_uid", "")
            if cuid and r.get("statut") != "non_partant":
                counts[cuid] += 1
        del data

    log.info(f"  → {len(counts)} courses comptées")
    return counts


# ================================================================
# COMBLAGE
# ================================================================

def combler_record(record, meteo_index, hippo_db, sire_index, partants_count):
    """Comble les trous d'un record. Retourne (record, nb_champs_combles)."""
    combles = 0

    date_iso = (record.get("date_reunion_iso") or "")[:10]
    hippo = (record.get("hippodrome_normalise") or "").lower().strip()
    nom = (record.get("nom_cheval") or "").upper().strip()
    cuid = record.get("course_uid", "")

    # === Pénétromètre ===
    if not record.get("penetrometre") or record.get("penetrometre") in (None, "", "inconnu"):
        key = f"{date_iso}|{hippo}"
        meteo = meteo_index.get(key, {})
        if meteo.get("penetrometre"):
            record["penetrometre"] = meteo["penetrometre"]
            record["_comble_penetrometre"] = True
            combles += 1

    # === Type piste ===
    if not record.get("type_piste") or record.get("type_piste") in (None, "", "inconnu"):
        # D'abord depuis les réunions enrichies
        key = f"{date_iso}|{hippo}"
        meteo = meteo_index.get(key, {})
        if meteo.get("type_piste"):
            record["type_piste"] = meteo["type_piste"]
            combles += 1
        elif hippo in hippo_db and hippo_db[hippo].get("type_piste"):
            record["type_piste"] = hippo_db[hippo]["type_piste"]
            combles += 1

    # === Corde ===
    if not record.get("corde") or record.get("corde") in (None, ""):
        if hippo in hippo_db and hippo_db[hippo].get("corde"):
            record["corde"] = hippo_db[hippo]["corde"]
            combles += 1

    # === Condition age ===
    if not record.get("condition_age") or record.get("condition_age") in (None, ""):
        cond_texte = record.get("conditions_texte", "")
        age = parse_condition_age(cond_texte)
        if age:
            record["condition_age"] = age
            record["_comble_condition_age"] = True
            combles += 1

    # === Pays cheval ===
    if not record.get("pays_cheval") or record.get("pays_cheval") in (None, ""):
        sire = sire_index.get(nom, {})
        if sire.get("pays"):
            record["pays_cheval"] = sire["pays"]
            combles += 1

    # === Éleveur ===
    if not record.get("eleveur") or record.get("eleveur") in (None, ""):
        sire = sire_index.get(nom, {})
        if sire.get("eleveur"):
            record["eleveur"] = sire["eleveur"]
            combles += 1

    # === Sexe ===
    if not record.get("sexe") or record.get("sexe") in (None, ""):
        sire = sire_index.get(nom, {})
        if sire.get("sexe"):
            record["sexe"] = sire["sexe"]
            combles += 1

    # === Nombre partants ===
    if not record.get("nombre_partants") or record.get("nombre_partants") in (None, 0):
        if cuid in partants_count:
            record["nombre_partants"] = partants_count[cuid]
            combles += 1

    # === Comblage par inférence (4.2) ===

    # Terrain probable si manquant (depuis météo + hippo)
    terrain = record.get("type_piste")
    if not terrain or terrain in (None, "", "inconnu"):
        # Inférer depuis discipline
        disc = (record.get("discipline") or "").lower()
        if disc in ("trot_attele", "trot_monte"):
            record["type_piste"] = "cendrée"
            combles += 1
        elif disc in ("steeple", "haies", "cross_country"):
            record["type_piste"] = "gazon"
            combles += 1
        elif disc == "plat":
            # PSF si hippo connu
            hippos_psf = {"pau", "deauville", "chantilly", "lyon-parilly", "pornichet",
                          "marseille-borely", "salon-de-provence", "agen"}
            if hippo in hippos_psf:
                record["type_piste"] = "psf"
            else:
                record["type_piste"] = "gazon"
            combles += 1

    # Distance réelle si manquante (inférer depuis type course + hippo)
    dist = record.get("distance")
    if not dist or dist in (None, 0):
        disc = (record.get("discipline") or "").lower()
        if disc in ("trot_attele", "trot_monte"):
            record["distance"] = 2700  # distance standard trot
            combles += 1
        elif disc == "plat":
            record["distance"] = 1600  # distance standard plat
            combles += 1

    # Poids porté si manquant (handicap officiel + surcharge)
    poids = record.get("poids_porte_kg")
    if poids is None:
        poids_base = record.get("poids_base_kg")
        surcharge = record.get("surcharge_decharge_kg")
        if poids_base is not None:
            if surcharge is not None:
                record["poids_porte_kg"] = round(poids_base + surcharge, 1)
            else:
                record["poids_porte_kg"] = poids_base
            combles += 1

    # Temps course si manquant (inférer depuis réduction km + distance)
    temps = record.get("temps_ms")
    if temps is None:
        red_km = record.get("reduction_km_ms")
        dist_val = record.get("distance")
        if red_km is not None and dist_val is not None:
            try:
                # temps_ms = red_km_ms * distance_km
                record["temps_ms"] = int(float(red_km) * float(dist_val) / 1000)
                combles += 1
            except (ValueError, TypeError, ZeroDivisionError):
                pass

    return record, combles


# ================================================================
# MAIN
# ================================================================

def main():
    log.info("=" * 70)
    log.info("COMBLAGE DE TROUS — Étape 4")
    log.info("=" * 70)

    # Source
    source_path = None
    for path in [os.path.join(BASE_DIR, "../../output", "nettoyage", "partants_nettoyes.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "02_liste_courses", "partants_normalises.jsonl"),
                 os.path.join(BASE_DIR, "../../output", "02_liste_courses", "partants_normalises.json")]:
        if os.path.exists(path):
            source_path = path
            break

    if not source_path:
        log.error("Aucun fichier partants trouvé")
        sys.exit(1)

    log.info(f"Source: {source_path}")

    # Charger les sources de comblage
    log.info("Chargement des sources de comblage...")
    meteo_index = load_reunions_meteo()
    hippo_db = load_hippodromes_db()
    sire_index = load_sire_ifce()
    partants_count = count_partants_per_course(source_path)

    # Comblage
    output_file = os.path.join(OUTPUT_DIR, "partants_combles.jsonl")
    total = 0
    total_combles = 0
    champs_combles = Counter()

    log.info("Comblage en cours...")

    with open(output_file, "w", encoding="utf-8") as fout:
        if source_path.endswith(".jsonl"):
            with open(source_path, "r", encoding="utf-8") as fin:
                for line in fin:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    total += 1
                    record, nb = combler_record(record, meteo_index, hippo_db, sire_index, partants_count)

                    if nb > 0:
                        total_combles += 1
                        # Compter quels champs ont été comblés
                        for k in list(record.keys()):
                            if k.startswith("_comble_"):
                                champs_combles[k.replace("_comble_", "")] += 1
                                del record[k]

                    fout.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

                    if total % 200000 == 0:
                        log.info(f"  {total} traités, {total_combles} comblés")
        else:
            with open(source_path, "r", encoding="utf-8") as fin:
                data = json.load(fin)
            for record in data:
                total += 1
                record, nb = combler_record(record, meteo_index, hippo_db, sire_index, partants_count)
                if nb > 0:
                    total_combles += 1
                fout.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                if total % 200000 == 0:
                    log.info(f"  {total} traités, {total_combles} comblés")
            del data

    log.info(f"Comblage terminé:")
    log.info(f"  Total: {total}")
    log.info(f"  Comblés: {total_combles} ({100*total_combles/max(total,1):.1f}%)")
    log.info(f"  Champs comblés: {dict(champs_combles)}")
    log.info(f"  Output: {output_file}")

    # Rapport
    rapport = {
        "total": total,
        "total_combles": total_combles,
        "champs_combles": dict(champs_combles),
        "sources_utilisees": {
            "meteo_reunions": len(meteo_index),
            "hippodromes_db": len(hippo_db),
            "sire_ifce": len(sire_index),
            "partants_count": len(partants_count),
        },
    }
    with open(os.path.join(OUTPUT_DIR, "comblage_rapport.json"), "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    log.info("=" * 70)
    log.info("TERMINÉ")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
