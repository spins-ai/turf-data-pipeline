#!/usr/bin/env python3
"""
Merge Pedigree Master v2 — Fusionne TOUTES les sources pedigree
Sources :
  ★ 02_partants_enrichis (8.1 GB, 2.7M partants — père/mère/race/robe/sexe/sire_*)
  + 08_pedigree (PMU)
  + 14_pedigree (scraper)
  + 17_sire_ifce (SIRE officiel)
  + 36_pedigree_query
  + 02b_liste_courses_2013
  + 02b_scraper_letrot
  + 24_canalturf
Output : data_master/pedigree_master.json + .parquet + .csv

Clé de jointure : nom_cheval normalisé
Priorité : SIRE (officiel) > partants_enrichis > PMU 08 > scraper 14 > canalturf > pedigree_query
"""

import json
import os
import re
import logging
import time
from collections import defaultdict

os.makedirs("data_master", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler("logs/merge_pedigree.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def normalize_name(name):
    """Normaliser un nom de cheval pour matching"""
    if not name:
        return ""
    n = name.upper().strip()
    n = re.sub(r'[^A-Z\s]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def load_source_02_partants():
    """★ SOURCE PRINCIPALE : partants_enrichis.json (8.1 GB, 2.7M records)
    Contient père, mère, race, robe, sexe, eleveur, sire_date_naissance, sire_pays_naissance..."""
    log.info("Chargement source 02_partants_enrichis (8.1 GB — streaming)...")
    records = {}

    import ijson

    pedigree_fields = ['pere', 'mere', 'pere_mere', 'eleveur', 'race', 'robe', 'sexe',
                       'sire_date_naissance', 'sire_annee_naissance', 'sire_pays_naissance',
                       'sire_vivant', 'sire_consommation', 'jument_pleine', 'age',
                       'pays_cheval', 'date_naissance']

    filepath = "output/02_liste_courses/partants_enrichis.json"
    if not os.path.exists(filepath):
        log.warning("  partants_enrichis.json non trouvé")
        return records

    count = 0
    try:
        with open(filepath, 'rb') as f:
            for item in ijson.items(f, 'item'):
                name = normalize_name(item.get("nom_cheval", item.get("nom", "")))
                if not name:
                    continue

                rec = {"nom": name, "source_02_partants": True}
                for field in pedigree_fields:
                    val = item.get(field)
                    if val and str(val) not in ("None", "", "0"):
                        rec[field] = val

                # Garder le record le plus complet si doublon
                if name in records:
                    old = records[name]
                    old_filled = sum(1 for v in old.values() if v and str(v) not in ("None", ""))
                    new_filled = sum(1 for v in rec.values() if v and str(v) not in ("None", ""))
                    if new_filled > old_filled:
                        records[name] = rec
                else:
                    records[name] = rec

                count += 1
                if count % 200000 == 0:
                    log.info(f"  02_partants [{count}] → {len(records)} chevaux uniques")

    except ImportError:
        log.warning("  ijson non installé — fallback lecture directe (lent)...")
        # Fallback sans ijson : lire par chunks
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            name = normalize_name(item.get("nom_cheval", item.get("nom", "")))
            if not name:
                continue
            rec = {"nom": name, "source_02_partants": True}
            for field in pedigree_fields:
                val = item.get(field)
                if val and str(val) not in ("None", "", "0"):
                    rec[field] = val
            if name not in records:
                records[name] = rec
            count += 1
            if count % 200000 == 0:
                log.info(f"  02_partants [{count}] → {len(records)} chevaux uniques")
    except Exception as e:
        log.error(f"  Erreur lecture partants_enrichis: {e}")

    log.info(f"  02_partants: {len(records)} chevaux ({count} partants traités)")
    return records


def load_source_02b():
    """02b_liste_courses_2013 et 02b_scraper_letrot"""
    log.info("Chargement sources 02b...")
    records = {}

    pedigree_fields = ['pere', 'mere', 'pere_mere', 'eleveur', 'race', 'robe', 'sexe', 'jument_pleine']

    for src_dir in ['output/02b_liste_courses_2013', 'output/02b_scraper_letrot']:
        if not os.path.exists(src_dir):
            continue
        for fname in os.listdir(src_dir):
            if not fname.endswith('.json') or fname.startswith('.'):
                continue
            fpath = os.path.join(src_dir, fname)
            fsize = os.path.getsize(fpath) / 1024 / 1024
            if fsize > 2000:  # Skip fichiers trop gros
                continue
            try:
                with open(fpath, encoding="utf-8") as fh:
                    data = json.load(fh)
                items = data if isinstance(data, list) else []
                for item in items:
                    # Peut être une course avec partants ou un partant direct
                    partants = item.get('partants', [item]) if isinstance(item, dict) else []
                    for p in partants:
                        if not isinstance(p, dict):
                            continue
                        name = normalize_name(p.get("nom_cheval", p.get("nom", "")))
                        if not name:
                            continue
                        rec = {"nom": name, "source_02b": True}
                        for field in pedigree_fields:
                            val = p.get(field)
                            if val and str(val) not in ("None", "", "0"):
                                rec[field] = val
                        if name not in records:
                            records[name] = rec
            except Exception:
                pass

    log.info(f"  02b sources: {len(records)} chevaux")
    return records


def load_source_24_canalturf():
    """CanalTurf : père, mère, robe"""
    log.info("Chargement source 24_canalturf...")
    records = {}
    path = "output/24_canalturf/canalturf_chevaux.json"
    if not os.path.exists(path):
        return records
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            name = normalize_name(item.get("nom_cheval", item.get("id_canalturf", "")))
            if not name:
                continue
            rec = {"nom": name, "source_canalturf": True}
            if item.get("père_"): rec["pere"] = item["père_"]
            if item.get("mère_"): rec["mere"] = item["mère_"]
            if item.get("robe_"): rec["robe"] = item["robe_"]
            if item.get("sexe/age_"): rec["sexe_age"] = item["sexe/age_"]
            records[name] = rec
    except Exception as e:
        log.warning(f"  Erreur canalturf: {e}")
    log.info(f"  24_canalturf: {len(records)} chevaux")
    return records


def load_source_08():
    """PMU pedigree : père, mère, père-mère"""
    log.info("Chargement source 08_pedigree...")
    records = {}
    path = "output/08_pedigree"
    for f in os.listdir(path):
        if f.endswith('.json') and not f.startswith('.'):
            try:
                with open(os.path.join(path, f)) as fh:
                    data = json.load(fh)
                items = data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []
                for item in items:
                    name = normalize_name(item.get("nom", item.get("nom_cheval", "")))
                    if not name:
                        continue
                    records[name] = {
                        "nom": name,
                        "pere": item.get("nomPere", item.get("pere", "")),
                        "mere": item.get("nomMere", item.get("mere", "")),
                        "pere_mere": item.get("nomPereMere", item.get("pere_mere", "")),
                        "source_08": True,
                    }
            except Exception as e:
                log.warning(f"  Erreur {f}: {e}")
    log.info(f"  08_pedigree: {len(records)} chevaux")
    return records


def load_source_14():
    """Scraper pedigree détaillé"""
    log.info("Chargement source 14_pedigree...")
    records = {}
    path = "output/14_pedigree"
    for f in os.listdir(path):
        if f.endswith('.json') and not f.startswith('.'):
            try:
                with open(os.path.join(path, f)) as fh:
                    data = json.load(fh)
                items = data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []
                for item in items:
                    name = normalize_name(item.get("nom", item.get("nom_cheval", item.get("name", ""))))
                    if not name:
                        continue
                    rec = {"nom": name, "source_14": True}
                    # Copier tous les champs pedigree disponibles
                    for k, v in item.items():
                        if v and k not in ("nom", "nom_cheval", "name"):
                            rec[k] = v
                    records[name] = rec
            except Exception as e:
                log.warning(f"  Erreur {f}: {e}")
    log.info(f"  14_pedigree: {len(records)} chevaux")
    return records


def load_source_17():
    """SIRE/IFCE — source officielle, prioritaire"""
    log.info("Chargement source 17_sire_ifce...")
    records = {}
    path = "output/17_sire_ifce"

    files_to_load = []
    for f in os.listdir(path):
        if f.endswith('.json') and not f.startswith('.'):
            files_to_load.append(f)

    log.info(f"  {len(files_to_load)} fichiers à charger")

    for idx, f in enumerate(files_to_load):
        try:
            with open(os.path.join(path, f)) as fh:
                data = json.load(fh)

            items = data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []

            for item in items:
                if not isinstance(item, dict):
                    continue
                name = normalize_name(item.get("nom", item.get("nom_cheval", item.get("name", ""))))
                if not name:
                    continue

                rec = {
                    "nom": name,
                    "source_sire": True,
                    "sire_id": item.get("sire_id", item.get("id", "")),
                }

                # Extraire les champs pedigree
                field_mapping = {
                    "pere": ["pere", "nomPere", "sire", "father"],
                    "mere": ["mere", "nomMere", "dam", "mother"],
                    "pere_mere": ["pere_mere", "nomPereMere", "sire_of_dam", "broodmare_sire"],
                    "sexe": ["sexe", "sex"],
                    "race": ["race", "breed"],
                    "robe": ["robe", "color", "colour"],
                    "date_naissance": ["date_naissance", "birth_date", "naissance"],
                    "pays_naissance": ["pays_naissance", "country", "pays"],
                    "eleveur": ["eleveur", "breeder"],
                    "naisseur": ["naisseur"],
                }

                for target_key, source_keys in field_mapping.items():
                    for sk in source_keys:
                        val = item.get(sk)
                        if val:
                            rec[target_key] = val
                            break

                # Copier aussi les champs non-mappés intéressants
                for k, v in item.items():
                    if v and k not in ("nom", "nom_cheval", "name") and k not in rec:
                        rec[k] = v

                records[name] = rec

        except Exception as e:
            log.warning(f"  Erreur {f}: {e}")

        if (idx + 1) % 10 == 0:
            log.info(f"  17_sire [{idx+1}/{len(files_to_load)}] → {len(records)} chevaux")

    log.info(f"  17_sire_ifce: {len(records)} chevaux")
    return records


def load_source_36():
    """Pedigree Query (8593 records)"""
    log.info("Chargement source 36_pedigree_query...")
    records = {}
    cache_dir = "output/36_pedigree_query/cache"
    if not os.path.exists(cache_dir):
        return records

    for f in os.listdir(cache_dir):
        if f.endswith('.json'):
            try:
                with open(os.path.join(cache_dir, f)) as fh:
                    item = json.load(fh)
                name = normalize_name(item.get("name", ""))
                if not name:
                    continue
                rec = {"nom": name, "source_36": True}
                for k, v in item.items():
                    if v and k != "name":
                        rec[k] = v
                records[name] = rec
            except:
                pass

    log.info(f"  36_pedigree_query: {len(records)} chevaux")
    return records


def merge_records(sire, partants, pmu, scraper, pquery, src_02b, canalturf):
    """Fusionner avec priorité : SIRE > partants_enrichis > PMU 08 > scraper 14 > 02b > canalturf > pquery"""
    log.info("=" * 60)
    log.info("FUSION DES SOURCES PEDIGREE")
    log.info("=" * 60)

    all_sources = [
        ("pedigree_query", pquery),
        ("canalturf", canalturf),
        ("02b", src_02b),
        ("scraper_14", scraper),
        ("pmu_08", pmu),
        ("partants_enrichis", partants),
        ("sire_ifce", sire),  # Plus prioritaire = dernier (écrase les autres)
    ]

    all_names = set()
    for _, src in all_sources:
        all_names.update(src.keys())

    log.info(f"  Chevaux uniques (tous noms): {len(all_names)}")

    master = {}

    for name in all_names:
        record = {"nom": name}

        sources_found = []
        for src_name, src_data in all_sources:
            if name in src_data:
                sources_found.append(src_name)
                for k, v in src_data[name].items():
                    if v and str(v) not in ("None", "", "0", "False"):
                        record[k] = v

        record["_sources"] = sources_found
        record["_nb_sources"] = len(sources_found)
        master[name] = record

    # Stats détaillées
    total = len(master)
    fields_stats = {}
    for field in ['pere', 'mere', 'pere_mere', 'eleveur', 'race', 'robe', 'sexe',
                  'sire_date_naissance', 'sire_pays_naissance', 'age', 'pays_cheval']:
        count = sum(1 for r in master.values() if r.get(field))
        fields_stats[field] = count
        pct = count * 100 / total if total > 0 else 0
        log.info(f"  {field}: {count} ({pct:.1f}%)")

    multi_source = sum(1 for r in master.values() if r.get("_nb_sources", 0) >= 2)
    log.info(f"  Total master: {total} chevaux")
    log.info(f"  Multi-source (2+): {multi_source}")

    return master


def main():
    start = time.time()

    log.info("=" * 60)
    log.info("MERGE PEDIGREE MASTER")
    log.info("=" * 60)

    # Charger TOUTES les sources
    src_02 = load_source_02_partants()  # ★ 2.7M partants — la mine d'or
    src_02b = load_source_02b()
    src_17 = load_source_17()
    src_08 = load_source_08()
    src_14 = load_source_14()
    src_36 = load_source_36()
    src_ct = load_source_24_canalturf()

    # Fusionner (priorité : sire > partants > pmu > scraper > 02b > canalturf > pquery)
    master = merge_records(src_17, src_02, src_08, src_14, src_36, src_02b, src_ct)

    # Sauvegarder en JSON
    log.info("Sauvegarde pedigree_master.json...")
    master_list = list(master.values())
    output = "data_master/pedigree_master.json"
    with open(output, "w") as f:
        json.dump(master_list, f, ensure_ascii=False, indent=None)
    size_mb = os.path.getsize(output) / 1024 / 1024
    log.info(f"  → {output}: {size_mb:.1f} MB, {len(master_list)} records")

    # Sauvegarder en CSV
    log.info("Sauvegarde pedigree_master.csv...")
    try:
        import csv
        all_keys = set()
        for r in master_list[:10000]:
            all_keys.update(r.keys())
        all_keys = sorted(all_keys)

        with open("data_master/pedigree_master.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            for r in master_list:
                # Convertir listes en strings
                row = {}
                for k, v in r.items():
                    if isinstance(v, (list, dict)):
                        row[k] = json.dumps(v, ensure_ascii=False)
                    else:
                        row[k] = v
                writer.writerow(row)
        csv_size = os.path.getsize("data_master/pedigree_master.csv") / 1024 / 1024
        log.info(f"  → pedigree_master.csv: {csv_size:.1f} MB")
    except Exception as e:
        log.warning(f"  CSV erreur: {e}")

    # Sauvegarder en Parquet
    log.info("Sauvegarde pedigree_master.parquet...")
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd

        df = pd.DataFrame(master_list)
        # Convertir colonnes problématiques en string
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)

        pq.write_table(pa.Table.from_pandas(df), "data_master/pedigree_master.parquet", compression="zstd")
        pq_size = os.path.getsize("data_master/pedigree_master.parquet") / 1024 / 1024
        log.info(f"  → pedigree_master.parquet: {pq_size:.1f} MB")
    except ImportError:
        log.warning("  pyarrow non installé — Parquet skippé (pip install pyarrow)")
    except Exception as e:
        log.warning(f"  Parquet erreur: {e}")

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(f"TERMINÉ en {elapsed:.0f}s")
    log.info(f"  {len(master_list)} chevaux dans pedigree_master")
    log.info(f"  Formats: JSON + CSV + Parquet")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
