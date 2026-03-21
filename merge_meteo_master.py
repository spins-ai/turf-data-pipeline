#!/usr/bin/env python3
"""
Merge Météo Master — Fusionne TOUTES les sources météo
Sources :
  + 13_meteo_historique (71 MB — météo par course depuis Open-Meteo)
  + 35_meteo_france (11 MB — Météo France stations)
  + 39_reunions_enrichies (2 GB — météo/incidents/paris par réunion)
  + meteo_complete/ (155 MB — merge intermédiaire existant)
  + 02_liste_courses (penetrometre, type_piste par course)
Output : data_master/meteo_master.json + .parquet + .csv

⚠️ NE SUPPRIME RIEN — lecture seule sur les sources, création dans data_master/
"""

import json, os, re, logging, time

os.makedirs("data_master", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.FileHandler("logs/merge_meteo.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)


def load_json_safe(path, label=""):
    """Charge un fichier JSON en toute sécurité"""
    if not os.path.exists(path):
        log.warning(f"  {label}: fichier non trouvé ({path})")
        return []
    try:
        size_mb = os.path.getsize(path) / 1024 / 1024
        if size_mb > 4000:
            log.warning(f"  {label}: fichier trop gros ({size_mb:.0f} MB) — skip")
            return []
        log.info(f"  {label}: chargement {size_mb:.0f} MB...")
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        items = data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []
        log.info(f"  {label}: {len(items)} records")
        return items
    except Exception as e:
        log.error(f"  {label}: erreur {e}")
        return []


def make_course_key(record):
    """Créer une clé unique par course depuis différents formats"""
    uid = record.get("course_uid", "")
    if uid:
        return uid
    date = record.get("date_reunion_iso", record.get("date", ""))
    hippo = record.get("hippodrome_normalise", record.get("hippodrome", ""))
    nr = record.get("numero_reunion", record.get("numReunion", ""))
    nc = record.get("numero_course", record.get("numCourse", ""))
    if date and hippo:
        return f"{date}|{str(hippo).lower()}|R{nr}|C{nc}"
    return ""


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("MERGE MÉTÉO MASTER")
    log.info("=" * 60)

    master = {}  # clé = course_uid ou date|hippo|R|C

    # 1. Météo historique (Open-Meteo)
    for fname in os.listdir(os.path.join(BASE_DIR, "output", "13_meteo_historique")):
        if not fname.endswith('.json') or fname.startswith('.'):
            continue
        items = load_json_safe(fos.path.join(BASE_DIR, "output", "13_meteo_historique", "{fname}"), f"13/{fname}")
        for item in items:
            key = make_course_key(item)
            if not key:
                continue
            if key not in master:
                master[key] = {"course_key": key, "_sources": []}
            for k, v in item.items():
                if v and str(v) not in ("None", ""):
                    master[key][k] = v
            if "meteo_historique" not in master[key]["_sources"]:
                master[key]["_sources"].append("meteo_historique")

    log.info(f"  Après 13_meteo: {len(master)} courses")

    # 2. Météo France stations
    for fname in os.listdir(os.path.join(BASE_DIR, "output", "35_meteo_france")) if os.path.exists(os.path.join(BASE_DIR, "output", "35_meteo_france")) else []:
        if not fname.endswith('.json') or fname.startswith('.'):
            continue
        items = load_json_safe(fos.path.join(BASE_DIR, "output", "35_meteo_france", "{fname}"), f"35/{fname}")
        for item in items:
            key = make_course_key(item)
            if not key:
                continue
            if key not in master:
                master[key] = {"course_key": key, "_sources": []}
            for k, v in item.items():
                if v and str(v) not in ("None", ""):
                    master[key][k] = v
            if "meteo_france" not in master[key]["_sources"]:
                master[key]["_sources"].append("meteo_france")

    log.info(f"  Après 35_meteo_france: {len(master)} courses")

    # 3. Réunions enrichies (météo + incidents + paris)
    for fname in os.listdir(os.path.join(BASE_DIR, "output", "39_reunions_enrichies")) if os.path.exists(os.path.join(BASE_DIR, "output", "39_reunions_enrichies")) else []:
        if not fname.endswith('.json') or fname.startswith('.'):
            continue
        fpath = fos.path.join(BASE_DIR, "output", "39_reunions_enrichies", "{fname}")
        fsize = os.path.getsize(fpath) / 1024 / 1024
        if fsize > 3000:
            continue
        items = load_json_safe(fpath, f"39/{fname}")
        for item in items:
            key = make_course_key(item)
            if not key:
                continue
            if key not in master:
                master[key] = {"course_key": key, "_sources": []}
            for k, v in item.items():
                if v and str(v) not in ("None", ""):
                    master[key][k] = v
            if "reunions_enrichies" not in master[key]["_sources"]:
                master[key]["_sources"].append("reunions_enrichies")

    log.info(f"  Après 39_reunions: {len(master)} courses")

    # 4. Merge intermédiaire existant
    if os.path.exists(os.path.join(BASE_DIR, "output", "meteo_complete")):
        for fname in os.listdir(os.path.join(BASE_DIR, "output", "meteo_complete")):
            if not fname.endswith('.json') or fname.startswith('.'):
                continue
            items = load_json_safe(fos.path.join(BASE_DIR, "output", "meteo_complete", "{fname}"), f"meteo_complete/{fname}")
            for item in items:
                key = make_course_key(item)
                if not key:
                    continue
                if key not in master:
                    master[key] = {"course_key": key, "_sources": []}
                for k, v in item.items():
                    if v and str(v) not in ("None", ""):
                        master[key][k] = v
                if "meteo_complete" not in master[key]["_sources"]:
                    master[key]["_sources"].append("meteo_complete")

    log.info(f"  Après meteo_complete: {len(master)} courses")

    # 5. Pénétromètre et type_piste depuis courses_normalisees
    log.info("  Extraction penetrometre/type_piste depuis courses_normalisees...")
    courses_path = os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.json")
    if os.path.exists(courses_path):
        items = load_json_safe(courses_path, "02_courses")
        for item in items:
            key = make_course_key(item)
            if not key:
                continue
            penetro = item.get("penetrometre")
            type_piste = item.get("type_piste")
            if penetro or type_piste:
                if key not in master:
                    master[key] = {"course_key": key, "_sources": []}
                if penetro and str(penetro) != "None":
                    master[key]["penetrometre"] = penetro
                if type_piste and str(type_piste) != "None":
                    master[key]["type_piste"] = type_piste
                if "courses_pmu" not in master[key]["_sources"]:
                    master[key]["_sources"].append("courses_pmu")

    log.info(f"  Après courses penetro: {len(master)} courses")

    # Stats
    total = len(master)
    meteo_fields = ['temperature', 'temperature_2m', 'humidity', 'humidite', 'vent', 'wind_speed',
                    'precipitation', 'pression', 'penetrometre', 'type_piste', 'nebulosite',
                    'meteo_code', 'weather_code']
    log.info("=" * 60)
    log.info(f"  TOTAL: {total} courses avec données météo")
    for field in meteo_fields:
        count = sum(1 for r in master.values() if r.get(field))
        if count > 0:
            log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    nb_sources = {}
    for r in master.values():
        n = len(r.get("_sources", []))
        nb_sources[n] = nb_sources.get(n, 0) + 1
    for n in sorted(nb_sources):
        log.info(f"  {n} source(s): {nb_sources[n]}")

    # Sauvegarder
    master_list = list(master.values())
    for r in master_list:
        r["_nb_sources"] = len(r.get("_sources", []))

    log.info("Sauvegarde meteo_master.json...")
    out = os.path.join(BASE_DIR, "data_master", "meteo_master.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(master_list, f, ensure_ascii=False)
    log.info(f"  → {os.path.getsize(out)/1024/1024:.1f} MB")

    log.info("Sauvegarde meteo_master.csv...")
    try:
        import csv
        all_keys = set()
        for r in master_list[:5000]:
            all_keys.update(r.keys())
        all_keys = sorted(all_keys)
        with open(os.path.join(BASE_DIR, "data_master", "meteo_master.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            w.writeheader()
            for r in master_list:
                row = {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v for k, v in r.items()}
                w.writerow(row)
        log.info(f"  → {os.path.getsize('data_master/meteo_master.csv')/1024/1024:.1f} MB")
    except Exception as e:
        log.warning(f"  CSV erreur: {e}")

    log.info("Sauvegarde meteo_master.parquet...")
    try:
        import pyarrow as pa, pyarrow.parquet as pq, pandas as pd
        df = pd.DataFrame(master_list)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
        pq.write_table(pa.Table.from_pandas(df), os.path.join(BASE_DIR, "data_master", "meteo_master.parquet"), compression="zstd")
        log.info(f"  → {os.path.getsize('data_master/meteo_master.parquet')/1024/1024:.1f} MB")
    except Exception as e:
        log.warning(f"  Parquet: {e}")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s — {total} courses météo")

if __name__ == "__main__":
    main()
