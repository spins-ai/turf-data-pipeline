#!/usr/bin/env python3
"""
Merge Rapports Master — Fusionne TOUS les rapports/résultats
Sources :
  + 04_resultats (482 MB — rapports PMU, EN COURS mais données partielles dispo)
  + 21_rapports_definitifs (1.4 GB)
  + 38_rapports_internet (1.6 GB)
  + rapports_merged/ (242 MB — merge intermédiaire existant)
Output : data_master/rapports_master.json + .parquet + .csv

⚠️ NE SUPPRIME RIEN — lecture seule sur les sources
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json, os, sys, time, ijson

os.makedirs("../../data_master", exist_ok=True)
os.makedirs("../../logs", exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("merge_rapports_master")


def make_course_key(record):
    uid = record.get("course_uid", "")
    if uid:
        return uid
    date = record.get("date_reunion_iso", record.get("date", ""))
    nr = record.get("numero_reunion", record.get("numReunion", ""))
    nc = record.get("numero_course", record.get("numCourse", ""))
    if date and nr and nc:
        return f"{date}|R{nr}|C{nc}"
    return ""


def stream_json(filepath, label):
    """Stream un gros fichier JSON array"""
    if not os.path.exists(filepath):
        log.warning(f"  {label}: non trouvé")
        return
    size_mb = os.path.getsize(filepath) / 1024 / 1024
    log.info(f"  {label}: streaming {size_mb:.0f} MB...")
    count = 0
    try:
        with open(filepath, 'rb') as f:
            for item in ijson.items(f, 'item'):
                yield item
                count += 1
                if count % 200000 == 0:
                    log.info(f"  {label}: {count} records lus...")
    except Exception as e:
        log.error(f"  {label}: erreur streaming {e}")
    log.info(f"  {label}: {count} records total")


def load_json_dir(dirpath, label):
    """Charge tous les JSON d'un dossier"""
    if not os.path.exists(dirpath):
        return []
    all_items = []
    for fname in sorted(os.listdir(dirpath)):
        if not fname.endswith('.json') or fname.startswith('.'):
            continue
        fpath = os.path.join(dirpath, fname)
        fsize = os.path.getsize(fpath) / 1024 / 1024
        if fsize > 5000:
            log.info(f"  {label}/{fname}: {fsize:.0f} MB — streaming")
            for item in stream_json(fpath, f"{label}/{fname}"):
                all_items.append(item)
        else:
            try:
                with open(fpath, encoding="utf-8") as f:
                    data = json.load(f)
                items = data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []
                all_items.extend(items)
                log.info(f"  {label}/{fname}: {len(items)} records ({fsize:.0f} MB)")
            except Exception as e:
                log.warning(f"  {label}/{fname}: erreur {e}")
    return all_items


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("MERGE RAPPORTS MASTER")
    log.info("=" * 60)

    master = {}

    # Sources par ordre de priorité (moins prioritaire en premier)
    sources = [
        ("rapports_merged", os.path.join(BASE_DIR, "../../output", "rapports_merged")),
        ("38_rapports_internet", os.path.join(BASE_DIR, "../../output", "38_rapports_internet")),
        ("21_rapports", os.path.join(BASE_DIR, "../../output", "21_rapports_definitifs")),
        ("04_resultats", os.path.join(BASE_DIR, "../../output", "04_resultats")),
    ]

    for src_name, src_path in sources:
        items = load_json_dir(src_path, src_name)
        added = 0
        for item in items:
            key = make_course_key(item)
            if not key:
                continue
            if key not in master:
                master[key] = {"course_key": key, "_sources": []}
            for k, v in item.items():
                if v and str(v) not in ("None", "", "[]", "{}"):
                    master[key][k] = v
            if src_name not in master[key]["_sources"]:
                master[key]["_sources"].append(src_name)
                added += 1
        log.info(f"  → Après {src_name}: {len(master)} courses ({added} nouvelles)")

    # Stats
    total = len(master)
    for field in ['ordre_arrivee', 'rapportsDefinitifs', 'rapports', 'dividendes',
                  'miseBase', 'combinaison', 'type_rapport']:
        count = sum(1 for r in master.values() if r.get(field))
        if count > 0:
            log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Sauvegarder
    master_list = list(master.values())
    for r in master_list:
        r["_nb_sources"] = len(r.get("_sources", []))

    log.info("Sauvegarde rapports_master.json...")
    out = os.path.join(BASE_DIR, "../../data_master", "rapports_master.json")
    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(master_list, f, ensure_ascii=False)
    os.replace(tmp, out)
    log.info(f"  → {os.path.getsize(out)/1024/1024:.1f} MB, {len(master_list)} records")

    log.info("Sauvegarde rapports_master.parquet...")
    try:
        import pyarrow as pa, pyarrow.parquet as pq, pandas as pd
        df = pd.DataFrame(master_list)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
        pq.write_table(pa.Table.from_pandas(df), os.path.join(BASE_DIR, "../../data_master", "rapports_master.parquet"), compression="zstd")
        log.info(f"  → {os.path.getsize('../../data_master/rapports_master.parquet')/1024/1024:.1f} MB")
    except Exception as e:
        log.warning(f"  Parquet: {e}")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s — {total} courses avec rapports")

if __name__ == "__main__":
    main()
