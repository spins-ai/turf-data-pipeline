#!/usr/bin/env python3
"""
Merge Équipements Master — Fusionne équipements + poids/handicaps
Sources :
  + 09_equipements (319 MB — œillères, déferré, changements)
  + 10_poids_handicaps (141 MB — poids porté, handicaps, écarts)
Output : data_master/equipements_master.json + .parquet

⚠️ NE SUPPRIME RIEN — lecture seule sur les sources
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json, os, sys, time

os.makedirs("../../data_master", exist_ok=True)
os.makedirs("../../logs", exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.loaders import load_json_safe

log = setup_logging("merge_equipements_master")


def make_key(record):
    uid = record.get("partant_uid", "")
    if uid:
        return uid
    course_uid = record.get("course_uid", "")
    num = record.get("num_pmu", record.get("numPmu", ""))
    nom = record.get("nom_cheval", "")
    date = record.get("date_reunion_iso", record.get("date", ""))
    if course_uid and num:
        return f"{course_uid}|P{num}"
    if course_uid and nom:
        return f"{course_uid}|{nom.upper().strip()}"
    if date and nom:
        return f"{date}|{nom.upper().strip()}"
    return ""


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("MERGE ÉQUIPEMENTS MASTER")
    log.info("=" * 60)

    master = {}

    # 09_equipements (319 MB — œillères, déferré, changements)
    items_09 = load_json_safe(os.path.join(BASE_DIR, "../../output", "09_equipements", "equipements_historique.json"), "09_equipements", log)
    for item in items_09:
        key = make_key(item)
        if not key:
            continue
        if key not in master:
            master[key] = {"partant_key": key, "_sources": []}
        for k, v in item.items():
            if v and str(v) not in ("None", "", "[]", "{}"):
                master[key][k] = v
        if "09_equipements" not in master[key]["_sources"]:
            master[key]["_sources"].append("09_equipements")

    log.info(f"  → Après 09_equipements: {len(master)} partants")

    # 10_poids_handicaps (141 MB)
    items_10 = load_json_safe(os.path.join(BASE_DIR, "../../output", "10_poids_handicaps", "poids_handicaps.json"), "10_poids", log)
    for item in items_10:
        key = make_key(item)
        if not key:
            continue
        if key not in master:
            master[key] = {"partant_key": key, "_sources": []}
        for k, v in item.items():
            if v and str(v) not in ("None", "", "[]", "{}"):
                master[key][k] = v
        if "10_poids" not in master[key]["_sources"]:
            master[key]["_sources"].append("10_poids")

    log.info(f"  → Après 10_poids: {len(master)} partants")

    # Stats
    total = len(master)
    for field in ['oeilleres', 'deferre', 'oeilleres_change', 'deferre_change',
                  'premiere_oeilleres', 'poids_porte_kg', 'handicap_valeur',
                  'poids_relatif', 'ecart_top_weight', 'evolution_poids']:
        count = sum(1 for r in master.values() if r.get(field))
        if count > 0:
            log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Sauvegarder
    master_list = list(master.values())
    for r in master_list:
        r["_nb_sources"] = len(r.get("_sources", []))

    log.info(f"Sauvegarde equipements_master.json ({len(master_list)} records)...")
    out = os.path.join(BASE_DIR, "../../data_master", "equipements_master.json")
    with open(out + ".tmp", "w", encoding="utf-8") as f:
        json.dump(master_list, f, ensure_ascii=False)
    os.replace(out + ".tmp", out)
    log.info(f"  → {os.path.getsize(out)/1024/1024:.1f} MB")

    log.info("Sauvegarde equipements_master.parquet...")
    try:
        import pyarrow as pa, pyarrow.parquet as pq, pandas as pd
        df = pd.DataFrame(master_list)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
        pq.write_table(pa.Table.from_pandas(df), os.path.join(BASE_DIR, "../../data_master", "equipements_master.parquet"), compression="zstd")
        log.info(f"  → {os.path.getsize('../../data_master/equipements_master.parquet')/1024/1024:.1f} MB")
    except Exception as e:
        log.warning(f"  Parquet: {e}")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s — {total} partants avec équipements/poids")

if __name__ == "__main__":
    main()
