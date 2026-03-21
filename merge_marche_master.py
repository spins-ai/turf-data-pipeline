#!/usr/bin/env python3
"""
Merge Marché Master — Fusionne TOUTES les données de cotes/paris/marché
Sources :
  + 07_cotes_marche (286 MB — cotes PMU)
  + 28_combinaisons_marche (1 GB+ — EN COURS, combinaisons de paris)
  + 30_smarkets_exchange (640 KB — cotes exchange Smarkets)
  + 40_enrichissement_partants (655 MB — cotes tendance/variation)
Output : data_master/marche_master.json + .parquet + .csv

⚠️ NE SUPPRIME RIEN — lecture seule sur les sources
"""

import json, os, logging, time

os.makedirs("data_master", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.FileHandler("logs/merge_marche.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)


def make_key(record):
    uid = record.get("course_uid", record.get("partant_uid", ""))
    if uid:
        return uid
    date = record.get("date_reunion_iso", record.get("date", ""))
    nr = record.get("numero_reunion", "")
    nc = record.get("numero_course", "")
    num = record.get("num_pmu", record.get("numPmu", ""))
    if date and nr and nc:
        base = f"{date}|R{nr}|C{nc}"
        if num:
            return f"{base}|P{num}"
        return base
    return ""


def load_json_safe(path, label):
    if not os.path.exists(path):
        return []
    size = os.path.getsize(path) / 1024 / 1024
    if size > 4000:
        log.info(f"  {label}: {size:.0f} MB — streaming avec ijson")
        try:
            import ijson
            items = []
            count = 0
            with open(path, 'rb') as f:
                for item in ijson.items(f, 'item'):
                    items.append(item)
                    count += 1
                    if count % 200000 == 0:
                        log.info(f"  {label}: {count} records...")
            return items
        except:
            log.warning(f"  {label}: trop gros et ijson échoue — skip")
            return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        items = data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []
        log.info(f"  {label}: {len(items)} records ({size:.0f} MB)")
        return items
    except Exception as e:
        log.warning(f"  {label}: erreur {e}")
        return []


def load_dir(dirpath, label):
    if not os.path.exists(dirpath):
        return []
    all_items = []
    for fname in sorted(os.listdir(dirpath)):
        if not fname.endswith('.json') or fname.startswith('.'):
            continue
        items = load_json_safe(os.path.join(dirpath, fname), f"{label}/{fname}")
        all_items.extend(items)
    return all_items


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("MERGE MARCHÉ MASTER")
    log.info("=" * 60)

    master = {}

    sources = [
        ("30_smarkets", "output/30_smarkets_exchange"),
        ("07_cotes", "output/07_cotes_marche"),
        ("28_combinaisons", "output/28_combinaisons_marche"),
        ("40_enrichissement", "output/40_enrichissement_partants"),
    ]

    for src_name, src_path in sources:
        items = load_dir(src_path, src_name)
        for item in items:
            key = make_key(item)
            if not key:
                continue
            if key not in master:
                master[key] = {"record_key": key, "_sources": []}
            for k, v in item.items():
                if v and str(v) not in ("None", "", "[]", "{}"):
                    master[key][k] = v
            if src_name not in master[key]["_sources"]:
                master[key]["_sources"].append(src_name)
        log.info(f"  → Après {src_name}: {len(master)} records")

    # Stats
    total = len(master)
    for field in ['cote_finale', 'cote_reference', 'cote_prob', 'proba_implicite',
                  'mise_base', 'rapport_simple_gagnant', 'rapport_couple']:
        count = sum(1 for r in master.values() if r.get(field))
        if count > 0:
            log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Sauvegarder
    master_list = list(master.values())
    for r in master_list:
        r["_nb_sources"] = len(r.get("_sources", []))

    log.info(f"Sauvegarde marche_master.json ({len(master_list)} records)...")
    out = "data_master/marche_master.json"
    with open(out + ".tmp", "w", encoding="utf-8") as f:
        json.dump(master_list, f, ensure_ascii=False)
    os.replace(out + ".tmp", out)
    log.info(f"  → {os.path.getsize(out)/1024/1024:.1f} MB")

    log.info("Sauvegarde marche_master.parquet...")
    try:
        import pyarrow as pa, pyarrow.parquet as pq, pandas as pd
        df = pd.DataFrame(master_list)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
        pq.write_table(pa.Table.from_pandas(df), "data_master/marche_master.parquet", compression="zstd")
        log.info(f"  → {os.path.getsize('data_master/marche_master.parquet')/1024/1024:.1f} MB")
    except Exception as e:
        log.warning(f"  Parquet: {e}")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s — {total} records marché")

if __name__ == "__main__":
    main()
