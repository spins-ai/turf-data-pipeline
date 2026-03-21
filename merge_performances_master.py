#!/usr/bin/env python3
"""
Merge Performances Master — Fusionne TOUTES les données de performances/historique
Sources :
  + 05_historique_chevaux (324 MB — stats agrégées par cheval)
  + 22_performances_detaillees (12 GB — perf course par course, STREAMING)
  + 11_sectionals (133 MB — temps sectionnels)
Output : data_master/performances_master.json + .parquet

⚠️ NE SUPPRIME RIEN — lecture seule sur les sources
"""

import json, os, logging, sys, time

os.makedirs("data_master", exist_ok=True)
os.makedirs("logs", exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("merge_performances_master")


def make_partant_key(record):
    uid = record.get("partant_uid", "")
    if uid:
        return uid
    date = record.get("date_reunion_iso", record.get("date", ""))
    course_uid = record.get("course_uid", "")
    num = record.get("num_pmu", record.get("numPmu", ""))
    nom = record.get("nom_cheval", "")
    if course_uid and num:
        return f"{course_uid}|P{num}"
    if date and nom:
        return f"{date}|{nom.upper().strip()}"
    return ""


def make_horse_key(record):
    """Clé par cheval (pour stats agrégées)"""
    nom = record.get("nom_cheval", "")
    if nom:
        return nom.upper().strip()
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
        except Exception as e:
            log.warning(f"  {label}: streaming échoue ({e}) — skip")
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


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("MERGE PERFORMANCES MASTER")
    log.info("=" * 60)

    # ── Partie 1 : Stats agrégées par cheval (05_historique) ──
    log.info("=== PARTIE 1 : Stats agrégées par cheval ===")
    horse_stats = {}

    items_05 = load_json_safe(os.path.join(BASE_DIR, "output", "05_historique_chevaux", "historique_chevaux.json"), "05_historique")
    for item in items_05:
        key = make_horse_key(item)
        if not key:
            continue
        if key not in horse_stats:
            horse_stats[key] = {"nom_cheval_norm": key, "_sources": []}
        for k, v in item.items():
            if v and str(v) not in ("None", "", "[]", "{}"):
                horse_stats[key][k] = v
        if "05_historique" not in horse_stats[key]["_sources"]:
            horse_stats[key]["_sources"].append("05_historique")

    log.info(f"  → Stats agrégées: {len(horse_stats)} chevaux")

    # Sauvegarder stats agrégées séparément (léger)
    stats_list = list(horse_stats.values())
    for r in stats_list:
        r["_nb_sources"] = len(r.get("_sources", []))
    out_stats = os.path.join(BASE_DIR, "data_master", "horse_stats_master.json")
    with open(out_stats + ".tmp", "w", encoding="utf-8") as f:
        json.dump(stats_list, f, ensure_ascii=False)
    os.replace(out_stats + ".tmp", out_stats)
    log.info(f"  → horse_stats_master.json: {os.path.getsize(out_stats)/1024/1024:.1f} MB")

    # ── Partie 2 : Performances détaillées — DIRECT STREAMING to JSON ──
    # Stratégie : on indexe les sectionals d'abord (petit), puis on streame
    # les 22_performances en écrivant directement dans le fichier JSON de sortie
    log.info("=== PARTIE 2 : Performances détaillées (stream-to-disk) ===")

    # Charger sectionals en mémoire (243K records = ~300MB OK)
    log.info("  Chargement index sectionals...")
    sect_index = {}  # partant_key -> dict de champs sect_*
    items_11 = load_json_safe(os.path.join(BASE_DIR, "output", "11_sectionals", "sectionals.json"), "11_sectionals")
    for item in items_11:
        key = make_partant_key(item)
        if not key:
            continue
        sect_data = {}
        for k, v in item.items():
            if v and str(v) not in ("None", "", "[]", "{}"):
                if k not in ("partant_uid", "course_uid", "nom_cheval", "date_reunion_iso", "num_pmu"):
                    sect_data[f"sect_{k}"] = v
        sect_index[key] = sect_data
    del items_11
    log.info(f"  → {len(sect_index)} sectionals indexés")

    # Stream 22_performances et écrire directement en JSON
    perf_path = os.path.join(BASE_DIR, "output", "22_performances_detaillees", "performances_detaillees.json")
    out = os.path.join(BASE_DIR, "data_master", "performances_master.json")
    tmp = out + ".tmp"
    total = 0
    seen_keys = set()

    if os.path.exists(perf_path):
        size = os.path.getsize(perf_path) / 1024 / 1024
        log.info(f"  22_performances: streaming {size:.0f} MB → disk...")
        try:
            import ijson
            with open(tmp, "w", encoding="utf-8") as fout:
                fout.write("[")
                first = True
                with open(perf_path, 'rb') as perf_fh:
                    for item in ijson.items(perf_fh, 'item'):
                        key = make_partant_key(item)
                        if not key or key in seen_keys:
                            continue
                        seen_keys.add(key)
                        # Enrichir avec sectionals si dispo
                        if key in sect_index:
                            item.update(sect_index[key])
                            item["_sources"] = ["22_performances", "11_sectionals"]
                        else:
                            item["_sources"] = ["22_performances"]
                        item["_nb_sources"] = len(item["_sources"])
                        if not first:
                            fout.write(",")
                        json.dump(item, fout, ensure_ascii=False)
                        first = False
                        total += 1
                        if total % 100000 == 0:
                            log.info(f"  Écrit {total} records...")

                # Ajouter les sectionals orphelins (pas dans 22_performances)
                orphan_count = 0
                for key, sdata in sect_index.items():
                    if key not in seen_keys:
                        rec = {"partant_key": key, "_sources": ["11_sectionals"], "_nb_sources": 1}
                        rec.update(sdata)
                        if not first:
                            fout.write(",")
                        json.dump(rec, fout, ensure_ascii=False)
                        first = False
                        orphan_count += 1
                        total += 1

                fout.write("]")
            os.replace(tmp, out)
            log.info(f"  → performances_master.json: {os.path.getsize(out)/1024/1024:.1f} MB, {total} records ({orphan_count} orphelins sect)")
        except Exception as e:
            log.error(f"  Streaming error: {e}")
            import traceback
            traceback.print_exc()

    del sect_index, seen_keys  # Libérer RAM

    log.info("Sauvegarde performances_master.parquet (via DuckDB ou pandas chunks)...")
    try:
        # Essayer DuckDB d'abord (plus efficace en RAM)
        import duckdb
        duckdb.sql(f"COPY (SELECT * FROM read_json_auto('{out}')) TO 'data_master/performances_master.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")
        log.info(f"  → {os.path.getsize('data_master/performances_master.parquet')/1024/1024:.1f} MB (DuckDB)")
    except Exception as e:
        log.info(f"  DuckDB non disponible ({e}), fallback pandas...")
        try:
            import pandas as pd, pyarrow as pa, pyarrow.parquet as pq
            # Lire par chunks avec ijson
            import ijson
            writer = None
            chunk = []
            chunk_size = 50000
            with open(out, 'rb') as f:
                for item in ijson.items(f, 'item'):
                    chunk.append(item)
                    if len(chunk) >= chunk_size:
                        df = pd.DataFrame(chunk)
                        for col in df.columns:
                            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
                        table = pa.Table.from_pandas(df)
                        if writer is None:
                            writer = pq.ParquetWriter(os.path.join(BASE_DIR, "data_master", "performances_master.parquet"), table.schema, compression="zstd")
                        writer.write_table(table)
                        chunk = []
            if chunk:
                df = pd.DataFrame(chunk)
                for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                        df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
                table = pa.Table.from_pandas(df)
                if writer is None:
                    writer = pq.ParquetWriter(os.path.join(BASE_DIR, "data_master", "performances_master.parquet"), table.schema, compression="zstd")
                writer.write_table(table)
            if writer:
                writer.close()
            log.info(f"  → {os.path.getsize('data_master/performances_master.parquet')/1024/1024:.1f} MB (pandas chunks)")
        except Exception as e:
            log.warning(f"  Parquet: {e}")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s — {total} partants + {len(horse_stats)} stats chevaux")

if __name__ == "__main__":
    main()
