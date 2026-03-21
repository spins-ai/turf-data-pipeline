#!/usr/bin/env python3
"""
Merge Stats Externes Master — Fusionne TOUTES les sources externes (hors PMU)
Sources :
  + 24_canalturf (41 MB — profils chevaux canalturf)
  + 25_turfostats (27 MB — courses + détails)
  + 26_geny (44 MB — pronostics/réunions geny)
  + 37_racing_post (5.6 GB — données internationales, STREAMING)
Output : data_master/stats_externes_master.json + .parquet

⚠️ NE SUPPRIME RIEN — lecture seule sur les sources
"""

import json, os, logging, time

os.makedirs("data_master", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.FileHandler("logs/merge_stats_externes.log"), logging.StreamHandler()])
log = logging.getLogger(__name__)


def normalize_name(name):
    if not name:
        return ""
    import re
    return re.sub(r'[^A-Z\s]', '', name.upper()).strip()


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
    log.info("MERGE STATS EXTERNES MASTER")
    log.info("=" * 60)

    # ── Partie A : Profils chevaux externes ──
    log.info("=== PARTIE A : Profils chevaux (canalturf) ===")
    horse_profiles = {}

    # 24_canalturf (profils chevaux)
    items_24 = load_json_safe("output/24_canalturf/canalturf_chevaux.json", "24_canalturf")
    for item in items_24:
        nom = item.get("nom_cheval", "")
        key = normalize_name(nom)
        if not key:
            continue
        if key not in horse_profiles:
            horse_profiles[key] = {"nom_cheval_norm": key, "_sources": []}
        for k, v in item.items():
            if v and str(v) not in ("None", "", "[]", "{}"):
                horse_profiles[key][f"ct_{k}"] = v
        if "canalturf" not in horse_profiles[key]["_sources"]:
            horse_profiles[key]["_sources"].append("canalturf")

    log.info(f"  → Profils canalturf: {len(horse_profiles)} chevaux")

    # ── Partie B : Courses/pronostics externes ──
    log.info("=== PARTIE B : Courses externes (turfostats, geny) ===")
    courses_ext = {}

    # 25_turfostats courses
    items_25c = load_json_safe("output/25_turfostats/turfostats_courses.json", "25_courses")
    for item in items_25c:
        cid = item.get("id_course", "")
        if not cid:
            continue
        key = f"turfo|{cid}"
        courses_ext[key] = {"course_ext_key": key, "_sources": ["turfostats"]}
        for k, v in item.items():
            if v and str(v) not in ("None", "", "[]", "{}"):
                courses_ext[key][f"turfo_{k}"] = v

    # 25_turfostats détails (partants par course)
    items_25d = load_json_safe("output/25_turfostats/turfostats_details.json", "25_details")
    for item in items_25d:
        cid = item.get("id_course", "")
        if not cid:
            continue
        key = f"turfo|{cid}"
        if key in courses_ext:
            partants = item.get("partants", [])
            if partants:
                courses_ext[key]["turfo_partants"] = partants
                courses_ext[key]["turfo_nb_partants"] = len(partants) if isinstance(partants, list) else 0

    log.info(f"  → Courses turfostats: {len(courses_ext)}")

    # 26_geny (pronostics par date/réunion)
    items_26 = load_json_safe("output/26_geny/geny_data.json", "26_geny")
    geny_count = 0
    for item in items_26:
        date = item.get("date", "")
        if not date:
            continue
        key = f"geny|{date}"
        if key not in courses_ext:
            courses_ext[key] = {"course_ext_key": key, "_sources": []}
        for k, v in item.items():
            if v and str(v) not in ("None", "", "[]", "{}"):
                courses_ext[key][f"geny_{k}"] = v
        if "geny" not in courses_ext[key]["_sources"]:
            courses_ext[key]["_sources"].append("geny")
        geny_count += 1

    log.info(f"  → Après geny: +{geny_count}, total {len(courses_ext)} entries")

    # ── Partie C : Racing Post (5.6 GB — données internationales) ──
    log.info("=== PARTIE C : Racing Post (international) ===")
    racing_post = {}
    rp_path = "output/37_racing_post/racing_post_data.json"
    if os.path.exists(rp_path):
        size = os.path.getsize(rp_path) / 1024 / 1024
        log.info(f"  37_racing_post: {size:.0f} MB — streaming...")
        try:
            import ijson
            count = 0
            with open(rp_path, 'rb') as f:
                for item in ijson.items(f, 'item'):
                    nom = item.get("horse_name", item.get("nom_cheval", ""))
                    key = normalize_name(nom) if nom else ""
                    if not key:
                        # Essayer clé par course
                        date = item.get("date", "")
                        race = item.get("race_id", item.get("course_id", ""))
                        if date and race:
                            key = f"rp|{date}|{race}"
                    if not key:
                        continue
                    if key not in racing_post:
                        racing_post[key] = {"rp_key": key, "_sources": ["racing_post"]}
                    for k, v in item.items():
                        if v and str(v) not in ("None", "", "[]", "{}"):
                            racing_post[key][f"rp_{k}"] = v
                    count += 1
                    if count % 200000 == 0:
                        log.info(f"  37_racing_post: {count} records, {len(racing_post)} uniques...")
            log.info(f"  37_racing_post: {count} total, {len(racing_post)} uniques")
        except Exception as e:
            log.error(f"  37_racing_post streaming error: {e}")
    else:
        # Essayer les fichiers du dossier
        rp_dir = "output/37_racing_post"
        if os.path.exists(rp_dir):
            for fname in sorted(os.listdir(rp_dir)):
                if not fname.endswith('.json') or fname.startswith('.'):
                    continue
                items = load_json_safe(os.path.join(rp_dir, fname), f"37/{fname}")
                for item in items:
                    nom = item.get("horse_name", item.get("nom_cheval", ""))
                    key = normalize_name(nom) if nom else ""
                    if not key:
                        continue
                    if key not in racing_post:
                        racing_post[key] = {"rp_key": key, "_sources": ["racing_post"]}
                    for k, v in item.items():
                        if v and str(v) not in ("None", "", "[]", "{}"):
                            racing_post[key][f"rp_{k}"] = v

    log.info(f"  → Racing Post: {len(racing_post)} entries")

    # ── Sauvegardes ──
    log.info("=" * 60)
    log.info("SAUVEGARDES")

    # 1. Horse profiles
    profiles_list = list(horse_profiles.values())
    for r in profiles_list:
        r["_nb_sources"] = len(r.get("_sources", []))
    out1 = "data_master/horse_profiles_externes.json"
    with open(out1 + ".tmp", "w", encoding="utf-8") as f:
        json.dump(profiles_list, f, ensure_ascii=False)
    os.replace(out1 + ".tmp", out1)
    log.info(f"  → horse_profiles_externes.json: {os.path.getsize(out1)/1024/1024:.1f} MB, {len(profiles_list)} chevaux")

    # 2. Courses externes
    courses_list = list(courses_ext.values())
    for r in courses_list:
        r["_nb_sources"] = len(r.get("_sources", []))
    out2 = "data_master/courses_externes.json"
    with open(out2 + ".tmp", "w", encoding="utf-8") as f:
        json.dump(courses_list, f, ensure_ascii=False)
    os.replace(out2 + ".tmp", out2)
    log.info(f"  → courses_externes.json: {os.path.getsize(out2)/1024/1024:.1f} MB, {len(courses_list)} entries")

    # 3. Racing Post
    rp_list = list(racing_post.values())
    for r in rp_list:
        r["_nb_sources"] = len(r.get("_sources", []))
    out3 = "data_master/racing_post_master.json"
    with open(out3 + ".tmp", "w", encoding="utf-8") as f:
        json.dump(rp_list, f, ensure_ascii=False)
    os.replace(out3 + ".tmp", out3)
    log.info(f"  → racing_post_master.json: {os.path.getsize(out3)/1024/1024:.1f} MB, {len(rp_list)} entries")

    # Parquet pour les 3
    try:
        import pyarrow as pa, pyarrow.parquet as pq, pandas as pd
        for name, data_list in [("horse_profiles_externes", profiles_list),
                                 ("courses_externes", courses_list),
                                 ("racing_post_master", rp_list)]:
            if not data_list:
                continue
            df = pd.DataFrame(data_list)
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
            pq_path = f"data_master/{name}.parquet"
            pq.write_table(pa.Table.from_pandas(df), pq_path, compression="zstd")
            log.info(f"  → {name}.parquet: {os.path.getsize(pq_path)/1024/1024:.1f} MB")
    except Exception as e:
        log.warning(f"  Parquet: {e}")

    elapsed = time.time() - start
    log.info(f"TERMINÉ en {elapsed:.0f}s — {len(horse_profiles)} profils + {len(courses_ext)} courses ext + {len(racing_post)} racing post")

if __name__ == "__main__":
    main()
