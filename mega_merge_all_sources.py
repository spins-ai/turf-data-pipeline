#!/usr/bin/env python3
"""
mega_merge_all_sources.py
Enriches partants_normalises.json (2.9M records, 4.6GB) by merging data from
all available output/ sources. Uses index dictionaries + streaming to avoid
loading the full dataset into memory.

Output: output/02_liste_courses/partants_enrichis.json
"""

import json
import time
import sys
import os
from collections import defaultdict

PARTANTS_PATH = os.path.join("output", "02_liste_courses", "partants_normalises.json")
OUTPUT_PATH = os.path.join("output", "02_liste_courses", "partants_enrichis.json")

# Track which fields came from which source
source_fields = {}
# Track fill rates
fill_counts_before = defaultdict(int)
fill_counts_after = defaultdict(int)
match_counts = defaultdict(int)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────
# Helper: stream JSON array items without loading entire file
# ─────────────────────────────────────────────────────────────
def stream_json_array(path, report_every=500000):
    """Yield items from a JSON array file one by one."""
    with open(path, 'r', encoding='utf-8') as f:
        # Skip opening bracket
        c = f.read(1)
        while c and c != '[':
            c = f.read(1)

        buf = ''
        depth = 0
        in_string = False
        escape = False
        count = 0

        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            for ch in chunk:
                if escape:
                    buf += ch
                    escape = False
                    continue
                if ch == '\\' and in_string:
                    buf += ch
                    escape = True
                    continue
                if ch == '"':
                    in_string = not in_string
                    buf += ch
                    continue
                if in_string:
                    buf += ch
                    continue
                if ch == '{':
                    depth += 1
                    buf += ch
                elif ch == '}':
                    depth -= 1
                    buf += ch
                    if depth == 0 and buf.strip():
                        yield json.loads(buf)
                        count += 1
                        buf = ''
                        if report_every and count % report_every == 0:
                            log(f"  ... streamed {count:,} records from {os.path.basename(path)}")
                elif ch == ']' and depth == 0:
                    break
                elif depth > 0:
                    buf += ch


# ─────────────────────────────────────────────────────────────
# 1. Load 05_historique_chevaux index (by nom_cheval)
# ─────────────────────────────────────────────────────────────
def load_05_historique_chevaux():
    path = os.path.join("output", "05_historique_chevaux", "historique_chevaux.json")
    log(f"Loading 05_historique_chevaux from {os.path.basename(path)}...")
    idx = {}
    # Fields we want (excluding courses_detail which is huge)
    keep_fields = [
        'nb_courses_total', 'nb_victoires_total', 'nb_places_total',
        'gains_total_euros', 'premiere_course_date', 'derniere_course_date',
        'taux_victoire', 'taux_place', 'forme_5', 'forme_10', 'forme_20',
        'jours_moyen_entre_courses'
    ]
    count = 0
    for rec in stream_json_array(path, report_every=100000):
        name = rec.get('nom_cheval', '').upper().strip()
        if name:
            entry = {}
            for f in keep_fields:
                if f in rec and rec[f] is not None:
                    entry[f'hist_{f}'] = rec[f]
            if entry:
                idx[name] = entry
        count += 1
    new_fields = [f'hist_{f}' for f in keep_fields]
    source_fields['05_historique_chevaux'] = new_fields
    log(f"  -> {len(idx):,} horses indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# 2. Load 06_historique_jockeys index (by jockey name)
# ─────────────────────────────────────────────────────────────
def load_06_historique_jockeys():
    path = os.path.join("output", "06_historique_jockeys", "historique_jockeys.json")
    log(f"Loading 06_historique_jockeys...")
    idx = {}
    keep_fields = [
        'nb_montes', 'nb_victoires', 'nb_places',
        'taux_victoire', 'taux_place', 'gains_total_euros',
        'chevaux_montes', 'premiere_course_date', 'derniere_course_date'
    ]
    count = 0
    for rec in stream_json_array(path, report_every=50000):
        name = rec.get('nom', '').upper().strip()
        if name:
            entry = {}
            for f in keep_fields:
                if f in rec and rec[f] is not None:
                    entry[f'jockey_{f}'] = rec[f]
            if entry:
                idx[name] = entry
        count += 1
    new_fields = [f'jockey_{f}' for f in keep_fields]
    source_fields['06_historique_jockeys'] = new_fields
    log(f"  -> {len(idx):,} jockeys indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# 3. Load 07_cotes_marche index (by partant_uid)
# ─────────────────────────────────────────────────────────────
def load_07_cotes_marche():
    path = os.path.join("output", "07_cotes_marche", "cotes_marche.json")
    log(f"Loading 07_cotes_marche...")
    idx = {}
    # Fields to extract (that add value beyond what partants already has)
    keep_fields = [
        'rang_cote', 'is_favori', 'is_outsider',
        'nb_partants_course', 'cote_moyenne_course',
        'cote_mediane_course', 'ecart_cote_moyenne'
    ]
    count = 0
    for rec in stream_json_array(path, report_every=500000):
        uid = rec.get('partant_uid', '')
        if uid:
            entry = {}
            for f in keep_fields:
                if f in rec and rec[f] is not None:
                    entry[f'cotes_{f}'] = rec[f]
            if entry:
                idx[uid] = entry
        count += 1
    new_fields = [f'cotes_{f}' for f in keep_fields]
    source_fields['07_cotes_marche'] = new_fields
    log(f"  -> {len(idx):,} partants indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# 4. Load 09_equipements index (by partant_uid)
# ─────────────────────────────────────────────────────────────
def load_09_equipements():
    path = os.path.join("output", "09_equipements", "equipements_historique.json")
    log(f"Loading 09_equipements...")
    idx = {}
    keep_fields = [
        'oeilleres_prev', 'deferre_prev',
        'oeilleres_change', 'deferre_change',
        'premiere_oeilleres', 'retrait_oeilleres',
        'nb_courses_avec_oeilleres', 'nb_courses_sans_oeilleres'
    ]
    count = 0
    for rec in stream_json_array(path, report_every=500000):
        uid = rec.get('partant_uid', '')
        if uid:
            entry = {}
            for f in keep_fields:
                if f in rec and rec[f] is not None:
                    entry[f'equip_{f}'] = rec[f]
            if entry:
                idx[uid] = entry
        count += 1
    new_fields = [f'equip_{f}' for f in keep_fields]
    source_fields['09_equipements'] = new_fields
    log(f"  -> {len(idx):,} partants indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# 5. Load 10_poids_handicaps index (by partant_uid)
# ─────────────────────────────────────────────────────────────
def load_10_poids_handicaps():
    path = os.path.join("output", "10_poids_handicaps", "poids_handicaps.json")
    log(f"Loading 10_poids_handicaps...")
    idx = {}
    keep_fields = [
        'poids_moyen_course', 'poids_relatif',
        'ecart_top_weight', 'poids_precedent',
        'evolution_poids', 'poids_par_km'
    ]
    count = 0
    for rec in stream_json_array(path, report_every=500000):
        uid = rec.get('partant_uid', '')
        if uid:
            entry = {}
            for f in keep_fields:
                if f in rec and rec[f] is not None:
                    entry[f'poids_{f}'] = rec[f]
            if entry:
                idx[uid] = entry
        count += 1
    new_fields = [f'poids_{f}' for f in keep_fields]
    source_fields['10_poids_handicaps'] = new_fields
    log(f"  -> {len(idx):,} partants indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# 6. Load 11_sectionals index (by partant_uid)
# ─────────────────────────────────────────────────────────────
def load_11_sectionals():
    path = os.path.join("output", "11_sectionals", "sectionals.json")
    log(f"Loading 11_sectionals...")
    idx = {}
    keep_fields = [
        'vitesse_kmh', 'reduction_km_sec', 'reduction_km_str',
        'vitesse_relative', 'ecart_temps_gagnant', 'ecart_redkm_gagnant'
    ]
    count = 0
    for rec in stream_json_array(path, report_every=500000):
        uid = rec.get('partant_uid', '')
        if uid:
            entry = {}
            for f in keep_fields:
                if f in rec and rec[f] is not None:
                    entry[f'sect_{f}'] = rec[f]
            if entry:
                idx[uid] = entry
        count += 1
    new_fields = [f'sect_{f}' for f in keep_fields]
    source_fields['11_sectionals'] = new_fields
    log(f"  -> {len(idx):,} partants indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# 7. Load 17_sire_ifce index (by horse name) - use index_par_nom
# ─────────────────────────────────────────────────────────────
def load_17_sire_ifce():
    path = os.path.join("output", "17_sire_ifce", "index_par_nom.json")
    log(f"Loading 17_sire_ifce (index_par_nom, 335MB)...")
    keep_fields = [
        'date_naissance', 'pays_naissance', 'consommation',
        'date_deces', 'annee_naissance', 'vivant'
    ]
    # Stream the dict - it's a top-level JSON object { "NAME": {...}, ... }
    # Load it directly since it's a dict (335MB is manageable)
    with open(path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    idx = {}
    for name, rec in raw.items():
        name_upper = name.upper().strip()
        entry = {}
        for field in keep_fields:
            if field in rec and rec[field] is not None:
                entry[f'sire_{field}'] = rec[field]
        if entry:
            idx[name_upper] = entry

    del raw
    new_fields = [f'sire_{field}' for field in keep_fields]
    source_fields['17_sire_ifce'] = new_fields
    log(f"  -> {len(idx):,} horses indexed")
    return idx


# ─────────────────────────────────────────────────────────────
# 8. Load 40_enrichissement_partants index (by cle_partant)
# ─────────────────────────────────────────────────────────────
def load_40_enrichissement():
    path = os.path.join("output", "40_enrichissement_partants", "enrichissement_partants.json")
    log(f"Loading 40_enrichissement_partants (655MB)...")
    idx = {}
    skip_fields = {'cle_partant'}
    new_fields_set = set()
    count = 0
    for rec in stream_json_array(path, report_every=500000):
        cle = rec.get('cle_partant', '')
        if cle:
            entry = {}
            for f, v in rec.items():
                if f not in skip_fields and v is not None:
                    prefixed = f'enrich_{f}'
                    entry[prefixed] = v
                    new_fields_set.add(prefixed)
            if entry:
                idx[cle] = entry
        count += 1
    new_fields = sorted(new_fields_set)
    source_fields['40_enrichissement_partants'] = new_fields
    log(f"  -> {len(idx):,} partants indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# 9. Load 39_reunions_enrichies index (by date+R+C)
# ─────────────────────────────────────────────────────────────
def load_39_reunions_enrichies():
    path = os.path.join("output", "39_reunions_enrichies", "reunions_enrichies.json")
    log(f"Loading 39_reunions_enrichies (1.6GB - streaming)...")
    idx = {}
    # Skip fields already in partants or that are huge nested structures
    skip_fields = {
        'course_uid', 'reunion_uid', 'date_reunion_iso',
        'numero_reunion', 'numero_course', 'hippodrome', 'discipline',
        'paris_detail', 'incidents_detail'  # These are large nested arrays
    }
    new_fields_set = set()
    count = 0
    for rec in stream_json_array(path, report_every=200000):
        date = rec.get('date_reunion_iso', '')
        nr = rec.get('numero_reunion')
        nc = rec.get('numero_course')
        if date and nr is not None and nc is not None:
            key = f"{date}|{nr}|{nc}"
            entry = {}
            for f, v in rec.items():
                if f not in skip_fields and v is not None:
                    prefixed = f'reunion_{f}'
                    entry[prefixed] = v
                    new_fields_set.add(prefixed)
            if entry:
                idx[key] = entry
        count += 1
    new_fields = sorted(new_fields_set)
    source_fields['39_reunions_enrichies'] = new_fields
    log(f"  -> {len(idx):,} courses indexed ({count:,} records)")
    return idx


# ─────────────────────────────────────────────────────────────
# MAIN: Build all indexes, then stream-merge
# ─────────────────────────────────────────────────────────────
def main():
    t0 = time.time()

    log("=" * 70)
    log("MEGA MERGE - Building index dictionaries from all sources")
    log("=" * 70)

    # Load all indexes
    idx_05 = load_05_historique_chevaux()
    idx_06 = load_06_historique_jockeys()
    idx_07 = load_07_cotes_marche()
    idx_09 = load_09_equipements()
    idx_10 = load_10_poids_handicaps()
    idx_11 = load_11_sectionals()
    idx_17 = load_17_sire_ifce()
    idx_40 = load_40_enrichissement()
    idx_39 = load_39_reunions_enrichies()

    t_index = time.time()
    log(f"All indexes built in {t_index - t0:.1f}s")
    log("=" * 70)
    log("Streaming through partants_normalises.json and enriching...")
    log("=" * 70)

    # Collect all new field names
    all_new_fields = set()
    for fields in source_fields.values():
        all_new_fields.update(fields)

    # Get existing fields from first record
    existing_fields = set()
    for rec in stream_json_array(PARTANTS_PATH, report_every=0):
        existing_fields = set(rec.keys())
        break

    log(f"Existing fields in partants: {len(existing_fields)}")
    log(f"New fields to add: {len(all_new_fields)}")

    # Stream and enrich
    total = 0
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as out:
        out.write('[\n')
        first = True

        for rec in stream_json_array(PARTANTS_PATH, report_every=0):
            total += 1

            # Track fill rates before enrichment for key fields
            for key_field in ['cote_finale', 'poids_porte_kg', 'handicap_valeur']:
                if rec.get(key_field) is not None:
                    fill_counts_before[key_field] += 1

            # ── 05: Horse history (join on nom_cheval) ──
            horse_name = (rec.get('nom_cheval') or '').upper().strip()
            if horse_name and horse_name in idx_05:
                rec.update(idx_05[horse_name])
                match_counts['05_historique_chevaux'] += 1

            # ── 06: Jockey history (join on jockey_driver) ──
            jockey = (rec.get('jockey_driver') or '').upper().strip()
            if jockey and jockey in idx_06:
                rec.update(idx_06[jockey])
                match_counts['06_historique_jockeys'] += 1

            # ── 07: Market odds (join on partant_uid) ──
            puid = rec.get('partant_uid', '')
            if puid and puid in idx_07:
                rec.update(idx_07[puid])
                match_counts['07_cotes_marche'] += 1

            # ── 09: Equipment (join on partant_uid) ──
            if puid and puid in idx_09:
                rec.update(idx_09[puid])
                match_counts['09_equipements'] += 1

            # ── 10: Weights/handicaps (join on partant_uid) ──
            if puid and puid in idx_10:
                rec.update(idx_10[puid])
                match_counts['10_poids_handicaps'] += 1

            # ── 11: Sectionals (join on partant_uid) ──
            if puid and puid in idx_11:
                rec.update(idx_11[puid])
                match_counts['11_sectionals'] += 1

            # ── 17: SIRE/IFCE (join on nom_cheval) ──
            if horse_name and horse_name in idx_17:
                rec.update(idx_17[horse_name])
                match_counts['17_sire_ifce'] += 1

            # ── 40: Enrichissement (join on cle_partant) ──
            cle = rec.get('cle_partant', '')
            if cle and cle in idx_40:
                rec.update(idx_40[cle])
                match_counts['40_enrichissement_partants'] += 1

            # ── 39: Reunions enrichies (join on date+R+C) ──
            date = rec.get('date_reunion_iso', '')
            nr = rec.get('numero_reunion')
            nc = rec.get('numero_course')
            if date and nr is not None and nc is not None:
                rkey = f"{date}|{nr}|{nc}"
                if rkey in idx_39:
                    rec.update(idx_39[rkey])
                    match_counts['39_reunions_enrichies'] += 1

            # Track fill rates after enrichment
            for key_field in ['cote_finale', 'poids_porte_kg', 'handicap_valeur',
                              'cotes_rang_cote', 'hist_taux_victoire', 'jockey_taux_victoire',
                              'sire_date_naissance', 'sect_vitesse_kmh',
                              'reunion_meteo_temperature']:
                if rec.get(key_field) is not None:
                    fill_counts_after[key_field] += 1

            # Write
            if not first:
                out.write(',\n')
            json.dump(rec, out, ensure_ascii=False, default=str)
            first = False

            if total % 200000 == 0:
                elapsed = time.time() - t_index
                rate = total / elapsed if elapsed > 0 else 0
                log(f"  Progress: {total:>10,} records | {elapsed:.0f}s | {rate:,.0f} rec/s")

        out.write('\n]')

    t_end = time.time()

    # ── Final report ──
    log("=" * 70)
    log("MERGE COMPLETE")
    log("=" * 70)
    log(f"Total records: {total:,}")
    log(f"Total time: {t_end - t0:.1f}s (indexing: {t_index - t0:.1f}s, streaming: {t_end - t_index:.1f}s)")
    log(f"Output: {OUTPUT_PATH}")
    out_size = os.path.getsize(OUTPUT_PATH)
    log(f"Output size: {out_size / (1024**3):.2f} GB")

    log("")
    log("── Match rates by source ──")
    for src in sorted(match_counts.keys()):
        cnt = match_counts[src]
        pct = 100.0 * cnt / total if total > 0 else 0
        nf = len(source_fields.get(src, []))
        log(f"  {src:35s} : {cnt:>10,} matches ({pct:5.1f}%) | {nf} new fields")

    log("")
    log("── New fields added per source ──")
    for src in sorted(source_fields.keys()):
        fields = source_fields[src]
        log(f"  {src}: {', '.join(fields[:8])}{'...' if len(fields) > 8 else ''}")

    log("")
    log("── Fill rates for key fields ──")
    log(f"  {'Field':40s} {'Before':>12s} {'After':>12s} {'Rate Before':>12s} {'Rate After':>12s}")
    all_tracked = sorted(set(list(fill_counts_before.keys()) + list(fill_counts_after.keys())))
    for field in all_tracked:
        before = fill_counts_before.get(field, 0)
        after = fill_counts_after.get(field, 0)
        pct_before = 100.0 * before / total if total > 0 else 0
        pct_after = 100.0 * after / total if total > 0 else 0
        log(f"  {field:40s} {before:>12,} {after:>12,} {pct_before:>11.1f}% {pct_after:>11.1f}%")

    log("")
    log("NOTE: 37_racing_post was skipped (raw UK racing HTML, not joinable to French data)")


if __name__ == '__main__':
    main()
