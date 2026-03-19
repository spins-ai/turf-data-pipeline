#!/usr/bin/env python3
"""
merge_all_enrichments.py
========================
Merge TOUS les fichiers enrichis en un seul partants_master_final.jsonl.

Pipeline :
  1. Lire partants_master_enrichi.jsonl (base enrichie)
  2. Ajouter les features Timeform (partants_master_enrichi_tf.jsonl)
  3. Ajouter les features Sporting Life (partants_master_enrichi_sl.jsonl)
  4. Produire partants_master_final.jsonl

Streaming JSONL -> JSONL pour supporter les fichiers de plusieurs GB
sans exploser la RAM. Les enrichissements sont charges en index (par partant_uid)
avant le streaming du fichier principal.

Usage :
    python merge_all_enrichments.py
"""

import json
import os
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"

# Fichier de base (le plus complet)
BASE_FILE = DATA_MASTER / "partants_master_enrichi.jsonl"
BASE_FALLBACK = DATA_MASTER / "partants_master.jsonl"

# Fichiers d'enrichissement supplementaires
ENRICHMENT_FILES = [
    {
        "path": DATA_MASTER / "partants_master_enrichi_tf.jsonl",
        "label": "Timeform",
        "prefix": "tf_",
        "key": "partant_uid",
    },
    {
        "path": DATA_MASTER / "partants_master_enrichi_sl.jsonl",
        "label": "Sporting Life",
        "prefix": "sl_",
        "key": "partant_uid",
    },
]

OUTPUT_FILE = DATA_MASTER / "partants_master_final.jsonl"


# -----------------------------------------------------------------------
# Index builder — charge un fichier JSONL en index {key: {champs extras}}
# -----------------------------------------------------------------------

def build_enrichment_index(fpath, key_field, prefix, base_fields_to_skip=None):
    """Charge un fichier JSONL d'enrichissement et retourne un index.

    Pour chaque record, on extrait les champs qui sont NOUVEAUX
    (pas dans le fichier de base) et on les prefixe si necessaire.

    Parameters
    ----------
    fpath : Path
        Chemin du fichier JSONL.
    key_field : str
        Champ utilise comme cle d'index (ex: partant_uid).
    prefix : str
        Prefixe a ajouter aux champs nouveaux (ex: "tf_").
    base_fields_to_skip : set or None
        Champs deja dans le fichier de base, a ne pas copier.

    Returns
    -------
    dict
        {key_value: {field: value, ...}} avec les champs supplementaires.
    """
    fpath = Path(fpath)
    if not fpath.exists():
        print(f"  [SKIP] {fpath.name} introuvable")
        return {}

    print(f"  Chargement index: {fpath.name} ...", end=" ", flush=True)
    t0 = time.time()

    if base_fields_to_skip is None:
        base_fields_to_skip = set()

    # Pour detecter les champs supplementaires, lire un echantillon
    # et reperer les champs qui ne sont PAS dans le fichier de base
    sample_fields = set()
    sample_count = 0

    index = {}
    total = 0

    with open(fpath, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            key_val = record.get(key_field)
            if not key_val:
                total += 1
                continue

            # Collecter les champs supplementaires du record
            extras = {}
            for field, value in record.items():
                if field == key_field:
                    continue
                # Si le champ est deja dans la base, on le skip
                # Sauf s'il commence par le prefixe (c'est un champ enrichi)
                if field in base_fields_to_skip and not field.startswith(prefix):
                    continue
                # Ajouter le prefixe si le champ ne l'a pas deja
                if prefix and not field.startswith(prefix):
                    out_field = prefix + field
                else:
                    out_field = field
                if value is not None:
                    extras[out_field] = value

            if extras:
                index[key_val] = extras

            total += 1

    dt = time.time() - t0
    print(f"{total:,} records lus, {len(index):,} indexes ({dt:.1f}s)")
    return index


def detect_base_fields(base_path, sample_size=1000):
    """Lit un echantillon du fichier de base pour detecter les champs existants."""
    fields = set()
    count = 0

    if not base_path.exists():
        return fields

    with open(base_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                fields.update(record.keys())
                count += 1
                if count >= sample_size:
                    break
            except json.JSONDecodeError:
                continue

    return fields


# -----------------------------------------------------------------------
# Streaming merge
# -----------------------------------------------------------------------

def streaming_merge(base_path, enrichment_indexes, output_path):
    """Lit le fichier de base en streaming, ajoute les enrichissements, ecrit le resultat.

    Parameters
    ----------
    base_path : Path
        Fichier JSONL de base.
    enrichment_indexes : list of (label, dict)
        Liste de (nom, index_dict) a merger.
    output_path : Path
        Fichier de sortie.
    """
    if not base_path.exists():
        print(f"  [ERREUR] {base_path} introuvable!")
        return 0

    print(f"\n  Streaming merge: {base_path.name} -> {output_path.name}")
    t0 = time.time()

    tmp_path = output_path.with_suffix(".jsonl.tmp")
    total = 0
    enriched_counts = {label: 0 for label, _ in enrichment_indexes}

    with open(base_path, "r", encoding="utf-8", errors="replace") as fin, \
         open(tmp_path, "w", encoding="utf-8", errors="replace") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                fout.write(line + "\n")
                total += 1
                continue

            uid = record.get("partant_uid")

            # Merger chaque source d'enrichissement
            if uid:
                for label, index in enrichment_indexes:
                    extras = index.get(uid)
                    if extras:
                        record.update(extras)
                        enriched_counts[label] += 1

            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            total += 1

            if total % 500000 == 0:
                print(f"    {total:,} lignes traitees ...")

    os.replace(str(tmp_path), str(output_path))
    dt = time.time() - t0

    print(f"    -> {total:,} lignes ecrites ({dt:.1f}s)")
    for label, count in enriched_counts.items():
        pct = count * 100 / max(total, 1)
        print(f"    -> {label}: {count:,} enrichis ({pct:.1f}%)")

    return total


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    t_start = time.time()
    print("=" * 70)
    print("MERGE ALL ENRICHMENTS -> partants_master_final.jsonl")
    print("=" * 70)

    # 1. Determiner le fichier de base
    base_path = BASE_FILE if BASE_FILE.exists() else BASE_FALLBACK
    if not base_path.exists():
        print(f"[ERREUR] Aucun fichier de base trouve:")
        print(f"  - {BASE_FILE}")
        print(f"  - {BASE_FALLBACK}")
        return

    print(f"\n[1] Fichier de base: {base_path.name}")
    base_size_mb = base_path.stat().st_size / 1024 / 1024
    print(f"    Taille: {base_size_mb:.0f} MB")

    # 2. Detecter les champs du fichier de base
    print(f"\n[2] Detection des champs de base ...")
    base_fields = detect_base_fields(base_path)
    print(f"    {len(base_fields)} champs detectes dans l'echantillon")

    # 3. Charger les index d'enrichissement
    print(f"\n[3] Chargement des enrichissements ...")
    enrichment_indexes = []

    for enr in ENRICHMENT_FILES:
        index = build_enrichment_index(
            fpath=enr["path"],
            key_field=enr["key"],
            prefix=enr["prefix"],
            base_fields_to_skip=base_fields,
        )
        if index:
            enrichment_indexes.append((enr["label"], index))

    if not enrichment_indexes:
        print("\n  Aucun enrichissement trouve. Copie simple du fichier de base.")
        # Copie streaming du fichier de base vers la sortie
        enrichment_indexes = []

    # 4. Streaming merge
    print(f"\n[4] Merge streaming ...")
    os.makedirs(str(DATA_MASTER), exist_ok=True)
    total = streaming_merge(base_path, enrichment_indexes, OUTPUT_FILE)

    # 5. Stats finales
    if OUTPUT_FILE.exists():
        out_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    else:
        out_size_mb = 0

    # Compter les champs du fichier final (echantillon)
    final_fields = detect_base_fields(OUTPUT_FILE, sample_size=500)

    new_fields = final_fields - base_fields
    print(f"\n" + "=" * 70)
    print("RESULTATS")
    print("=" * 70)
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Lignes: {total:,}")
    print(f"  Taille: {out_size_mb:.1f} MB")
    print(f"  Champs base: {len(base_fields)}")
    print(f"  Champs final: {len(final_fields)}")
    print(f"  Nouveaux champs: {len(new_fields)}")

    if new_fields:
        print(f"\n  Nouveaux champs ajoutes:")
        for f in sorted(new_fields):
            print(f"    + {f}")

    dt_total = time.time() - t_start
    print(f"\nTermine en {dt_total:.0f}s ({dt_total / 60:.1f} min)")


if __name__ == "__main__":
    main()
