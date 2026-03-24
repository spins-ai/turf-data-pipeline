#!/usr/bin/env python3
"""
Feature Engineering PRINCIPAL — Orchestre tous les modules de features.

Prend partants_complets.json (sortie de entity_resolution.py)
et ajoute 400+ features calculées.

Résultat : features_matrix.json / .parquet — prêt pour les modèles.

⚠️ POINT-IN-TIME : chaque feature est calculée avec UNIQUEMENT
les données disponibles AVANT la date de la course.
On ne regarde JAMAIS le futur.

Usage : python3 feature_engineering.py

Modules :
  - feat_historique.py     → forme glissante (5/10/20 courses)
  - feat_croisements.py    → cheval × hippodrome, cheval × distance, cheval × terrain
  - feat_jockey.py         → stats jockey, entraîneur, combo
  - feat_interactions.py   → interactions entre features + signaux marché
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import json, os, time
from datetime import datetime

from utils.logging_setup import setup_logging
log = setup_logging("feature_engineering")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "../data_master")
INPUT = os.path.join(DATA_DIR, "partants_complets.json")
OUTPUT = os.path.join(DATA_DIR, "features_matrix.json")


def load_partants():
    """Charge partants_complets.json"""
    log.info(f"Chargement {INPUT}...")
    with open(INPUT, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"  → {len(data)} partants")
    return data


def sort_by_date(partants):
    """Trie les partants par date (crucial pour point-in-time)"""
    log.info("Tri par date...")
    def get_date(r):
        d = r.get("date_reunion_iso", "1900-01-01")
        return str(d)[:10]
    partants.sort(key=get_date)
    log.info(f"  → Du {get_date(partants[0])} au {get_date(partants[-1])}")
    return partants


def apply_features_historique(partants):
    """Features de forme glissante — POINT-IN-TIME SAFE"""
    from feat_historique import compute_historique
    return compute_historique(partants)


def apply_features_croisements(partants):
    """Features croisements cheval × contexte — POINT-IN-TIME SAFE"""
    from feat_croisements import compute_croisements
    return compute_croisements(partants)


def apply_features_jockey(partants):
    """Features jockey/entraîneur — POINT-IN-TIME SAFE"""
    from feat_jockey import compute_jockey_features
    return compute_jockey_features(partants)


def apply_features_interactions(partants):
    """Features d'interactions et signaux marché"""
    from feat_interactions import compute_interactions
    return compute_interactions(partants)


def apply_features_pedigree(partants):
    """Features pedigree avancées — lignée × terrain/distance"""
    from feat_pedigree import compute_pedigree_features
    return compute_pedigree_features(partants)


def apply_features_temporel(partants):
    """Features temporelles — saisonnalité, cycles"""
    from feat_temporel import compute_temporel
    return compute_temporel(partants)


def apply_features_sequences(partants):
    """Features séquences — patterns, musique, tendances"""
    from feat_sequences import compute_sequences
    return compute_sequences(partants)


def count_features(partants):
    """Compte le nombre de features uniques"""
    all_keys = set()
    for r in partants[:5000]:
        all_keys.update(r.keys())
    return len(all_keys)


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("FEATURE ENGINEERING — 500+ features")
    log.info("=" * 60)

    # Charger
    partants = load_partants()
    n_initial = count_features(partants)
    log.info(f"Features initiales: {n_initial}")

    # Trier par date (CRUCIAL pour point-in-time)
    partants = sort_by_date(partants)

    # Appliquer chaque module
    log.info("\n" + "=" * 60)
    log.info("MODULE 1 : Historique glissant")
    log.info("=" * 60)
    partants = apply_features_historique(partants)
    log.info(f"  → Features: {count_features(partants)}")

    log.info("\n" + "=" * 60)
    log.info("MODULE 2 : Croisements cheval × contexte")
    log.info("=" * 60)
    partants = apply_features_croisements(partants)
    log.info(f"  → Features: {count_features(partants)}")

    log.info("\n" + "=" * 60)
    log.info("MODULE 3 : Jockey / Entraîneur")
    log.info("=" * 60)
    partants = apply_features_jockey(partants)
    log.info(f"  → Features: {count_features(partants)}")

    log.info("\n" + "=" * 60)
    log.info("MODULE 4 : Interactions + Marché")
    log.info("=" * 60)
    partants = apply_features_interactions(partants)
    log.info(f"  → Features: {count_features(partants)}")

    log.info("\n" + "=" * 60)
    log.info("MODULE 5 : Pedigree avancé")
    log.info("=" * 60)
    partants = apply_features_pedigree(partants)
    log.info(f"  → Features: {count_features(partants)}")

    log.info("\n" + "=" * 60)
    log.info("MODULE 6 : Temporel / Saisonnalité")
    log.info("=" * 60)
    partants = apply_features_temporel(partants)
    log.info(f"  → Features: {count_features(partants)}")

    log.info("\n" + "=" * 60)
    log.info("MODULE 7 : Séquences / Patterns")
    log.info("=" * 60)
    partants = apply_features_sequences(partants)
    n_final = count_features(partants)
    log.info(f"  → Features: {n_final}")

    log.info(f"\n📊 RÉSUMÉ: {n_initial} → {n_final} features (+{n_final - n_initial})")

    # Sauvegarder
    log.info(f"\n💾 Sauvegarde {OUTPUT}...")
    tmp = OUTPUT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(partants, f, ensure_ascii=False)
    os.replace(tmp, OUTPUT)
    size = os.path.getsize(OUTPUT) / 1024 / 1024
    log.info(f"  → {size:.0f} MB")

    # Parquet
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        log.info("  Export Parquet...")
        table = pa.Table.from_pylist(partants)
        pq_path = OUTPUT.replace(".json", ".parquet")
        pq.write_table(table, pq_path, compression="zstd")
        log.info(f"  → {pq_path}: {os.path.getsize(pq_path)/1024/1024:.0f} MB")
    except Exception as e:
        log.warning(f"  Parquet échoué: {e}")

    elapsed = time.time() - start
    log.info(f"\n✅ FEATURE ENGINEERING TERMINÉ en {elapsed/60:.1f} min")
    log.info(f"   {len(partants)} partants × {n_final} features")


if __name__ == "__main__":
    main()
