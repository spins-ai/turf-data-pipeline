"""
feature_builders.precomputed_entity_joiner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Joins pre-computed per-entity data from scripts 05/06/08.
These files are indexed by name (nom_cheval, jockey name, sire/dam name)
and contain aggregated career-level stats.
"""

from __future__ import annotations

import json
import os
from typing import Any


_OUTPUT_BASE = os.path.join(os.path.dirname(__file__), "..", "output")


def _load_json_index(path: str, key: str) -> dict[str, dict]:
    """Load a JSON file and build a lookup dict by key."""
    if not os.path.exists(path):
        print(f"  [entity] Not found (skipped): {path}")
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    index = {}
    for rec in data:
        k = rec.get(key)
        if k:
            index[k] = rec
    print(f"  [entity] Loaded {len(index)} entities from {os.path.basename(path)}")
    return index


def _normalize_name(name: str | None) -> str:
    """Normalize a name for matching (uppercase, strip)."""
    if not name:
        return ""
    return name.strip().upper()


def build_precomputed_entity_features(partants: list[dict]) -> list[dict]:
    """Join pre-computed per-entity features from scripts 05, 06, 08.

    New features added (prefix ent_ = entity-level):

    From historique_chevaux (script 05) - 6 new:
    - ent_cheval_nb_courses_total: total career races (from aggregated history)
    - ent_cheval_gains_total: total career earnings (aggregated)
    - ent_cheval_nb_disciplines: number of distinct disciplines raced
    - ent_cheval_nb_hippodromes: number of distinct hippodromes
    - ent_cheval_anciennete_jours: days between first and current race
    - ent_cheval_nb_distances: number of distinct distance categories

    From historique_jockeys (script 06) - 5 new:
    - ent_jockey_nb_montes_total: total career rides
    - ent_jockey_taux_victoire_global: overall career win rate
    - ent_jockey_taux_place_global: overall career place rate
    - ent_jockey_nb_chevaux_montes: number of distinct horses ridden
    - ent_jockey_gains_total: total career earnings

    From historique_entraineurs (script 06) - 5 new:
    - ent_entraineur_nb_partants_total: total career starters
    - ent_entraineur_taux_victoire_global: overall career win rate
    - ent_entraineur_taux_place_global: overall career place rate
    - ent_entraineur_nb_chevaux: number of distinct horses trained
    - ent_entraineur_gains_total: total career earnings

    From pedigree enrichi (script 08) - 6 new:
    - ent_pere_nb_descendants: sire total offspring count
    - ent_pere_taux_victoire: sire offspring win rate
    - ent_pere_nb_disciplines: sire offspring discipline diversity
    - ent_mere_nb_descendants: dam total offspring count
    - ent_mere_taux_victoire: dam offspring win rate
    - ent_mere_nb_disciplines: dam offspring discipline diversity
    """
    from datetime import datetime

    # Load all entity files
    cheval_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "05_historique_chevaux", "historique_chevaux.json"),
        key="nom_cheval",
    )
    jockey_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "06_historique_jockeys", "historique_jockeys.json"),
        key="nom",
    )
    entraineur_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "06_historique_jockeys", "historique_entraineurs.json"),
        key="nom",
    )
    pere_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "08_pedigree", "pedigree_peres.json"),
        key="nom_pere",
    )
    mere_idx = _load_json_index(
        os.path.join(_OUTPUT_BASE, "08_pedigree", "pedigree_meres.json"),
        key="nom_mere",
    )

    # Normalize all keys for fuzzy matching
    cheval_norm = {_normalize_name(k): v for k, v in cheval_idx.items()}
    jockey_norm = {_normalize_name(k): v for k, v in jockey_idx.items()}
    entraineur_norm = {_normalize_name(k): v for k, v in entraineur_idx.items()}
    pere_norm = {_normalize_name(k): v for k, v in pere_idx.items()}
    mere_norm = {_normalize_name(k): v for k, v in mere_idx.items()}

    results = []
    stats = {"cheval": 0, "jockey": 0, "entraineur": 0, "pere": 0, "mere": 0}

    for p in partants:
        uid = p.get("partant_uid")
        row: dict[str, Any] = {"partant_uid": uid}

        # --- Historique cheval (script 05) ---
        nom = _normalize_name(p.get("nom_cheval"))
        cheval = cheval_norm.get(nom, {})
        if cheval:
            stats["cheval"] += 1

        row["ent_cheval_nb_courses_total"] = cheval.get("nb_courses_total")
        row["ent_cheval_gains_total"] = cheval.get("gains_total_euros")

        disciplines = cheval.get("disciplines")
        row["ent_cheval_nb_disciplines"] = len(disciplines) if isinstance(disciplines, list) else None

        hippos = cheval.get("hippodromes")
        row["ent_cheval_nb_hippodromes"] = len(hippos) if isinstance(hippos, list) else None

        distances = cheval.get("distances_courues")
        row["ent_cheval_nb_distances"] = len(set(distances)) if isinstance(distances, list) else None

        # Ancienneté: days since first race
        premiere = cheval.get("premiere_course_date")
        date_course = p.get("date_reunion_iso")
        if premiere and date_course:
            try:
                d1 = datetime.fromisoformat(str(premiere)[:10])
                d2 = datetime.fromisoformat(str(date_course)[:10])
                row["ent_cheval_anciennete_jours"] = (d2 - d1).days
            except (ValueError, TypeError):
                row["ent_cheval_anciennete_jours"] = None
        else:
            row["ent_cheval_anciennete_jours"] = None

        # --- Historique jockey (script 06) ---
        jockey_name = _normalize_name(p.get("jockey_driver"))
        jockey = jockey_norm.get(jockey_name, {})
        if jockey:
            stats["jockey"] += 1

        row["ent_jockey_nb_montes_total"] = jockey.get("nb_montes")
        row["ent_jockey_taux_victoire_global"] = jockey.get("taux_victoire")
        row["ent_jockey_taux_place_global"] = jockey.get("taux_place")
        row["ent_jockey_nb_chevaux_montes"] = jockey.get("chevaux_montes")
        row["ent_jockey_gains_total"] = jockey.get("gains_total_euros")

        # --- Historique entraineur (script 06) ---
        ent_name = _normalize_name(p.get("entraineur"))
        entraineur = entraineur_norm.get(ent_name, {})
        if entraineur:
            stats["entraineur"] += 1

        row["ent_entraineur_nb_partants_total"] = entraineur.get("nb_montes")
        row["ent_entraineur_taux_victoire_global"] = entraineur.get("taux_victoire")
        row["ent_entraineur_taux_place_global"] = entraineur.get("taux_place")
        row["ent_entraineur_nb_chevaux"] = entraineur.get("chevaux_montes")
        row["ent_entraineur_gains_total"] = entraineur.get("gains_total_euros")

        # --- Pedigree père (script 08) ---
        pere_name = _normalize_name(p.get("pere"))
        pere = pere_norm.get(pere_name, {})
        if pere:
            stats["pere"] += 1

        row["ent_pere_nb_descendants"] = pere.get("nb_descendants_courses")
        row["ent_pere_taux_victoire"] = pere.get("taux_victoire_descendants")
        pere_disc = pere.get("disciplines")
        row["ent_pere_nb_disciplines"] = len(pere_disc) if isinstance(pere_disc, list) else None

        # --- Pedigree mère (script 08) ---
        mere_name = _normalize_name(p.get("mere"))
        mere = mere_norm.get(mere_name, {})
        if mere:
            stats["mere"] += 1

        row["ent_mere_nb_descendants"] = mere.get("nb_descendants_courses")
        row["ent_mere_taux_victoire"] = mere.get("taux_victoire_descendants")
        mere_disc = mere.get("disciplines")
        row["ent_mere_nb_disciplines"] = len(mere_disc) if isinstance(mere_disc, list) else None

        results.append(row)

    n = len(partants)
    print(f"  [entity] Match rates: cheval={stats['cheval']}/{n}, jockey={stats['jockey']}/{n}, "
          f"entraineur={stats['entraineur']}/{n}, pere={stats['pere']}/{n}, mere={stats['mere']}/{n}")
    return results
