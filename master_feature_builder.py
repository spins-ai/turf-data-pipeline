#!/usr/bin/env python3
"""
master_feature_builder.py  (STREAMING version)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Two-pass streaming pipeline that builds 400+ features per partant
WITHOUT loading all records into memory.

Pass 1: Scan partants_master.jsonl line by line to build lightweight
        indexes (defaultdicts with only the fields each builder needs).
        RAM usage: ~2-4 GB for indexes vs 50 GB for full records.

Pass 2: Re-scan partants_master.jsonl, compute ALL features for each
        record using the indexes, and write immediately to output JSONL.

Usage:
    python master_feature_builder.py
    python master_feature_builder.py --input data_master/partants_master.jsonl
    python master_feature_builder.py --input ... --output output/features/features_matrix.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from typing import Any, Optional

# ============================================================================
# PATHS
# ============================================================================

from utils.logging_setup import setup_logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DEFAULT = os.path.join(BASE_DIR, "data_master", "partants_master.jsonl")
OUTPUT_DEFAULT = os.path.join(BASE_DIR, "output", "features", "features_matrix.jsonl")

# External data paths
SMARKETS_PATH = os.path.join(BASE_DIR, "output", "30_smarkets", "smarkets.jsonl")
RP_PATH = os.path.join(BASE_DIR, "output", "37_racing_post", "racing_post.jsonl")
REUNIONS_PATH = os.path.join(BASE_DIR, "output", "39_reunions_enrichies", "reunions.jsonl")
ENRICHED_PATH = os.path.join(BASE_DIR, "output", "40_partants_enrichis", "partants_enrichis.jsonl")
CT_PATH = os.path.join(BASE_DIR, "output", "24_canalturf", "canalturf.jsonl")
TS_PATH = os.path.join(BASE_DIR, "output", "25_turfostats", "turfostats.jsonl")
GENY_PATH = os.path.join(BASE_DIR, "output", "26_geny", "geny.jsonl")
COURSES_PATH = os.path.join(BASE_DIR, "output", "02_liste_courses", "courses_normalisees.jsonl")
PERF_DET_PATH = os.path.join(BASE_DIR, "output", "22_performances_detaillees", "performances_detaillees.jsonl")

# Pre-computed data paths
_OUTPUT_BASE = os.path.join(BASE_DIR, "output")
COTES_PATH = os.path.join(_OUTPUT_BASE, "07_cotes_marche", "cotes_marche.json")
EQUIP_HIST_PATH = os.path.join(_OUTPUT_BASE, "09_equipements", "equipements_historique.json")
POIDS_HIST_PATH = os.path.join(_OUTPUT_BASE, "10_poids_handicaps", "poids_handicaps.json")
SECT_PATH = os.path.join(_OUTPUT_BASE, "11_sectionals", "sectionals.json")
CHEVAL_HIST_PATH = os.path.join(_OUTPUT_BASE, "05_historique_chevaux", "historique_chevaux.json")
JOCKEY_HIST_PATH = os.path.join(_OUTPUT_BASE, "06_historique_jockeys", "historique_jockeys.json")
ENTRAINEUR_HIST_PATH = os.path.join(_OUTPUT_BASE, "06_historique_jockeys", "historique_entraineurs.json")
PERE_PATH = os.path.join(_OUTPUT_BASE, "08_pedigree", "pedigree_peres.json")
MERE_PATH = os.path.join(_OUTPUT_BASE, "08_pedigree", "pedigree_meres.json")

# ============================================================================
# GENERIC HELPERS
# ============================================================================

def _safe_mean(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 4)


def _safe_stdev(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = sum(clean) / len(clean)
    variance = sum((v - mean) ** 2 for v in clean) / (len(clean) - 1)
    return round(variance ** 0.5, 4)


def _safe_min(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    return min(clean) if clean else None


def _safe_max(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    return max(clean) if clean else None


def _safe_median(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return round(statistics.median(clean), 2)


def _safe_percentile_rank(val, sorted_vals: list) -> Optional[float]:
    if val is None or not sorted_vals:
        return None
    below = sum(1 for v in sorted_vals if v < val)
    return round(below / len(sorted_vals), 4)


def _get_float(row: dict, key: str) -> Optional[float]:
    val = row.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _multiply(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return round(a * b, 6)


def _norm_name(name) -> Optional[str]:
    if not name:
        return None
    n = str(name).upper().strip()
    return n if len(n) >= 2 and n not in ("INCONNU", "NC", "N/A") else None


def _normalize_name(name) -> str:
    if not name:
        return ""
    import unicodedata
    n = str(name).upper().strip()
    n = unicodedata.normalize("NFD", n)
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")
    n = re.sub(r"[^A-Z0-9 ]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _distance_category(dist) -> Optional[str]:
    if dist is None:
        return None
    try:
        d = int(dist)
    except (ValueError, TypeError):
        return None
    if d < 1400:
        return "sprint"
    elif d < 1800:
        return "mile"
    elif d < 2200:
        return "inter"
    elif d < 2800:
        return "long"
    else:
        return "marathon"


def _sort_key(p: dict):
    """Standard chronological sort key."""
    return (
        str(p.get("date_reunion_iso", "") or ""),
        p.get("numero_reunion", 0) or 0,
        p.get("numero_course", 0) or 0,
        p.get("num_pmu", 0) or 0,
    )


def load_json_file(path: str, logger: logging.Logger) -> Any:
    """Load a JSON or JSONL file. Returns list of dicts."""
    if not os.path.exists(path):
        alt = path.replace(".jsonl", ".json") if path.endswith(".jsonl") else path.replace(".json", ".jsonl")
        if os.path.exists(alt):
            path = alt
        else:
            logger.warning("File not found: %s (skipping)", path)
            return []

    records = []
    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]

    logger.info("  Loaded %d records from %s", len(records), os.path.basename(path))
    return records


def load_json_index(path: str, key_field: str, logger: logging.Logger) -> dict:
    """Load a JSON/JSONL file and build a dict keyed by key_field."""
    records = load_json_file(path, logger)
    index = {}
    for rec in records:
        k = rec.get(key_field)
        if k:
            index[k] = rec
    return index


# ============================================================================
# MUSIQUE DECODER
# ============================================================================

_POSITION_PATTERN = re.compile(r'(\d+|[DTAR])([amphsc])', re.IGNORECASE)
_DISC_MAP = {
    "attele": "a", "monte": "m", "plat": "p", "haies": "h",
    "steeple": "s", "cross": "c", "trot attele": "a", "trot monte": "m",
}

_OEILLERES_VALUES = {
    "AUSTRALIENNES": 1, "OEILLERES": 2, "OEILLERES AUSTRALIENNES": 1,
    "OEILLERES NORMALES": 2,
}
_DEFERRE_VALUES = {
    "DA": 1, "DP": 2, "D4": 3, "DAP": 4, "DPG": 2, "DAG": 1,
}
_OEILLERES_TYPE_VALUES = {
    "AUSTRALIENNES": 1, "OEILLERES": 2, "OEILLERES AUSTRALIENNES": 1,
    "OEILLERES NORMALES": 2,
}
_DEFERRE_TYPE_VALUES = {
    "DA": 1, "DP": 2, "D4": 3, "DAP": 4, "DPG": 2, "DAG": 1,
}

SEXE_MAP = {
    "MALES": 0, "MALE": 0, "M": 0, "H": 0,
    "FEMELLES": 1, "FEMELLE": 1, "F": 1,
    "HONGRES": 2, "HONGRE": 2,
}

RACE_MAP = {
    "PUR-SANG": 0, "PS": 0, "THOROUGHBRED": 0,
    "AQPS": 1,
    "TROTTEUR": 2, "TROTTEUR FRANCAIS": 2, "TF": 2,
}

ROBE_MAP = {
    "BAI": 1, "B": 1,
    "BAI BRUN": 2, "BB": 2, "BAI FONCE": 2, "BBF": 2,
    "ALEZAN": 3, "AL": 3,
    "GRIS": 4, "GR": 4,
    "NOIR": 5, "N": 5,
    "BAI CLAIR": 6, "BC": 6,
    "ROUAN": 7,
    "AUBERE": 8,
}

BREED_MAP = {
    "PUR-SANG": 0, "PS": 0, "THOROUGHBRED": 0,
    "AQPS": 1,
    "TROTTEUR FRANCAIS": 2, "TF": 2, "TROTTEUR": 2,
    "ANGLO-ARABE": 3, "AA": 3,
    "ARABE": 4, "AR": 4,
    "SELLE FRANCAIS": 5, "SF": 5,
    "STANDARDBRED": 6,
}


def _decode_musique(musique: str | None) -> list[dict]:
    if not musique:
        return []
    results = []
    for m in _POSITION_PATTERN.finditer(musique):
        pos_str, disc = m.group(1), m.group(2).lower()
        if pos_str.isdigit():
            pos = int(pos_str)
        else:
            pos = None
        results.append({"position": pos, "discipline": disc, "raw": m.group(0)})
    return results


# ============================================================================
# PASS 1: BUILD INDEXES
# ============================================================================

def pass1_build_indexes(input_path: str, logger: logging.Logger) -> dict:
    """
    Single pass over partants_master.jsonl to build ALL lightweight indexes
    needed by the feature builders.

    Returns a dict of indexes keyed by name.
    """
    logger.info("=" * 72)
    logger.info("PASS 1: Building indexes (streaming, line-by-line)")
    logger.info("=" * 72)

    t0 = time.time()

    # ---- Indexes to build ----

    # Horse history: cheval -> list of {date, position, gains, temps_ms,
    #   reduction_km_ms, distance, discipline, hippodrome, allocation,
    #   surface, oeilleres, deferre, poids, cote, is_gagnant, is_place}
    horse_history = defaultdict(list)

    # Jockey history: jockey -> list of {date, position, gains, is_gagnant}
    jockey_stats = defaultdict(lambda: {"total": 0, "wins": 0})

    # Trainer history
    trainer_stats = defaultdict(lambda: {"total": 0, "wins": 0})

    # Horse stats for affinity
    horse_stats = defaultdict(lambda: {"total": 0, "wins": 0})

    # Combo histories: key -> list of {date, gagnant, place, gains}
    jt_history = defaultdict(list)   # jockey||trainer
    jh_history = defaultdict(list)   # jockey||horse
    th_history = defaultdict(list)   # trainer||horse
    j_hippo_history = defaultdict(list)
    t_hippo_history = defaultdict(list)
    j_dist_history = defaultdict(list)
    t_dist_history = defaultdict(list)
    horse_last_jockey = {}

    # Cheval-jockey duo: key -> list of {cl, gains}
    duo_history = defaultdict(list)

    # Cheval-hippodrome: key -> list of {cl, gains}
    cheval_hippo_history = defaultdict(list)

    # Cheval-distance: key -> list of {cl, gains, dist_cat}
    cheval_dist_history = defaultdict(list)

    # Cheval-terrain: key -> list of {cl, gains}
    cheval_terrain_history = defaultdict(list)

    # Jockey-entraineur combo
    je_combo_history = defaultdict(list)

    # Entraineur-hippodrome
    ent_hippo_history = defaultdict(list)

    # Course-level data: course_uid -> list of minimal runner info
    course_runners = defaultdict(list)

    # Course count per horse
    course_nb_partants = defaultdict(int)

    # Record count
    n_records = 0

    # Temporary list to store sort keys + line offsets for sorted pass2
    # We store (sort_key, line_number) so we can process in order in pass 2
    record_order = []

    with open(input_path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Bad JSON at line %d", lineno)
                continue

            n_records += 1

            # Extract common fields
            cheval = (p.get("nom_cheval") or "").upper().strip()
            jockey = (p.get("jockey_driver") or p.get("jockey") or p.get("nom_jockey") or "").upper().strip()
            trainer = (p.get("entraineur") or "").upper().strip()
            date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
            course_uid = p.get("course_uid", "")
            hippo = (p.get("hippodrome_normalise") or "").upper().strip()
            distance = p.get("distance")
            discipline = (p.get("discipline") or "").upper().strip()
            position = p.get("position_arrivee") or p.get("classement")
            is_gagnant = bool(p.get("is_gagnant"))
            is_place = bool(p.get("is_place"))

            gains_course = 0
            try:
                gains_course = float(p.get("gains_course") or p.get("gains") or 0)
            except (ValueError, TypeError):
                pass

            try:
                position_int = int(position) if position is not None else None
            except (ValueError, TypeError):
                position_int = None

            temps_ms = p.get("temps_ms")
            red_km = p.get("reduction_km_ms")
            poids = p.get("poids_porte_kg")
            cote = p.get("cote_finale") or p.get("rapport_pmu")
            oeilleres = p.get("oeilleres")
            deferre = p.get("deferre")
            proba = p.get("proba_implicite")

            allocation = p.get("allocation_totale")
            surface = (p.get("type_piste") or "").upper().strip()
            dist_cat = _distance_category(distance)

            # Store sort key for ordering in pass 2
            sk = (date_iso, p.get("numero_reunion", 0) or 0,
                  p.get("numero_course", 0) or 0, p.get("num_pmu", 0) or 0)
            record_order.append((sk, lineno))

            # --- Build horse history ---
            if cheval:
                horse_history[cheval].append({
                    "date": date_iso,
                    "position": position_int,
                    "gains": gains_course,
                    "temps": temps_ms,
                    "reduction_km": red_km,
                    "distance": distance,
                    "discipline": discipline,
                    "hippodrome": hippo,
                    "allocation": allocation,
                    "surface": surface,
                    "oeilleres": oeilleres,
                    "deferre": deferre,
                    "poids": poids,
                    "cote": cote,
                    "is_gagnant": is_gagnant,
                    "is_place": is_place,
                })

            # --- Course runners ---
            course_runners[course_uid].append({
                "cheval": cheval,
                "jockey": jockey,
                "position": position_int,
                "temps_ms": temps_ms,
                "reduction_km_ms": red_km,
                "poids": poids,
                "cote": cote,
                "proba": proba,
                "gains": gains_course,
                "is_gagnant": is_gagnant,
                "num_pmu": p.get("num_pmu"),
            })

            if course_uid:
                course_nb_partants[course_uid] += 1

            # --- Combo histories ---
            entry = {"date": date_iso, "gagnant": is_gagnant, "place": is_place, "gains": gains_course}

            if jockey and trainer:
                jt_history[f"{jockey}||{trainer}"].append(entry)
            if jockey and cheval:
                jh_history[f"{jockey}||{cheval}"].append(entry)
            if trainer and cheval:
                th_history[f"{trainer}||{cheval}"].append(entry)
            if jockey and hippo:
                j_hippo_history[f"{jockey}||{hippo}"].append(entry)
            if trainer and hippo:
                t_hippo_history[f"{trainer}||{hippo}"].append(entry)
            if jockey and dist_cat:
                j_dist_history[f"{jockey}||{dist_cat}"].append(entry)
            if trainer and dist_cat:
                t_dist_history[f"{trainer}||{dist_cat}"].append(entry)

            # --- Duo history (cheval-jockey affinity) ---
            cheval_norm = _norm_name(p.get("nom_cheval"))
            jockey_norm = _norm_name(p.get("jockey") or p.get("nom_jockey"))
            if cheval_norm and jockey_norm:
                duo_history[f"{cheval_norm}|{jockey_norm}"].append({
                    "cl": position_int, "gains": gains_course,
                })
                horse_stats[cheval_norm]["total"] += 1
                jockey_stats[jockey_norm]["total"] += 1
                if position_int == 1:
                    horse_stats[cheval_norm]["wins"] += 1
                    jockey_stats[jockey_norm]["wins"] += 1

            # --- Cheval-hippodrome affinity ---
            if cheval_norm and hippo:
                cheval_hippo_history[f"{cheval_norm}|{hippo}"].append({
                    "cl": position_int, "gains": gains_course,
                })

            # --- Cheval-distance affinity ---
            if cheval_norm and dist_cat:
                cheval_dist_history[f"{cheval_norm}|{dist_cat}"].append({
                    "cl": position_int, "gains": gains_course,
                })

            # --- Cheval-terrain affinity ---
            terrain = discipline or surface
            if cheval_norm and terrain:
                cheval_terrain_history[f"{cheval_norm}|{terrain}"].append({
                    "cl": position_int, "gains": gains_course,
                })

            # --- Jockey-entraineur combo ---
            if jockey_norm and trainer:
                je_combo_history[f"{jockey_norm}|{trainer}"].append({
                    "cl": position_int, "gains": gains_course,
                })

            # --- Entraineur-hippodrome ---
            if trainer and hippo:
                ent_hippo_history[f"{trainer}|{hippo}"].append({
                    "cl": position_int, "gains": gains_course,
                })

            # --- Trainer stats ---
            if trainer:
                trainer_stats[trainer]["total"] += 1
                if is_gagnant:
                    trainer_stats[trainer]["wins"] += 1

            # --- Horse last jockey ---
            if cheval and jockey:
                horse_last_jockey[cheval] = jockey

            if n_records % 100_000 == 0:
                logger.info("  Pass 1: %d records scanned ...", n_records)

    # Sort each horse_history by date
    for key in horse_history:
        horse_history[key].sort(key=lambda r: r["date"])

    # Pre-compute course-level stats
    course_stats = {}
    for cuid, runners in course_runners.items():
        times = [r["temps_ms"] for r in runners if r.get("temps_ms")]
        reductions = [r["reduction_km_ms"] for r in runners if r.get("reduction_km_ms")]
        weights = [r["poids"] for r in runners if r.get("poids") is not None]
        winner_time = None
        for r in runners:
            if r.get("is_gagnant") and r.get("temps_ms"):
                winner_time = r["temps_ms"]
                break
        # Also find winner by position=1
        if winner_time is None:
            for r in runners:
                if r.get("position") == 1 and r.get("temps_ms"):
                    winner_time = r["temps_ms"]
                    break

        stats = {}
        if times:
            stats["avg_time"] = sum(times) / len(times)
            stats["times_sorted"] = sorted(times)
        if reductions:
            stats["avg_reduction"] = sum(reductions) / len(reductions)
        if weights:
            stats["avg_weight"] = sum(weights) / len(weights)
            stats["max_weight"] = max(weights)
            stats["min_weight"] = min(weights)
            stats["weights_sorted"] = sorted(weights, reverse=True)
        stats["winner_time"] = winner_time
        stats["nb_runners"] = len(runners)
        course_stats[cuid] = stats

    # Sort record_order for pass 2 processing order
    record_order.sort(key=lambda x: x[0])

    elapsed = time.time() - t0
    logger.info("  Pass 1 complete: %d records, %d horses, %d courses (%.1fs)",
                n_records, len(horse_history), len(course_runners), elapsed)
    logger.info("  Index sizes: horse_history=%d, course_runners=%d, duo=%d",
                len(horse_history), len(course_runners), len(duo_history))

    return {
        "horse_history": horse_history,
        "horse_stats": horse_stats,
        "jockey_stats": jockey_stats,
        "trainer_stats": trainer_stats,
        "jt_history": jt_history,
        "jh_history": jh_history,
        "th_history": th_history,
        "j_hippo_history": j_hippo_history,
        "t_hippo_history": t_hippo_history,
        "j_dist_history": j_dist_history,
        "t_dist_history": t_dist_history,
        "horse_last_jockey": horse_last_jockey,
        "duo_history": duo_history,
        "cheval_hippo_history": cheval_hippo_history,
        "cheval_dist_history": cheval_dist_history,
        "cheval_terrain_history": cheval_terrain_history,
        "je_combo_history": je_combo_history,
        "ent_hippo_history": ent_hippo_history,
        "course_runners": course_runners,
        "course_stats": course_stats,
        "course_nb_partants": course_nb_partants,
        "n_records": n_records,
        "record_order": record_order,
    }


# ============================================================================
# EXTERNAL DATA LOADERS
# ============================================================================

def load_external_indexes(logger: logging.Logger) -> dict:
    """Load all external data files into indexes (these are small)."""
    logger.info("")
    logger.info("Loading external data indexes ...")

    ext = {}

    # Courses lookup
    courses = load_json_file(COURSES_PATH, logger)
    course_lookup = {}
    for c in courses:
        cuid = c.get("course_uid")
        if cuid:
            course_lookup[cuid] = c
    ext["course_lookup"] = course_lookup
    logger.info("  course_lookup: %d", len(course_lookup))

    # Smarkets
    try:
        from feature_builders.smarkets_builder import index_smarkets, load_json_or_jsonl as sm_load
        if os.path.exists(SMARKETS_PATH):
            sm_data = sm_load(SMARKETS_PATH, logger)
            ext["smarkets_idx"] = index_smarkets(sm_data, logger) if sm_data else {}
        else:
            ext["smarkets_idx"] = {}
    except Exception as exc:
        logger.warning("  smarkets index failed: %s", exc)
        ext["smarkets_idx"] = {}

    # Racing Post
    try:
        from feature_builders.racing_post_builder import index_rp_data, load_json_or_jsonl as rp_load
        if os.path.exists(RP_PATH):
            rp_data = rp_load(RP_PATH, logger)
            ext["rp_idx"] = index_rp_data(rp_data, logger) if rp_data else {}
        else:
            ext["rp_idx"] = {}
    except Exception as exc:
        logger.warning("  racing_post index failed: %s", exc)
        ext["rp_idx"] = {}

    # Reunions
    try:
        from feature_builders.reunions_builder import index_reunions, load_json_or_jsonl as reu_load
        if os.path.exists(REUNIONS_PATH):
            reu_data = reu_load(REUNIONS_PATH, logger)
            ext["reunions_idx"] = index_reunions(reu_data, logger) if reu_data else {}
        else:
            ext["reunions_idx"] = {}
    except Exception as exc:
        logger.warning("  reunions index failed: %s", exc)
        ext["reunions_idx"] = {}

    # Enrichissement
    try:
        from feature_builders.enrichissement_builder import index_enriched, load_json_or_jsonl as enr_load
        if os.path.exists(ENRICHED_PATH):
            enr_data = enr_load(ENRICHED_PATH, logger)
            ext["enriched_idx"] = index_enriched(enr_data, logger) if enr_data else {}
        else:
            ext["enriched_idx"] = {}
    except Exception as exc:
        logger.warning("  enrichissement index failed: %s", exc)
        ext["enriched_idx"] = {}

    # Canalturf
    try:
        from feature_builders.canalturf_builder import index_ct_data, load_json_or_jsonl as ct_load
        if os.path.exists(CT_PATH):
            ct_data = ct_load(CT_PATH, logger)
            ext["ct_idx"] = index_ct_data(ct_data, logger) if ct_data else {}
        else:
            ext["ct_idx"] = {}
    except Exception as exc:
        logger.warning("  canalturf index failed: %s", exc)
        ext["ct_idx"] = {}

    # Turfostats
    try:
        from feature_builders.turfostats_builder import index_ts_data, load_json_or_jsonl as ts_load
        if os.path.exists(TS_PATH):
            ts_data = ts_load(TS_PATH, logger)
            ext["ts_idx"] = index_ts_data(ts_data, logger) if ts_data else {}
        else:
            ext["ts_idx"] = {}
    except Exception as exc:
        logger.warning("  turfostats index failed: %s", exc)
        ext["ts_idx"] = {}

    # Geny
    try:
        from feature_builders.geny_builder import index_geny_data, build_race_consensus, load_json_or_jsonl as geny_load
        if os.path.exists(GENY_PATH):
            geny_data = geny_load(GENY_PATH, logger)
            ext["geny_idx"] = index_geny_data(geny_data, logger) if geny_data else {}
            ext["geny_consensus"] = build_race_consensus(geny_data, logger) if geny_data else {}
        else:
            ext["geny_idx"] = {}
            ext["geny_consensus"] = {}
    except Exception as exc:
        logger.warning("  geny index failed: %s", exc)
        ext["geny_idx"] = {}
        ext["geny_consensus"] = {}

    # Meteo index
    try:
        from feature_builders.meteo_features import load_meteo_index
        ext["meteo_idx"] = load_meteo_index(logger) if hasattr(load_meteo_index, '__call__') else {}
    except Exception:
        ext["meteo_idx"] = {}

    # Precomputed partant-level indexes
    ext["cotes_idx"] = load_json_index(COTES_PATH, "partant_uid", logger) if os.path.exists(COTES_PATH) else {}
    ext["equip_idx"] = load_json_index(EQUIP_HIST_PATH, "partant_uid", logger) if os.path.exists(EQUIP_HIST_PATH) else {}
    ext["poids_idx"] = load_json_index(POIDS_HIST_PATH, "partant_uid", logger) if os.path.exists(POIDS_HIST_PATH) else {}
    ext["sect_idx"] = load_json_index(SECT_PATH, "partant_uid", logger) if os.path.exists(SECT_PATH) else {}

    # Precomputed entity-level indexes
    cheval_idx = load_json_index(CHEVAL_HIST_PATH, "nom_cheval", logger) if os.path.exists(CHEVAL_HIST_PATH) else {}
    jockey_idx = load_json_index(JOCKEY_HIST_PATH, "nom", logger) if os.path.exists(JOCKEY_HIST_PATH) else {}
    entraineur_idx = load_json_index(ENTRAINEUR_HIST_PATH, "nom", logger) if os.path.exists(ENTRAINEUR_HIST_PATH) else {}
    pere_idx = load_json_index(PERE_PATH, "nom_pere", logger) if os.path.exists(PERE_PATH) else {}
    mere_idx = load_json_index(MERE_PATH, "nom_mere", logger) if os.path.exists(MERE_PATH) else {}

    ext["cheval_entity_idx"] = {_normalize_name(k): v for k, v in cheval_idx.items()}
    ext["jockey_entity_idx"] = {_normalize_name(k): v for k, v in jockey_idx.items()}
    ext["entraineur_entity_idx"] = {_normalize_name(k): v for k, v in entraineur_idx.items()}
    ext["pere_entity_idx"] = {_normalize_name(k): v for k, v in pere_idx.items()}
    ext["mere_entity_idx"] = {_normalize_name(k): v for k, v in mere_idx.items()}

    # Calculation scripts: try to load their indexes
    try:
        sys.path.insert(0, BASE_DIR)
        from importlib import import_module

        # 42 - Racing Post x PMU
        try:
            mod42 = import_module("42_croisement_racing_post_pmu")
            rp_data_42 = []
            for rp_path in [os.path.join(BASE_DIR, "output", "37_racing_post", "racing_post_fr.jsonl"),
                            os.path.join(BASE_DIR, "output", "37_racing_post", "racing_post_fr.json")]:
                if os.path.exists(rp_path):
                    rp_data_42 = load_json_file(rp_path, logger)
                    break
            ext["rp_index_42"] = mod42.build_rp_index(rp_data_42) if rp_data_42 else {}
            ext["mod42"] = mod42
        except Exception as exc:
            logger.warning("  42_croisement load failed: %s", exc)
            ext["rp_index_42"] = {}
            ext["mod42"] = None

        # 43 - meteo courses
        try:
            mod43 = import_module("43_croisement_meteo_courses")
            ext["meteo_idx_43"] = mod43.load_meteo_index() if hasattr(mod43, "load_meteo_index") else {}
            ext["mod43"] = mod43
        except Exception as exc:
            logger.warning("  43_croisement load failed: %s", exc)
            ext["meteo_idx_43"] = {}
            ext["mod43"] = None

        # 44 - pedigree partants
        try:
            mod44 = import_module("44_croisement_pedigree_partants")
            ext["pedigree_idx_44"] = mod44.load_pedigree_index() if hasattr(mod44, "load_pedigree_index") else {}
            ext["mod44"] = mod44
        except Exception as exc:
            logger.warning("  44_croisement load failed: %s", exc)
            ext["pedigree_idx_44"] = {}
            ext["mod44"] = None

        # 48 - parse conditions texte
        try:
            ext["mod48"] = import_module("48_parse_conditions_texte")
        except Exception as exc:
            logger.warning("  48_parse_conditions load failed: %s", exc)
            ext["mod48"] = None

        # 49 - ecart cotes
        try:
            mod49 = import_module("49_ecart_cotes_internet_national")
            ext["rapports_internet_49"] = mod49.load_rapports_internet() if hasattr(mod49, "load_rapports_internet") else {}
            ext["rapports_nationaux_49"] = mod49.load_rapports_nationaux() if hasattr(mod49, "load_rapports_nationaux") else {}
            ext["cotes_marche_49"] = mod49.load_cotes_marche() if hasattr(mod49, "load_cotes_marche") else {}
            ext["mod49"] = mod49
        except Exception as exc:
            logger.warning("  49_ecart_cotes load failed: %s", exc)
            ext["mod49"] = None

    except Exception as exc:
        logger.warning("  Calculation scripts import failed: %s", exc)

    logger.info("  External indexes loaded.")
    return ext


# ============================================================================
# FEATURE COMPUTATION FUNCTIONS (per-record, using indexes)
# ============================================================================

def compute_musique_features(p: dict) -> dict:
    """22 features from musique string."""
    decoded = _decode_musique(p.get("musique"))
    feat = {}

    if decoded:
        nb = len(decoded)
        positions = [d["position"] for d in decoded if d["position"] is not None]
        valid_positions = [pos for pos in positions if pos > 0]

        feat["musique_nb_courses"] = nb
        feat["musique_nb_victoires"] = sum(1 for pos in positions if pos == 1)
        feat["musique_nb_places"] = sum(1 for pos in positions if pos <= 3)
        feat["musique_nb_2eme"] = sum(1 for pos in positions if pos == 2)
        feat["musique_nb_3eme"] = sum(1 for pos in positions if pos == 3)
        feat["musique_nb_dnf"] = sum(
            1 for d in decoded if d["position"] is None or d["position"] == 0
        )
        feat["musique_nb_zero"] = sum(1 for d in decoded if d["position"] == 0)
        feat["musique_nb_disqualifications"] = sum(
            1 for d in decoded
            if d["position"] is None and d["raw"][0].upper() in ("D", "A", "T")
        )
        feat["musique_taux_victoire"] = round(feat["musique_nb_victoires"] / nb, 3) if nb else None
        feat["musique_taux_place"] = round(feat["musique_nb_places"] / nb, 3) if nb else None
        feat["musique_derniere_pos"] = decoded[0]["position"] if decoded else None
        feat["musique_avant_derniere_pos"] = decoded[1]["position"] if len(decoded) > 1 else None
        valid_5 = valid_positions[:5]
        valid_10 = valid_positions[:10]
        feat["musique_avg_pos_5"] = round(sum(valid_5) / len(valid_5), 2) if valid_5 else None
        feat["musique_avg_pos_10"] = round(sum(valid_10) / len(valid_10), 2) if valid_10 else None
        feat["musique_last_5_positions"] = [d["position"] for d in decoded[:5]]

        recent_3 = valid_positions[:3]
        prev_3 = valid_positions[3:6]
        if len(recent_3) >= 2 and len(prev_3) >= 2:
            avg_recent = sum(recent_3) / len(recent_3)
            avg_prev = sum(prev_3) / len(prev_3)
            trend_val = round(avg_prev - avg_recent, 2)
            feat["musique_trend"] = trend_val
            feat["musique_trend_label"] = 1 if trend_val > 0.5 else (-1 if trend_val < -0.5 else 0)
        else:
            feat["musique_trend"] = None
            feat["musique_trend_label"] = None

        disciplines = set(d["discipline"] for d in decoded)
        feat["musique_nb_disciplines"] = len(disciplines)
        current_disc = (p.get("discipline") or "").lower()
        current_code = _DISC_MAP.get(current_disc, "")
        if current_code and nb > 0:
            same = sum(1 for d in decoded if d["discipline"] == current_code)
            feat["musique_pct_meme_discipline"] = round(same / nb, 3)
        else:
            feat["musique_pct_meme_discipline"] = None

        consec_places = 0
        for d in decoded:
            if d["position"] is not None and 1 <= d["position"] <= 3:
                consec_places += 1
            else:
                break
        feat["musique_consecutive_places"] = consec_places

        consec_hors = 0
        for d in decoded:
            if d["position"] is None or d["position"] == 0 or d["position"] > 3:
                consec_hors += 1
            else:
                break
        feat["musique_consecutive_hors_places"] = consec_hors

        disc_list = [d["discipline"] for d in decoded]
        surface_changes = sum(1 for i in range(1, len(disc_list)) if disc_list[i] != disc_list[i - 1])
        feat["musique_surface_changes"] = surface_changes
    else:
        for k in ("musique_nb_courses", "musique_nb_victoires", "musique_nb_places",
                   "musique_nb_dnf", "musique_nb_zero", "musique_nb_disqualifications",
                   "musique_nb_2eme", "musique_nb_3eme",
                   "musique_taux_victoire", "musique_taux_place",
                   "musique_derniere_pos", "musique_avant_derniere_pos",
                   "musique_avg_pos_5", "musique_avg_pos_10",
                   "musique_last_5_positions",
                   "musique_trend", "musique_trend_label",
                   "musique_nb_disciplines", "musique_pct_meme_discipline",
                   "musique_consecutive_places", "musique_consecutive_hors_places",
                   "musique_surface_changes"):
            feat[k] = None

    return feat


def compute_profil_cheval_features(p: dict, course_nb: dict) -> dict:
    """24 horse profile features."""
    feat = {}
    age = p.get("age")
    feat["profil_age"] = age
    if age is not None:
        feat["profil_age_category"] = 2 if age <= 2 else (3 if age == 3 else (4 if age <= 5 else 5))
    else:
        feat["profil_age_category"] = None

    sexe = (p.get("sexe") or "").upper().strip()
    feat["profil_sexe_code"] = SEXE_MAP.get(sexe, 0)
    feat["profil_is_male"] = 1 if SEXE_MAP.get(sexe, 0) == 0 and sexe else 0
    feat["profil_is_female"] = 1 if SEXE_MAP.get(sexe) == 1 else 0
    feat["profil_is_hongre"] = 1 if SEXE_MAP.get(sexe) == 2 else 0

    race = (p.get("race") or "").upper().strip()
    feat["profil_race_code"] = RACE_MAP.get(race, 3)
    feat["profil_race_breed_encoded"] = BREED_MAP.get(race, 99)

    robe = (p.get("robe") or "").upper().strip()
    feat["profil_robe_encoded"] = ROBE_MAP.get(robe, 0)

    gains_c = p.get("gains_carriere_euros")
    gains_a = p.get("gains_annee_euros")
    feat["profil_gains_carriere_log"] = round(math.log1p(gains_c), 2) if gains_c is not None and gains_c >= 0 else None
    feat["profil_gains_annee_log"] = round(math.log1p(gains_a), 2) if gains_a is not None and gains_a >= 0 else None

    nb_courses = p.get("nb_courses_carriere")
    feat["profil_nb_courses_carriere"] = nb_courses
    feat["profil_is_inedit"] = 1 if p.get("is_inedit") else 0

    if nb_courses is not None:
        feat["profil_carriere_longueur"] = 0 if nb_courses <= 2 else (1 if nb_courses <= 10 else (2 if nb_courses <= 30 else 3))
    else:
        feat["profil_carriere_longueur"] = None

    nb_vic = p.get("nb_victoires_carriere")
    nb_place_c = p.get("nb_places_carriere")
    if nb_courses is not None and nb_courses > 0:
        feat["profil_taux_victoire_carriere"] = round((nb_vic or 0) / nb_courses, 3)
        feat["profil_taux_place_carriere"] = round((nb_place_c or 0) / nb_courses, 3)
        feat["profil_gains_par_course"] = round(gains_c / nb_courses, 2) if gains_c is not None and gains_c >= 0 else None
    else:
        feat["profil_taux_victoire_carriere"] = None
        feat["profil_taux_place_carriere"] = None
        feat["profil_gains_par_course"] = None

    cuid = p.get("course_uid")
    corde = p.get("place_corde")
    feat["profil_place_corde"] = corde
    nb = course_nb.get(cuid, 0)
    feat["profil_place_corde_relative"] = round(corde / nb, 3) if corde is not None and nb > 0 else None
    feat["profil_engagement"] = p.get("engagement") if p.get("engagement") is not None else 0
    feat["profil_jument_pleine"] = 1 if p.get("jument_pleine") else 0

    return feat


def compute_temps_features(p: dict, horse_hist: list, c_stats: dict) -> dict:
    """15 time/speed features."""
    feat = {}
    cheval = (p.get("nom_cheval") or "").upper().strip()
    date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
    cuid = p.get("course_uid")
    distance = p.get("distance")
    temps_ms = p.get("temps_ms")
    red_km = p.get("reduction_km_ms")

    feat["temps_temps_ms"] = temps_ms
    feat["temps_reduction_km_ms"] = red_km

    if temps_ms and distance and temps_ms > 0:
        feat["temps_vitesse_kmh"] = round((distance / 1000) / (temps_ms / 3_600_000), 2)
    else:
        feat["temps_vitesse_kmh"] = None

    stats = c_stats.get(cuid, {})
    if temps_ms and stats.get("winner_time"):
        feat["temps_relatif_vainqueur"] = temps_ms - stats["winner_time"]
        feat["temps_ecart_gagnant_pct"] = round(
            ((temps_ms - stats["winner_time"]) / stats["winner_time"]) * 100, 3
        ) if stats["winner_time"] > 0 else None
    else:
        feat["temps_relatif_vainqueur"] = None
        feat["temps_ecart_gagnant_pct"] = None

    if temps_ms and stats.get("avg_time"):
        feat["temps_ecart_moyen_champ"] = round(temps_ms - stats["avg_time"], 1)
    else:
        feat["temps_ecart_moyen_champ"] = None

    if temps_ms and stats.get("times_sorted"):
        feat["temps_rang_vitesse"] = sum(1 for t in stats["times_sorted"] if t < temps_ms) + 1
    else:
        feat["temps_rang_vitesse"] = None

    if red_km and stats.get("avg_reduction"):
        feat["temps_reduction_relative"] = round(red_km - stats["avg_reduction"], 1)
    else:
        feat["temps_reduction_relative"] = None

    # Historical reduction
    if cheval and horse_hist:
        past = [r for r in horse_hist if r["date"] < date_iso]
        prior_reds = [r["reduction_km"] for r in reversed(past) if r.get("reduction_km")]
        if prior_reds:
            last_5 = prior_reds[:5]
            last_10 = prior_reds[:10]
            feat["temps_avg_reduction_5"] = round(sum(last_5) / len(last_5), 1)
            feat["temps_avg_reduction_10"] = round(sum(last_10) / len(last_10), 1)
            feat["temps_best_reduction_5"] = min(last_5)
            feat["temps_best_reduction_10"] = min(last_10)
            if len(prior_reds) >= 3:
                mean_red = sum(prior_reds) / len(prior_reds)
                variance = sum((r - mean_red) ** 2 for r in prior_reds) / len(prior_reds)
                feat["temps_speed_consistency"] = round(variance ** 0.5, 1)
            else:
                feat["temps_speed_consistency"] = None
            recent_3 = prior_reds[:3]
            prev_3r = prior_reds[3:6]
            if len(recent_3) >= 2 and len(prev_3r) >= 2:
                feat["temps_reduction_trend"] = round(
                    sum(prev_3r) / len(prev_3r) - sum(recent_3) / len(recent_3), 1
                )
            else:
                feat["temps_reduction_trend"] = None
        else:
            for k in ("temps_avg_reduction_5", "temps_avg_reduction_10",
                       "temps_best_reduction_5", "temps_best_reduction_10",
                       "temps_speed_consistency", "temps_reduction_trend"):
                feat[k] = None
    else:
        for k in ("temps_avg_reduction_5", "temps_avg_reduction_10",
                   "temps_best_reduction_5", "temps_best_reduction_10",
                   "temps_speed_consistency", "temps_reduction_trend"):
            feat[k] = None

    return feat


def compute_equipement_features(p: dict, horse_hist: list) -> dict:
    """16 equipment features."""
    cheval = (p.get("nom_cheval") or "").upper().strip()
    date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
    oeilleres = p.get("oeilleres")
    deferre = p.get("deferre")

    oeil_code = _OEILLERES_VALUES.get(oeilleres, 0)
    def_code = _DEFERRE_VALUES.get(deferre, 0)

    feat = {}
    feat["equip_oeilleres_code"] = oeil_code
    feat["equip_has_oeilleres"] = 1 if oeil_code > 0 else 0
    feat["equip_oeilleres_type"] = _OEILLERES_TYPE_VALUES.get(oeilleres, 0)
    feat["equip_deferre_code"] = def_code
    feat["equip_has_deferre"] = 1 if def_code > 0 else 0
    feat["equip_deferre_type"] = _DEFERRE_TYPE_VALUES.get(deferre, 0)
    feat["equip_poids_monte_change"] = 1 if p.get("poids_monte_change") else 0

    # Historical equipment changes
    if cheval and horse_hist:
        past = [r for r in horse_hist if r["date"] < date_iso]
        if past:
            last = past[-1]
            last_oeil = _OEILLERES_VALUES.get(last.get("oeilleres"), 0)
            last_def = _DEFERRE_VALUES.get(last.get("deferre"), 0)
            feat["equip_oeilleres_change"] = 1 if oeil_code != last_oeil else 0
            feat["equip_deferre_change"] = 1 if def_code != last_def else 0
            feat["equip_oeilleres_added"] = 1 if oeil_code > 0 and last_oeil == 0 else 0
            feat["equip_oeilleres_removed"] = 1 if oeil_code == 0 and last_oeil > 0 else 0
            feat["equip_deferre_added"] = 1 if def_code > 0 and last_def == 0 else 0
            feat["equip_deferre_removed"] = 1 if def_code == 0 and last_def > 0 else 0

            # Count changes in last 5
            recent_5 = past[-5:]
            oeil_changes = sum(1 for i in range(1, len(recent_5))
                               if _OEILLERES_VALUES.get(recent_5[i].get("oeilleres"), 0) !=
                               _OEILLERES_VALUES.get(recent_5[i - 1].get("oeilleres"), 0))
            feat["equip_nb_oeilleres_changes_5"] = oeil_changes
            feat["equip_nb_courses_with_oeilleres"] = sum(
                1 for r in past if _OEILLERES_VALUES.get(r.get("oeilleres"), 0) > 0
            )
        else:
            for k in ("equip_oeilleres_change", "equip_deferre_change",
                       "equip_oeilleres_added", "equip_oeilleres_removed",
                       "equip_deferre_added", "equip_deferre_removed",
                       "equip_nb_oeilleres_changes_5", "equip_nb_courses_with_oeilleres"):
                feat[k] = None
    else:
        for k in ("equip_oeilleres_change", "equip_deferre_change",
                   "equip_oeilleres_added", "equip_oeilleres_removed",
                   "equip_deferre_added", "equip_deferre_removed",
                   "equip_nb_oeilleres_changes_5", "equip_nb_courses_with_oeilleres"):
            feat[k] = None

    return feat


def compute_poids_features(p: dict, horse_hist: list, c_stats: dict) -> dict:
    """15 weight/handicap features."""
    feat = {}
    cheval = (p.get("nom_cheval") or "").upper().strip()
    date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
    cuid = p.get("course_uid")
    poids = p.get("poids_porte_kg")

    feat["poids_poids_porte_kg"] = poids

    stats = c_stats.get(cuid, {})
    if poids is not None and stats.get("avg_weight"):
        feat["poids_ecart_moyen"] = round(poids - stats["avg_weight"], 2)
        feat["poids_ecart_max"] = round(poids - stats.get("max_weight", poids), 2)
        feat["poids_ecart_min"] = round(poids - stats.get("min_weight", poids), 2)
        w_sorted = stats.get("weights_sorted", [])
        feat["poids_rang"] = sum(1 for w in w_sorted if w > poids) + 1 if w_sorted else None
    else:
        feat["poids_ecart_moyen"] = None
        feat["poids_ecart_max"] = None
        feat["poids_ecart_min"] = None
        feat["poids_rang"] = None

    # Historical weight features
    if cheval and horse_hist:
        past = [r for r in horse_hist if r["date"] < date_iso]
        past_weights = [r["poids"] for r in past if r.get("poids") is not None]
        if past_weights and poids is not None:
            feat["poids_change_vs_last"] = round(poids - past_weights[-1], 2)
            feat["poids_avg_career"] = round(sum(past_weights) / len(past_weights), 2)
            feat["poids_change_vs_avg"] = round(poids - feat["poids_avg_career"], 2)
            feat["poids_max_career"] = max(past_weights)
            feat["poids_min_career"] = min(past_weights)
            feat["poids_is_lightest"] = 1 if poids <= min(past_weights) else 0
            feat["poids_is_heaviest"] = 1 if poids >= max(past_weights) else 0
        else:
            for k in ("poids_change_vs_last", "poids_avg_career", "poids_change_vs_avg",
                       "poids_max_career", "poids_min_career",
                       "poids_is_lightest", "poids_is_heaviest"):
                feat[k] = None
    else:
        for k in ("poids_change_vs_last", "poids_avg_career", "poids_change_vs_avg",
                   "poids_max_career", "poids_min_career",
                   "poids_is_lightest", "poids_is_heaviest"):
            feat[k] = None

    return feat


def compute_combo_features(p: dict, indexes: dict) -> dict:
    """13 jockey-trainer-horse combination features."""
    feat = {}
    cheval = (p.get("nom_cheval") or "").upper().strip()
    jockey = (p.get("jockey_driver") or "").upper().strip()
    trainer = (p.get("entraineur") or "").upper().strip()
    date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
    hippo = (p.get("hippodrome_normalise") or "").upper().strip()
    dist_cat = _distance_category(p.get("distance"))

    def _past_stats(history_key, combo_key):
        if not combo_key:
            return 0, 0, 0
        hist = indexes.get(history_key, {}).get(combo_key, [])
        past = [r for r in hist if r["date"] < date_iso]
        nb = len(past)
        wins = sum(1 for r in past if r["gagnant"])
        places = sum(1 for r in past if r["place"])
        return nb, wins, places

    jt_key = f"{jockey}||{trainer}" if jockey and trainer else None
    jh_key = f"{jockey}||{cheval}" if jockey and cheval else None
    th_key = f"{trainer}||{cheval}" if trainer and cheval else None

    jt_nb, jt_wins, jt_places = _past_stats("jt_history", jt_key)
    jh_nb, jh_wins, jh_places = _past_stats("jh_history", jh_key)
    th_nb, th_wins, th_places = _past_stats("th_history", th_key)

    feat["combo_jt_nb"] = jt_nb
    feat["combo_jt_taux_vic"] = round(jt_wins / jt_nb, 4) if jt_nb > 0 else None
    feat["combo_jt_taux_place"] = round(jt_places / jt_nb, 4) if jt_nb > 0 else None
    feat["combo_jh_nb"] = jh_nb
    feat["combo_jh_taux_vic"] = round(jh_wins / jh_nb, 4) if jh_nb > 0 else None
    feat["combo_th_nb"] = th_nb
    feat["combo_th_taux_vic"] = round(th_wins / th_nb, 4) if th_nb > 0 else None

    # Jockey changed?
    last_jockey = indexes.get("horse_last_jockey", {}).get(cheval)
    feat["combo_jockey_change"] = 1 if last_jockey and last_jockey != jockey else 0

    # Jockey on hippodrome
    j_hippo_key = f"{jockey}||{hippo}" if jockey and hippo else None
    j_hippo_nb, j_hippo_wins, _ = _past_stats("j_hippo_history", j_hippo_key)
    feat["combo_jockey_hippo_nb"] = j_hippo_nb
    feat["combo_jockey_hippo_taux_vic"] = round(j_hippo_wins / j_hippo_nb, 4) if j_hippo_nb > 0 else None

    # Trainer on hippodrome
    t_hippo_key = f"{trainer}||{hippo}" if trainer and hippo else None
    t_hippo_nb, t_hippo_wins, _ = _past_stats("t_hippo_history", t_hippo_key)
    feat["combo_trainer_hippo_nb"] = t_hippo_nb
    feat["combo_trainer_hippo_taux_vic"] = round(t_hippo_wins / t_hippo_nb, 4) if t_hippo_nb > 0 else None

    return feat


def compute_class_change_features(p: dict, horse_hist: list, course_lookup: dict) -> dict:
    """11 class-change features."""
    feat = {}
    cheval = (p.get("nom_cheval") or "").upper().strip()
    date_iso = str(p.get("date_reunion_iso", "") or "")[:10]
    course_uid = p.get("course_uid", "")
    distance = p.get("distance")
    discipline = (p.get("discipline") or "").upper().strip()
    hippo = (p.get("hippodrome_normalise") or "").upper().strip()

    course_info = course_lookup.get(course_uid, {})
    allocation = course_info.get("allocation_totale") or p.get("allocation_totale")

    if cheval and horse_hist:
        past = [r for r in horse_hist if r["date"] < date_iso]
        if past and allocation is not None:
            last = past[-1]
            last_alloc = last.get("allocation")
            if last_alloc is not None and last_alloc > 0:
                feat["allocation_diff_vs_last"] = round(allocation - last_alloc, 2)
                feat["allocation_ratio_vs_last"] = round(allocation / last_alloc, 4)
                feat["is_class_up"] = 1 if allocation > last_alloc else 0
                feat["is_class_down"] = 1 if allocation < last_alloc else 0
            else:
                feat["allocation_diff_vs_last"] = None
                feat["allocation_ratio_vs_last"] = None
                feat["is_class_up"] = None
                feat["is_class_down"] = None

            past_allocs = [r["allocation"] for r in past if r.get("allocation") is not None]
            feat["allocation_rank_career"] = _safe_percentile_rank(allocation, past_allocs) if past_allocs else None

            # Distance change
            last_dist = last.get("distance")
            if last_dist and distance:
                feat["distance_change"] = distance - last_dist
                feat["distance_change_pct"] = round((distance - last_dist) / last_dist * 100, 2) if last_dist > 0 else None
            else:
                feat["distance_change"] = None
                feat["distance_change_pct"] = None

            # Discipline change
            last_disc = last.get("discipline", "")
            feat["is_discipline_change"] = 1 if discipline and last_disc and discipline != last_disc else 0

            # Hippodrome change
            last_hippo = last.get("hippodrome", "")
            feat["is_hippodrome_change"] = 1 if hippo and last_hippo and hippo != last_hippo else 0

            # Days since last race
            if last.get("date") and date_iso:
                try:
                    from datetime import date as dt_date
                    d1 = dt_date.fromisoformat(last["date"])
                    d2 = dt_date.fromisoformat(date_iso)
                    feat["jours_depuis_derniere"] = (d2 - d1).days
                except Exception:
                    feat["jours_depuis_derniere"] = None
            else:
                feat["jours_depuis_derniere"] = None
        else:
            for k in ("allocation_diff_vs_last", "allocation_ratio_vs_last",
                       "allocation_rank_career", "is_class_up", "is_class_down",
                       "distance_change", "distance_change_pct",
                       "is_discipline_change", "is_hippodrome_change",
                       "jours_depuis_derniere"):
                feat[k] = None
    else:
        for k in ("allocation_diff_vs_last", "allocation_ratio_vs_last",
                   "allocation_rank_career", "is_class_up", "is_class_down",
                   "distance_change", "distance_change_pct",
                   "is_discipline_change", "is_hippodrome_change",
                   "jours_depuis_derniere"):
            feat[k] = None

    return feat


def compute_perf_detaillees_features(p: dict, horse_hist: list) -> dict:
    """40-60 rolling performance features."""
    feat = {}
    cheval = (p.get("nom_cheval") or "").upper().strip()
    date_iso = str(p.get("date_reunion_iso", "") or "")[:10]

    if not cheval or not horse_hist:
        return feat

    past = [r for r in horse_hist if r["date"] < date_iso]
    if not past:
        return feat

    for wname, w in [("5", 5), ("10", 10), ("20", 20)]:
        window = past[-w:]
        positions = [r["position"] for r in window if r.get("position")]
        temps = [r["temps"] for r in window if r.get("temps")]
        reductions = [r["reduction_km"] for r in window if r.get("reduction_km")]
        gains = [r["gains"] for r in window if r.get("gains") is not None]

        feat[f"perf_pos_moy_{wname}"] = _safe_mean(positions)
        feat[f"perf_pos_std_{wname}"] = _safe_stdev(positions)
        feat[f"perf_pos_min_{wname}"] = _safe_min(positions)
        feat[f"perf_pos_max_{wname}"] = _safe_max(positions)
        feat[f"perf_pos_median_{wname}"] = _safe_median(positions)
        feat[f"perf_temps_moy_{wname}"] = _safe_mean(temps)
        feat[f"perf_temps_std_{wname}"] = _safe_stdev(temps)
        feat[f"perf_temps_best_{wname}"] = _safe_min(temps)
        feat[f"perf_red_moy_{wname}"] = _safe_mean(reductions)
        feat[f"perf_red_best_{wname}"] = _safe_min(reductions)
        feat[f"perf_gains_total_{wname}"] = round(sum(gains), 2) if gains else None
        feat[f"perf_gains_moy_{wname}"] = round(sum(gains) / len(gains), 2) if gains else None
        feat[f"perf_nb_victoires_{wname}"] = sum(1 for r in window if r.get("position") == 1)
        feat[f"perf_nb_places_{wname}"] = sum(1 for r in window if r.get("position") is not None and r["position"] <= 3)
        feat[f"perf_taux_victoire_{wname}"] = round(
            feat[f"perf_nb_victoires_{wname}"] / len(window), 3
        ) if window else None
        feat[f"perf_taux_place_{wname}"] = round(
            feat[f"perf_nb_places_{wname}"] / len(window), 3
        ) if window else None

    # Forme features
    feat["forme_victoire_5"] = feat.get("perf_taux_victoire_5")
    feat["forme_place_5"] = feat.get("perf_taux_place_5")

    return feat


def compute_affinity_features(p: dict, indexes: dict) -> dict:
    """All affinity features (cheval-jockey, cheval-hippo, cheval-distance, cheval-terrain,
    jockey-entraineur, entraineur-hippodrome)."""
    feat = {}

    cheval_norm = _norm_name(p.get("nom_cheval"))
    jockey_norm = _norm_name(p.get("jockey") or p.get("nom_jockey"))
    hippo = (p.get("hippodrome_normalise") or "").upper().strip()
    dist_cat = _distance_category(p.get("distance"))
    discipline = (p.get("discipline") or "").upper().strip()
    surface = (p.get("type_piste") or "").upper().strip()
    terrain = discipline or surface
    trainer = (p.get("entraineur") or "").upper().strip()

    def _affinity_stats(history_dict, key, prefix):
        """Compute affinity features from a history list."""
        hist = history_dict.get(key, []) if key else []
        # Point-in-time: for affinity scripts the data is already accumulated in order
        # so we use all entries up to current index (already sorted chronologically)
        n = len(hist)
        if n == 0:
            return {}
        # Since we use all entries (the affinity scripts operate on already-sorted data),
        # we treat the last entry as the current one and use all previous
        # Actually for pass-2 we use ALL accumulated history, which represents
        # all races up to the current one (because we process in chronological order).
        # The current record's own entry was added in pass 1, so we use n-1 for "past"
        past_n = n - 1  # exclude the current record itself
        if past_n <= 0:
            feat_out = {f"{prefix}_nb_courses": 0, f"{prefix}_is_first_time": True}
            return feat_out
        past = hist[:past_n]
        wins = sum(1 for r in past if r.get("cl") == 1)
        places = sum(1 for r in past if r.get("cl") is not None and r["cl"] <= 3)
        gains = sum(r.get("gains", 0) for r in past)
        feat_out = {
            f"{prefix}_nb_courses": past_n,
            f"{prefix}_victoires": wins,
            f"{prefix}_places": places,
            f"{prefix}_taux_vic": round(wins / past_n, 4),
            f"{prefix}_taux_place": round(places / past_n, 4),
            f"{prefix}_gains_total": round(gains, 2),
            f"{prefix}_gains_moy": round(gains / past_n, 2),
            f"{prefix}_is_first_time": False,
            f"{prefix}_last_result": past[-1].get("cl"),
        }
        return feat_out

    # Cheval-Jockey affinity
    if cheval_norm and jockey_norm:
        duo_key = f"{cheval_norm}|{jockey_norm}"
        cj = _affinity_stats(indexes.get("duo_history", {}), duo_key, "aff_cj")
        feat.update(cj)

        # Affinity score
        if cj.get("aff_cj_nb_courses", 0) > 0:
            h_st = indexes.get("horse_stats", {}).get(cheval_norm, {"total": 0, "wins": 0})
            j_st = indexes.get("jockey_stats", {}).get(jockey_norm, {"total": 0, "wins": 0})
            h_rate = h_st["wins"] / h_st["total"] if h_st["total"] > 0 else 0
            j_rate = j_st["wins"] / j_st["total"] if j_st["total"] > 0 else 0
            expected = (h_rate + j_rate) / 2
            duo_rate = cj.get("aff_cj_victoires", 0) / cj["aff_cj_nb_courses"]
            feat["aff_cj_affinity_score"] = round(duo_rate / expected, 2) if expected > 0 else None

    # Cheval-Hippodrome affinity
    if cheval_norm and hippo:
        ch_key = f"{cheval_norm}|{hippo}"
        feat.update(_affinity_stats(indexes.get("cheval_hippo_history", {}), ch_key, "aff_ch"))

    # Cheval-Distance affinity
    if cheval_norm and dist_cat:
        cd_key = f"{cheval_norm}|{dist_cat}"
        feat.update(_affinity_stats(indexes.get("cheval_dist_history", {}), cd_key, "aff_cd"))

    # Cheval-Terrain affinity
    if cheval_norm and terrain:
        ct_key = f"{cheval_norm}|{terrain}"
        ct_feats = _affinity_stats(indexes.get("cheval_terrain_history", {}), ct_key, "aff_ct")
        # Map to match original feat_cheval_terrain_affinity naming
        feat.update(ct_feats)
        if ct_feats.get("aff_ct_taux_victoire"):
            feat["affin_disc_taux_victoire"] = ct_feats.get("aff_ct_taux_vic")

    # Jockey-Entraineur combo
    if jockey_norm and trainer:
        je_key = f"{jockey_norm}|{trainer}"
        feat.update(_affinity_stats(indexes.get("je_combo_history", {}), je_key, "aff_je"))

    # Entraineur-Hippodrome
    if trainer and hippo:
        eh_key = f"{trainer}|{hippo}"
        feat.update(_affinity_stats(indexes.get("ent_hippo_history", {}), eh_key, "aff_eh"))

    return feat


def compute_value_betting_features(p: dict) -> dict:
    """Value betting features from odds/probability data."""
    feat = {}
    cote = p.get("cote_finale") or p.get("rapport_pmu")
    proba = p.get("proba_implicite") or p.get("proba_normalisee")

    try:
        cote_f = float(cote) if cote is not None else None
    except (ValueError, TypeError):
        cote_f = None

    try:
        proba_f = float(proba) if proba is not None else None
    except (ValueError, TypeError):
        proba_f = None

    if cote_f is not None and cote_f > 1:
        feat["vb_cote_finale"] = cote_f
        feat["vb_proba_implicite"] = round(1 / cote_f, 4)
    else:
        feat["vb_cote_finale"] = cote_f
        feat["vb_proba_implicite"] = proba_f

    if proba_f is not None:
        feat["vb_proba_normalisee"] = proba_f
        feat["vb_log_proba"] = round(math.log(proba_f), 4) if proba_f > 0 else None
    else:
        feat["vb_proba_normalisee"] = None
        feat["vb_log_proba"] = None

    feat["vb_is_favori"] = 1 if p.get("is_favori") else 0
    feat["vb_rang_cote"] = p.get("rang_cote")

    nb_partants = p.get("nb_partants")
    rang_cote = p.get("rang_cote")
    if rang_cote is not None and nb_partants is not None and nb_partants > 0:
        feat["vb_rang_cote_pct"] = round(rang_cote / nb_partants, 4)
    else:
        feat["vb_rang_cote_pct"] = None

    return feat


def compute_meteo_terrain_interaction(p: dict) -> dict:
    """Meteo-terrain interaction features."""
    feat = {}
    temp = p.get("meteo_temperature_c") or p.get("temperature")
    precip = p.get("meteo_precipitation_mm") or p.get("precipitation_mm")
    wind = p.get("meteo_wind_speed_kmh") or p.get("wind_speed_kmh")
    terrain = (p.get("etat_terrain") or "").upper().strip()

    try:
        temp_f = float(temp) if temp is not None else None
    except (ValueError, TypeError):
        temp_f = None
    try:
        precip_f = float(precip) if precip is not None else None
    except (ValueError, TypeError):
        precip_f = None
    try:
        wind_f = float(wind) if wind is not None else None
    except (ValueError, TypeError):
        wind_f = None

    # Terrain quality score
    terrain_scores = {"BON": 1, "TRES BON": 0.5, "ASSEZ BON": 1.5, "LEGER": 1, "SOUPLE": 2,
                      "TRES SOUPLE": 2.5, "COLLANT": 3, "LOURD": 3.5, "TRES LOURD": 4}
    terrain_score = terrain_scores.get(terrain)
    feat["mti_terrain_score"] = terrain_score

    if temp_f is not None and terrain_score is not None:
        feat["mti_temp_x_terrain"] = round(temp_f * terrain_score, 2)
    else:
        feat["mti_temp_x_terrain"] = None

    if precip_f is not None and terrain_score is not None:
        feat["mti_precip_x_terrain"] = round(precip_f * terrain_score, 2)
    else:
        feat["mti_precip_x_terrain"] = None

    if wind_f is not None:
        feat["mti_wind_category"] = 0 if wind_f < 15 else (1 if wind_f < 30 else 2)
    else:
        feat["mti_wind_category"] = None

    return feat


def compute_pedigree_discipline_match(p: dict) -> dict:
    """Pedigree-discipline match features."""
    feat = {}
    discipline = (p.get("discipline") or "").upper().strip()
    pere = (p.get("pere") or p.get("nom_pere") or "").upper().strip()
    mere = (p.get("mere") or p.get("nom_mere") or "").upper().strip()

    # These features require the entity indexes, so they are basic placeholders
    # The real computation happens in compute_precomputed_entity_features
    feat["pdm_has_pere"] = 1 if pere else 0
    feat["pdm_has_mere"] = 1 if mere else 0

    return feat


def compute_field_strength_features(p: dict, course_runners_data: list) -> dict:
    """10 field strength features."""
    feat = {}
    nb_partants = len(course_runners_data)
    if nb_partants < 2:
        return feat

    probas = []
    for r in course_runners_data:
        pi = r.get("proba")
        try:
            pi = float(pi) if pi else None
        except (ValueError, TypeError):
            pi = None
        probas.append(pi)

    valid_probas = [pi for pi in probas if pi is not None and pi > 0]
    if not valid_probas:
        return feat

    avg_proba = sum(valid_probas) / len(valid_probas)
    sorted_probas = sorted(valid_probas, reverse=True)
    hhi = sum(pi ** 2 for pi in valid_probas)
    entropy = -sum(pi * math.log(pi) for pi in valid_probas if pi > 0)

    if len(valid_probas) > 1:
        mean_p = avg_proba
        var_p = sum((pi - mean_p) ** 2 for pi in valid_probas) / len(valid_probas)
        heterogeneity = var_p ** 0.5
    else:
        heterogeneity = 0

    seuil = 1.0 / (2 * nb_partants)
    nb_competitifs = sum(1 for pi in valid_probas if pi > seuil)
    favori_dom = sorted_probas[0] - sorted_probas[1] if len(sorted_probas) >= 2 else None
    pct_above = sum(1 for pi in valid_probas if pi > avg_proba) / len(valid_probas)

    proba_ranking = {}
    for rank, pi in enumerate(sorted_probas, 1):
        if pi not in proba_ranking:
            proba_ranking[pi] = rank

    feat["fs_field_avg_proba"] = round(avg_proba, 4)
    feat["fs_field_hhi"] = round(hhi, 4)
    feat["fs_field_entropy"] = round(entropy, 4)
    feat["fs_field_heterogeneity"] = round(heterogeneity, 4)
    feat["fs_field_nb_competitifs"] = nb_competitifs
    feat["fs_favori_dominance"] = round(favori_dom, 4) if favori_dom is not None else None
    feat["fs_pct_above_avg"] = round(pct_above, 4)

    # Per-runner fields: need to find this runner in the course
    my_proba = p.get("proba_implicite")
    try:
        my_proba = float(my_proba) if my_proba else None
    except (ValueError, TypeError):
        my_proba = None

    if my_proba is not None and my_proba > 0:
        feat["fs_rank_in_field"] = proba_ranking.get(my_proba, nb_partants)
        feat["fs_is_top3_market"] = proba_ranking.get(my_proba, nb_partants) <= 3
        feat["fs_relative_strength"] = round(my_proba / avg_proba, 4) if avg_proba > 0 else None
    else:
        feat["fs_rank_in_field"] = None
        feat["fs_is_top3_market"] = None
        feat["fs_relative_strength"] = None

    feat["nb_partants"] = nb_partants

    return feat


def compute_precomputed_partant_features(p: dict, ext: dict) -> dict:
    """14 pre-computed per-partant features."""
    feat = {}
    uid = p.get("partant_uid")

    cotes = ext.get("cotes_idx", {}).get(uid, {})
    feat["pc_cote_moyenne_course"] = cotes.get("cote_moyenne_course")
    feat["pc_cote_mediane_course"] = cotes.get("cote_mediane_course")
    feat["pc_ecart_cote_moyenne"] = cotes.get("ecart_cote_moyenne")

    equip = ext.get("equip_idx", {}).get(uid, {})
    feat["pc_oeilleres_prev"] = equip.get("oeilleres_prev")
    feat["pc_retrait_oeilleres"] = 1 if equip.get("retrait_oeilleres") else 0
    feat["pc_nb_courses_sans_oeilleres"] = equip.get("nb_courses_sans_oeilleres")
    feat["pc_deferre_prev"] = equip.get("deferre_prev")

    poids = ext.get("poids_idx", {}).get(uid, {})
    feat["pc_poids_precedent"] = poids.get("poids_precedent")
    feat["pc_ecart_poids"] = poids.get("ecart_poids")
    feat["pc_handicap_valeur"] = poids.get("handicap_valeur")

    sect = ext.get("sect_idx", {}).get(uid, {})
    feat["pc_sectional_200m"] = sect.get("sectional_200m")
    feat["pc_sectional_400m"] = sect.get("sectional_400m")
    feat["pc_sectional_600m"] = sect.get("sectional_600m")
    feat["pc_sectional_rank"] = sect.get("sectional_rank")

    return feat


def compute_precomputed_entity_features(p: dict, ext: dict) -> dict:
    """22 pre-computed entity features."""
    feat = {}

    nom = _normalize_name(p.get("nom_cheval"))
    cheval = ext.get("cheval_entity_idx", {}).get(nom, {})
    feat["ent_cheval_nb_courses_total"] = cheval.get("nb_courses_total")
    feat["ent_cheval_gains_total"] = cheval.get("gains_total_euros")
    disciplines = cheval.get("disciplines")
    feat["ent_cheval_nb_disciplines"] = len(disciplines) if isinstance(disciplines, list) else None

    jockey_name = _normalize_name(p.get("jockey_driver") or p.get("jockey") or p.get("nom_jockey"))
    jockey = ext.get("jockey_entity_idx", {}).get(jockey_name, {})
    feat["ent_jockey_nb_courses_total"] = jockey.get("nb_courses_total")
    feat["ent_jockey_taux_victoire"] = jockey.get("taux_victoire")
    feat["ent_jockey_taux_place"] = jockey.get("taux_place")
    feat["ent_jockey_gains_total"] = jockey.get("gains_total_euros")
    feat["jockey_taux_victoire_365j"] = jockey.get("taux_victoire_365j") or jockey.get("taux_victoire")

    ent_name = _normalize_name(p.get("entraineur"))
    entraineur = ext.get("entraineur_entity_idx", {}).get(ent_name, {})
    feat["ent_entraineur_nb_courses_total"] = entraineur.get("nb_courses_total")
    feat["ent_entraineur_taux_victoire"] = entraineur.get("taux_victoire")
    feat["ent_entraineur_taux_place"] = entraineur.get("taux_place")
    feat["ent_entraineur_gains_total"] = entraineur.get("gains_total_euros")

    pere_name = _normalize_name(p.get("pere") or p.get("nom_pere"))
    pere = ext.get("pere_entity_idx", {}).get(pere_name, {})
    feat["ent_pere_nb_produits"] = pere.get("nb_produits")
    feat["ent_pere_taux_victoire"] = pere.get("taux_victoire")
    feat["ent_pere_gains_moy_produit"] = pere.get("gains_moy_produit")

    mere_name = _normalize_name(p.get("mere") or p.get("nom_mere"))
    mere = ext.get("mere_entity_idx", {}).get(mere_name, {})
    feat["ent_mere_nb_produits"] = mere.get("nb_produits")
    feat["ent_mere_taux_victoire"] = mere.get("taux_victoire")
    feat["ent_mere_gains_moy_produit"] = mere.get("gains_moy_produit")

    return feat


def compute_external_builder_features(p: dict, ext: dict, logger: logging.Logger) -> dict:
    """Features from external data builders (smarkets, racing_post, etc.)."""
    feat = {}

    # Each of these builders expects (partants, idx, logger) and returns a list.
    # We call them with a single-record list and extract the result.
    uid = p.get("partant_uid")
    single = [dict(p)]  # shallow copy to avoid mutation

    # Smarkets
    smarkets_idx = ext.get("smarkets_idx", {})
    if smarkets_idx:
        try:
            from feature_builders.smarkets_builder import build_smarkets_features
            result = build_smarkets_features(single, smarkets_idx, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # Racing Post
    rp_idx = ext.get("rp_idx", {})
    if rp_idx:
        try:
            from feature_builders.racing_post_builder import build_racing_post_features
            result = build_racing_post_features(single, rp_idx, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # Reunions
    reunions_idx = ext.get("reunions_idx", {})
    if reunions_idx:
        try:
            from feature_builders.reunions_builder import build_reunions_features
            result = build_reunions_features(single, reunions_idx, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # Enrichissement
    enriched_idx = ext.get("enriched_idx", {})
    if enriched_idx:
        try:
            from feature_builders.enrichissement_builder import build_enrichissement_features
            result = build_enrichissement_features(single, enriched_idx, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # Canalturf
    ct_idx = ext.get("ct_idx", {})
    if ct_idx:
        try:
            from feature_builders.canalturf_builder import build_canalturf_features
            result = build_canalturf_features(single, ct_idx, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # Turfostats
    ts_idx = ext.get("ts_idx", {})
    if ts_idx:
        try:
            from feature_builders.turfostats_builder import build_turfostats_features
            result = build_turfostats_features(single, ts_idx, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # Geny
    geny_idx = ext.get("geny_idx", {})
    geny_consensus = ext.get("geny_consensus", {})
    if geny_idx:
        try:
            from feature_builders.geny_builder import build_geny_features
            result = build_geny_features(single, geny_idx, geny_consensus, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # Pedigree advanced
    try:
        from feature_builders.pedigree_advanced_builder import build_pedigree_advanced_features
        result = build_pedigree_advanced_features(single, logger)
        if result:
            for k, v in result[0].items():
                if k != "partant_uid" and k not in p:
                    feat[k] = v
    except Exception:
        pass

    # Meteo
    meteo_idx = ext.get("meteo_idx", {})
    if meteo_idx:
        try:
            from feature_builders.meteo_features import build_meteo_features
            result = build_meteo_features(single, meteo_idx, logger)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    return feat


def compute_calculation_script_features(p: dict, ext: dict, logger: logging.Logger) -> dict:
    """Features from calculation scripts 42-49."""
    feat = {}
    single = [dict(p)]

    # 42 - croisement Racing Post x PMU
    mod42 = ext.get("mod42")
    rp_index_42 = ext.get("rp_index_42", {})
    if mod42 and rp_index_42:
        try:
            result = mod42.compute_croisement(single, rp_index_42)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # 43 - croisement meteo courses
    mod43 = ext.get("mod43")
    meteo_idx_43 = ext.get("meteo_idx_43", {})
    if mod43 and meteo_idx_43:
        try:
            result = mod43.compute_croisement(single, meteo_idx_43)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # 44 - croisement pedigree partants
    mod44 = ext.get("mod44")
    if mod44:
        try:
            pedigree_idx_44 = ext.get("pedigree_idx_44", {})
            sire_stats_44 = mod44.build_sire_stats(single) if hasattr(mod44, "build_sire_stats") else {}
            result = mod44.compute_croisement(single, pedigree_idx_44, sire_stats_44)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    # 48 - parse conditions texte
    mod48 = ext.get("mod48")
    if mod48:
        try:
            texte = p.get("conditions_texte") or p.get("libelle") or ""
            parsed = mod48.parse_conditions(texte) if texte else {}
            if parsed:
                feat.update(parsed)
        except Exception:
            pass

    # 49 - ecart cotes
    mod49 = ext.get("mod49")
    if mod49:
        try:
            rapports_internet = ext.get("rapports_internet_49", {})
            rapports_nationaux = ext.get("rapports_nationaux_49", {})
            cotes_marche = ext.get("cotes_marche_49", {})
            result = mod49.compute_ecart_features(single, rapports_internet, rapports_nationaux, cotes_marche)
            if result:
                for k, v in result[0].items():
                    if k != "partant_uid" and k not in p:
                        feat[k] = v
        except Exception:
            pass

    return feat


def compute_interaction_features(row: dict) -> dict:
    """10 cross-feature interaction terms (runs after all other features merged)."""
    forme = (
        _get_float(row, "forme_victoire_5")
        or _get_float(row, "musique_taux_victoire")
        or _get_float(row, "taux_victoire_carriere")
    )
    proba = _get_float(row, "proba_implicite") or _get_float(row, "proba_normalisee")
    age = _get_float(row, "profil_age") or _get_float(row, "age")
    distance = _get_float(row, "distance")
    poids = _get_float(row, "poids_porte_kg")
    jockey_taux = (
        _get_float(row, "jockey_taux_victoire_90j")
        or _get_float(row, "jockey_taux_victoire_365j")
    )
    cheval_taux = (
        _get_float(row, "forme_victoire_5")
        or _get_float(row, "taux_victoire_carriere")
    )
    affin_terrain = (
        _get_float(row, "affin_disc_taux_victoire")
        or _get_float(row, "affin_hippo_taux_victoire")
    )
    rang_cote_pct = _get_float(row, "rang_cote_pct") or _get_float(row, "rang_cote")
    nb_partants = _get_float(row, "nb_partants")
    allocation_rel = (
        _get_float(row, "allocation_relative")
        or _get_float(row, "allocation_diff_vs_last")
    )
    jours_repos = _get_float(row, "jours_depuis_derniere")
    nb_courses = (
        _get_float(row, "profil_nb_courses_carriere")
        or _get_float(row, "nb_courses_avant")
    )
    is_favori = _get_float(row, "is_favori")

    dist_norm = distance / 1000.0 if distance is not None else None
    rest_norm = math.log1p(jours_repos) if jours_repos is not None and jours_repos >= 0 else None
    exp_norm = math.log1p(nb_courses) if nb_courses is not None and nb_courses >= 0 else None

    return {
        "forme_x_cote": _multiply(forme, proba),
        "age_x_distance": _multiply(age, dist_norm),
        "poids_x_distance": _multiply(poids, dist_norm),
        "jockey_taux_x_cheval_taux": _multiply(jockey_taux, cheval_taux),
        "forme_x_terrain": _multiply(forme, affin_terrain),
        "cote_x_nb_partants": _multiply(rang_cote_pct, nb_partants),
        "allocation_x_forme": _multiply(allocation_rel, forme),
        "rest_x_forme": _multiply(rest_norm, forme),
        "age_x_nb_courses": _multiply(age, exp_norm),
        "is_favori_x_forme": _multiply(is_favori, forme),
    }


# ============================================================================
# PASS 2: COMPUTE & WRITE
# ============================================================================

ID_KEYS = frozenset({
    "partant_uid", "course_uid", "date_reunion_iso", "nom_cheval",
    "jockey_driver", "jockey", "nom_jockey", "entraineur",
    "hippodrome_normalise", "distance", "discipline",
    "position_arrivee", "is_gagnant", "is_place", "cote_finale",
    "numero_reunion", "numero_course", "horse_id", "num_pmu",
})


def pass2_compute_and_write(input_path: str, output_path: str,
                            indexes: dict, ext: dict,
                            logger: logging.Logger):
    """
    Second pass: re-read the JSONL, compute all features per record
    using pre-built indexes, write immediately to output.
    """
    logger.info("")
    logger.info("=" * 72)
    logger.info("PASS 2: Computing features & writing output (streaming)")
    logger.info("=" * 72)

    t0 = time.time()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    horse_history = indexes["horse_history"]
    course_stats = indexes["course_stats"]
    course_runners = indexes["course_runners"]
    course_nb_partants = indexes["course_nb_partants"]
    course_lookup = ext.get("course_lookup", {})

    n_written = 0
    n_total = indexes["n_records"]

    # Feature key tracker for stats
    all_feature_keys = set()
    feature_fill_counts = defaultdict(int)

    # Suppress repeated log messages from sub-builders called per-record
    # by setting their log level higher during pass 2
    for name in ["smarkets_builder", "racing_post_builder", "reunions_builder",
                 "enrichissement_builder", "canalturf_builder", "turfostats_builder",
                 "geny_builder", "pedigree_advanced_builder", "meteo_features",
                 "musique_features", "temps_features", "profil_cheval_features",
                 "equipement_features", "poids_features", "combo_features",
                 "interaction_features", "perf_detaillees_builder"]:
        sub_logger = logging.getLogger(name)
        sub_logger.setLevel(logging.WARNING)

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:

        for lineno, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                p = json.loads(line)
            except json.JSONDecodeError:
                continue

            cheval = (p.get("nom_cheval") or "").upper().strip()
            horse_hist = horse_history.get(cheval, [])
            course_uid = p.get("course_uid", "")

            # ---- Compute all feature groups ----

            # 1. Musique features (per-record, no index needed)
            p.update(compute_musique_features(p))

            # 2. Profil cheval features
            p.update(compute_profil_cheval_features(p, course_nb_partants))

            # 3. Temps features
            p.update(compute_temps_features(p, horse_hist, course_stats))

            # 4. Equipement features
            p.update(compute_equipement_features(p, horse_hist))

            # 5. Poids features
            p.update(compute_poids_features(p, horse_hist, course_stats))

            # 6. Combo features
            p.update(compute_combo_features(p, indexes))

            # 7. Class change features
            p.update(compute_class_change_features(p, horse_hist, course_lookup))

            # 8. Perf detaillees features
            p.update(compute_perf_detaillees_features(p, horse_hist))

            # 9. Precomputed partant features
            p.update(compute_precomputed_partant_features(p, ext))

            # 10. Precomputed entity features
            p.update(compute_precomputed_entity_features(p, ext))

            # 11. Affinity features (all 6 affinity types)
            p.update(compute_affinity_features(p, indexes))

            # 12. Value betting features
            p.update(compute_value_betting_features(p))

            # 13. Meteo-terrain interaction
            p.update(compute_meteo_terrain_interaction(p))

            # 14. Pedigree discipline match
            p.update(compute_pedigree_discipline_match(p))

            # 15. Field strength features
            runners_data = course_runners.get(course_uid, [])
            p.update(compute_field_strength_features(p, runners_data))

            # 16. External builder features (smarkets, RP, reunions, etc.)
            p.update(compute_external_builder_features(p, ext, logger))

            # 17. Calculation script features (42-49)
            p.update(compute_calculation_script_features(p, ext, logger))

            # 18. Interaction features (cross-feature, must be last)
            p.update(compute_interaction_features(p))

            # ---- Write immediately ----
            fout.write(json.dumps(p, ensure_ascii=False, default=str) + "\n")
            n_written += 1

            # Track feature keys for stats
            for k in p:
                if k not in ID_KEYS:
                    all_feature_keys.add(k)
                    if p[k] is not None:
                        feature_fill_counts[k] += 1

            if n_written % 50_000 == 0:
                elapsed = time.time() - t0
                rate = n_written / elapsed if elapsed > 0 else 0
                logger.info("  Pass 2: %d / %d written (%.0f rec/s, %.1fs)",
                            n_written, n_total, rate, elapsed)

    elapsed = time.time() - t0
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info("  Pass 2 complete: %d records written to %s (%.1f MB, %.1fs)",
                n_written, output_path, file_size_mb, elapsed)

    return n_written, all_feature_keys, feature_fill_counts


# ============================================================================
# STATS & SUMMARY
# ============================================================================

def print_summary(n_records: int, all_feature_keys: set, feature_fill_counts: dict,
                  elapsed: float, logger: logging.Logger):
    """Print human-readable summary."""
    n_features = len(all_feature_keys)

    logger.info("")
    logger.info("=" * 72)
    logger.info("MASTER FEATURE BUILDER - SUMMARY (STREAMING)")
    logger.info("=" * 72)
    logger.info("  Total partants:  %d", n_records)
    logger.info("  Total features:  %d", n_features)
    logger.info("  Target:          400+")
    if n_features >= 400:
        logger.info("  Status:          TARGET REACHED (%d features)", n_features)
    else:
        logger.info("  Status:          %d features short of 400 target", 400 - n_features)
    logger.info("")

    # Build stats
    stats = {}
    for k in sorted(all_feature_keys):
        filled = feature_fill_counts.get(k, 0)
        null_count = n_records - filled
        stats[k] = {
            "count": filled,
            "null_count": null_count,
            "null_rate": round(null_count / n_records, 4) if n_records else 0,
            "fill_rate": round(filled / n_records, 4) if n_records else 0,
        }

    # Top 20 best-filled
    sorted_by_fill = sorted(stats.items(), key=lambda x: -x[1]["fill_rate"])
    logger.info("  Top 20 best-filled features:")
    for k, s in sorted_by_fill[:20]:
        bar = "#" * int(s["fill_rate"] * 30)
        logger.info("    %-45s %5.1f%% %s", k, s["fill_rate"] * 100, bar)

    # Top 20 most-null
    logger.info("")
    logger.info("  Top 20 most-null features:")
    sorted_by_null = sorted(stats.items(), key=lambda x: -x[1]["null_rate"])
    for k, s in sorted_by_null[:20]:
        logger.info("    %-45s %5.1f%% null", k, s["null_rate"] * 100)

    logger.info("")
    logger.info("  Total time: %.1fs", elapsed)
    logger.info("=" * 72)

    return stats


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_pipeline(input_path: str, output_path: str, logger: logging.Logger):
    """Run the full two-pass streaming pipeline."""
    t_global = time.time()

    logger.info("=" * 72)
    logger.info("MASTER FEATURE BUILDER - STREAMING PIPELINE")
    logger.info("  Input:  %s", input_path)
    logger.info("  Output: %s", output_path)
    logger.info("  Mode:   Two-pass streaming (low memory)")
    logger.info("=" * 72)

    if not os.path.exists(input_path):
        logger.error("Input file not found: %s", input_path)
        return

    # ------------------------------------------------------------------
    # Pass 1: Build indexes
    # ------------------------------------------------------------------
    indexes = pass1_build_indexes(input_path, logger)

    if indexes["n_records"] == 0:
        logger.error("No records found. Aborting.")
        return

    # ------------------------------------------------------------------
    # Load external data
    # ------------------------------------------------------------------
    ext = load_external_indexes(logger)

    # ------------------------------------------------------------------
    # Pass 2: Compute features & write
    # ------------------------------------------------------------------
    n_written, all_feature_keys, feature_fill_counts = pass2_compute_and_write(
        input_path, output_path, indexes, ext, logger
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    elapsed = time.time() - t_global
    stats = print_summary(n_written, all_feature_keys, feature_fill_counts, elapsed, logger)

    # Save stats sidecar
    stats_path = output_path.replace(".jsonl", "_stats.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump({
            "total_partants": n_written,
            "total_features": len(all_feature_keys),
            "elapsed_seconds": round(elapsed, 1),
            "mode": "streaming_two_pass",
            "features": stats,
        }, f, ensure_ascii=False, indent=2)
    logger.info("  Stats saved: %s", stats_path)


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Master Feature Builder (STREAMING) - two-pass, low-memory pipeline."
    )
    parser.add_argument(
        "--input", "-i",
        default=INPUT_DEFAULT,
        help="Path to partants_master.jsonl",
    )
    parser.add_argument(
        "--output", "-o",
        default=OUTPUT_DEFAULT,
        help="Output path for features_matrix.jsonl",
    )
    args = parser.parse_args()

    logger = setup_logging("master_feature_builder")

    # Ensure BASE_DIR is on sys.path
    if BASE_DIR not in sys.path:
        sys.path.insert(0, BASE_DIR)

    run_pipeline(args.input, args.output, logger)


if __name__ == "__main__":
    main()
