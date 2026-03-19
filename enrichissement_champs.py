#!/usr/bin/env python3
"""
enrichissement_champs.py
========================

Enrichit les 14 champs à faible taux de remplissage dans partants_master.jsonl.

Champs traités :
  1. commentaire_apres_course (0.5%)  -> extrait depuis rapports_master
  2. pays_entrainement (8.1%)         -> lookup SIRE/IFCE + heuristique
  3. poids_base_kg (8.7%)             -> depuis poids_handicaps
  4. surcharge_decharge_kg (8.7%)     -> calcul poids_porte - poids_base
  5. ecart_precedent (31.9%)          -> calcul depuis historique cheval
  6. reduction_km_ms (39.1%)          -> calcul temps_ms / distance * 1000
  7. temps_ms (39.1%)                 -> depuis sectionals ou Racing Post
  8. pere_mere (44.8%)                -> depuis pedigree_master
  9. poids_porte_kg (45.8%)           -> depuis poids_handicaps

Streaming JSONL -> JSONL pour supporter les 2.9M lignes sans exploser la RAM.

Usage:
    python enrichissement_champs.py
"""

import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"

# Input
PARTANTS_IN = DATA_MASTER / "partants_master.jsonl"

# Output
PARTANTS_OUT = DATA_MASTER / "partants_master_enrichi.jsonl"
PARTANTS_TMP = DATA_MASTER / "partants_master_enrichi.jsonl.tmp"

# Sources pour enrichissement
PEDIGREE_PATH = DATA_MASTER / "pedigree_master.json"
RAPPORTS_PATH = DATA_MASTER / "rapports_master.json"
POIDS_DIR = BASE_DIR / "output" / "10_poids_handicaps"
SECTIONALS_DIR = BASE_DIR / "output" / "11_sectionals"
RACING_POST_PATH = DATA_MASTER / "racing_post_master.json"
SIRE_DIR = BASE_DIR / "output" / "17_sire_ifce"


# -----------------------------------------------------------------------
# Index builders — chargés en mémoire avant le streaming
# -----------------------------------------------------------------------

def build_pedigree_index() -> dict:
    """Index {nom_cheval_upper: {pere_mere, pere, mere, pays_naissance}}."""
    idx = {}
    if not PEDIGREE_PATH.exists():
        print("  [WARN] pedigree_master.json introuvable, skip pere_mere")
        return idx

    print(f"  Chargement pedigree depuis {PEDIGREE_PATH} ...")
    t0 = time.time()
    with open(PEDIGREE_PATH, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    for rec in data:
        nom = (rec.get("nom") or "").strip().upper()
        if not nom:
            continue
        entry = {}
        pm = (rec.get("pere_mere") or "").strip()
        if pm:
            entry["pere_mere"] = pm
        pays = (rec.get("pays_naissance") or "").strip()
        if pays:
            entry["pays_naissance"] = pays
        if entry:
            idx[nom] = entry

    print(f"    -> {len(idx):,} chevaux indexés en {time.time()-t0:.1f}s")
    return idx


def build_rapports_index() -> dict:
    """Index {course_uid: {num_pmu: commentaire}} depuis rapports_master."""
    idx = {}
    if not RAPPORTS_PATH.exists():
        print("  [WARN] rapports_master.json introuvable, skip commentaires")
        return idx

    print(f"  Chargement rapports depuis {RAPPORTS_PATH} ...")
    t0 = time.time()
    with open(RAPPORTS_PATH, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)

    for rec in data:
        uid = rec.get("course_uid") or rec.get("rap_course_key") or ""
        if not uid:
            continue
        # Les rapports contiennent parfois des commentaires par cheval
        commentaires = rec.get("commentaires_chevaux") or {}
        if commentaires:
            idx[uid] = commentaires

    print(f"    -> {len(idx):,} courses avec commentaires en {time.time()-t0:.1f}s")
    return idx


def build_poids_index() -> dict:
    """Index {course_uid: {num_pmu: {poids_base, poids_porte}}} depuis poids_handicaps."""
    idx = defaultdict(dict)
    if not POIDS_DIR.exists():
        print("  [WARN] output/10_poids_handicaps introuvable, skip poids")
        return dict(idx)

    print(f"  Chargement poids depuis {POIDS_DIR} ...")
    t0 = time.time()
    count = 0
    for fname in os.listdir(POIDS_DIR):
        if not fname.endswith(".json"):
            continue
        fpath = POIDS_DIR / fname
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        records = data if isinstance(data, list) else [data]
        for rec in records:
            uid = rec.get("course_uid", "")
            num = rec.get("num_pmu") or rec.get("numero")
            poids_base = rec.get("poids_base") or rec.get("poidsConditionMonte")
            poids_porte = rec.get("poids_porte") or rec.get("poidsPorte") or rec.get("poidsMonte")
            if uid and num is not None:
                entry = {}
                if poids_base is not None:
                    try:
                        entry["poids_base_kg"] = float(poids_base)
                    except (ValueError, TypeError):
                        pass
                if poids_porte is not None:
                    try:
                        entry["poids_porte_kg"] = float(poids_porte)
                    except (ValueError, TypeError):
                        pass
                if entry:
                    idx[uid][int(num)] = entry
                    count += 1

    print(f"    -> {count:,} entrées poids en {time.time()-t0:.1f}s")
    return dict(idx)


def build_sectionals_index() -> dict:
    """Index {course_uid: {num_pmu: temps_ms}} depuis sectionals."""
    idx = defaultdict(dict)
    if not SECTIONALS_DIR.exists():
        print("  [WARN] output/11_sectionals introuvable, skip temps")
        return dict(idx)

    print(f"  Chargement sectionals depuis {SECTIONALS_DIR} ...")
    t0 = time.time()
    count = 0
    for fname in os.listdir(SECTIONALS_DIR):
        if not fname.endswith(".json"):
            continue
        fpath = SECTIONALS_DIR / fname
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        records = data if isinstance(data, list) else [data]
        for rec in records:
            uid = rec.get("course_uid", "")
            num = rec.get("num_pmu") or rec.get("numero")
            temps = rec.get("temps_total_ms") or rec.get("temps_ms") or rec.get("chrono_ms")
            if uid and num is not None and temps is not None:
                try:
                    idx[uid][int(num)] = int(temps)
                    count += 1
                except (ValueError, TypeError):
                    pass

    print(f"    -> {count:,} entrées sectionals en {time.time()-t0:.1f}s")
    return dict(idx)


def build_racing_post_index() -> dict:
    """Index {course_uid: {num_pmu: temps_ms}} depuis Racing Post master."""
    idx = defaultdict(dict)
    if not RACING_POST_PATH.exists():
        print("  [WARN] racing_post_master.json introuvable, skip Racing Post temps")
        return dict(idx)

    print(f"  Chargement Racing Post depuis {RACING_POST_PATH} ...")
    t0 = time.time()
    count = 0
    try:
        with open(RACING_POST_PATH, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        print("    -> Erreur lecture, skip")
        return dict(idx)

    records = data if isinstance(data, list) else [data]
    for rec in records:
        uid = rec.get("course_uid", "")
        num = rec.get("num_pmu") or rec.get("numero")
        temps = rec.get("temps_ms") or rec.get("time_ms")
        if uid and num is not None and temps is not None:
            try:
                idx[uid][int(num)] = int(temps)
                count += 1
            except (ValueError, TypeError):
                pass

    print(f"    -> {count:,} entrées Racing Post en {time.time()-t0:.1f}s")
    return dict(idx)


def build_sire_pays_index() -> dict:
    """Index {nom_cheval_upper: pays_entrainement} depuis SIRE/IFCE."""
    idx = {}
    if not SIRE_DIR.exists():
        print("  [WARN] output/17_sire_ifce introuvable, skip pays_entrainement SIRE")
        return idx

    print(f"  Chargement SIRE/IFCE depuis {SIRE_DIR} ...")
    t0 = time.time()
    for fname in os.listdir(SIRE_DIR):
        if not fname.endswith(".json"):
            continue
        fpath = SIRE_DIR / fname
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        records = data if isinstance(data, list) else [data]
        for rec in records:
            nom = (rec.get("nom") or rec.get("nom_cheval") or "").strip().upper()
            pays = (rec.get("pays_entrainement") or rec.get("pays_activite")
                    or rec.get("lieu_stationnement_pays") or "").strip()
            if nom and pays:
                idx[nom] = pays

    print(f"    -> {len(idx):,} chevaux avec pays entrainement en {time.time()-t0:.1f}s")
    return idx


# -----------------------------------------------------------------------
# Enrichissement d'un partant
# -----------------------------------------------------------------------

def enrich_partant(
    partant: dict,
    pedigree_idx: dict,
    rapports_idx: dict,
    poids_idx: dict,
    sectionals_idx: dict,
    racing_post_idx: dict,
    sire_pays_idx: dict,
    horse_history: dict,
    stats: dict,
) -> dict:
    """Enrichit un partant avec les 14 champs manquants."""

    nom_upper = (partant.get("nom_cheval") or "").strip().upper()
    course_uid = partant.get("course_uid", "")
    num_pmu = partant.get("num_pmu")
    num_pmu_int = None
    if num_pmu is not None:
        try:
            num_pmu_int = int(num_pmu)
        except (ValueError, TypeError):
            pass

    # 1. commentaire_apres_course — depuis rapports
    if not (partant.get("commentaire_apres_course") or "").strip():
        course_comms = rapports_idx.get(course_uid, {})
        if num_pmu_int is not None:
            comm = course_comms.get(str(num_pmu_int)) or course_comms.get(num_pmu_int)
            if comm:
                partant["commentaire_apres_course"] = str(comm).strip()
                stats["commentaire_apres_course"] += 1

    # 2. pays_entrainement — SIRE/IFCE ou heuristique
    if not (partant.get("pays_entrainement") or "").strip():
        pays = sire_pays_idx.get(nom_upper, "")
        if not pays:
            # Heuristique : si pays_cheval = France et discipline trot -> France
            pays_cheval = (partant.get("pays_cheval") or partant.get("pgr_pays_naissance") or "").strip()
            if pays_cheval.upper() in ("FRANCE", "FR"):
                pays = "France"
        if pays:
            partant["pays_entrainement"] = pays
            stats["pays_entrainement"] += 1

    # 3 & 9. poids_base_kg & poids_porte_kg — depuis poids_handicaps
    poids_info = poids_idx.get(course_uid, {}).get(num_pmu_int, {}) if num_pmu_int else {}

    if partant.get("poids_base_kg") is None and "poids_base_kg" in poids_info:
        partant["poids_base_kg"] = poids_info["poids_base_kg"]
        stats["poids_base_kg"] += 1

    if partant.get("poids_porte_kg") is None and "poids_porte_kg" in poids_info:
        partant["poids_porte_kg"] = poids_info["poids_porte_kg"]
        stats["poids_porte_kg"] += 1

    # 4. surcharge_decharge_kg — calcul poids_porte - poids_base
    if partant.get("surcharge_decharge_kg") is None:
        pb = partant.get("poids_base_kg")
        pp = partant.get("poids_porte_kg")
        if pb is not None and pp is not None:
            try:
                partant["surcharge_decharge_kg"] = round(float(pp) - float(pb), 1)
                stats["surcharge_decharge_kg"] += 1
            except (ValueError, TypeError):
                pass

    # 5. ecart_precedent — jours depuis la dernière course du cheval
    if not (partant.get("ecart_precedent") or ""):
        date_iso = partant.get("date_reunion_iso", "")
        if nom_upper and date_iso:
            prev_dates = horse_history.get(nom_upper, [])
            if prev_dates:
                # Trouver la dernière course AVANT celle-ci
                last_before = None
                for d in reversed(prev_dates):
                    if d < date_iso:
                        last_before = d
                        break
                if last_before:
                    try:
                        from datetime import date as dt_date
                        d1 = dt_date.fromisoformat(last_before)
                        d2 = dt_date.fromisoformat(date_iso)
                        ecart = (d2 - d1).days
                        partant["ecart_precedent"] = str(ecart)
                        stats["ecart_precedent"] += 1
                    except (ValueError, TypeError):
                        pass

    # 6 & 7. temps_ms & reduction_km_ms
    # 7. temps_ms — depuis sectionals ou Racing Post
    if partant.get("temps_ms") is None or partant.get("temps_ms") == 0:
        temps = None
        if num_pmu_int:
            temps = sectionals_idx.get(course_uid, {}).get(num_pmu_int)
            if temps is None:
                temps = racing_post_idx.get(course_uid, {}).get(num_pmu_int)
        if temps is not None:
            partant["temps_ms"] = int(temps)
            stats["temps_ms"] += 1

    # 6. reduction_km_ms — calcul temps_ms / distance * 1000
    if (partant.get("reduction_km_ms") is None or partant.get("reduction_km_ms") == 0):
        temps = partant.get("temps_ms")
        distance = partant.get("distance") or partant.get("handicap_distance_m")
        if temps and distance:
            try:
                temps_f = float(temps)
                dist_f = float(distance)
                if dist_f > 0 and temps_f > 0:
                    # reduction = temps en ms par km
                    reduction = temps_f / dist_f * 1000.0
                    partant["reduction_km_ms"] = int(round(reduction))
                    stats["reduction_km_ms"] += 1
            except (ValueError, TypeError):
                pass

    # 8. pere_mere — depuis pedigree_master
    if not (partant.get("pere_mere") or "").strip():
        ped_info = pedigree_idx.get(nom_upper, {})
        pm = ped_info.get("pere_mere", "")
        if pm:
            partant["pere_mere"] = pm
            stats["pere_mere"] += 1

    return partant


# -----------------------------------------------------------------------
# Construction de l'historique cheval (dates de courses)
# -----------------------------------------------------------------------

def build_horse_history() -> dict:
    """Pré-scan du JSONL pour construire {nom_upper: [date_iso triées]}."""
    print("  Construction historique chevaux (pré-scan) ...")
    t0 = time.time()
    history = defaultdict(set)

    with open(PARTANTS_IN, "r", encoding="utf-8", errors="replace") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            nom = (rec.get("nom_cheval") or "").strip().upper()
            date_iso = rec.get("date_reunion_iso", "")
            if nom and date_iso:
                history[nom].add(date_iso)

            if line_num % 500_000 == 0:
                print(f"    pré-scan: {line_num:>10,} lignes ...")

    # Trier les dates
    sorted_history = {}
    for nom, dates in history.items():
        sorted_history[nom] = sorted(dates)

    print(f"    -> {len(sorted_history):,} chevaux, {line_num:,} lignes en {time.time()-t0:.1f}s")
    return sorted_history


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    t0 = time.time()
    print("=" * 70)
    print("ENRICHISSEMENT DES 14 CHAMPS — partants_master.jsonl")
    print("=" * 70)

    if not PARTANTS_IN.exists():
        print(f"ERREUR: {PARTANTS_IN} introuvable")
        sys.exit(1)

    # Charger les index
    print("\n--- Chargement des index ---")
    pedigree_idx = build_pedigree_index()
    rapports_idx = build_rapports_index()
    poids_idx = build_poids_index()
    sectionals_idx = build_sectionals_index()
    racing_post_idx = build_racing_post_index()
    sire_pays_idx = build_sire_pays_index()
    horse_history = build_horse_history()

    print(f"\nIndex chargés en {time.time()-t0:.1f}s")

    # Compteurs
    stats = defaultdict(int)
    total = 0
    already_filled = defaultdict(int)

    fields_to_track = [
        "commentaire_apres_course", "pays_entrainement", "poids_base_kg",
        "surcharge_decharge_kg", "ecart_precedent", "reduction_km_ms",
        "temps_ms", "pere_mere", "poids_porte_kg",
    ]

    # Streaming
    print(f"\n--- Streaming enrichissement ---")
    print(f"  Input:  {PARTANTS_IN}")
    print(f"  Output: {PARTANTS_OUT}")

    t1 = time.time()
    with open(PARTANTS_IN, "r", encoding="utf-8", errors="replace") as fin, \
         open(PARTANTS_TMP, "w", encoding="utf-8", errors="replace") as fout:

        for line_num, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue

            try:
                partant = json.loads(line)
            except json.JSONDecodeError:
                # Écrire tel quel en cas d'erreur
                fout.write(line + "\n")
                continue

            total += 1

            # Compter les champs déjà remplis AVANT enrichissement
            for field in fields_to_track:
                val = partant.get(field)
                if val is not None and val != "" and val != 0:
                    already_filled[field] += 1

            # Enrichir
            partant = enrich_partant(
                partant, pedigree_idx, rapports_idx, poids_idx,
                sectionals_idx, racing_post_idx, sire_pays_idx,
                horse_history, stats,
            )

            fout.write(json.dumps(partant, ensure_ascii=False) + "\n")

            if line_num % 200_000 == 0:
                elapsed = time.time() - t1
                rate = line_num / elapsed if elapsed > 0 else 0
                print(f"  {line_num:>10,} / ~2,930,290  "
                      f"({line_num * 100 / 2_930_290:.1f}%)  "
                      f"[{rate:.0f} rec/s]", flush=True)

    # Remplacement atomique
    if PARTANTS_TMP.exists():
        os.replace(str(PARTANTS_TMP), str(PARTANTS_OUT))

    elapsed_total = time.time() - t0

    # Rapport
    print("\n" + "=" * 70)
    print("RAPPORT D'ENRICHISSEMENT")
    print("=" * 70)
    print(f"\nTotal partants traités : {total:,}")
    print(f"\n{'Champ':<30s} {'Avant':>10s} {'Enrichis':>10s} {'Après':>10s} {'Taux avant':>10s} {'Taux après':>10s}")
    print("-" * 80)

    for field in fields_to_track:
        before = already_filled[field]
        enriched = stats[field]
        after = before + enriched
        pct_before = (before / total * 100) if total > 0 else 0
        pct_after = (after / total * 100) if total > 0 else 0
        print(f"  {field:<28s} {before:>10,} {enriched:>10,} {after:>10,} "
              f"{pct_before:>9.1f}% {pct_after:>9.1f}%")

    print(f"\nFichier enrichi : {PARTANTS_OUT}")
    size_mb = PARTANTS_OUT.stat().st_size / (1024 * 1024) if PARTANTS_OUT.exists() else 0
    print(f"Taille : {size_mb:.1f} MB")
    print(f"Temps total : {elapsed_total:.1f}s")
    print("Terminé.")


if __name__ == "__main__":
    main()
