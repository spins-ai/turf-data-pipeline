#!/usr/bin/env python3
"""
build_jockey_stats.py
=====================
Construit les statistiques par jockey/driver a partir de partants_master.

Calculs :
  - Taux de victoire global et par hippodrome/discipline/distance
  - Forme recente (30 / 90 / 365 jours)
  - Top partenariats (entraineurs les plus frequents avec taux victoire)
  - Volume de courses, gains totaux

Output : data_master/jockey_stats.jsonl

Streaming JSONL -> regroupement en memoire -> ecriture JSONL.

Usage :
    python build_jockey_stats.py
"""

import json
import os
import time
import unicodedata
import re
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"

INPUT_FILES = [
    DATA_MASTER / "partants_master_enrichi.jsonl",
    DATA_MASTER / "partants_master.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.json",
]

OUTPUT_FILE = DATA_MASTER / "jockey_stats.jsonl"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def strip_accents(text):
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_actor(name):
    """Normalise un nom de jockey pour le regroupement."""
    if not name:
        return ""
    name = str(name).strip()
    name = strip_accents(name)
    name = name.upper()
    name = re.sub(r"[^A-Z ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def safe_int(val, default=None):
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val, default=None):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def distance_category(dist):
    if dist is None:
        return "inconnu"
    if dist <= 1200:
        return "sprint"
    elif dist <= 1600:
        return "mile"
    elif dist <= 2200:
        return "intermediaire"
    elif dist <= 3000:
        return "long"
    return "marathon"


def safe_rate(count, total):
    if total == 0:
        return 0.0
    return round(count / total, 4)


# -----------------------------------------------------------------------
# Chargement
# -----------------------------------------------------------------------

def load_partants():
    """Charge les partants depuis le premier fichier disponible."""
    for fpath in INPUT_FILES:
        if not fpath.exists():
            continue

        print(f"  Source: {fpath}")
        records = []
        t0 = time.time()

        if str(fpath).endswith(".jsonl"):
            with open(fpath, "r", encoding="utf-8", errors="replace", buffering=1024*1024) as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        else:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, list):
                records = data

        dt = time.time() - t0
        print(f"  -> {len(records):,} partants charges ({dt:.1f}s)")
        return records

    print("  [ERREUR] Aucun fichier partants trouve!")
    return []


# -----------------------------------------------------------------------
# Construction des stats
# -----------------------------------------------------------------------

def build_jockey_history(partants):
    """Regroupe les courses par jockey."""
    print(f"\n  Regroupement par jockey ...")
    t0 = time.time()

    jockey_races = defaultdict(list)

    for p in partants:
        jockey_raw = (p.get("jockey_driver") or "").strip()
        if not jockey_raw:
            continue

        jockey = normalize_actor(jockey_raw)
        if not jockey:
            continue

        date_iso = p.get("date_reunion_iso", "")
        position = safe_int(p.get("position_arrivee"))
        is_gagnant = p.get("is_gagnant")
        is_place = p.get("is_place")
        distance = safe_int(p.get("distance"))
        hippodrome = (p.get("hippodrome_normalise") or p.get("hippodrome") or "").lower().strip()
        discipline = (p.get("discipline") or "").upper().strip()
        gains = safe_float(p.get("gains_prix_euros"))
        entraineur_raw = (p.get("entraineur") or "").strip()
        entraineur = normalize_actor(entraineur_raw) if entraineur_raw else ""
        nom_cheval = (p.get("nom_cheval") or "").strip()

        jockey_races[jockey].append({
            "date": date_iso,
            "pos": position,
            "win": bool(is_gagnant) if is_gagnant is not None else (position == 1 if position else False),
            "place": bool(is_place) if is_place is not None else (position is not None and position <= 3),
            "dist": distance,
            "hippo": hippodrome,
            "disc": discipline,
            "gains": gains,
            "entraineur": entraineur,
            "cheval": nom_cheval,
            "jockey_raw": jockey_raw,
        })

    print(f"  -> {len(jockey_races):,} jockeys uniques ({time.time() - t0:.1f}s)")
    return jockey_races


def compute_jockey_stats(jockey, races):
    """Calcule les stats d'un jockey."""
    races_sorted = sorted(races, key=lambda r: r["date"])
    total = len(races_sorted)
    wins = sum(1 for r in races_sorted if r["win"])
    places = sum(1 for r in races_sorted if r["place"])
    gains_list = [r["gains"] for r in races_sorted if r["gains"] is not None and r["gains"] > 0]
    total_gains = sum(gains_list) if gains_list else 0

    # -- Taux global --
    win_rate = safe_rate(wins, total)
    place_rate = safe_rate(places, total)

    # -- Forme recente (par fenetre temporelle) --
    derniere_date = races_sorted[-1]["date"] if races_sorted else ""
    try:
        dt_ref = datetime.strptime(derniere_date[:10], "%Y-%m-%d") if derniere_date else None
    except (ValueError, TypeError):
        dt_ref = None

    forme = {}
    for window_days, label in [(30, "30j"), (90, "90j"), (365, "1an")]:
        if dt_ref is None:
            continue
        cutoff = dt_ref - timedelta(days=window_days)
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        recent = [r for r in races_sorted if r["date"] >= cutoff_str]
        if recent:
            r_total = len(recent)
            r_wins = sum(1 for r in recent if r["win"])
            r_places = sum(1 for r in recent if r["place"])
            forme[f"courses_{label}"] = r_total
            forme[f"wins_{label}"] = r_wins
            forme[f"win_rate_{label}"] = safe_rate(r_wins, r_total)
            forme[f"place_rate_{label}"] = safe_rate(r_places, r_total)

    # -- Taux par hippodrome --
    hippo_stats = defaultdict(lambda: {"runs": 0, "wins": 0})
    for r in races_sorted:
        h = r["hippo"]
        if h:
            hippo_stats[h]["runs"] += 1
            if r["win"]:
                hippo_stats[h]["wins"] += 1

    win_rate_by_hippo = {}
    for h, s in hippo_stats.items():
        if s["runs"] >= 3:
            win_rate_by_hippo[h] = {"runs": s["runs"], "win_rate": safe_rate(s["wins"], s["runs"])}

    # Top 5 hippodromes par volume
    top_hippos = sorted(win_rate_by_hippo.items(), key=lambda x: x[1]["runs"], reverse=True)[:5]
    top_hippos_dict = {h: v for h, v in top_hippos}

    # -- Taux par discipline --
    disc_stats = defaultdict(lambda: {"runs": 0, "wins": 0})
    for r in races_sorted:
        d = r["disc"]
        if d:
            disc_stats[d]["runs"] += 1
            if r["win"]:
                disc_stats[d]["wins"] += 1

    win_rate_by_disc = {}
    for d, s in disc_stats.items():
        if s["runs"] >= 3:
            win_rate_by_disc[d] = {"runs": s["runs"], "win_rate": safe_rate(s["wins"], s["runs"])}

    # -- Taux par distance --
    dist_stats = defaultdict(lambda: {"runs": 0, "wins": 0})
    for r in races_sorted:
        cat = distance_category(r["dist"])
        dist_stats[cat]["runs"] += 1
        if r["win"]:
            dist_stats[cat]["wins"] += 1

    win_rate_by_dist = {}
    for cat, s in dist_stats.items():
        if s["runs"] >= 3:
            win_rate_by_dist[cat] = {"runs": s["runs"], "win_rate": safe_rate(s["wins"], s["runs"])}

    # -- Top partenariats (entraineurs) --
    trainer_stats = defaultdict(lambda: {"runs": 0, "wins": 0})
    for r in races_sorted:
        e = r["entraineur"]
        if e:
            trainer_stats[e]["runs"] += 1
            if r["win"]:
                trainer_stats[e]["wins"] += 1

    partnerships = {}
    for e, s in trainer_stats.items():
        if s["runs"] >= 5:
            partnerships[e] = {
                "runs": s["runs"],
                "wins": s["wins"],
                "win_rate": safe_rate(s["wins"], s["runs"]),
            }

    # Top 10 partenariats par volume
    top_partnerships = sorted(partnerships.items(), key=lambda x: x[1]["runs"], reverse=True)[:10]
    top_partnerships_dict = {e: v for e, v in top_partnerships}

    # -- Nombre de chevaux montes --
    chevaux = set(r["cheval"] for r in races_sorted if r["cheval"])

    # Construire le record
    record = {
        "jockey": jockey,
        "jockey_raw": races_sorted[0].get("jockey_raw", jockey) if races_sorted else jockey,
        "nb_courses": total,
        "nb_victoires": wins,
        "nb_places": places,
        "gains_total": round(total_gains, 2),
        "taux_victoire": win_rate,
        "taux_place": place_rate,
        "nb_chevaux_montes": len(chevaux),
        "derniere_course": derniere_date,
        "win_rate_by_hippodrome": top_hippos_dict if top_hippos_dict else None,
        "win_rate_by_discipline": win_rate_by_disc if win_rate_by_disc else None,
        "win_rate_by_distance": win_rate_by_dist if win_rate_by_dist else None,
        "top_partenariats_entraineurs": top_partnerships_dict if top_partnerships_dict else None,
    }

    # Ajouter la forme recente
    record.update(forme)

    # Strike rate category
    if win_rate >= 0.20:
        record["categorie"] = "top"
    elif win_rate >= 0.12:
        record["categorie"] = "bon"
    elif win_rate >= 0.06:
        record["categorie"] = "moyen"
    else:
        record["categorie"] = "faible"

    return record


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    t_start = time.time()
    print("=" * 70)
    print("BUILD JOCKEY STATS")
    print("=" * 70)

    # 1. Charger
    print("\n[1] Chargement des partants ...")
    partants = load_partants()
    if not partants:
        print("[ERREUR] Aucun partant. Abandon.")
        return

    # 2. Regrouper
    print("\n[2] Construction des historiques ...")
    jockey_races = build_jockey_history(partants)
    del partants

    # 3. Calculer et ecrire
    print(f"\n[3] Calcul des stats pour {len(jockey_races):,} jockeys ...")
    os.makedirs(str(DATA_MASTER), exist_ok=True)
    tmp_path = OUTPUT_FILE.with_suffix(".jsonl.tmp")

    written = 0
    t0 = time.time()

    with open(tmp_path, "w", encoding="utf-8", errors="replace") as fout:
        for jockey, races in jockey_races.items():
            stats = compute_jockey_stats(jockey, races)
            fout.write(json.dumps(stats, ensure_ascii=False) + "\n")
            written += 1

            if written % 10000 == 0:
                print(f"    {written:,} jockeys traites ...")

    os.replace(str(tmp_path), str(OUTPUT_FILE))
    dt = time.time() - t0

    size_mb = os.path.getsize(str(OUTPUT_FILE)) / 1024 / 1024
    print(f"    -> {written:,} jockeys ecrits, {size_mb:.1f} MB ({dt:.1f}s)")

    # Stats
    print("\n" + "=" * 70)
    print("RESULTATS")
    print("=" * 70)
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Jockeys: {written:,}")
    print(f"  Taille: {size_mb:.1f} MB")

    dt_total = time.time() - t_start
    print(f"\nTermine en {dt_total:.0f}s ({dt_total / 60:.1f} min)")


if __name__ == "__main__":
    main()
