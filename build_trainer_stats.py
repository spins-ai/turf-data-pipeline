#!/usr/bin/env python3
"""
build_trainer_stats.py
======================
Construit les statistiques par entraineur a partir de partants_master.

Calculs :
  - Taux de victoire par hippodrome / discipline
  - Strike rate global
  - Forme recente (30j / 90j / 365j)
  - Volume de courses, gains
  - Top jockeys utilises

Output : data_master/trainer_stats.jsonl

Streaming JSONL -> regroupement en memoire -> ecriture JSONL.

Usage :
    python build_trainer_stats.py
"""

import json
import os
import time
import unicodedata
import re
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta

from utils.types import safe_int, safe_float

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"

INPUT_FILES = [
    DATA_MASTER / "partants_master_enrichi.jsonl",
    DATA_MASTER / "partants_master.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.json",
]

OUTPUT_FILE = DATA_MASTER / "trainer_stats.jsonl"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def strip_accents(text):
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_actor(name):
    """Normalise un nom d'entraineur pour le regroupement."""
    if not name:
        return ""
    name = str(name).strip()
    name = strip_accents(name)
    name = name.upper()
    name = re.sub(r"[^A-Z ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def safe_rate(count, total):
    if total == 0:
        return 0.0
    return round(count / total, 4)


# -----------------------------------------------------------------------
# Chargement streaming + regroupement
# -----------------------------------------------------------------------

def stream_and_group_trainers():
    """Charge les partants en streaming et regroupe par entraineur directement."""
    trainer_races = defaultdict(list)
    total = 0

    for fpath in INPUT_FILES:
        if not fpath.exists():
            continue

        print(f"  Source: {fpath}")
        t0 = time.time()

        if str(fpath).endswith(".jsonl"):
            with open(fpath, "r", encoding="utf-8", errors="replace", buffering=1048576) as f:
                line = f.readline()
                while line:
                    stripped = line.strip()
                    if stripped:
                        try:
                            p = json.loads(stripped)
                            entraineur_raw = (p.get("entraineur") or "").strip()
                            if entraineur_raw:
                                entraineur = normalize_actor(entraineur_raw)
                                if entraineur:
                                    total += 1
                                    position = safe_int(p.get("position_arrivee"))
                                    is_gagnant = p.get("is_gagnant")
                                    is_place = p.get("is_place")
                                    jockey_raw = (p.get("jockey_driver") or "").strip()
                                    trainer_races[entraineur].append({
                                        "date": p.get("date_reunion_iso", ""),
                                        "pos": position,
                                        "win": bool(is_gagnant) if is_gagnant is not None else (position == 1 if position else False),
                                        "place": bool(is_place) if is_place is not None else (position is not None and position <= 3),
                                        "dist": safe_int(p.get("distance")),
                                        "hippo": (p.get("hippodrome_normalise") or p.get("hippodrome") or "").lower().strip(),
                                        "disc": (p.get("discipline") or "").upper().strip(),
                                        "gains": safe_float(p.get("gains_prix_euros")),
                                        "jockey": normalize_actor(jockey_raw) if jockey_raw else "",
                                        "cheval": (p.get("nom_cheval") or "").strip(),
                                        "cote": safe_float(p.get("cote_finale")),
                                        "entraineur_raw": entraineur_raw,
                                    })
                                    if total % 500000 == 0:
                                        print(f"    {total:,} partants traites ...")
                        except json.JSONDecodeError:
                            pass
                    line = f.readline()
        else:
            print(f"  [SKIP] JSON trop gros, utiliser JSONL")
            continue

        dt = time.time() - t0
        print(f"  -> {total:,} partants groupes en {len(trainer_races):,} entraineurs ({dt:.1f}s)")
        return trainer_races, total

    print("  [ERREUR] Aucun fichier partants trouve!")
    return trainer_races, 0


def compute_trainer_stats(entraineur, races):
    """Calcule les stats d'un entraineur."""
    races_sorted = sorted(races, key=lambda r: r["date"])
    total = len(races_sorted)
    wins = sum(1 for r in races_sorted if r["win"])
    places = sum(1 for r in races_sorted if r["place"])
    gains_list = [r["gains"] for r in races_sorted if r["gains"] is not None and r["gains"] > 0]
    total_gains = sum(gains_list) if gains_list else 0

    # -- Taux global (strike rate) --
    strike_rate = safe_rate(wins, total)
    place_rate = safe_rate(places, total)

    # -- Forme recente --
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
            forme[f"strike_rate_{label}"] = safe_rate(r_wins, r_total)
            forme[f"place_rate_{label}"] = safe_rate(r_places, r_total)

    # -- Taux par hippodrome --
    hippo_stats = defaultdict(lambda: {"runs": 0, "wins": 0, "places": 0})
    for r in races_sorted:
        h = r["hippo"]
        if h:
            hippo_stats[h]["runs"] += 1
            if r["win"]:
                hippo_stats[h]["wins"] += 1
            if r["place"]:
                hippo_stats[h]["places"] += 1

    win_rate_by_hippo = {}
    for h, s in hippo_stats.items():
        if s["runs"] >= 5:
            win_rate_by_hippo[h] = {
                "runs": s["runs"],
                "strike_rate": safe_rate(s["wins"], s["runs"]),
                "place_rate": safe_rate(s["places"], s["runs"]),
            }

    # Top 5 hippodromes par volume
    top_hippos = sorted(win_rate_by_hippo.items(), key=lambda x: x[1]["runs"], reverse=True)[:5]
    top_hippos_dict = {h: v for h, v in top_hippos}

    # -- Taux par discipline --
    disc_stats = defaultdict(lambda: {"runs": 0, "wins": 0, "places": 0})
    for r in races_sorted:
        d = r["disc"]
        if d:
            disc_stats[d]["runs"] += 1
            if r["win"]:
                disc_stats[d]["wins"] += 1
            if r["place"]:
                disc_stats[d]["places"] += 1

    win_rate_by_disc = {}
    for d, s in disc_stats.items():
        if s["runs"] >= 5:
            win_rate_by_disc[d] = {
                "runs": s["runs"],
                "strike_rate": safe_rate(s["wins"], s["runs"]),
                "place_rate": safe_rate(s["places"], s["runs"]),
            }

    # -- Top jockeys utilises --
    jockey_stats = defaultdict(lambda: {"runs": 0, "wins": 0})
    for r in races_sorted:
        j = r["jockey"]
        if j:
            jockey_stats[j]["runs"] += 1
            if r["win"]:
                jockey_stats[j]["wins"] += 1

    partnerships = {}
    for j, s in jockey_stats.items():
        if s["runs"] >= 5:
            partnerships[j] = {
                "runs": s["runs"],
                "wins": s["wins"],
                "strike_rate": safe_rate(s["wins"], s["runs"]),
            }

    top_jockeys = sorted(partnerships.items(), key=lambda x: x[1]["runs"], reverse=True)[:10]
    top_jockeys_dict = {j: v for j, v in top_jockeys}

    # -- Nombre de chevaux entraines --
    chevaux = set(r["cheval"] for r in races_sorted if r["cheval"])

    # -- Premiere / derniere course --
    premiere_date = races_sorted[0]["date"] if races_sorted else None

    # -- ROI si cote disponible --
    # Benefice si on avait mise 1 euro sur chaque cheval de cet entraineur
    cotes_gagnants = [r["cote"] for r in races_sorted if r["win"] and r["cote"] is not None and r["cote"] > 0]
    roi = None
    if total > 10 and cotes_gagnants:
        retour_total = sum(cotes_gagnants)
        roi = round((retour_total - total) / total * 100, 2)

    # Construire le record
    record = {
        "entraineur": entraineur,
        "entraineur_raw": races_sorted[0].get("entraineur_raw", entraineur) if races_sorted else entraineur,
        "nb_courses": total,
        "nb_victoires": wins,
        "nb_places": places,
        "gains_total": round(total_gains, 2),
        "strike_rate": strike_rate,
        "place_rate": place_rate,
        "nb_chevaux": len(chevaux),
        "premiere_course": premiere_date,
        "derniere_course": derniere_date,
        "strike_rate_by_hippodrome": top_hippos_dict if top_hippos_dict else None,
        "strike_rate_by_discipline": win_rate_by_disc if win_rate_by_disc else None,
        "top_jockeys": top_jockeys_dict if top_jockeys_dict else None,
        "roi_pct": roi,
    }

    # Ajouter la forme recente
    record.update(forme)

    # Categorie
    if strike_rate >= 0.18:
        record["categorie"] = "top"
    elif strike_rate >= 0.10:
        record["categorie"] = "bon"
    elif strike_rate >= 0.05:
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
    print("BUILD TRAINER STATS")
    print("=" * 70)

    # 1+2. Charger et regrouper en streaming
    print("\n[1] Chargement streaming + regroupement par entraineur ...")
    trainer_races, total_partants = stream_and_group_trainers()
    if total_partants == 0:
        print("[ERREUR] Aucun partant. Abandon.")
        return

    # 3. Calculer et ecrire
    print(f"\n[2] Calcul des stats pour {len(trainer_races):,} entraineurs ...")
    os.makedirs(str(DATA_MASTER), exist_ok=True)
    tmp_path = OUTPUT_FILE.with_suffix(".jsonl.tmp")

    written = 0
    t0 = time.time()

    with open(tmp_path, "w", encoding="utf-8", errors="replace") as fout:
        for entraineur, races in trainer_races.items():
            stats = compute_trainer_stats(entraineur, races)
            fout.write(json.dumps(stats, ensure_ascii=False) + "\n")
            written += 1

            if written % 5000 == 0:
                print(f"    {written:,} entraineurs traites ...")

    os.replace(str(tmp_path), str(OUTPUT_FILE))
    dt = time.time() - t0

    size_mb = os.path.getsize(str(OUTPUT_FILE)) / 1024 / 1024
    print(f"    -> {written:,} entraineurs ecrits, {size_mb:.1f} MB ({dt:.1f}s)")

    # Stats
    print("\n" + "=" * 70)
    print("RESULTATS")
    print("=" * 70)
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Entraineurs: {written:,}")
    print(f"  Taille: {size_mb:.1f} MB")

    dt_total = time.time() - t_start
    print(f"\nTermine en {dt_total:.0f}s ({dt_total / 60:.1f} min)")


if __name__ == "__main__":
    main()
