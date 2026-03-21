#!/usr/bin/env python3
"""
build_horse_career_stats.py
===========================
Construit les statistiques de carriere par cheval a partir de partants_master.

Calculs :
  - Total courses, victoires, places, gains
  - Taux de victoire par distance, terrain, hippodrome
  - Meilleures performances (top positions)
  - Forme actuelle (5 dernieres courses)
  - Jours moyens entre courses, regularite

Output : data_master/horse_career_stats.jsonl

Streaming : lit partants_master.jsonl en un seul passage pour construire
l'historique par cheval, puis ecrit les stats.

Usage :
    python build_horse_career_stats.py
"""

import json
import os
import time
from collections import defaultdict
from pathlib import Path
from datetime import datetime

from utils.normalize import normalize_name
from utils.types import safe_int, safe_float

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"

INPUT_FILES = [
    DATA_MASTER / "partants_master_enrichi.jsonl",
    DATA_MASTER / "partants_master.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.json",
]

OUTPUT_FILE = DATA_MASTER / "horse_career_stats.jsonl"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

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


# -----------------------------------------------------------------------
# Chargement des partants (streaming)
# -----------------------------------------------------------------------

def stream_and_group_partants():
    """Charge les partants en streaming et regroupe par cheval directement.
    Ne charge jamais tous les records en memoire - uniquement le dict par cheval."""
    horse_races = defaultdict(list)
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
                            nom_raw = p.get("nom_cheval") or p.get("nom") or ""
                            nom = normalize_name(nom_raw)
                            if nom:
                                total += 1
                                horse_races[nom].append({
                                    "date": p.get("date_reunion_iso", ""),
                                    "pos": safe_int(p.get("position_arrivee")),
                                    "win": bool(p.get("is_gagnant")) if p.get("is_gagnant") is not None else (safe_int(p.get("position_arrivee")) == 1 if safe_int(p.get("position_arrivee")) else None),
                                    "place": bool(p.get("is_place")) if p.get("is_place") is not None else (safe_int(p.get("position_arrivee")) is not None and safe_int(p.get("position_arrivee")) <= 3),
                                    "dist": safe_int(p.get("distance")),
                                    "hippo": (p.get("hippodrome_normalise") or p.get("hippodrome") or "").lower().strip(),
                                    "disc": (p.get("discipline") or "").upper().strip(),
                                    "gains": safe_float(p.get("gains_prix_euros") or p.get("gains_carriere_euros")),
                                    "cote": safe_float(p.get("cote_finale")),
                                    "terrain": (p.get("type_piste") or p.get("meteo_type_piste") or "").lower().strip(),
                                    "jockey": (p.get("jockey_driver") or "").strip(),
                                    "entraineur": (p.get("entraineur") or "").strip(),
                                    "red_km": safe_float(p.get("reduction_km_ms")),
                                    "temps": safe_float(p.get("temps_ms")),
                                    "nb_partants": safe_int(p.get("nb_partants")),
                                    "nom_raw": (p.get("nom_cheval") or p.get("nom") or "").strip(),
                                })
                                if total % 500000 == 0:
                                    print(f"    {total:,} partants traites ...")
                        except json.JSONDecodeError:
                            pass
                    line = f.readline()
        else:
            # JSON trop gros, skip
            print(f"  [SKIP] JSON trop gros, utiliser JSONL")
            continue

        dt = time.time() - t0
        print(f"  -> {total:,} partants groupes en {len(horse_races):,} chevaux ({dt:.1f}s)")
        return horse_races, total

    print("  [ERREUR] Aucun fichier partants trouve!")
    return horse_races, 0


# -----------------------------------------------------------------------
# Construction des stats par cheval
# -----------------------------------------------------------------------

def build_stats(partants):
    """Construit les statistiques de carriere pour chaque cheval."""
    print(f"\n  Regroupement par cheval ...")
    t0 = time.time()

    # Regrouper par nom de cheval normalise
    horse_races = defaultdict(list)

    for p in partants:
        nom_raw = p.get("nom_cheval") or p.get("nom") or ""
        nom = normalize_name(nom_raw)
        if not nom:
            continue

        date_iso = p.get("date_reunion_iso", "")
        position = safe_int(p.get("position_arrivee"))
        is_gagnant = p.get("is_gagnant")
        is_place = p.get("is_place")
        distance = safe_int(p.get("distance"))
        hippodrome = (p.get("hippodrome_normalise") or p.get("hippodrome") or "").lower().strip()
        discipline = (p.get("discipline") or "").upper().strip()
        gains = safe_float(p.get("gains_prix_euros") or p.get("gains_carriere_euros"))
        cote = safe_float(p.get("cote_finale"))
        terrain = (p.get("type_piste") or p.get("meteo_type_piste") or "").lower().strip()
        jockey = (p.get("jockey_driver") or "").strip()
        entraineur = (p.get("entraineur") or "").strip()
        reduction = safe_float(p.get("reduction_km_ms"))
        temps = safe_float(p.get("temps_ms"))
        nb_partants = safe_int(p.get("nb_partants"))

        horse_races[nom].append({
            "date": date_iso,
            "pos": position,
            "win": bool(is_gagnant) if is_gagnant is not None else (position == 1 if position else None),
            "place": bool(is_place) if is_place is not None else (position is not None and position <= 3),
            "dist": distance,
            "hippo": hippodrome,
            "disc": discipline,
            "gains": gains,
            "cote": cote,
            "terrain": terrain,
            "jockey": jockey,
            "entraineur": entraineur,
            "red_km": reduction,
            "temps": temps,
            "nb_partants": nb_partants,
            "nom_raw": (p.get("nom_cheval") or p.get("nom") or "").strip(),
        })

    print(f"  -> {len(horse_races):,} chevaux uniques ({time.time() - t0:.1f}s)")
    return horse_races


def compute_horse_stats(nom, races):
    """Calcule les stats de carriere pour un cheval."""
    # Trier par date
    races_sorted = sorted(races, key=lambda r: r["date"])

    total = len(races_sorted)
    wins = sum(1 for r in races_sorted if r["win"])
    places = sum(1 for r in races_sorted if r["place"])

    positions = [r["pos"] for r in races_sorted if r["pos"] is not None]
    gains_list = [r["gains"] for r in races_sorted if r["gains"] is not None and r["gains"] > 0]
    cotes = [r["cote"] for r in races_sorted if r["cote"] is not None]
    distances = [r["dist"] for r in races_sorted if r["dist"] is not None]

    total_gains = sum(gains_list) if gains_list else 0

    # -- Taux global --
    win_rate = wins / total if total > 0 else 0
    place_rate = places / total if total > 0 else 0

    # -- Forme actuelle (5 dernieres) --
    last5 = races_sorted[-5:]
    last5_positions = [r["pos"] for r in last5 if r["pos"] is not None]
    last5_wins = sum(1 for r in last5 if r["win"])
    last5_places = sum(1 for r in last5 if r["place"])
    forme_5 = "".join(str(min(r["pos"], 9)) if r["pos"] is not None else "0" for r in last5)

    # -- Taux par distance --
    dist_stats = defaultdict(lambda: {"runs": 0, "wins": 0, "places": 0})
    for r in races_sorted:
        cat = distance_category(r["dist"])
        dist_stats[cat]["runs"] += 1
        if r["win"]:
            dist_stats[cat]["wins"] += 1
        if r["place"]:
            dist_stats[cat]["places"] += 1

    win_rate_by_dist = {}
    for cat, s in dist_stats.items():
        if s["runs"] >= 2:
            win_rate_by_dist[cat] = round(s["wins"] / s["runs"], 4)

    # -- Taux par terrain --
    terrain_stats = defaultdict(lambda: {"runs": 0, "wins": 0})
    for r in races_sorted:
        t = r["terrain"]
        if t:
            terrain_stats[t]["runs"] += 1
            if r["win"]:
                terrain_stats[t]["wins"] += 1

    win_rate_by_terrain = {}
    for t, s in terrain_stats.items():
        if s["runs"] >= 2:
            win_rate_by_terrain[t] = round(s["wins"] / s["runs"], 4)

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
        if s["runs"] >= 2:
            win_rate_by_hippo[h] = round(s["wins"] / s["runs"], 4)

    # -- Meilleures performances --
    best_pos = min(positions) if positions else None
    best_3 = sorted(positions)[:3] if positions else []

    # -- Jours entre courses --
    dates_parsed = []
    for r in races_sorted:
        if r["date"]:
            try:
                dates_parsed.append(datetime.strptime(r["date"][:10], "%Y-%m-%d"))
            except (ValueError, TypeError):
                pass

    jours_entre = []
    for i in range(1, len(dates_parsed)):
        delta = (dates_parsed[i] - dates_parsed[i - 1]).days
        if delta > 0:
            jours_entre.append(delta)

    jours_moyen = round(sum(jours_entre) / len(jours_entre), 1) if jours_entre else None

    # -- Duree carriere --
    career_days = None
    if len(dates_parsed) >= 2:
        career_days = (dates_parsed[-1] - dates_parsed[0]).days

    # -- Position moyenne --
    pos_moyenne = round(sum(positions) / len(positions), 2) if positions else None

    # -- Cote moyenne --
    cote_moyenne = round(sum(cotes) / len(cotes), 2) if cotes else None

    # -- Disciplines courues --
    disciplines = list(set(r["disc"] for r in races_sorted if r["disc"]))

    # -- Hippodromes courus --
    hippodromes = list(set(r["hippo"] for r in races_sorted if r["hippo"]))

    # -- Distance preferee --
    dist_pref = None
    if distances:
        dist_pref = round(sum(distances) / len(distances))

    # -- Jockeys utilises --
    jockeys = list(set(r["jockey"] for r in races_sorted if r["jockey"]))

    # -- Derniere course --
    derniere_date = races_sorted[-1]["date"] if races_sorted else None

    # Construire le record
    record = {
        "nom_cheval": nom,
        "nom_cheval_raw": races_sorted[0].get("nom_raw", nom) if races_sorted else nom,
        "nb_courses": total,
        "nb_victoires": wins,
        "nb_places": places,
        "gains_total": round(total_gains, 2),
        "taux_victoire": round(win_rate, 4),
        "taux_place": round(place_rate, 4),
        "position_moyenne": pos_moyenne,
        "cote_moyenne": cote_moyenne,
        "meilleure_position": best_pos,
        "top_3_positions": best_3,
        "forme_5": forme_5,
        "forme_5_wins": last5_wins,
        "forme_5_places": last5_places,
        "forme_5_positions": last5_positions,
        "win_rate_by_distance": win_rate_by_dist if win_rate_by_dist else None,
        "win_rate_by_terrain": win_rate_by_terrain if win_rate_by_terrain else None,
        "win_rate_by_hippodrome": win_rate_by_hippo if win_rate_by_hippo else None,
        "jours_moyen_entre_courses": jours_moyen,
        "career_days": career_days,
        "career_years": round(career_days / 365.25, 1) if career_days and career_days > 0 else None,
        "distance_preferee": dist_pref,
        "nb_disciplines": len(disciplines),
        "disciplines": disciplines,
        "nb_hippodromes": len(hippodromes),
        "nb_jockeys": len(jockeys),
        "derniere_course": derniere_date,
        "gains_par_course": round(total_gains / total, 2) if total > 0 else 0,
    }

    # Categorisation
    if total_gains > 500000:
        record["classe"] = "elite"
    elif total_gains > 100000:
        record["classe"] = "top"
    elif total_gains > 30000:
        record["classe"] = "bon"
    elif total_gains > 5000:
        record["classe"] = "moyen"
    else:
        record["classe"] = "faible"

    if win_rate > 0.30:
        record["performance"] = "crack"
    elif win_rate > 0.15:
        record["performance"] = "regulier"
    elif win_rate > 0.05:
        record["performance"] = "moyen"
    else:
        record["performance"] = "faible"

    # Experience
    if total >= 30:
        record["experience"] = "veteran"
    elif total >= 15:
        record["experience"] = "confirme"
    elif total >= 5:
        record["experience"] = "intermediaire"
    else:
        record["experience"] = "debutant"

    return record


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    t_start = time.time()
    print("=" * 70)
    print("BUILD HORSE CAREER STATS")
    print("=" * 70)

    # 1+2. Charger et regrouper en streaming (1 seule passe)
    print("\n[1] Chargement streaming + regroupement par cheval ...")
    horse_races, total_partants = stream_and_group_partants()
    if total_partants == 0:
        print("[ERREUR] Aucun partant. Abandon.")
        return

    # 3. Calculer et ecrire
    print(f"\n[2] Calcul des stats pour {len(horse_races):,} chevaux ...")
    os.makedirs(str(DATA_MASTER), exist_ok=True)
    tmp_path = OUTPUT_FILE.with_suffix(".jsonl.tmp")

    written = 0
    t0 = time.time()

    with open(tmp_path, "w", encoding="utf-8", errors="replace") as fout:
        for nom, races in horse_races.items():
            stats = compute_horse_stats(nom, races)
            fout.write(json.dumps(stats, ensure_ascii=False) + "\n")
            written += 1

            if written % 100000 == 0:
                print(f"    {written:,} chevaux traites ...")

    os.replace(str(tmp_path), str(OUTPUT_FILE))
    dt = time.time() - t0

    size_mb = os.path.getsize(str(OUTPUT_FILE)) / 1024 / 1024
    print(f"    -> {written:,} chevaux ecrits, {size_mb:.1f} MB ({dt:.1f}s)")

    # Stats resume
    print("\n" + "=" * 70)
    print("RESULTATS")
    print("=" * 70)
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Chevaux: {written:,}")
    print(f"  Taille: {size_mb:.1f} MB")

    dt_total = time.time() - t_start
    print(f"\nTermine en {dt_total:.0f}s ({dt_total / 60:.1f} min)")


if __name__ == "__main__":
    main()
