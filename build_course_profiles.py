#!/usr/bin/env python3
"""
build_course_profiles.py
========================
Construit les profils de course par hippodrome a partir des donnees historiques.

Calculs par hippodrome (+ par discipline quand assez de data) :
  - Taille de champ moyenne (nb partants)
  - Taux de victoire du favori (rang_cote == 1)
  - Draw bias (biais de corde / numero de stall)
  - Pace bias (front-runner vs closer)
  - Terrain dominant, distances courues
  - Stats gains / cotes

Output : data_master/course_profiles.jsonl

Usage :
    python build_course_profiles.py
"""

import json
import os
import time
import math
from collections import defaultdict
from pathlib import Path

from utils.normalize import strip_accents
from utils.types import safe_int, safe_float

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"

# Sources : partants pour les stats partants-niveau, courses pour les metadonnees
PARTANTS_FILES = [
    DATA_MASTER / "partants_master_enrichi.jsonl",
    DATA_MASTER / "partants_master.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.jsonl",
    BASE_DIR / "output" / "02_liste_courses" / "partants_normalises.json",
]

OUTPUT_FILE = DATA_MASTER / "course_profiles.jsonl"


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def safe_mean(values):
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def safe_median(values):
    clean = sorted(v for v in values if v is not None)
    if not clean:
        return None
    n = len(clean)
    mid = n // 2
    if n % 2 == 0:
        return (clean[mid - 1] + clean[mid]) / 2
    return clean[mid]


def safe_stdev(values):
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = sum(clean) / len(clean)
    variance = sum((x - mean) ** 2 for x in clean) / (len(clean) - 1)
    return math.sqrt(variance)


# -----------------------------------------------------------------------
# Chargement
# -----------------------------------------------------------------------

def stream_and_group_courses():
    """Charge les partants en streaming et regroupe par course_uid.
    Ne garde que les champs necessaires pour reduire la RAM."""
    courses = defaultdict(list)
    total = 0

    for fpath in PARTANTS_FILES:
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
                            cuid = p.get("course_uid")
                            if cuid:
                                total += 1
                                # Ne garder que les champs utilises
                                courses[cuid].append({
                                    "hippodrome": (p.get("hippodrome_normalise") or p.get("hippodrome") or "").lower().strip(),
                                    "discipline": (p.get("discipline") or "").upper().strip(),
                                    "distance": safe_int(p.get("distance")),
                                    "terrain": (p.get("type_piste") or p.get("meteo_type_piste") or "").lower().strip(),
                                    "position_arrivee": safe_int(p.get("position_arrivee")),
                                    "cote_finale": safe_float(p.get("cote_finale")),
                                    "rang_cote": safe_int(p.get("rang_cote")),
                                    "is_gagnant": p.get("is_gagnant"),
                                    "num_pmu": safe_int(p.get("num_pmu")),
                                    "gains_prix_euros": safe_float(p.get("gains_prix_euros")),
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
        print(f"  -> {total:,} partants en {len(courses):,} courses ({dt:.1f}s)")
        return courses, total

    print("  [ERREUR] Aucun fichier partants trouve!")
    return courses, 0


def build_hippo_profiles(courses_dict):
    """Construit les profils par hippodrome."""
    print(f"\n  Construction des profils par hippodrome ...")
    t0 = time.time()

    # Accumulateurs par hippodrome
    hippo_data = defaultdict(lambda: {
        "nb_courses": 0,
        "field_sizes": [],
        "favori_wins": 0,
        "favori_total": 0,
        "distances": [],
        "disciplines": defaultdict(int),
        "terrains": defaultdict(int),
        "draw_positions_win": [],  # (num_pmu, nb_partants) pour les gagnants
        "draw_positions_all": [],  # (num_pmu, nb_partants) pour tous
        "front_runner_wins": 0,
        "front_runner_total": 0,
        "cotes_gagnants": [],
        "gains_courses": [],
    })

    for cuid, partants_course in courses_dict.items():
        if not partants_course:
            continue

        # Info course (depuis le premier partant)
        p0 = partants_course[0]
        hippo = p0.get("hippodrome", "")
        if not hippo:
            continue

        discipline = p0.get("discipline", "")
        distance = p0.get("distance")
        terrain = p0.get("terrain", "")
        nb_partants = len(partants_course)

        data = hippo_data[hippo]
        data["nb_courses"] += 1
        data["field_sizes"].append(nb_partants)

        if distance is not None:
            data["distances"].append(distance)
        if discipline:
            data["disciplines"][discipline] += 1
        if terrain:
            data["terrains"][terrain] += 1

        # Trouver le favori (rang_cote == 1 ou cote la plus basse)
        favori = None
        gagnant = None
        cotes_course = []

        for p in partants_course:
            pos = p.get("position_arrivee")
            cote = p.get("cote_finale")
            rang_cote = p.get("rang_cote")
            is_gagnant = p.get("is_gagnant") or (pos == 1 if pos else False)
            num_pmu = p.get("num_pmu")

            if cote is not None and cote > 0:
                cotes_course.append((cote, p))

            if rang_cote == 1 or (favori is None and cote is not None):
                if favori is None or (cote is not None and (favori[0] is None or cote < favori[0])):
                    favori = (cote, p)

            if is_gagnant:
                gagnant = p
                if cote is not None:
                    data["cotes_gagnants"].append(cote)

            # Draw data
            if num_pmu is not None:
                data["draw_positions_all"].append((num_pmu, nb_partants))
                if is_gagnant:
                    data["draw_positions_win"].append((num_pmu, nb_partants))

        # Favori gagne ?
        if favori is not None:
            data["favori_total"] += 1
            favori_p = favori[1]
            favori_pos = safe_int(favori_p.get("position_arrivee"))
            favori_win = favori_p.get("is_gagnant") or (favori_pos == 1 if favori_pos else False)
            if favori_win:
                data["favori_wins"] += 1

        # Pace bias : le gagnant partait-il devant ? (heuristique via num_pmu bas)
        if gagnant is not None and nb_partants >= 5:
            gagnant_num = gagnant.get("num_pmu")
            if gagnant_num is not None:
                # "Front runner" = premier tiers du champ
                seuil = max(nb_partants // 3, 1)
                data["front_runner_total"] += 1
                if gagnant_num <= seuil:
                    data["front_runner_wins"] += 1

    print(f"  -> {len(hippo_data):,} hippodromes ({time.time() - t0:.1f}s)")
    return hippo_data


def compute_draw_bias(draw_win, draw_all, nb_partants_max=20):
    """Calcule le biais de stall/corde.

    Pour chaque position (1..nb_partants_max), compare le taux de victoire
    observe vs le taux attendu (1/nb_partants).

    Retourne un dict {position: bias_ratio} ou bias > 1 = avantage.
    """
    if len(draw_win) < 20:
        return None

    # Compteur par position relative (1-based)
    pos_wins = defaultdict(int)
    pos_total = defaultdict(int)

    for num, nb in draw_all:
        if num is not None and 1 <= num <= nb_partants_max:
            pos_total[num] += 1

    for num, nb in draw_win:
        if num is not None and 1 <= num <= nb_partants_max:
            pos_wins[num] += 1

    if not pos_total:
        return None

    bias = {}
    for pos in sorted(pos_total.keys()):
        total_at_pos = pos_total[pos]
        if total_at_pos < 10:
            continue
        wins_at_pos = pos_wins.get(pos, 0)
        observed_rate = wins_at_pos / total_at_pos
        # Expected rate = moyenne des 1/nb_partants pour les courses ou cette position existait
        # Simplification : 1 / field_size moyen
        # Plus simple : ratio observe/attendu
        expected = 1.0 / max(pos, 1)  # approximation
        bias_ratio = round(observed_rate / expected, 3) if expected > 0 else None
        if bias_ratio is not None:
            bias[str(pos)] = {
                "runs": total_at_pos,
                "wins": wins_at_pos,
                "win_rate": round(observed_rate, 4),
                "bias_ratio": bias_ratio,
            }

    return bias if bias else None


def compute_profile(hippo, data):
    """Calcule le profil d'un hippodrome."""
    nb_courses = data["nb_courses"]
    if nb_courses == 0:
        return None

    field_sizes = data["field_sizes"]
    avg_field = round(safe_mean(field_sizes), 1) if field_sizes else None
    median_field = safe_median(field_sizes)

    # Taux favori
    favori_win_rate = None
    if data["favori_total"] > 0:
        favori_win_rate = round(data["favori_wins"] / data["favori_total"], 4)

    # Distances
    distances = data["distances"]
    dist_min = min(distances) if distances else None
    dist_max = max(distances) if distances else None
    dist_avg = round(safe_mean(distances)) if distances else None

    # Discipline dominante
    disciplines = dict(data["disciplines"])
    disc_dominante = max(disciplines, key=disciplines.get) if disciplines else None

    # Terrain dominant
    terrains = dict(data["terrains"])
    terrain_dominant = max(terrains, key=terrains.get) if terrains else None

    # Draw bias
    draw_bias = compute_draw_bias(
        data["draw_positions_win"],
        data["draw_positions_all"]
    )

    # Pace bias
    pace_bias = None
    if data["front_runner_total"] >= 20:
        fr_rate = data["front_runner_wins"] / data["front_runner_total"]
        # Attendu ~33% si pas de biais (premier tiers)
        pace_bias = {
            "front_runner_win_rate": round(fr_rate, 4),
            "courses_analysees": data["front_runner_total"],
            "biais": "front" if fr_rate > 0.40 else "closer" if fr_rate < 0.25 else "neutre",
        }

    # Cotes gagnants
    cotes_gag = data["cotes_gagnants"]
    cote_gagnant_moy = round(safe_mean(cotes_gag), 2) if cotes_gag else None
    cote_gagnant_med = round(safe_median(cotes_gag), 2) if cotes_gag else None

    record = {
        "hippodrome": hippo,
        "nb_courses": nb_courses,
        "field_size_moyen": avg_field,
        "field_size_median": median_field,
        "favori_win_rate": favori_win_rate,
        "distance_min": dist_min,
        "distance_max": dist_max,
        "distance_moyenne": dist_avg,
        "discipline_dominante": disc_dominante,
        "disciplines": disciplines,
        "terrain_dominant": terrain_dominant,
        "terrains": terrains,
        "cote_gagnant_moyenne": cote_gagnant_moy,
        "cote_gagnant_mediane": cote_gagnant_med,
        "draw_bias": draw_bias,
        "pace_bias": pace_bias,
    }

    return record


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    t_start = time.time()
    print("=" * 70)
    print("BUILD COURSE PROFILES — Profils par hippodrome")
    print("=" * 70)

    # 1+2. Charger et regrouper en streaming
    print("\n[1] Chargement streaming + regroupement par course ...")
    courses_dict, total_partants = stream_and_group_courses()
    if total_partants == 0:
        print("[ERREUR] Aucun partant. Abandon.")
        return

    # 3. Construire profils par hippodrome
    print("\n[2] Construction des profils ...")
    hippo_data = build_hippo_profiles(courses_dict)
    del courses_dict

    # 4. Calculer et ecrire
    print(f"\n[4] Calcul des profils pour {len(hippo_data):,} hippodromes ...")
    os.makedirs(str(DATA_MASTER), exist_ok=True)
    tmp_path = OUTPUT_FILE.with_suffix(".jsonl.tmp")

    written = 0
    skipped = 0
    t0 = time.time()

    with open(tmp_path, "w", encoding="utf-8", errors="replace") as fout:
        for hippo in sorted(hippo_data.keys()):
            data = hippo_data[hippo]
            profile = compute_profile(hippo, data)
            if profile is None:
                skipped += 1
                continue

            fout.write(json.dumps(profile, ensure_ascii=False) + "\n")
            written += 1

    os.replace(str(tmp_path), str(OUTPUT_FILE))
    dt = time.time() - t0

    size_mb = os.path.getsize(str(OUTPUT_FILE)) / 1024 / 1024
    print(f"    -> {written:,} profils ecrits, {skipped} ignores, {size_mb:.1f} MB ({dt:.1f}s)")

    # Stats
    print("\n" + "=" * 70)
    print("RESULTATS")
    print("=" * 70)
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Hippodromes: {written:,}")
    print(f"  Taille: {size_mb:.1f} MB")

    dt_total = time.time() - t_start
    print(f"\nTermine en {dt_total:.0f}s ({dt_total / 60:.1f} min)")


if __name__ == "__main__":
    main()
