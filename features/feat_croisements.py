#!/usr/bin/env python3
"""
Feature Engineering — Module Croisements Cheval × Contexte

Pour chaque partant, calcule ses stats sur un CONTEXTE spécifique
en utilisant UNIQUEMENT les courses passées (point-in-time safe).

Features produites (~60) :
  CHEVAL × HIPPODROME :
    - cross_hippo_nb_courses      → nb de courses sur cet hippodrome
    - cross_hippo_victoires       → victoires sur cet hippodrome
    - cross_hippo_taux_vic        → taux victoire hippodrome
    - cross_hippo_taux_place      → taux place hippodrome
    - cross_hippo_classement_moy  → classement moyen hippodrome
    - cross_hippo_is_specialist   → True si > 3 courses et taux > 25%

  CHEVAL × DISTANCE :
    - cross_dist_nb_courses       → nb courses sur cette catégorie de distance
    - cross_dist_victoires        → victoires sur cette distance
    - cross_dist_taux_vic         → taux victoire distance
    - cross_dist_is_specialist    → True si bon sur cette distance

  CHEVAL × TERRAIN :
    - cross_terrain_nb_courses    → nb courses sur ce terrain
    - cross_terrain_victoires     → victoires sur ce terrain
    - cross_terrain_taux_vic      → taux victoire terrain
    - cross_terrain_is_specialist → True si bon sur ce terrain

  CHEVAL × DISCIPLINE :
    - cross_disc_nb_courses       → nb courses dans cette discipline
    - cross_disc_victoires        → victoires dans cette discipline
    - cross_disc_taux_vic         → taux victoire discipline

  CHEVAL × DISTANCE × TERRAIN (combo ultra-spécifique) :
    - cross_combo_nb_courses      → nb courses dans ce contexte exact
    - cross_combo_taux_vic        → taux victoire contexte exact
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def normalize_hippo(name):
    if not name:
        return None
    return str(name).lower().strip()


def get_distance_cat(partant):
    """Récupère la catégorie de distance"""
    return partant.get("rapport_distance_category") or partant.get("distance_category")


def get_terrain(partant):
    """Récupère le terrain"""
    return partant.get("meteo_terrain_category") or partant.get("terrain_category")


def get_discipline(partant):
    """Récupère la discipline"""
    return partant.get("rapport_discipline_norm") or partant.get("discipline_norm")


def calc_stats(history_list):
    """Calcule les stats depuis une liste de résultats"""
    n = len(history_list)
    if n == 0:
        return {}
    victoires = sum(1 for r in history_list if r.get("classement") == 1)
    places = sum(1 for r in history_list if r.get("classement") is not None and r["classement"] <= 3)
    finishers = [r for r in history_list if r.get("classement") is not None]
    classements = [r["classement"] for r in finishers]
    return {
        "nb": n,
        "vic": victoires,
        "places": places,
        "taux_vic": round(victoires / n, 4),
        "taux_place": round(places / n, 4),
        "classement_moy": round(sum(classements) / len(classements), 2) if classements else None,
    }


def compute_croisements(partants):
    """
    Calcule les features croisées pour chaque partant.
    Les partants DOIVENT être triés par date.
    """
    log.info(f"Calcul des features croisements sur {len(partants)} partants...")

    # Historique structuré par cheval
    # nom → { "hippo:{hippo}": [results], "dist:{dist}": [results], ... }
    horse_context = defaultdict(lambda: defaultdict(list))

    enriched = 0
    for i, partant in enumerate(partants):
        nom = partant.get("nom_cheval", "")
        if not nom:
            continue
        nom_norm = nom.upper().strip()

        hippo = normalize_hippo(partant.get("hippodrome"))
        dist_cat = get_distance_cat(partant)
        terrain = get_terrain(partant)
        discipline = get_discipline(partant)

        ctx = horse_context[nom_norm]
        has_data = False

        # ── Cheval × Hippodrome ──
        if hippo and len(ctx[f"hippo:{hippo}"]) > 0:
            has_data = True
            s = calc_stats(ctx[f"hippo:{hippo}"])
            partant["cross_hippo_nb_courses"] = s["nb"]
            partant["cross_hippo_victoires"] = s["vic"]
            partant["cross_hippo_taux_vic"] = s["taux_vic"]
            partant["cross_hippo_taux_place"] = s["taux_place"]
            partant["cross_hippo_classement_moy"] = s["classement_moy"]
            partant["cross_hippo_is_specialist"] = s["nb"] >= 3 and s["taux_vic"] >= 0.25

        # ── Cheval × Distance ──
        if dist_cat and len(ctx[f"dist:{dist_cat}"]) > 0:
            has_data = True
            s = calc_stats(ctx[f"dist:{dist_cat}"])
            partant["cross_dist_nb_courses"] = s["nb"]
            partant["cross_dist_victoires"] = s["vic"]
            partant["cross_dist_taux_vic"] = s["taux_vic"]
            partant["cross_dist_taux_place"] = s["taux_place"]
            partant["cross_dist_is_specialist"] = s["nb"] >= 3 and s["taux_vic"] >= 0.20

        # ── Cheval × Terrain ──
        if terrain and len(ctx[f"terrain:{terrain}"]) > 0:
            has_data = True
            s = calc_stats(ctx[f"terrain:{terrain}"])
            partant["cross_terrain_nb_courses"] = s["nb"]
            partant["cross_terrain_victoires"] = s["vic"]
            partant["cross_terrain_taux_vic"] = s["taux_vic"]
            partant["cross_terrain_is_specialist"] = s["nb"] >= 2 and s["taux_vic"] >= 0.30

        # ── Cheval × Discipline ──
        if discipline and len(ctx[f"disc:{discipline}"]) > 0:
            has_data = True
            s = calc_stats(ctx[f"disc:{discipline}"])
            partant["cross_disc_nb_courses"] = s["nb"]
            partant["cross_disc_victoires"] = s["vic"]
            partant["cross_disc_taux_vic"] = s["taux_vic"]

        # ── Combo : Cheval × Distance × Terrain ──
        combo_key = f"combo:{dist_cat}:{terrain}"
        if dist_cat and terrain and len(ctx[combo_key]) > 0:
            s = calc_stats(ctx[combo_key])
            partant["cross_combo_nb_courses"] = s["nb"]
            partant["cross_combo_taux_vic"] = s["taux_vic"]
            partant["cross_combo_taux_place"] = s["taux_place"]

        if has_data:
            enriched += 1

        # ── Ajouter cette course au contexte (pour les futures) ──
        classement = partant.get("classement") or partant.get("arrivee") or partant.get("place")
        try:
            classement = int(classement) if classement is not None else None
        except (ValueError, TypeError):
            classement = None

        result = {"classement": classement}

        if hippo:
            ctx[f"hippo:{hippo}"].append(result)
        if dist_cat:
            ctx[f"dist:{dist_cat}"].append(result)
        if terrain:
            ctx[f"terrain:{terrain}"].append(result)
        if discipline:
            ctx[f"disc:{discipline}"].append(result)
        if dist_cat and terrain:
            ctx[f"combo:{dist_cat}:{terrain}"].append(result)

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)} traités, {enriched} enrichis...")

    log.info(f"  → {enriched}/{len(partants)} partants avec croisements ({enriched*100/max(len(partants),1):.1f}%)")
    return partants
