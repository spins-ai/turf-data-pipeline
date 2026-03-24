#!/usr/bin/env python3
"""
Feature Engineering — Module Pedigree Avancé

Features calculées à partir du pedigree (père, mère, lignée).
Analyse les performances PASSÉES de la descendance pour prédire.

Features produites (~40) :

  LIGNÉE PÈRE :
    - ped_pere_nb_descendants_vus  → combien de descendants du père on a déjà vus
    - ped_pere_taux_vic            → taux victoire des descendants du père
    - ped_pere_taux_place          → taux place des descendants du père
    - ped_pere_gains_moy           → gains moyens des descendants
    - ped_pere_taux_vic_terrain_X  → taux victoire descendants sur CE terrain
    - ped_pere_taux_vic_dist_X     → taux victoire descendants sur CETTE distance
    - ped_pere_taux_vic_disc_X     → taux victoire descendants dans CETTE discipline
    - ped_pere_is_top              → père dans le top 20 des meilleurs pères

  LIGNÉE PÈRE-MÈRE (sire de dam) :
    - ped_pm_nb_descendants_vus    → descendants du père de la mère
    - ped_pm_taux_vic              → taux victoire

  RACE :
    - ped_race_norm                → race normalisée
    - ped_race_taux_vic_dist       → taux victoire de cette race sur cette distance
    - ped_race_taux_vic_terrain    → taux victoire de cette race sur ce terrain

  ROBE :
    - ped_robe_norm                → robe normalisée
    - ped_robe_taux_vic            → certaines robes ont des stats différentes

  ÂGE CALCULÉ :
    - ped_age_exact                → âge exact en années
    - ped_age_category             → jeune / prime / mature / veteran
    - ped_age_x_discipline         → âge idéal pour la discipline ?

  CONSANGUINITÉ (si données disponibles) :
    - ped_inbreeding               → True si ancêtre commun détecté
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


class SireTracker:
    """Suit les performances des descendants d'un étalon"""

    def __init__(self):
        self.total = 0
        self.victoires = 0
        self.places = 0
        self.gains = 0.0
        self.by_terrain = defaultdict(lambda: {"total": 0, "vic": 0})
        self.by_distance = defaultdict(lambda: {"total": 0, "vic": 0})
        self.by_discipline = defaultdict(lambda: {"total": 0, "vic": 0})

    def get_stats(self, terrain=None, distance=None, discipline=None):
        if self.total == 0:
            return {}
        stats = {
            "nb_descendants_vus": self.total,
            "taux_vic": round(self.victoires / self.total, 4),
            "taux_place": round(self.places / self.total, 4),
            "gains_moy": round(self.gains / self.total, 2),
        }
        if terrain and terrain in self.by_terrain and self.by_terrain[terrain]["total"] > 0:
            t = self.by_terrain[terrain]
            stats["taux_vic_terrain"] = round(t["vic"] / t["total"], 4)
            stats["nb_terrain"] = t["total"]
        if distance and distance in self.by_distance and self.by_distance[distance]["total"] > 0:
            d = self.by_distance[distance]
            stats["taux_vic_dist"] = round(d["vic"] / d["total"], 4)
            stats["nb_dist"] = d["total"]
        if discipline and discipline in self.by_discipline and self.by_discipline[discipline]["total"] > 0:
            dc = self.by_discipline[discipline]
            stats["taux_vic_disc"] = round(dc["vic"] / dc["total"], 4)
            stats["nb_disc"] = dc["total"]
        return stats

    def add_result(self, classement, gains=0, terrain=None, distance=None, discipline=None):
        self.total += 1
        if classement == 1:
            self.victoires += 1
        if classement is not None and classement <= 3:
            self.places += 1
        self.gains += gains or 0
        if terrain:
            self.by_terrain[terrain]["total"] += 1
            if classement == 1:
                self.by_terrain[terrain]["vic"] += 1
        if distance:
            self.by_distance[distance]["total"] += 1
            if classement == 1:
                self.by_distance[distance]["vic"] += 1
        if discipline:
            self.by_discipline[discipline]["total"] += 1
            if classement == 1:
                self.by_discipline[discipline]["vic"] += 1


def normalize_race(race):
    if not race:
        return None
    r = str(race).upper().strip()
    # Grouper les variantes
    mappings = {
        "PUR SANG": "PURSANG",
        "PUR-SANG": "PURSANG",
        "THOROUGHBRED": "PURSANG",
        "TROTTEUR FRANCAIS": "TROTTEUR_FR",
        "TROTTEUR FR": "TROTTEUR_FR",
        "SELLE FRANCAIS": "SELLE_FR",
        "SELLE FR": "SELLE_FR",
        "ANGLO-ARABE": "ANGLO_ARABE",
        "ANGLO ARABE": "ANGLO_ARABE",
        "AQPS": "AQPS",
    }
    for key, val in mappings.items():
        if key in r:
            return val
    return r


def compute_pedigree_features(partants):
    """
    Calcule les features pedigree avancées.
    Les partants DOIVENT être triés par date.
    """
    log.info(f"Calcul des features pedigree sur {len(partants)} partants...")

    # Trackers par père et père-mère
    sires = defaultdict(SireTracker)
    sires_dam = defaultdict(SireTracker)
    race_trackers = defaultdict(SireTracker)
    robe_trackers = defaultdict(lambda: {"total": 0, "vic": 0})

    enriched = 0
    for i, partant in enumerate(partants):
        pere = partant.get("ped_pere")
        pere_mere = partant.get("ped_pere_mere")
        race = normalize_race(partant.get("ped_race"))
        robe = partant.get("ped_robe")
        terrain = partant.get("meteo_terrain_category")
        dist_cat = partant.get("rapport_distance_category")
        discipline = partant.get("rapport_discipline_norm")

        has_data = False

        # ── Âge calculé ──
        annee_naiss = partant.get("ped_annee_naissance")
        date_course = str(partant.get("date_reunion_iso", ""))[:4]
        if annee_naiss and date_course:
            try:
                age = int(date_course) - int(annee_naiss)
                if 1 <= age <= 25:
                    partant["ped_age_exact"] = age
                    partant["ped_age_category"] = (
                        "jeune" if age <= 3 else
                        "prime" if age <= 6 else
                        "mature" if age <= 10 else
                        "veteran"
                    )
                    # Âge idéal par discipline
                    if discipline == "plat":
                        partant["ped_age_ideal_disc"] = 3 <= age <= 5
                    elif discipline in ("haie", "steeple", "cross"):
                        partant["ped_age_ideal_disc"] = 4 <= age <= 8
                    elif discipline in ("trot_attele", "trot_monte"):
                        partant["ped_age_ideal_disc"] = 3 <= age <= 10
            except (ValueError, TypeError):
                pass

        # ── Race normalisée ──
        if race:
            partant["ped_race_norm"] = race

        # ── Stats du père (descendance) ──
        if pere and sires[pere].total > 0:
            has_data = True
            stats = sires[pere].get_stats(terrain, dist_cat, discipline)
            for k, v in stats.items():
                partant[f"ped_pere_{k}"] = v

        # ── Stats du père de la mère ──
        if pere_mere and sires_dam[pere_mere].total > 0:
            has_data = True
            stats = sires_dam[pere_mere].get_stats(terrain, dist_cat, discipline)
            for k, v in stats.items():
                partant[f"ped_pm_{k}"] = v

        # ── Stats de la race ──
        if race and race_trackers[race].total > 0:
            stats = race_trackers[race].get_stats(terrain, dist_cat, discipline)
            for k, v in stats.items():
                partant[f"ped_race_{k}"] = v

        # ── Stats de la robe ──
        if robe and robe in robe_trackers and robe_trackers[robe]["total"] > 0:
            rt = robe_trackers[robe]
            partant["ped_robe_taux_vic"] = round(rt["vic"] / rt["total"], 4)

        # ── Consanguinité simple (père et père-mère identiques = inbreeding) ──
        gpp = partant.get("ped_grand_pere_paternel")
        gpm = partant.get("ped_grand_pere_maternel")
        if gpp and gpm and gpp == gpm:
            partant["ped_inbreeding_gp"] = True

        # ── Sexe normalisé ──
        sexe = partant.get("ped_sexe")
        if sexe:
            s = str(sexe).upper().strip()
            partant["ped_is_male"] = s in ("M", "H", "MALE", "HONGRE")
            partant["ped_is_hongre"] = s in ("H", "HONGRE")

        if has_data:
            enriched += 1

        # ── Enregistrer résultat ──
        classement = partant.get("classement") or partant.get("arrivee") or partant.get("place")
        try:
            classement = int(classement) if classement is not None else None
        except (ValueError, TypeError):
            classement = None

        gains = 0
        try:
            gains = float(partant.get("gains_course") or partant.get("gains") or 0)
        except (ValueError, TypeError):
            pass

        if pere:
            sires[pere].add_result(classement, gains, terrain, dist_cat, discipline)
        if pere_mere:
            sires_dam[pere_mere].add_result(classement, gains, terrain, dist_cat, discipline)
        if race:
            race_trackers[race].add_result(classement, gains, terrain, dist_cat, discipline)
        if robe:
            robe_trackers[robe]["total"] += 1
            if classement == 1:
                robe_trackers[robe]["vic"] += 1

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(sires)} pères, {enriched} enrichis")

    # Top 20 pères
    top_sires = sorted(sires.items(), key=lambda x: -x[1].victoires)[:20]
    top_sires_set = {name for name, _ in top_sires}
    for partant in partants:
        pere = partant.get("ped_pere")
        partant["ped_pere_is_top20"] = pere in top_sires_set if pere else False

    log.info(f"  → {enriched}/{len(partants)} enrichis, {len(sires)} pères, {len(sires_dam)} père-mère")
    return partants
