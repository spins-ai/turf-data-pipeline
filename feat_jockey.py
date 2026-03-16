#!/usr/bin/env python3
"""
Feature Engineering — Module Jockey / Entraîneur

Features POINT-IN-TIME : utilise uniquement les courses passées.

Features produites (~50) :
  JOCKEY :
    - jock_nb_courses             → nb total de courses
    - jock_victoires              → nb victoires
    - jock_taux_vic               → taux victoire global
    - jock_taux_place             → taux place global
    - jock_forme_20               → taux victoire sur 20 dernières courses
    - jock_gains_total            → gains totaux
    - jock_gains_moy              → gains moyens par course
    - jock_nb_hippodromes         → nb hippodromes différents
    - jock_taux_vic_hippo         → taux victoire sur CET hippodrome
    - jock_taux_vic_discipline    → taux victoire dans CETTE discipline

  ENTRAÎNEUR :
    - entr_nb_courses             → nb total de courses
    - entr_victoires              → nb victoires
    - entr_taux_vic               → taux victoire global
    - entr_taux_place             → taux place global
    - entr_forme_20               → taux victoire sur 20 dernières
    - entr_taux_vic_hippo         → taux victoire sur CET hippodrome
    - entr_taux_vic_discipline    → taux victoire dans CETTE discipline
    - entr_is_top50               → True si dans le top 50 entraîneurs

  COMBO JOCKEY × ENTRAÎNEUR :
    - combo_je_nb_courses         → nb courses ensemble
    - combo_je_victoires          → nb victoires ensemble
    - combo_je_taux_vic           → taux victoire ensemble
    - combo_je_synergie           → taux combo vs (taux jockey + taux entr) / 2
"""

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def normalize_person(name):
    """Normalise un nom de jockey/entraîneur"""
    if not name:
        return None
    name = str(name).upper().strip()
    if len(name) < 2 or name in ("INCONNU", "NC", "N/A", ""):
        return None
    return name


class PersonTracker:
    """Suit les stats d'une personne (jockey ou entraîneur) au fil du temps"""

    def __init__(self):
        self.total = 0
        self.victoires = 0
        self.places = 0
        self.gains = 0.0
        self.hippodromes = set()
        self.recent_results = []  # les 20 derniers résultats
        self.by_hippo = defaultdict(lambda: {"total": 0, "vic": 0})
        self.by_discipline = defaultdict(lambda: {"total": 0, "vic": 0})

    def get_stats(self, hippo=None, discipline=None):
        """Retourne les stats calculées"""
        stats = {}
        if self.total == 0:
            return stats

        stats["nb_courses"] = self.total
        stats["victoires"] = self.victoires
        stats["taux_vic"] = round(self.victoires / self.total, 4)
        stats["taux_place"] = round(self.places / self.total, 4)
        stats["gains_total"] = self.gains
        stats["gains_moy"] = round(self.gains / self.total, 2)
        stats["nb_hippodromes"] = len(self.hippodromes)

        # Forme récente (20 dernières)
        if self.recent_results:
            n = len(self.recent_results)
            vic_recent = sum(1 for r in self.recent_results if r == 1)
            stats["forme_20"] = round(vic_recent / n, 4)

        # Par hippodrome
        if hippo and hippo in self.by_hippo:
            h = self.by_hippo[hippo]
            if h["total"] > 0:
                stats["taux_vic_hippo"] = round(h["vic"] / h["total"], 4)
                stats["nb_courses_hippo"] = h["total"]

        # Par discipline
        if discipline and discipline in self.by_discipline:
            d = self.by_discipline[discipline]
            if d["total"] > 0:
                stats["taux_vic_discipline"] = round(d["vic"] / d["total"], 4)
                stats["nb_courses_discipline"] = d["total"]

        return stats

    def add_result(self, classement, gains=0, hippo=None, discipline=None):
        """Enregistre un nouveau résultat"""
        self.total += 1
        if classement == 1:
            self.victoires += 1
        if classement is not None and classement <= 3:
            self.places += 1
        self.gains += gains or 0
        if hippo:
            self.hippodromes.add(hippo)
            self.by_hippo[hippo]["total"] += 1
            if classement == 1:
                self.by_hippo[hippo]["vic"] += 1
        if discipline:
            self.by_discipline[discipline]["total"] += 1
            if classement == 1:
                self.by_discipline[discipline]["vic"] += 1
        # Garder les 20 derniers
        self.recent_results.append(classement)
        if len(self.recent_results) > 20:
            self.recent_results.pop(0)


class ComboTracker:
    """Suit les stats d'un combo jockey + entraîneur"""

    def __init__(self):
        self.total = 0
        self.victoires = 0
        self.places = 0

    def get_stats(self):
        if self.total == 0:
            return {}
        return {
            "nb_courses": self.total,
            "victoires": self.victoires,
            "taux_vic": round(self.victoires / self.total, 4),
            "taux_place": round(self.places / self.total, 4),
        }

    def add_result(self, classement):
        self.total += 1
        if classement == 1:
            self.victoires += 1
        if classement is not None and classement <= 3:
            self.places += 1


def compute_jockey_features(partants):
    """
    Calcule les features jockey/entraîneur.
    Les partants DOIVENT être triés par date.
    """
    log.info(f"Calcul des features jockey/entraîneur sur {len(partants)} partants...")

    jockeys = defaultdict(PersonTracker)
    entraineurs = defaultdict(PersonTracker)
    combos = defaultdict(ComboTracker)

    enriched = 0
    for i, partant in enumerate(partants):
        jockey = normalize_person(partant.get("jockey") or partant.get("nom_jockey"))
        entraineur = normalize_person(partant.get("entraineur") or partant.get("nom_entraineur"))
        hippo = str(partant.get("hippodrome", "")).lower().strip() if partant.get("hippodrome") else None
        discipline = partant.get("rapport_discipline_norm") or partant.get("discipline_norm")

        has_data = False

        # ── Stats Jockey ──
        if jockey and jockeys[jockey].total > 0:
            has_data = True
            stats = jockeys[jockey].get_stats(hippo=hippo, discipline=discipline)
            for k, v in stats.items():
                partant[f"jock_{k}"] = v

        # ── Stats Entraîneur ──
        if entraineur and entraineurs[entraineur].total > 0:
            has_data = True
            stats = entraineurs[entraineur].get_stats(hippo=hippo, discipline=discipline)
            for k, v in stats.items():
                partant[f"entr_{k}"] = v

        # ── Combo Jockey × Entraîneur ──
        if jockey and entraineur:
            combo_key = f"{jockey}|{entraineur}"
            if combos[combo_key].total > 0:
                combo_stats = combos[combo_key].get_stats()
                for k, v in combo_stats.items():
                    partant[f"combo_je_{k}"] = v

                # Synergie : le combo est-il meilleur que la moyenne des deux ?
                jock_tv = jockeys[jockey].get_stats().get("taux_vic", 0)
                entr_tv = entraineurs[entraineur].get_stats().get("taux_vic", 0)
                combo_tv = combo_stats.get("taux_vic", 0)
                expected = (jock_tv + entr_tv) / 2
                if expected > 0:
                    partant["combo_je_synergie"] = round(combo_tv / expected, 2)

        if has_data:
            enriched += 1

        # ── Enregistrer le résultat ──
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

        if jockey:
            jockeys[jockey].add_result(classement, gains, hippo, discipline)
        if entraineur:
            entraineurs[entraineur].add_result(classement, gains, hippo, discipline)
        if jockey and entraineur:
            combos[f"{jockey}|{entraineur}"].add_result(classement)

        if (i + 1) % 100000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {len(jockeys)} jockeys, {len(entraineurs)} entraîneurs")

    # Top 50 entraîneurs (par victoires)
    top_entr = sorted(entraineurs.items(), key=lambda x: -x[1].victoires)[:50]
    top_entr_set = {name for name, _ in top_entr}
    for partant in partants:
        entr = normalize_person(partant.get("entraineur") or partant.get("nom_entraineur"))
        partant["entr_is_top50"] = entr in top_entr_set if entr else False

    # Top 50 jockeys
    top_jock = sorted(jockeys.items(), key=lambda x: -x[1].victoires)[:50]
    top_jock_set = {name for name, _ in top_jock}
    for partant in partants:
        jock = normalize_person(partant.get("jockey") or partant.get("nom_jockey"))
        partant["jock_is_top50"] = jock in top_jock_set if jock else False

    log.info(f"  → {enriched}/{len(partants)} enrichis, {len(jockeys)} jockeys, {len(entraineurs)} entraîneurs")
    return partants
