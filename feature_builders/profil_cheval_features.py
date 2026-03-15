"""
feature_builders.profil_cheval_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Builds horse profile features from static attributes (age, sex, race, gains, etc.)
and contextual race features (place_corde, engagement, etc.).
"""

from __future__ import annotations

from typing import Any


def build_profil_cheval_features(partants: list[dict]) -> list[dict]:
    """Build horse profile and contextual features.

    Features produced (24):
    - profil_age: age of horse
    - profil_age_category: age group (2=2yo, 3=3yo, 4=4-5yo, 5=6+)
    - profil_sexe_code: encoded sex (male=0, female=1, hongre=2)
    - profil_is_male: boolean
    - profil_is_female: boolean
    - profil_is_hongre: boolean (gelding)
    - profil_race_code: encoded race (PUR-SANG=0, AQPS=1, TROTTEUR=2, other=3)
    - profil_robe_encoded: numeric encoding of coat color
    - profil_race_breed_encoded: numeric encoding of breed (more granular)
    - profil_gains_carriere_log: log of career earnings (reduces skew)
    - profil_gains_annee_log: log of current year earnings
    - profil_gains_par_course: average earnings per start
    - profil_nb_courses_carriere: career race count
    - profil_carriere_longueur: career length category (0=debut, 1=short, 2=medium, 3=long)
    - profil_taux_victoire_carriere: career win rate
    - profil_taux_place_carriere: career place rate
    - profil_is_inedit: first race ever
    - profil_place_corde: stall/rope position
    - profil_place_corde_relative: corde relative to field size
    - profil_engagement: engagement amount
    - profil_jument_pleine: pregnant mare flag
    """
    import math

    # Group by course for relative place_corde
    courses: dict[str, list[dict]] = {}
    for p in partants:
        cuid = p.get("course_uid")
        if cuid:
            courses.setdefault(cuid, []).append(p)

    course_nb_partants: dict[str, int] = {}
    for cuid, runners in courses.items():
        course_nb_partants[cuid] = len(runners)

    sexe_map = {
        "MALES": 0, "MALE": 0, "M": 0, "H": 0,
        "FEMELLES": 1, "FEMELLE": 1, "F": 1,
        "HONGRES": 2, "HONGRE": 2,
    }

    race_map = {
        "PUR-SANG": 0, "PS": 0, "THOROUGHBRED": 0,
        "AQPS": 1,
        "TROTTEUR": 2, "TROTTEUR FRANCAIS": 2, "TF": 2,
    }

    robe_map = {
        "BAI": 1, "B": 1,
        "BAI BRUN": 2, "BB": 2, "BAI FONCE": 2, "BBF": 2,
        "ALEZAN": 3, "AL": 3,
        "GRIS": 4, "GR": 4,
        "NOIR": 5, "N": 5,
        "BAI CLAIR": 6, "BC": 6,
        "ROUAN": 7,
        "AUBERE": 8,
    }

    breed_map = {
        "PUR-SANG": 0, "PS": 0, "THOROUGHBRED": 0,
        "AQPS": 1,
        "TROTTEUR FRANCAIS": 2, "TF": 2, "TROTTEUR": 2,
        "ANGLO-ARABE": 3, "AA": 3,
        "ARABE": 4, "AR": 4,
        "SELLE FRANCAIS": 5, "SF": 5,
        "STANDARDBRED": 6,
    }

    results = []
    for p in partants:
        uid = p.get("partant_uid")
        cuid = p.get("course_uid")
        row: dict[str, Any] = {"partant_uid": uid}

        # Age
        age = p.get("age")
        row["profil_age"] = age
        # Age category: 2=2yo, 3=3yo, 4=4-5yo, 5=6+
        if age is not None:
            if age <= 2:
                row["profil_age_category"] = 2
            elif age == 3:
                row["profil_age_category"] = 3
            elif age <= 5:
                row["profil_age_category"] = 4
            else:
                row["profil_age_category"] = 5
        else:
            row["profil_age_category"] = None

        # Sex encoding
        sexe = (p.get("sexe") or "").upper().strip()
        row["profil_sexe_code"] = sexe_map.get(sexe, 0)
        row["profil_is_male"] = 1 if sexe_map.get(sexe, 0) == 0 and sexe else 0
        row["profil_is_female"] = 1 if sexe_map.get(sexe) == 1 else 0
        row["profil_is_hongre"] = 1 if sexe_map.get(sexe) == 2 else 0

        # Race encoding
        race = (p.get("race") or "").upper().strip()
        row["profil_race_code"] = race_map.get(race, 3)
        row["profil_race_breed_encoded"] = breed_map.get(race, 99)

        # Robe (coat color) encoding
        robe = (p.get("robe") or "").upper().strip()
        row["profil_robe_encoded"] = robe_map.get(robe, 0)

        # Gains (log-transformed)
        gains_c = p.get("gains_carriere_euros")
        gains_a = p.get("gains_annee_euros")
        row["profil_gains_carriere_log"] = round(math.log1p(gains_c), 2) if gains_c is not None and gains_c >= 0 else None
        row["profil_gains_annee_log"] = round(math.log1p(gains_a), 2) if gains_a is not None and gains_a >= 0 else None

        # Career stats
        nb_courses = p.get("nb_courses_carriere")
        row["profil_nb_courses_carriere"] = nb_courses
        row["profil_is_inedit"] = 1 if p.get("is_inedit") else 0

        # Career length category: 0=debut(0-2), 1=short(3-10), 2=medium(11-30), 3=long(31+)
        if nb_courses is not None:
            if nb_courses <= 2:
                row["profil_carriere_longueur"] = 0
            elif nb_courses <= 10:
                row["profil_carriere_longueur"] = 1
            elif nb_courses <= 30:
                row["profil_carriere_longueur"] = 2
            else:
                row["profil_carriere_longueur"] = 3
        else:
            row["profil_carriere_longueur"] = None

        # Career win rate and place rate
        nb_vic = p.get("nb_victoires_carriere")
        nb_place = p.get("nb_places_carriere")
        if nb_courses is not None and nb_courses > 0:
            row["profil_taux_victoire_carriere"] = round((nb_vic or 0) / nb_courses, 3)
            row["profil_taux_place_carriere"] = round((nb_place or 0) / nb_courses, 3)
            # Average earnings per start
            if gains_c is not None and gains_c >= 0:
                row["profil_gains_par_course"] = round(gains_c / nb_courses, 2)
            else:
                row["profil_gains_par_course"] = None
        else:
            row["profil_taux_victoire_carriere"] = None
            row["profil_taux_place_carriere"] = None
            row["profil_gains_par_course"] = None

        # Place à la corde
        corde = p.get("place_corde")
        row["profil_place_corde"] = corde
        nb = course_nb_partants.get(cuid)
        if corde is not None and nb and nb > 0:
            row["profil_place_corde_relative"] = round(corde / nb, 3)
        else:
            row["profil_place_corde_relative"] = None

        # Other
        eng = p.get("engagement")
        row["profil_engagement"] = eng if eng is not None else 0
        row["profil_jument_pleine"] = 1 if p.get("jument_pleine") else 0

        results.append(row)

    return results
