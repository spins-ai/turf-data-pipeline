"""
feature_builders.equipement_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Builds equipment-related features: oeillères, déferré, and their changes.
"""

from __future__ import annotations

from typing import Any


_OEILLERES_VALUES = {
    "SANS": 0,
    None: 0,
    "": 0,
    "AVEC": 1,
    "AUSTRALIENNES": 2,
}

_OEILLERES_TYPE_VALUES = {
    "AUSTRALIENNES": 1,
    "CLASSIQUES": 2,
    "AVEC": 2,           # "AVEC" treated as classiques
    "AMERICAINES": 3,
    "SANS": 0,
    None: 0,
    "": 0,
}

_DEFERRE_VALUES = {
    "SANS": 0,
    None: 0,
    "": 0,
    "DEFERRE_ANTERIEURS": 1,
    "DEFERRE_POSTERIEURS": 2,
    "DEFERRE_4_PIEDS": 3,
}

_DEFERRE_TYPE_VALUES = {
    "SANS": 0,
    None: 0,
    "": 0,
    "DEFERRE_ANTERIEURS": 1,
    "DEFERRE_POSTERIEURS": 2,
    "DEFERRE_4_PIEDS": 3,
    "DEFERRE_ANTERIEURS_GAUCHE": 4,
    "DEFERRE_ANTERIEURS_DROIT": 5,
    "DEFERRE_POSTERIEURS_GAUCHE": 6,
    "DEFERRE_POSTERIEURS_DROIT": 7,
}


def build_equipement_features(partants: list[dict]) -> list[dict]:
    """Build equipment features for each partant.

    Features produced (16):
    - equip_oeilleres_code: encoded oeillères (0=sans, 1=avec, 2=australiennes)
    - equip_has_oeilleres: boolean
    - equip_oeilleres_type: detailed type (australiennes=1, classiques=2, americaines=3)
    - equip_first_time_oeilleres: first race with blinkers (1/0/None)
    - equip_deferre_code: encoded déferré (0-3)
    - equip_has_deferre: boolean
    - equip_deferre_type: detailed déferré encoding (0-7)
    - equip_deferre_change: déferré changed vs previous race (1/0/None)
    - equip_oeilleres_change: oeillères changed vs previous race (1/0/None)
    - equip_premier_oeilleres: first time wearing oeillères (1/0/None)
    - equip_premier_deferre: first time running unshod (1/0/None)
    - equip_nb_courses_avec_oeilleres: count of prior races with oeillères
    - equip_poids_monte_change: weight/rider change flag
    - equip_nb_equipement_changes_5: total equipment changes in last 5 races
    """
    # Build horse history sorted by date
    horse_history: dict[str, list[dict]] = {}
    for p in partants:
        nom = p.get("nom_cheval")
        if nom:
            horse_history.setdefault(nom, []).append(p)

    for nom in horse_history:
        horse_history[nom].sort(key=lambda x: x.get("date_reunion_iso", ""))

    # Build index of race position for each horse
    horse_race_idx: dict[str, dict[str, int]] = {}
    for nom, races in horse_history.items():
        idx = {}
        for i, r in enumerate(races):
            uid = r.get("partant_uid")
            if uid:
                idx[uid] = i
        horse_race_idx[nom] = idx

    results = []
    for p in partants:
        uid = p.get("partant_uid")
        nom = p.get("nom_cheval")
        row: dict[str, Any] = {"partant_uid": uid}

        oeilleres = p.get("oeilleres")
        deferre = p.get("deferre")

        # Current equipment
        oeil_code = _OEILLERES_VALUES.get(oeilleres, 0)
        def_code = _DEFERRE_VALUES.get(deferre, 0)

        row["equip_oeilleres_code"] = oeil_code
        row["equip_has_oeilleres"] = 1 if oeil_code > 0 else 0
        row["equip_oeilleres_type"] = _OEILLERES_TYPE_VALUES.get(oeilleres, 0)
        row["equip_deferre_code"] = def_code
        row["equip_has_deferre"] = 1 if def_code > 0 else 0
        row["equip_deferre_type"] = _DEFERRE_TYPE_VALUES.get(deferre, 0)
        row["equip_poids_monte_change"] = 1 if p.get("poids_monte_change") else 0

        # Compare with previous race
        prev = None
        if nom and nom in horse_race_idx:
            idx_map = horse_race_idx[nom]
            cur_idx = idx_map.get(uid)
            if cur_idx is not None and cur_idx > 0:
                prev = horse_history[nom][cur_idx - 1]

        if prev is not None:
            prev_oeil = _OEILLERES_VALUES.get(prev.get("oeilleres"), 0)
            prev_def = _DEFERRE_VALUES.get(prev.get("deferre"), 0)
            row["equip_oeilleres_change"] = 1 if oeil_code != prev_oeil else 0
            row["equip_deferre_change"] = 1 if def_code != prev_def else 0
        else:
            row["equip_oeilleres_change"] = None
            row["equip_deferre_change"] = None

        # First time with oeillères / déferré
        if nom and nom in horse_history:
            idx_map = horse_race_idx.get(nom, {})
            cur_idx = idx_map.get(uid)
            if cur_idx is not None:
                prior_races = horse_history[nom][:cur_idx]
                prior_oeil = [_OEILLERES_VALUES.get(r.get("oeilleres"), 0) for r in prior_races]
                prior_def = [_DEFERRE_VALUES.get(r.get("deferre"), 0) for r in prior_races]

                if oeil_code > 0 and all(v == 0 for v in prior_oeil):
                    row["equip_premier_oeilleres"] = 1
                else:
                    row["equip_premier_oeilleres"] = 0

                if def_code > 0 and all(v == 0 for v in prior_def):
                    row["equip_premier_deferre"] = 1
                else:
                    row["equip_premier_deferre"] = 0

                row["equip_nb_courses_avec_oeilleres"] = sum(1 for v in prior_oeil if v > 0)

                # First time with oeillères (alias for equip_premier_oeilleres)
                row["equip_first_time_oeilleres"] = row["equip_premier_oeilleres"]

                # Count equipment changes in last 5 races
                last_5 = prior_races[-5:] if len(prior_races) >= 5 else prior_races
                nb_changes = 0
                for i_prev in range(1, len(last_5)):
                    o_prev = _OEILLERES_VALUES.get(last_5[i_prev - 1].get("oeilleres"), 0)
                    o_curr = _OEILLERES_VALUES.get(last_5[i_prev].get("oeilleres"), 0)
                    d_prev = _DEFERRE_VALUES.get(last_5[i_prev - 1].get("deferre"), 0)
                    d_curr = _DEFERRE_VALUES.get(last_5[i_prev].get("deferre"), 0)
                    if o_prev != o_curr or d_prev != d_curr:
                        nb_changes += 1
                row["equip_nb_equipement_changes_5"] = nb_changes
            else:
                row["equip_premier_oeilleres"] = None
                row["equip_premier_deferre"] = None
                row["equip_nb_courses_avec_oeilleres"] = None
                row["equip_first_time_oeilleres"] = None
                row["equip_nb_equipement_changes_5"] = None
        else:
            row["equip_premier_oeilleres"] = None
            row["equip_premier_deferre"] = None
            row["equip_nb_courses_avec_oeilleres"] = None
            row["equip_first_time_oeilleres"] = None
            row["equip_nb_equipement_changes_5"] = None

        results.append(row)

    return results
