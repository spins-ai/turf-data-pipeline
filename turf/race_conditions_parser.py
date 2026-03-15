"""
turf.race_conditions_parser
===========================

Parse the free-text PMU race conditions field into structured features.

PMU conditions texts typically look like::

    "Pour poulains et pouliches de 3 ans, n'ayant pas gagné.
     Poids : 58 k. Poules de 3 ans. Gains : de 0 à 4 500 euros."

This module uses regex heuristics to extract age limits, sex restrictions,
race category, gain brackets, and jockey status flags.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _search_int(pattern: str, text: str) -> int | None:
    """Return first integer captured by *pattern*, or None."""
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        raw = m.group(1).replace(" ", "").replace(".", "").replace(",", "")
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _has(pattern: str, text: str) -> bool:
    return bool(re.search(pattern, text, re.IGNORECASE))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_conditions(conditions_texte: str) -> dict[str, Any]:
    """Extract structured features from a PMU conditions text.

    Parameters
    ----------
    conditions_texte : str
        Free-text conditions string attached to a race.

    Returns
    -------
    dict
        Keys: age_min, age_max, sexe_condition, categorie,
        gains_min, gains_max, is_apprenti, is_amateur,
        is_reclamer, is_international.
    """
    txt = (conditions_texte or "").strip()
    txt_lower = txt.lower()

    # ----- Age -----------------------------------------------------------
    age_min: int | None = None
    age_max: int | None = None

    # "de 3 ans et au-dessus" / "3 ans et plus"
    m_age_plus = re.search(r"(\d)\s*ans\s*et\s*(au[- ]?dessus|plus)", txt_lower)
    if m_age_plus:
        age_min = int(m_age_plus.group(1))
        age_max = None
    else:
        # "de 3 à 5 ans"
        m_age_range = re.search(r"de\s+(\d)\s*[àa]\s*(\d)\s*ans", txt_lower)
        if m_age_range:
            age_min = int(m_age_range.group(1))
            age_max = int(m_age_range.group(2))
        else:
            # "de 3 ans" / "pour ... 3 ans"
            m_age_exact = re.search(r"(\d)\s*ans", txt_lower)
            if m_age_exact:
                age_min = int(m_age_exact.group(1))
                age_max = int(m_age_exact.group(1))

    # ----- Sex -----------------------------------------------------------
    sexe_condition = "tous"
    if _has(r"pouliches|femelles|juments", txt_lower):
        if _has(r"poulains|mâles|hongres|entiers", txt_lower):
            sexe_condition = "tous"
        else:
            sexe_condition = "femelles"
    elif _has(r"poulains|mâles|hongres|entiers", txt_lower):
        if not _has(r"pouliches|femelles|juments", txt_lower):
            sexe_condition = "males"

    # "poulains et pouliches" → tous (already handled above)

    # ----- Category ------------------------------------------------------
    categorie = "conditions"
    if _has(r"handicap", txt_lower):
        categorie = "handicap"
    elif _has(r"réclamer|reclamer|à réclamer|a reclamer", txt_lower):
        categorie = "claimer"
    elif _has(r"list[ée]e?|group[ée]?|gr\.\s*[123I]", txt_lower):
        categorie = "listed"
    elif _has(r"apprenti", txt_lower):
        categorie = "apprenti"
    elif _has(r"amateur", txt_lower):
        categorie = "amateur"

    # ----- Gains ---------------------------------------------------------
    gains_min: int | None = None
    gains_max: int | None = None

    # "gains de 2 500 à 12 000 euros"
    m_gains_range = re.search(
        r"gains?\s*(?::)?\s*(?:de\s+)?([\d\s.,]+)\s*(?:€|euros?)\s*[àa]\s*([\d\s.,]+)\s*(?:€|euros?)?",
        txt_lower,
    )
    if m_gains_range:
        raw_min = m_gains_range.group(1).replace(" ", "").replace(".", "").replace(",", "")
        raw_max = m_gains_range.group(2).replace(" ", "").replace(".", "").replace(",", "")
        try:
            gains_min = int(raw_min)
        except ValueError:
            pass
        try:
            gains_max = int(raw_max)
        except ValueError:
            pass
    else:
        # "de 0 à 4 500 euros"
        m_range2 = re.search(
            r"de\s+([\d\s.,]+)\s*[àa]\s*([\d\s.,]+)\s*(?:€|euros?)",
            txt_lower,
        )
        if m_range2:
            raw_min = m_range2.group(1).replace(" ", "").replace(".", "").replace(",", "")
            raw_max = m_range2.group(2).replace(" ", "").replace(".", "").replace(",", "")
            try:
                gains_min = int(raw_min)
            except ValueError:
                pass
            try:
                gains_max = int(raw_max)
            except ValueError:
                pass

    if gains_min is None:
        gains_min = _search_int(r"ayant\s+gagn[ée]\s+au\s+moins\s+([\d\s.,]+)", txt)
    if gains_max is None:
        gains_max = _search_int(r"n'?ayant\s+pas\s+gagn[ée]\s+([\d\s.,]+)", txt)

    # ----- Flags ---------------------------------------------------------
    is_apprenti = _has(r"apprenti", txt_lower)
    is_amateur = _has(r"amateur", txt_lower)
    is_reclamer = _has(r"réclamer|reclamer|à réclamer|a reclamer", txt_lower)
    is_international = _has(r"international|étranger|etranger|invit", txt_lower)

    return {
        "age_min": age_min,
        "age_max": age_max,
        "sexe_condition": sexe_condition,
        "categorie": categorie,
        "gains_min": gains_min,
        "gains_max": gains_max,
        "is_apprenti": is_apprenti,
        "is_amateur": is_amateur,
        "is_reclamer": is_reclamer,
        "is_international": is_international,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data_path = Path(__file__).resolve().parent.parent / "output" / "02_liste_courses" / "partants_normalises.json"
    with open(data_path, encoding="utf-8") as f:
        partants = json.load(f)

    # The partants_normalises may not carry a conditions_texte field on
    # every record; run on whatever is available.
    n_parsed = 0
    categories: dict[str, int] = {}
    sexe_counts: dict[str, int] = {}
    age_mins: list[int] = []

    for p in partants:
        cond = p.get("conditions_texte") or p.get("conditions") or ""
        if not cond:
            continue
        result = parse_conditions(cond)
        n_parsed += 1
        cat = result["categorie"]
        categories[cat] = categories.get(cat, 0) + 1
        sc = result["sexe_condition"]
        sexe_counts[sc] = sexe_counts.get(sc, 0) + 1
        if result["age_min"] is not None:
            age_mins.append(result["age_min"])

    print(f"Conditions parsées : {n_parsed}")
    if n_parsed:
        print(f"Catégories        : {categories}")
        print(f"Sexe conditions   : {sexe_counts}")
        if age_mins:
            print(f"Age min – min={min(age_mins)}, max={max(age_mins)}, "
                  f"moy={sum(age_mins)/len(age_mins):.1f}")
    else:
        print("Aucun champ 'conditions_texte' trouvé dans les données. "
              "Le module fonctionne quand ce champ est disponible.")
