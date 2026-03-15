"""
turf.runner_status_manager
==========================

Manage runner statuses and compute rest / recovery / form features.

For each partant, the module looks at the same horse's prior races to derive
temporal activity indicators (courses in the last 30/90/365 days, days since
last run, etc.) and recent form metrics (victories and places over the last
5 starts).
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any


def _parse_date(iso: str) -> date | None:
    """Parse an ISO date string (YYYY-MM-DD). Return None on failure."""
    try:
        return date.fromisoformat(iso)
    except (ValueError, TypeError):
        return None


def compute_runner_status(partants: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute rest and recent-form features for every partant.

    Parameters
    ----------
    partants : list[dict]
        List of PartantNormalisé dicts.  Required keys: ``nom_cheval``,
        ``date_reunion_iso``, ``partant_uid``, ``course_uid``.
        Optional but used: ``position_arrivee``, ``is_gagnant``, ``is_place``.

    Returns
    -------
    list[dict]
        One dict per partant with keys:
        partant_uid, course_uid, nom_cheval, date,
        jours_depuis_derniere_course, nb_courses_30j, nb_courses_90j,
        nb_courses_365j, is_rentree, is_debutant,
        nb_victoires_dernieres_5, nb_places_dernieres_5.
    """
    # Group by horse
    by_horse: dict[str, list[dict]] = {}
    for p in partants:
        nom = p.get("nom_cheval", "")
        if nom:
            by_horse.setdefault(nom, []).append(p)

    results: list[dict[str, Any]] = []

    for nom, runs in by_horse.items():
        # Sort chronologically
        runs_sorted = sorted(
            runs,
            key=lambda r: (r.get("date_reunion_iso", ""), r.get("numero_course", 0)),
        )

        for i, curr in enumerate(runs_sorted):
            curr_date = _parse_date(curr.get("date_reunion_iso", ""))

            # Previous runs (strictly before this one in the sorted list)
            prior = runs_sorted[:i]

            # Days since last race
            jours_depuis: int | None = None
            if prior and curr_date:
                prev_date = _parse_date(prior[-1].get("date_reunion_iso", ""))
                if prev_date:
                    jours_depuis = (curr_date - prev_date).days

            # Count races within windows
            nb_30 = 0
            nb_90 = 0
            nb_365 = 0
            if curr_date:
                for pr in prior:
                    pr_date = _parse_date(pr.get("date_reunion_iso", ""))
                    if pr_date is None:
                        continue
                    delta = (curr_date - pr_date).days
                    if delta <= 30:
                        nb_30 += 1
                    if delta <= 90:
                        nb_90 += 1
                    if delta <= 365:
                        nb_365 += 1

            is_rentree = (jours_depuis is not None and jours_depuis > 90)
            is_debutant = (i == 0)

            # Recent form – last 5 prior starts
            last5 = prior[-5:] if len(prior) >= 5 else prior
            nb_vic_5 = sum(1 for r in last5 if r.get("is_gagnant", False))
            nb_plc_5 = sum(1 for r in last5 if r.get("is_place", False))

            results.append({
                "partant_uid": curr.get("partant_uid"),
                "course_uid": curr.get("course_uid"),
                "nom_cheval": nom,
                "date": curr.get("date_reunion_iso"),
                "jours_depuis_derniere_course": jours_depuis,
                "nb_courses_30j": nb_30,
                "nb_courses_90j": nb_90,
                "nb_courses_365j": nb_365,
                "is_rentree": is_rentree,
                "is_debutant": is_debutant,
                "nb_victoires_dernieres_5": nb_vic_5,
                "nb_places_dernieres_5": nb_plc_5,
            })

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data_path = Path(__file__).resolve().parent.parent / "output" / "02_liste_courses" / "partants_normalises.json"
    with open(data_path, encoding="utf-8") as f:
        partants = json.load(f)

    statuses = compute_runner_status(partants)

    print(f"Partants traités  : {len(statuses)}")

    debutants = sum(1 for s in statuses if s["is_debutant"])
    rentrees = sum(1 for s in statuses if s["is_rentree"])
    print(f"Débutants         : {debutants}")
    print(f"Rentrées (>90j)   : {rentrees}")

    jours_vals = [s["jours_depuis_derniere_course"] for s in statuses
                  if s["jours_depuis_derniere_course"] is not None]
    if jours_vals:
        print(f"Jours depuis dernière course – min={min(jours_vals)}, "
              f"max={max(jours_vals)}, moy={sum(jours_vals)/len(jours_vals):.1f}")

    nb90_vals = [s["nb_courses_90j"] for s in statuses]
    if nb90_vals:
        print(f"Courses 90j – min={min(nb90_vals)}, max={max(nb90_vals)}, "
              f"moy={sum(nb90_vals)/len(nb90_vals):.1f}")

    vic5 = [s["nb_victoires_dernieres_5"] for s in statuses]
    if vic5:
        print(f"Victoires /5 dernières – min={min(vic5)}, max={max(vic5)}, "
              f"moy={sum(vic5)/len(vic5):.2f}")
