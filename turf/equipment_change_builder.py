"""
turf.equipment_change_builder
=============================

Detect equipment changes (oeillères, déferré) between consecutive races for
the same horse.

Equipment changes are well-known market signals in horse racing: a horse
putting on blinkers for the first time, or having them removed, often
indicates a deliberate training strategy shift.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def detect_equipment_changes(partants: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Detect equipment changes between consecutive races for each horse.

    Parameters
    ----------
    partants : list[dict]
        List of PartantNormalisé dicts.  Must contain at least the keys
        ``nom_cheval``, ``date_reunion_iso``, ``course_uid``, ``partant_uid``,
        ``oeilleres``, ``deferre``.  The list does **not** need to be pre-sorted.

    Returns
    -------
    list[dict]
        One dict per partant that has at least one prior race for the same
        horse, with keys: partant_uid, course_uid, nom_cheval, date,
        oeilleres_change, deferre_change, oeilleres_prev, oeilleres_curr,
        deferre_prev, deferre_curr, premiere_oeilleres, retrait_oeilleres.
    """
    # Group by horse
    by_horse: dict[str, list[dict]] = {}
    for p in partants:
        nom = p.get("nom_cheval", "")
        if nom:
            by_horse.setdefault(nom, []).append(p)

    results: list[dict[str, Any]] = []

    for nom, runs in by_horse.items():
        # Sort by date then by course number for deterministic ordering
        runs_sorted = sorted(
            runs,
            key=lambda r: (r.get("date_reunion_iso", ""), r.get("numero_course", 0)),
        )

        for i, curr in enumerate(runs_sorted):
            if i == 0:
                continue  # no previous race to compare

            prev = runs_sorted[i - 1]

            oeil_prev = (prev.get("oeilleres") or "").strip().lower()
            oeil_curr = (curr.get("oeilleres") or "").strip().lower()
            def_prev = (prev.get("deferre") or "").strip().lower()
            def_curr = (curr.get("deferre") or "").strip().lower()

            oeil_changed = oeil_prev != oeil_curr
            def_changed = def_prev != def_curr

            # "Première oeillères" = horse goes from "sans" (or empty) to wearing
            premiere_oeil = (
                oeil_changed
                and oeil_prev in ("sans", "")
                and oeil_curr not in ("sans", "")
            )
            # Check if it is truly the first time ever across all prior runs
            if premiere_oeil:
                # Scan all runs before index i
                for j in range(i):
                    prev_oeil_j = (runs_sorted[j].get("oeilleres") or "").strip().lower()
                    if prev_oeil_j not in ("sans", ""):
                        premiere_oeil = False
                        break

            retrait_oeil = (
                oeil_changed
                and oeil_prev not in ("sans", "")
                and oeil_curr in ("sans", "")
            )

            results.append({
                "partant_uid": curr.get("partant_uid"),
                "course_uid": curr.get("course_uid"),
                "nom_cheval": nom,
                "date": curr.get("date_reunion_iso"),
                "oeilleres_change": oeil_changed,
                "deferre_change": def_changed,
                "oeilleres_prev": prev.get("oeilleres"),
                "oeilleres_curr": curr.get("oeilleres"),
                "deferre_prev": prev.get("deferre"),
                "deferre_curr": curr.get("deferre"),
                "premiere_oeilleres": premiere_oeil,
                "retrait_oeilleres": retrait_oeil,
            })

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data_path = Path(__file__).resolve().parent.parent / "output" / "02_liste_courses" / "partants_normalises.json"
    with open(data_path, encoding="utf-8") as f:
        partants = json.load(f)

    changes = detect_equipment_changes(partants)

    print(f"Partants avec historique comparé : {len(changes)}")

    oeil_changes = sum(1 for c in changes if c["oeilleres_change"])
    def_changes = sum(1 for c in changes if c["deferre_change"])
    premieres = sum(1 for c in changes if c["premiere_oeilleres"])
    retraits = sum(1 for c in changes if c["retrait_oeilleres"])

    print(f"Changements d'oeillères    : {oeil_changes}")
    print(f"Changements de déferré     : {def_changes}")
    print(f"Premières oeillères        : {premieres}")
    print(f"Retraits d'oeillères       : {retraits}")
