"""
feature_builders.musique_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Decodes PMU "musique" string into structured features.

Musique format: "1a2a3a0a5m4a..." where digit = position, letter = discipline/surface.
- a = attelé (trot)
- m = monté (trot monté)
- p = plat (galop)
- h = haies (galop haies)
- s = steeple (galop steeple)
- c = cross (galop cross)
- 0 = tombé/arrêté/disqualifié
- D = disqualifié
- T = tombé
- A = arrêté
- R = refusé (obstacles)
- Ret = retiré
"""

from __future__ import annotations

import re
from typing import Any


_POSITION_PATTERN = re.compile(r'(\d+|[DTAR])([amphsc])', re.IGNORECASE)


def _decode_musique(musique: str | None) -> list[dict]:
    """Decode musique string into list of {position, discipline} dicts.

    Most recent result is first in the list.
    """
    if not musique:
        return []

    results = []
    for m in _POSITION_PATTERN.finditer(musique):
        pos_str, disc = m.group(1), m.group(2).lower()
        if pos_str.isdigit():
            pos = int(pos_str)
        else:
            pos = None  # D, T, A, R = non-finish
        results.append({"position": pos, "discipline": disc, "raw": m.group(0)})

    return results


def build_musique_features(partants: list[dict]) -> list[dict]:
    """Build features from decoded musique string.

    Features produced (22):
    - musique_nb_courses: total courses in musique
    - musique_nb_victoires: count of 1st place
    - musique_nb_places: count of top-3
    - musique_nb_dnf: count of non-finishes (0/D/T/A/R)
    - musique_nb_zero: count of "0" positions (unplaced/fallen/stopped)
    - musique_nb_disqualifications: count of D/A/T non-finishes
    - musique_taux_victoire: win rate from musique
    - musique_taux_place: place rate from musique
    - musique_derniere_pos: most recent position (None if DNF)
    - musique_avant_derniere_pos: second most recent position
    - musique_avg_pos_5: average position last 5 (excluding DNF)
    - musique_avg_pos_10: average position last 10 (excluding DNF)
    - musique_last_5_positions: list of last 5 positions (None for DNF)
    - musique_trend: trend (avg pos last 3 vs last 3-6, positive = improving)
    - musique_trend_label: improving/declining/stable label (encoded: 1/−1/0)
    - musique_nb_disciplines: number of different disciplines
    - musique_pct_meme_discipline: % of races in current discipline
    - musique_consecutive_places: consecutive top-3 streak
    - musique_consecutive_hors_places: consecutive non-placed streak
    - musique_surface_changes: number of discipline switches in musique
    - musique_nb_2eme: count of 2nd place finishes
    - musique_nb_3eme: count of 3rd place finishes
    """
    results = []

    for p in partants:
        uid = p.get("partant_uid")
        row: dict[str, Any] = {"partant_uid": uid}

        decoded = _decode_musique(p.get("musique"))

        if not decoded:
            for k in ("musique_nb_courses", "musique_nb_victoires", "musique_nb_places",
                       "musique_nb_dnf", "musique_nb_zero", "musique_nb_disqualifications",
                       "musique_nb_2eme", "musique_nb_3eme",
                       "musique_taux_victoire", "musique_taux_place",
                       "musique_derniere_pos", "musique_avant_derniere_pos",
                       "musique_avg_pos_5", "musique_avg_pos_10",
                       "musique_last_5_positions",
                       "musique_trend", "musique_trend_label",
                       "musique_nb_disciplines", "musique_pct_meme_discipline",
                       "musique_consecutive_places", "musique_consecutive_hors_places",
                       "musique_surface_changes"):
                row[k] = None
            results.append(row)
            continue

        nb = len(decoded)
        positions = [d["position"] for d in decoded if d["position"] is not None]
        valid_positions = [pos for pos in positions if pos > 0]

        row["musique_nb_courses"] = nb
        row["musique_nb_victoires"] = sum(1 for pos in positions if pos == 1)
        row["musique_nb_places"] = sum(1 for pos in positions if pos <= 3)
        row["musique_nb_2eme"] = sum(1 for pos in positions if pos == 2)
        row["musique_nb_3eme"] = sum(1 for pos in positions if pos == 3)
        row["musique_nb_dnf"] = sum(1 for d in decoded if d["position"] is None or d["position"] == 0)
        row["musique_nb_zero"] = sum(1 for d in decoded if d["position"] == 0)
        row["musique_nb_disqualifications"] = sum(
            1 for d in decoded if d["position"] is None and d["raw"][0].upper() in ("D", "A", "T")
        )

        row["musique_taux_victoire"] = round(row["musique_nb_victoires"] / nb, 3) if nb else None
        row["musique_taux_place"] = round(row["musique_nb_places"] / nb, 3) if nb else None

        # Recent positions
        row["musique_derniere_pos"] = decoded[0]["position"] if decoded else None
        row["musique_avant_derniere_pos"] = decoded[1]["position"] if len(decoded) > 1 else None

        # Average position last N
        valid_5 = [pos for pos in valid_positions[:5]]
        valid_10 = [pos for pos in valid_positions[:10]]
        row["musique_avg_pos_5"] = round(sum(valid_5) / len(valid_5), 2) if valid_5 else None
        row["musique_avg_pos_10"] = round(sum(valid_10) / len(valid_10), 2) if valid_10 else None

        # Last 5 positions (as individual features for direct model access)
        last_5 = [d["position"] for d in decoded[:5]]
        row["musique_last_5_positions"] = last_5

        # Trend: compare recent 3 vs previous 3 (lower position = better, so negative trend = improving)
        recent_3 = [pos for pos in valid_positions[:3]]
        prev_3 = [pos for pos in valid_positions[3:6]]
        if len(recent_3) >= 2 and len(prev_3) >= 2:
            avg_recent = sum(recent_3) / len(recent_3)
            avg_prev = sum(prev_3) / len(prev_3)
            trend_val = round(avg_prev - avg_recent, 2)
            row["musique_trend"] = trend_val  # positive = improving
            # Trend label: 1=improving, -1=declining, 0=stable
            if trend_val > 0.5:
                row["musique_trend_label"] = 1
            elif trend_val < -0.5:
                row["musique_trend_label"] = -1
            else:
                row["musique_trend_label"] = 0
        else:
            row["musique_trend"] = None
            row["musique_trend_label"] = None

        # Discipline diversity
        disciplines = set(d["discipline"] for d in decoded)
        row["musique_nb_disciplines"] = len(disciplines)

        # Percentage same discipline as current race
        current_disc = p.get("discipline", "").lower()
        disc_map = {"attele": "a", "monte": "m", "plat": "p", "haies": "h",
                     "steeple": "s", "cross": "c", "trot attele": "a", "trot monte": "m"}
        current_code = disc_map.get(current_disc, "")
        if current_code and nb > 0:
            same = sum(1 for d in decoded if d["discipline"] == current_code)
            row["musique_pct_meme_discipline"] = round(same / nb, 3)
        else:
            row["musique_pct_meme_discipline"] = None

        # Consecutive streaks
        consec_places = 0
        for d in decoded:
            if d["position"] is not None and 1 <= d["position"] <= 3:
                consec_places += 1
            else:
                break
        row["musique_consecutive_places"] = consec_places

        consec_hors = 0
        for d in decoded:
            if d["position"] is None or d["position"] == 0 or d["position"] > 3:
                consec_hors += 1
            else:
                break
        row["musique_consecutive_hors_places"] = consec_hors

        # Surface/discipline changes count
        disc_list = [d["discipline"] for d in decoded]
        surface_changes = 0
        for i_d in range(1, len(disc_list)):
            if disc_list[i_d] != disc_list[i_d - 1]:
                surface_changes += 1
        row["musique_surface_changes"] = surface_changes

        results.append(row)

    return results
