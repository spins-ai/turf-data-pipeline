"""
turf.musique_decoder
====================

Decode the PMU "musique" string into structured race-history data.

The musique string encodes a horse's recent results in reverse chronological
order.  Each *entry* is composed of:

    <position><discipline>

Where **position** is one of:
    0-9   finishing position (0 = 10th or unplaced beyond 9)
    D     disqualified
    A     arrêté (pulled up / stopped)
    T     tombé (fell)
    R     refusé (refused)

And **discipline** is one of (case-insensitive):
    a     attelé (harness)
    m     monté (mounted trot)
    p     plat (flat)
    s     steeple-chase
    h     haies (hurdles)
    c     cross-country

Year separators are bare two-digit numbers (e.g. "15" for 2015) inserted
between entries.

An optional ``(XX)`` token encodes a rest period in days.

Examples
--------
>>> decode_musique("2p152p5p4p")
[{'position': 2, 'discipline': 'plat', ...}, ...]  # 4 entries
"""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DISCIPLINE_MAP = {
    "a": "attelé",
    "m": "monté",
    "p": "plat",
    "s": "steeple",
    "h": "haies",
    "c": "cross",
}

_STATUS_LETTERS = {"D", "A", "T", "R"}

# Regex that captures, in order:
#   1. rest-period tokens  (XX)
#   2. year separators     two bare digits NOT followed by a discipline letter
#   3. race entries        position-char + discipline-letter
_TOKEN_RE = re.compile(
    r"(?P<repos>\(\d+\))"             # (14) rest days
    r"|(?P<year>\d{2})(?=[0-9DATR]|$)"  # year separator: 2 digits not followed by discipline
    r"|(?P<entry>[0-9DATR][AaMmPpSsHhCc])",  # race entry
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def decode_musique(musique: str) -> list[dict[str, Any]]:
    """Decode a PMU musique string into a list of structured entries.

    Parameters
    ----------
    musique : str
        Raw musique string, e.g. ``"6a5a2a0m1p4p3p1p5p"``.

    Returns
    -------
    list[dict]
        One dict per race entry with keys:
        - position (int | None) – finishing position, or None for D/A/T/R
        - discipline (str) – full discipline name
        - is_dq (bool)
        - is_arrete (bool)
        - is_tombe (bool)
        - repos_jours (int | None) – rest days *preceding* this entry, if any
    """
    if not musique:
        return []

    entries: list[dict[str, Any]] = []
    pending_repos: int | None = None

    for m in _TOKEN_RE.finditer(musique):
        if m.group("repos"):
            # Extract number of rest days
            pending_repos = int(m.group("repos").strip("()"))
        elif m.group("year"):
            # Year separator – skip
            continue
        elif m.group("entry"):
            raw = m.group("entry")
            pos_char = raw[0].upper()
            disc_char = raw[1].lower()

            is_dq = pos_char == "D"
            is_arrete = pos_char == "A"
            is_tombe = pos_char == "T"
            is_refuse = pos_char == "R"

            if pos_char.isdigit():
                position: int | None = int(pos_char)
            else:
                position = None

            entries.append({
                "position": position,
                "discipline": _DISCIPLINE_MAP.get(disc_char, disc_char),
                "is_dq": is_dq,
                "is_arrete": is_arrete,
                "is_tombe": is_tombe,
                "is_refuse": is_refuse,
                "repos_jours": pending_repos,
            })
            pending_repos = None

    return entries


def musique_features(musique: str) -> dict[str, Any]:
    """Compute derived features from a musique string.

    Parameters
    ----------
    musique : str
        Raw musique string.

    Returns
    -------
    dict
        nb_courses_musique, nb_victoires_musique, nb_places_musique (top 3),
        taux_victoire_recent, taux_place_recent, derniere_position,
        avant_derniere_position, nb_dq_recent, discipline_dominante,
        repos_moyen_jours.
    """
    entries = decode_musique(musique)
    n = len(entries)

    positions = [e["position"] for e in entries if e["position"] is not None]
    victoires = sum(1 for p in positions if p == 1)
    places = sum(1 for p in positions if p <= 3)
    nb_dq = sum(1 for e in entries if e["is_dq"])

    derniere = entries[0]["position"] if entries else None
    avant_derniere = entries[1]["position"] if len(entries) >= 2 else None

    # Discipline dominante
    disc_counter: Counter[str] = Counter(e["discipline"] for e in entries)
    discipline_dominante = disc_counter.most_common(1)[0][0] if disc_counter else None

    # Repos moyen
    repos_vals = [e["repos_jours"] for e in entries if e["repos_jours"] is not None]
    repos_moyen = round(sum(repos_vals) / len(repos_vals), 1) if repos_vals else None

    return {
        "nb_courses_musique": n,
        "nb_victoires_musique": victoires,
        "nb_places_musique": places,
        "taux_victoire_recent": round(victoires / n, 4) if n else 0.0,
        "taux_place_recent": round(places / n, 4) if n else 0.0,
        "derniere_position": derniere,
        "avant_derniere_position": avant_derniere,
        "nb_dq_recent": nb_dq,
        "discipline_dominante": discipline_dominante,
        "repos_moyen_jours": repos_moyen,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    data_path = Path(__file__).resolve().parent.parent / "output" / "02_liste_courses" / "partants_normalises.json"
    with open(data_path, encoding="utf-8") as f:
        partants = json.load(f)

    total = len(partants)
    decoded_counts: list[int] = []
    feat_accum: dict[str, list] = {}

    for p in partants:
        m = p.get("musique", "")
        if not m:
            continue
        feats = musique_features(m)
        decoded_counts.append(feats["nb_courses_musique"])
        for k, v in feats.items():
            feat_accum.setdefault(k, []).append(v)

    print(f"Partants analysés : {total}")
    print(f"Avec musique non-vide : {len(decoded_counts)}")
    if decoded_counts:
        print(f"Nb courses par musique – min={min(decoded_counts)}, "
              f"max={max(decoded_counts)}, moy={sum(decoded_counts)/len(decoded_counts):.1f}")

    for key in ("nb_victoires_musique", "nb_places_musique", "nb_dq_recent"):
        vals = [v for v in feat_accum.get(key, []) if v is not None]
        if vals:
            print(f"{key} – min={min(vals)}, max={max(vals)}, moy={sum(vals)/len(vals):.2f}")

    # Top disciplines
    disc_vals = [v for v in feat_accum.get("discipline_dominante", []) if v]
    if disc_vals:
        dc = Counter(disc_vals)
        print(f"Disciplines dominantes : {dc.most_common(5)}")
