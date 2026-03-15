"""
feature_builders.jockey_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-jockey/driver and per-trainer historical features.
Temporal integrity: for any partant at date D, only races with date < D are used.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional


def _safe_rate(count: int, total: int) -> Optional[float]:
    if total == 0:
        return None
    return count / total


def _safe_mean(values: list) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _build_actor_features(
    partants: list[dict],
    actor_field: str,
    prefix: str,
) -> list[dict]:
    """Generic builder for jockey or trainer features.

    Parameters
    ----------
    partants : list[dict]
        All partant records sorted by date.
    actor_field : str
        Field name for the actor (e.g. 'jockey_driver' or 'entraineur').
    prefix : str
        Prefix for feature names (e.g. 'jockey_' or 'entraineur_').

    Returns
    -------
    list[dict]
        One dict per partant_uid with computed features.
    """
    sorted_p = sorted(
        partants,
        key=lambda p: (p.get("date_reunion_iso", ""), p.get("course_uid", ""), p.get("num_pmu", 0)),
    )

    # actor -> list of past race records
    actor_history: dict[str, list[dict]] = defaultdict(list)

    results = []

    for p in sorted_p:
        uid = p.get("partant_uid")
        actor = p.get(actor_field, "") or ""
        date_iso = p.get("date_reunion_iso", "")
        hippo = p.get("hippodrome_normalise", "")
        dist = p.get("distance")
        cheval = p.get("nom_cheval", "")

        # Past races for this actor, strictly before current date
        past = [r for r in actor_history.get(actor, []) if r["date"] < date_iso]

        # Parse current date for time-window calculations
        try:
            dt_current = datetime.strptime(date_iso, "%Y-%m-%d")
        except (ValueError, TypeError):
            dt_current = None

        disc = p.get("discipline", "")

        # -- Time-windowed stats --
        def _window_stats(days: int):
            if dt_current is None:
                return 0, None, None, None, None
            cutoff = (dt_current - timedelta(days=days)).strftime("%Y-%m-%d")
            window = [r for r in past if r["date"] >= cutoff]
            nb = len(window)
            tv = _safe_rate(sum(1 for r in window if r["gagnant"]), nb)
            tp = _safe_rate(sum(1 for r in window if r["place"]), nb)
            # avg position in window
            positions = [r["position"] for r in window if r.get("position") is not None]
            avg_pos = _safe_mean(positions)
            # total gains in window
            total_gains = sum(r.get("gains", 0) or 0 for r in window)
            return nb, tv, tp, avg_pos, total_gains

        nb_30, tv_30, tp_30, avg_pos_30, gains_30 = _window_stats(30)
        nb_90, tv_90, tp_90, avg_pos_90, gains_90 = _window_stats(90)
        nb_365, tv_365, tp_365, avg_pos_365, _gains_365 = _window_stats(365)

        # -- streak victoires (consecutive wins from most recent) --
        streak_v = 0
        for r in reversed(past):
            if r["gagnant"]:
                streak_v += 1
            else:
                break

        # -- nb_hippodromes in last 30 days (diversity) --
        nb_hippo_30 = None
        if dt_current is not None:
            cutoff_30 = (dt_current - timedelta(days=30)).strftime("%Y-%m-%d")
            hippos_30 = set(r["hippo"] for r in past if r["date"] >= cutoff_30 and r["hippo"])
            nb_hippo_30 = len(hippos_30)

        # -- specialite_taux: win rate in same discipline --
        past_disc = [r for r in past if r.get("discipline") == disc]
        tv_specialite = _safe_rate(
            sum(1 for r in past_disc if r["gagnant"]),
            len(past_disc),
        )

        # -- taux victoire at hippodrome --
        past_hippo = [r for r in past if r["hippo"] == hippo]
        tv_hippo = _safe_rate(
            sum(1 for r in past_hippo if r["gagnant"]),
            len(past_hippo),
        )

        # -- taux victoire at similar distance (+-200m) --
        past_dist = [
            r for r in past
            if dist is not None and r["distance"] is not None and abs(r["distance"] - dist) <= 200
        ]
        tv_dist = _safe_rate(
            sum(1 for r in past_dist if r["gagnant"]),
            len(past_dist),
        )

        # -- actor x horse stats --
        past_cheval = [r for r in past if r["cheval"] == cheval]
        nb_montes_cheval = len(past_cheval)
        tv_cheval = _safe_rate(
            sum(1 for r in past_cheval if r["gagnant"]),
            nb_montes_cheval,
        )

        feat = {
            "partant_uid": uid,
            f"{prefix}nb_montes_30j": nb_30,
            f"{prefix}nb_montes_90j": nb_90,
            f"{prefix}nb_montes_365j": nb_365,
            f"{prefix}taux_victoire_30j": tv_30,
            f"{prefix}taux_victoire_90j": tv_90,
            f"{prefix}taux_victoire_365j": tv_365,
            f"{prefix}taux_place_30j": tp_30,
            f"{prefix}taux_place_90j": tp_90,
            f"{prefix}taux_place_365j": tp_365,
            f"{prefix}avg_position_30j": avg_pos_30,
            f"{prefix}avg_position_90j": avg_pos_90,
            f"{prefix}avg_position_365j": avg_pos_365,
            f"{prefix}streak_victoires": streak_v,
            f"{prefix}gains_30j": gains_30,
            f"{prefix}gains_90j": gains_90,
            f"{prefix}nb_hippodromes_30j": nb_hippo_30,
            f"{prefix}specialite_taux": tv_specialite,
            f"{prefix}taux_victoire_hippo": tv_hippo,
            f"{prefix}taux_victoire_distance": tv_dist,
            f"{prefix}nb_montes_cheval": nb_montes_cheval,
            f"{prefix}taux_victoire_cheval": tv_cheval,
        }
        results.append(feat)

        # Record this race for future lookbacks
        actor_history[actor].append({
            "date": date_iso,
            "gagnant": bool(p.get("is_gagnant")),
            "place": bool(p.get("is_place")),
            "hippo": hippo,
            "distance": dist,
            "cheval": cheval,
            "position": p.get("position_arrivee"),
            "gains": p.get("gains_carriere_euros", 0),
            "discipline": disc,
        })

    return results


def build_jockey_features(partants: list[dict]) -> list[dict]:
    """Build per-jockey historical features for every partant."""
    return _build_actor_features(partants, "jockey_driver", "jockey_")


def build_entraineur_features(partants: list[dict]) -> list[dict]:
    """Build per-trainer historical features for every partant."""
    return _build_actor_features(partants, "entraineur", "entraineur_")


if __name__ == "__main__":
    import os
    base = os.path.join(os.path.dirname(__file__), "..", "output", "02_liste_courses")
    path = os.path.join(base, "partants_normalises.json")
    with open(path, encoding="utf-8") as f:
        partants = json.load(f)

    for builder, name in [
        (build_jockey_features, "jockey"),
        (build_entraineur_features, "entraineur"),
    ]:
        feats = builder(partants)
        print(f"\n=== {name} features ===")
        print(f"Built {len(feats)} records.")
        if feats:
            keys = [k for k in feats[0] if k != "partant_uid"]
            print(f"Features ({len(keys)}): {', '.join(keys)}")
            for k in keys:
                filled = sum(1 for r in feats if r.get(k) is not None)
                print(f"  {k}: {filled}/{len(feats)} ({100*filled/len(feats):.1f}%)")
