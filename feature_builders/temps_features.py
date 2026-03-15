"""
feature_builders.temps_features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Builds performance timing features from temps_ms and reduction_km_ms.
"""

from __future__ import annotations

from typing import Any


def build_temps_features(partants: list[dict]) -> list[dict]:
    """Build timing/speed features for each partant.

    Features produced (15):
    - temps_temps_ms: raw finish time in ms
    - temps_reduction_km_ms: reduction kilométrique in ms
    - temps_vitesse_kmh: average speed in km/h
    - temps_relatif_vainqueur: time difference vs winner (ms)
    - temps_ecart_gagnant_pct: % slower than winner
    - temps_rang_vitesse: rank by speed within race
    - temps_reduction_relative: reduction km relative to race average
    - temps_avg_reduction_5: average reduction km over last 5 races
    - temps_avg_reduction_10: average reduction km over last 10 races
    - temps_best_reduction_5: best reduction km over last 5 races
    - temps_best_reduction_10: best reduction km over last 10 races
    - temps_reduction_trend: trend in reduction km (positive = improving)
    - temps_ecart_moyen_champ: time gap to field average
    - temps_speed_consistency: std deviation of reduction km (lower = more consistent)
    """
    # Group by course for relative computations
    courses: dict[str, list[dict]] = {}
    for p in partants:
        cuid = p.get("course_uid")
        if cuid:
            courses.setdefault(cuid, []).append(p)

    # Pre-compute per-course timing stats
    course_stats: dict[str, dict] = {}
    for cuid, runners in courses.items():
        times = [r.get("temps_ms") for r in runners if r.get("temps_ms")]
        reductions = [r.get("reduction_km_ms") for r in runners if r.get("reduction_km_ms")]

        winner_time = None
        for r in runners:
            if r.get("position_arrivee") == 1 and r.get("temps_ms"):
                winner_time = r["temps_ms"]
                break

        stats: dict[str, Any] = {}
        if times:
            stats["avg_time"] = sum(times) / len(times)
            stats["min_time"] = min(times)
            stats["times_sorted"] = sorted(times)
        if reductions:
            stats["avg_reduction"] = sum(reductions) / len(reductions)
        stats["winner_time"] = winner_time
        course_stats[cuid] = stats

    # Build horse history for trend computation
    horse_history: dict[str, list[dict]] = {}
    for p in partants:
        nom = p.get("nom_cheval")
        if nom:
            horse_history.setdefault(nom, []).append(p)

    for nom in horse_history:
        horse_history[nom].sort(key=lambda x: x.get("date_reunion_iso", ""))

    # Index to find position in horse history
    horse_idx: dict[str, dict[str, int]] = {}
    for nom, races in horse_history.items():
        idx = {}
        for i, r in enumerate(races):
            uid = r.get("partant_uid")
            if uid:
                idx[uid] = i
        horse_idx[nom] = idx

    results = []
    for p in partants:
        uid = p.get("partant_uid")
        cuid = p.get("course_uid")
        nom = p.get("nom_cheval")
        distance = p.get("distance")
        row: dict[str, Any] = {"partant_uid": uid}

        temps_ms = p.get("temps_ms")
        red_km = p.get("reduction_km_ms")

        row["temps_temps_ms"] = temps_ms
        row["temps_reduction_km_ms"] = red_km

        # Speed in km/h
        if temps_ms and distance and temps_ms > 0:
            speed = (distance / 1000) / (temps_ms / 3_600_000)
            row["temps_vitesse_kmh"] = round(speed, 2)
        else:
            row["temps_vitesse_kmh"] = None

        # Relative to race
        stats = course_stats.get(cuid, {})

        if temps_ms and stats.get("winner_time"):
            row["temps_relatif_vainqueur"] = temps_ms - stats["winner_time"]
            # Percentage slower than winner
            if stats["winner_time"] > 0:
                row["temps_ecart_gagnant_pct"] = round(
                    ((temps_ms - stats["winner_time"]) / stats["winner_time"]) * 100, 3
                )
            else:
                row["temps_ecart_gagnant_pct"] = None
        else:
            row["temps_relatif_vainqueur"] = None
            row["temps_ecart_gagnant_pct"] = None

        if temps_ms and stats.get("avg_time"):
            row["temps_ecart_moyen_champ"] = round(temps_ms - stats["avg_time"], 1)
        else:
            row["temps_ecart_moyen_champ"] = None

        if temps_ms and stats.get("times_sorted"):
            row["temps_rang_vitesse"] = sum(1 for t in stats["times_sorted"] if t < temps_ms) + 1
        else:
            row["temps_rang_vitesse"] = None

        if red_km and stats.get("avg_reduction"):
            row["temps_reduction_relative"] = round(red_km - stats["avg_reduction"], 1)
        else:
            row["temps_reduction_relative"] = None

        # Historical reduction km stats
        if nom and nom in horse_idx:
            cur_idx = horse_idx[nom].get(uid)
            if cur_idx is not None and cur_idx > 0:
                prior = horse_history[nom][:cur_idx]
                prior_reds = [r.get("reduction_km_ms") for r in prior if r.get("reduction_km_ms")]
                prior_reds.reverse()  # most recent first

                last_5 = prior_reds[:5]
                last_10 = prior_reds[:10]

                row["temps_avg_reduction_5"] = round(sum(last_5) / len(last_5), 1) if last_5 else None
                row["temps_avg_reduction_10"] = round(sum(last_10) / len(last_10), 1) if last_10 else None
                row["temps_best_reduction_5"] = min(last_5) if last_5 else None
                row["temps_best_reduction_10"] = min(last_10) if last_10 else None

                # Speed consistency: std deviation of reduction km (lower = more consistent)
                if len(prior_reds) >= 3:
                    mean_red = sum(prior_reds) / len(prior_reds)
                    variance = sum((r - mean_red) ** 2 for r in prior_reds) / len(prior_reds)
                    row["temps_speed_consistency"] = round(variance ** 0.5, 1)
                else:
                    row["temps_speed_consistency"] = None

                # Trend: compare last 3 vs previous 3 (lower = faster = better for trot)
                recent_3 = prior_reds[:3]
                prev_3 = prior_reds[3:6]
                if len(recent_3) >= 2 and len(prev_3) >= 2:
                    row["temps_reduction_trend"] = round(
                        sum(prev_3) / len(prev_3) - sum(recent_3) / len(recent_3), 1
                    )
                else:
                    row["temps_reduction_trend"] = None
            else:
                row["temps_avg_reduction_5"] = None
                row["temps_avg_reduction_10"] = None
                row["temps_best_reduction_5"] = None
                row["temps_best_reduction_10"] = None
                row["temps_speed_consistency"] = None
                row["temps_reduction_trend"] = None
        else:
            row["temps_avg_reduction_5"] = None
            row["temps_avg_reduction_10"] = None
            row["temps_best_reduction_5"] = None
            row["temps_best_reduction_10"] = None
            row["temps_speed_consistency"] = None
            row["temps_reduction_trend"] = None

        results.append(row)

    return results
