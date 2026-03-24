#!/usr/bin/env python3
"""
Feature Engineering — Meteo x Terrain Interaction

8 features: rain x going, frozen ground, headwind, weather impact.

Pour chaque partant, calcule les interactions meteo-terrain.

Features produites (~8) :
  - mti_rain_x_souple       -> True si pluie + terrain souple (double impact)
  - mti_rain_intensity       -> intensite pluie (0=sec, 1=legere, 2=moderee, 3=forte)
  - mti_terrain_degradation  -> terrain se degrade (pluie recente)
  - mti_frozen_risk          -> risque de gel (temperature < 3)
  - mti_headwind             -> True si vent > 30 km/h
  - mti_wind_impact          -> score impact vent (0=calme .. 3=tempete)
  - mti_heat_stress          -> True si temperature > 30 (stress thermique)
  - mti_ideal_conditions     -> True si bon terrain + pas de pluie + vent faible
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..'))  # project root

import logging

log = logging.getLogger(__name__)


def _parse_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def compute_meteo_terrain_interaction(partants):
    """
    Calcule les features d'interaction meteo x terrain.
    Pas de dependance temporelle : utilise les donnees de la course elle-meme.
    """
    log.info(f"Calcul interaction meteo-terrain sur {len(partants)} partants...")

    enriched = 0
    for i, p in enumerate(partants):
        feat = {}
        has_data = False

        # --- Parse terrain ---
        terrain = (
            p.get("meteo_terrain_category")
            or p.get("terrain_category")
            or p.get("etat_terrain")
            or p.get("reu_terrain")
            or ""
        )
        terrain_l = str(terrain).lower()
        is_souple = any(w in terrain_l for w in ("souple", "lourd", "collant", "soft", "heavy", "tres_souple"))
        is_bon = any(w in terrain_l for w in ("bon", "good", "ferme", "firm", "standard"))

        # --- Parse meteo ---
        temperature = _parse_float(
            p.get("temperature") or p.get("reu_temperature") or p.get("meteo_temperature")
        )
        precipitation = _parse_float(
            p.get("precipitation") or p.get("reu_precipitation") or p.get("meteo_pluie")
        )
        vent = _parse_float(
            p.get("vent_vitesse") or p.get("reu_vent_vitesse") or p.get("meteo_vent")
        )

        meteo_label = str(
            p.get("meteo_label") or p.get("reu_meteo_label") or p.get("meteo") or ""
        ).lower()

        # Detect rain from label if no numeric precipitation
        has_rain = False
        if precipitation and precipitation > 0:
            has_rain = True
        elif any(w in meteo_label for w in ("pluie", "rain", "averse", "bruine", "drizzle")):
            has_rain = True
            if precipitation is None:
                precipitation = 2.0  # default moderate

        # --- Rain intensity ---
        rain_intensity = 0
        if precipitation:
            if precipitation < 1:
                rain_intensity = 1  # legere
            elif precipitation < 5:
                rain_intensity = 2  # moderee
            else:
                rain_intensity = 3  # forte

        if has_rain or terrain_l:
            has_data = True

        # --- Rain x Souple (double handicap for certain horses) ---
        feat["mti_rain_x_souple"] = has_rain and is_souple
        feat["mti_rain_intensity"] = rain_intensity

        # --- Terrain degradation (rain on initially good terrain) ---
        feat["mti_terrain_degradation"] = has_rain and is_bon

        # --- Frozen ground risk ---
        if temperature is not None:
            has_data = True
            feat["mti_frozen_risk"] = temperature < 3
            feat["mti_heat_stress"] = temperature > 30
        else:
            feat["mti_frozen_risk"] = None
            feat["mti_heat_stress"] = None

        # --- Wind impact ---
        if vent is not None:
            has_data = True
            feat["mti_headwind"] = vent > 30
            if vent < 10:
                feat["mti_wind_impact"] = 0  # calme
            elif vent < 20:
                feat["mti_wind_impact"] = 1  # leger
            elif vent < 35:
                feat["mti_wind_impact"] = 2  # modere
            else:
                feat["mti_wind_impact"] = 3  # tempete
        else:
            feat["mti_headwind"] = None
            feat["mti_wind_impact"] = None

        # --- Ideal conditions ---
        if terrain_l:
            ideal = (
                is_bon
                and not has_rain
                and (vent is None or vent < 15)
                and (temperature is None or 10 <= temperature <= 25)
            )
            feat["mti_ideal_conditions"] = ideal
        else:
            feat["mti_ideal_conditions"] = None

        if has_data:
            enriched += 1
            p.update(feat)

        if (i + 1) % 200000 == 0:
            log.info(f"  {i+1}/{len(partants)}, {enriched} enrichis")

    log.info(f"  -> {enriched}/{len(partants)} enrichis ({enriched*100/max(len(partants),1):.1f}%)")
    return partants
