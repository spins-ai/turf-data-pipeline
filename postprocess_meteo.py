#!/usr/bin/env python3
"""
Post-processing météo — Ajoute les flags calculés manquants
Enrichit meteo_master.json avec :
  - is_cold, is_hot, is_windy (booléens)
  - meteo_score (score composite 0-10)
  - terrain_category (bon/souple/lourd/très_lourd)

⚠️ NE SUPPRIME RIEN — écrit un nouveau fichier enrichi
"""

import json, os, logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)
nBASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Mapping pénétromètre texte → valeur numérique ──
# Pistes en herbe : valeur = pénétrabilité (plus haut = plus lourd)
# PSF (Piste en Sable Fibré) : échelle différente, on normalise
PENETRO_TEXT_TO_NUMERIC = {
    # Herbe — classique
    "sec":              2.0,
    "très leger":       2.2,
    "tres leger":       2.2,
    "très léger":       2.2,
    "leger":            2.4,
    "léger":            2.4,
    "bon léger":        2.6,
    "bon leger":        2.6,
    "bon":              2.8,
    "bon souple":       3.3,
    "assez souple":     3.5,
    "souple":           3.8,
    "très souple":      4.3,
    "tres souple":      4.3,
    "tres_souple":      4.3,
    "collant":          4.6,
    "lourd":            4.8,
    "très lourd":       5.5,
    "tres lourd":       5.5,
    "profond":          5.8,
    # PSF (Piste en Sable Fibré) — normalisé sur même échelle
    "psf tres rapide":  2.0,
    "psf rapide":       2.3,
    "psf standard":     2.8,
    "psf":              2.8,
    "psf lente":        3.5,
    # Encodages cassés (UTF-8 mal décodé : ï¿½ = caractère de remplacement)
    "trï¿½s souple":    4.3,
    "trï¿½s lourd":     5.5,
    "lï¿½ger":          2.4,
    "bon lï¿½ger":      2.6,
}

# Catégorie terrain à partir de la valeur numérique
def _terrain_cat_from_numeric(val):
    if val < 2.3:   return "tres_sec"
    if val < 2.7:   return "leger"
    if val < 3.1:   return "bon"
    if val < 3.5:   return "bon_souple"
    if val < 4.0:   return "souple"
    if val < 4.5:   return "tres_souple"
    if val < 5.0:   return "lourd"
    return "tres_lourd"

# Type de piste (herbe vs PSF)
def _is_psf(text):
    return text.startswith("psf")


def enrich_meteo(record):
    """Ajoute les champs calculés à un record météo"""

    # Température
    temp = record.get("temperature_c") or record.get("temp_max_c")
    if temp is not None:
        try:
            temp = float(temp)
            record["is_cold"] = temp < 5
            record["is_hot"] = temp > 30
            record["temp_category"] = (
                "gel" if temp < 0 else
                "froid" if temp < 5 else
                "frais" if temp < 12 else
                "doux" if temp < 20 else
                "chaud" if temp < 30 else
                "canicule"
            )
        except (ValueError, TypeError):
            pass

    # Vent
    wind = record.get("wind_speed_kmh") or record.get("wind_max_kmh")
    if wind is not None:
        try:
            wind = float(wind)
            record["is_windy"] = wind > 30
            record["wind_category"] = (
                "calme" if wind < 10 else
                "leger" if wind < 20 else
                "modere" if wind < 30 else
                "fort" if wind < 50 else
                "tempete"
            )
        except (ValueError, TypeError):
            pass

    # Humidité
    humidity = record.get("humidity_pct")
    if humidity is not None:
        try:
            humidity = float(humidity)
            record["is_humid"] = humidity > 80
            record["humidity_category"] = (
                "sec" if humidity < 40 else
                "normal" if humidity < 70 else
                "humide" if humidity < 85 else
                "tres_humide"
            )
        except (ValueError, TypeError):
            pass

    # Pénétromètre → valeur numérique + catégorie terrain
    penetro_raw = record.get("penetrometre")
    penetro = None
    if penetro_raw is not None:
        # Essayer d'abord en numérique direct
        try:
            penetro = float(penetro_raw)
        except (ValueError, TypeError):
            # Sinon, mapping texte → numérique
            text = str(penetro_raw).strip().lower()
            if text in PENETRO_TEXT_TO_NUMERIC:
                penetro = PENETRO_TEXT_TO_NUMERIC[text]
                record["penetrometre_numeric"] = penetro
                record["is_psf"] = _is_psf(text)
            elif text != "inconnu":
                record["penetrometre_numeric"] = None  # texte non reconnu

        if penetro is not None:
            record["penetrometre_numeric"] = penetro
            record["terrain_category"] = _terrain_cat_from_numeric(penetro)

    # Score météo composite (0 = conditions parfaites, 10 = conditions extrêmes)
    score = 0
    if temp is not None:
        try:
            temp = float(temp)
            if temp < 0: score += 3
            elif temp < 5: score += 2
            elif temp > 30: score += 2
            elif temp > 25: score += 1
        except (ValueError, TypeError): pass

    if wind is not None:
        try:
            wind = float(wind)
            if wind > 50: score += 3
            elif wind > 30: score += 2
            elif wind > 20: score += 1
        except (ValueError, TypeError): pass

    if humidity is not None:
        try:
            humidity = float(humidity)
            if humidity > 90: score += 2
            elif humidity > 80: score += 1
        except (ValueError, TypeError): pass

    if penetro is not None:
        try:
            penetro = float(penetro)
            if penetro > 5.0: score += 2
            elif penetro > 4.0: score += 1
        except (ValueError, TypeError): pass

    record["meteo_score"] = min(score, 10)

    return record


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("POST-PROCESSING MÉTÉO")
    log.info("=" * 60)

    path = os.path.join(BASE_DIR, "data_master", "meteo_master.json")
    log.info(f"Chargement {path}...")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"  → {len(data)} records")

    # Enrichir
    log.info("Enrichissement...")
    for r in data:
        enrich_meteo(r)

    # Stats
    total = len(data)
    for field in ['is_cold', 'is_hot', 'is_windy', 'is_humid', 'is_psf',
                  'temp_category', 'wind_category', 'humidity_category',
                  'terrain_category', 'penetrometre_numeric', 'meteo_score']:
        count = sum(1 for r in data if r.get(field) is not None)
        log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Distribution meteo_score
    scores = {}
    for r in data:
        s = r.get("meteo_score", -1)
        scores[s] = scores.get(s, 0) + 1
    log.info(f"  Scores météo: {dict(sorted(scores.items()))}")

    # Distribution terrain_category
    terrains = {}
    for r in data:
        t = r.get("terrain_category")
        if t:
            terrains[t] = terrains.get(t, 0) + 1
    log.info(f"  Terrains: {dict(sorted(terrains.items(), key=lambda x: -x[1]))}")

    # PSF vs Herbe
    psf_count = sum(1 for r in data if r.get("is_psf") is True)
    herbe_count = sum(1 for r in data if r.get("is_psf") is False)
    log.info(f"  PSF: {psf_count}, Herbe: {herbe_count}")

    # Sauvegarder (écraser le master)
    log.info("Sauvegarde meteo_master.json enrichi...")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)
    log.info(f"  → {os.path.getsize(path)/1024/1024:.1f} MB")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s")

if __name__ == "__main__":
    main()
