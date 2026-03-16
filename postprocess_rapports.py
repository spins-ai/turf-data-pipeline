#!/usr/bin/env python3
"""
Post-processing rapports — Enrichit rapports_master.json avec :
  - is_quinte, is_tierce, is_quarte (booléens — type de course)
  - nb_partants_arrivee (calculé depuis la combinaison)
  - jour_semaine (lundi=0..dimanche=6) + jour_semaine_label
  - mois, saison
  - discipline_norm (normalisation des disciplines)
  - distance_category (sprint/moyenne/longue/marathon)
  - rapport_surprise (rapport simple gagnant élevé = outsider gagne)

⚠️ NE SUPPRIME RIEN — enrichit le fichier existant
"""

import json, os, logging, time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

JOURS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
SAISONS = {1: "hiver", 2: "hiver", 3: "printemps", 4: "printemps", 5: "printemps",
           6: "ete", 7: "ete", 8: "ete", 9: "automne", 10: "automne", 11: "automne", 12: "hiver"}

# Normalisation des disciplines
DISCIPLINE_MAP = {
    "plat": "plat",
    "haie": "haie",
    "steeple": "steeple",
    "steeplechase": "steeple",
    "steeple-chase": "steeple",
    "cross": "cross",
    "cross-country": "cross",
    "trot_attele": "trot_attele",
    "trot attele": "trot_attele",
    "trot attelé": "trot_attele",
    "attele": "trot_attele",
    "attelé": "trot_attele",
    "trot_monte": "trot_monte",
    "trot monte": "trot_monte",
    "trot monté": "trot_monte",
    "monte": "trot_monte",
    "monté": "trot_monte",
}


def enrich_rapport(record):
    """Ajoute les champs calculés à un record rapport"""

    # ── Date features ──
    date_str = record.get("date_reunion_iso")
    if date_str:
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            record["jour_semaine"] = dt.weekday()  # 0=lundi
            record["jour_semaine_label"] = JOURS[dt.weekday()]
            record["mois"] = dt.month
            record["saison"] = SAISONS.get(dt.month, "inconnu")
            record["annee"] = dt.year
        except (ValueError, TypeError):
            pass

    # ── Type de course (quinté, quarté, tiercé) ──
    record["is_quinte"] = record.get("rapport_quinte_ordre") is not None
    record["is_quarte"] = record.get("rapport_quarte_ordre") is not None
    record["is_tierce"] = record.get("rapport_tierce_ordre") is not None

    # ── Nombre de partants estimé depuis la combinaison ──
    combi = record.get("combinaison", "")
    if isinstance(combi, str) and "-" in combi:
        parts = [p.strip() for p in combi.split("-") if p.strip()]
        try:
            nums = [int(p) for p in parts]
            record["nb_partants_arrivee"] = len(nums)
            record["dernier_numero_arrivee"] = max(nums) if nums else None
        except ValueError:
            pass

    # ── Discipline normalisée ──
    disc = record.get("discipline")
    if disc:
        disc_lower = str(disc).strip().lower()
        record["discipline_norm"] = DISCIPLINE_MAP.get(disc_lower, disc_lower)

    # ── Distance catégorie ──
    distance = record.get("distance")
    if distance is not None:
        try:
            d = int(distance)
            record["distance_m"] = d
            record["distance_category"] = (
                "sprint" if d < 1400 else
                "mile" if d < 1800 else
                "moyenne" if d < 2200 else
                "classique" if d < 2800 else
                "longue" if d < 3500 else
                "marathon"
            )
        except (ValueError, TypeError):
            pass

    # ── Rapport surprise (outsider qui gagne) ──
    rsg = record.get("rapport_simple_gagnant")
    if rsg is not None:
        try:
            rsg = float(rsg)
            # Rapport en centimes : 300 = 3.00€ pour 1.50€ misé
            rapport_euros = rsg / 100.0
            record["rapport_gagnant_euros"] = rapport_euros
            record["is_surprise"] = rapport_euros > 10.0   # > 10€ pour 1.50€
            record["is_super_surprise"] = rapport_euros > 30.0  # > 30€
            record["is_favori_gagne"] = rapport_euros < 3.0  # < 3€
        except (ValueError, TypeError):
            pass

    # ── Rapport quinté/quarté en euros (plus lisible) ──
    for field_in, field_out in [
        ("rapport_quinte_ordre", "quinte_euros"),
        ("rapport_quarte_ordre", "quarte_euros"),
        ("rapport_tierce_ordre", "tierce_euros"),
    ]:
        val = record.get(field_in)
        if val is not None:
            try:
                record[field_out] = float(val) / 100.0
            except (ValueError, TypeError):
                pass

    return record


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("POST-PROCESSING RAPPORTS")
    log.info("=" * 60)

    path = "data_master/rapports_master.json"
    log.info(f"Chargement {path}...")
    with open(path) as f:
        data = json.load(f)
    log.info(f"  → {len(data)} records")

    # Enrichir
    log.info("Enrichissement...")
    for r in data:
        enrich_rapport(r)

    # Stats
    total = len(data)
    new_fields = [
        'jour_semaine', 'jour_semaine_label', 'mois', 'saison', 'annee',
        'is_quinte', 'is_quarte', 'is_tierce',
        'nb_partants_arrivee', 'discipline_norm', 'distance_category', 'distance_m',
        'rapport_gagnant_euros', 'is_surprise', 'is_super_surprise', 'is_favori_gagne',
        'quinte_euros', 'quarte_euros', 'tierce_euros',
    ]
    for field in new_fields:
        count = sum(1 for r in data if r.get(field) is not None)
        log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Distributions intéressantes
    # Jour de la semaine
    jours = {}
    for r in data:
        j = r.get("jour_semaine_label")
        if j:
            jours[j] = jours.get(j, 0) + 1
    log.info(f"  Jours: {dict(sorted(jours.items(), key=lambda x: JOURS.index(x[0])))}")

    # Saisons
    saisons = {}
    for r in data:
        s = r.get("saison")
        if s:
            saisons[s] = saisons.get(s, 0) + 1
    log.info(f"  Saisons: {saisons}")

    # Disciplines
    discs = {}
    for r in data:
        d = r.get("discipline_norm")
        if d:
            discs[d] = discs.get(d, 0) + 1
    log.info(f"  Disciplines: {dict(sorted(discs.items(), key=lambda x: -x[1]))}")

    # Distances
    dists = {}
    for r in data:
        d = r.get("distance_category")
        if d:
            dists[d] = dists.get(d, 0) + 1
    log.info(f"  Distances: {dict(sorted(dists.items(), key=lambda x: -x[1]))}")

    # Surprises
    surprises = sum(1 for r in data if r.get("is_surprise"))
    super_s = sum(1 for r in data if r.get("is_super_surprise"))
    favori = sum(1 for r in data if r.get("is_favori_gagne"))
    log.info(f"  Favoris gagnent: {favori}, Surprises: {surprises}, Super surprises: {super_s}")

    # Sauvegarder
    log.info("Sauvegarde rapports_master.json enrichi...")
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)
    log.info(f"  → {os.path.getsize(path)/1024/1024:.1f} MB")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s")

if __name__ == "__main__":
    main()
