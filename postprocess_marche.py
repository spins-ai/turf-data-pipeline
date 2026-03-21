#!/usr/bin/env python3
"""
Post-processing marché — Enrichit marche_master.json avec :
  - cote_category (grand_favori / favori / outsider / longshot / extreme)
  - value_indicator (sur-coté / neutre / sous-coté par rapport à la médiane)
  - taille_course_category (petit / moyen / grand champ)
  - proba_category (très probable / probable / possible / improbable)

⚠️ NE SUPPRIME RIEN — enrichit le fichier existant
"""

import json, os, logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)


def enrich_marche(record):
    """Ajoute les champs calculés à un record marché"""

    # ── Catégorie de cote ──
    cote = record.get("cote_finale")
    if cote is not None:
        try:
            c = float(cote)
            record["cote_category"] = (
                "grand_favori" if c < 3.0 else
                "favori" if c < 6.0 else
                "deuxieme_favori" if c < 10.0 else
                "outsider" if c < 20.0 else
                "longshot" if c < 50.0 else
                "extreme"
            )
        except (ValueError, TypeError):
            pass

    # ── Value indicator (écart vs cote médiane) ──
    ecart = record.get("ecart_cote_moyenne")
    cote_moy = record.get("cote_moyenne_course")
    if ecart is not None and cote_moy is not None:
        try:
            e = float(ecart)
            m = float(cote_moy)
            if m > 0:
                ratio = e / m
                record["value_ratio"] = round(ratio, 3)
                record["value_indicator"] = (
                    "tres_surcote" if ratio > 1.0 else
                    "surcote" if ratio > 0.3 else
                    "neutre" if ratio > -0.3 else
                    "souscote" if ratio > -0.7 else
                    "tres_souscote"
                )
        except (ValueError, TypeError):
            pass

    # ── Catégorie de probabilité implicite ──
    proba = record.get("proba_implicite")
    if proba is not None:
        try:
            p = float(proba)
            record["proba_category"] = (
                "tres_probable" if p > 0.25 else
                "probable" if p > 0.12 else
                "possible" if p > 0.05 else
                "improbable" if p > 0.02 else
                "tres_improbable"
            )
        except (ValueError, TypeError):
            pass

    # ── Taille du champ ──
    nb = record.get("nb_partants_course")
    if nb is not None:
        try:
            n = int(nb)
            record["taille_champ"] = (
                "petit" if n <= 8 else
                "moyen" if n <= 12 else
                "grand" if n <= 16 else
                "tres_grand"
            )
        except (ValueError, TypeError):
            pass

    # ── Position dans le rang de cote (favori = 1er rang) ──
    rang = record.get("rang_cote")
    if rang is not None:
        try:
            r = int(rang)
            record["is_top3_cote"] = r <= 3
            record["is_top5_cote"] = r <= 5
        except (ValueError, TypeError):
            pass

    # ── Part de masse (popularité du cheval chez les parieurs) ──
    pct = record.get("pct_masse")
    if pct is not None:
        try:
            p = float(pct)
            record["popularite"] = (
                "star" if p > 20.0 else
                "populaire" if p > 10.0 else
                "moyen" if p > 5.0 else
                "discret" if p > 2.0 else
                "ignore"
            )
        except (ValueError, TypeError):
            pass

    return record


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("POST-PROCESSING MARCHÉ")
    log.info("=" * 60)

    path = "data_master/marche_master.json"
    log.info(f"Chargement {path}...")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"  → {len(data)} records")

    # Enrichir
    log.info("Enrichissement...")
    for r in data:
        enrich_marche(r)

    # Stats
    total = len(data)
    new_fields = [
        'cote_category', 'value_ratio', 'value_indicator',
        'proba_category', 'taille_champ',
        'is_top3_cote', 'is_top5_cote', 'popularite',
    ]
    for field in new_fields:
        count = sum(1 for r in data if r.get(field) is not None)
        log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Distributions
    for cat_field in ['cote_category', 'value_indicator', 'proba_category', 'taille_champ', 'popularite']:
        vals = {}
        for r in data:
            v = r.get(cat_field)
            if v:
                vals[v] = vals.get(v, 0) + 1
        log.info(f"  {cat_field}: {dict(sorted(vals.items(), key=lambda x: -x[1]))}")

    # Sauvegarder
    log.info("Sauvegarde marche_master.json enrichi...")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)
    log.info(f"  → {os.path.getsize(path)/1024/1024:.1f} MB")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s")

if __name__ == "__main__":
    main()
