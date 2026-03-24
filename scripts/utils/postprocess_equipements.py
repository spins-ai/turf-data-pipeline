#!/usr/bin/env python3
"""
Post-processing équipements — Enrichit equipements_master.json avec :
  - poids_category (plume / leger / moyen / lourd / surcharge)
  - oeilleres_category (sans / australiennes / classiques / etc.)
  - deferre_category (aucun / anterieurs / posterieurs / 4_pieds)
  - equipment_change_score (0-3 : nombre de changements d'équipement)
  - handicap_category (réclamer / handicap / conditionne / listed / groupe)

⚠️ NE SUPPRIME RIEN — enrichit le fichier existant
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json, os, time

from utils.logging_setup import setup_logging
log = setup_logging("postprocess_equipements")
nBASE_DIR = os.path.dirname(os.path.abspath(__file__))


def enrich_equipement(record):
    """Ajoute les champs calculés à un record équipement"""

    # ── Catégorie de poids ──
    poids = record.get("poids_porte_kg")
    if poids is not None:
        try:
            p = float(poids)
            record["poids_category"] = (
                "plume" if p < 52.0 else
                "leger" if p < 55.0 else
                "moyen" if p < 58.0 else
                "lourd" if p < 62.0 else
                "surcharge"
            )
        except (ValueError, TypeError):
            pass

    # ── Évolution de poids ──
    evol = record.get("evolution_poids")
    if evol is not None:
        try:
            e = float(evol)
            record["poids_direction"] = (
                "allegement" if e < -1.0 else
                "stable" if e <= 1.0 else
                "surcharge"
            )
            record["poids_change_abs"] = abs(e)
        except (ValueError, TypeError):
            pass

    # ── Catégorie œillères ──
    oeil = record.get("oeilleres")
    if oeil is not None:
        oeil_lower = str(oeil).strip().lower()
        if oeil_lower in ("sans", "sans oeillères", "sans oeilleres", ""):
            record["oeilleres_bool"] = False
        else:
            record["oeilleres_bool"] = True

    # ── Catégorie déferré ──
    deferre = record.get("deferre")
    if deferre is not None:
        d = str(deferre).strip().lower()
        if d in ("non", "aucun", "", "sans"):
            record["deferre_bool"] = False
            record["deferre_norm"] = "aucun"
        elif "4" in d or "quatre" in d or ("ant" in d and "post" in d):
            record["deferre_bool"] = True
            record["deferre_norm"] = "4_pieds"
        elif "ant" in d:
            record["deferre_bool"] = True
            record["deferre_norm"] = "anterieurs"
        elif "post" in d:
            record["deferre_bool"] = True
            record["deferre_norm"] = "posterieurs"
        else:
            record["deferre_bool"] = True
            record["deferre_norm"] = d

    # ── Score de changement d'équipement (0 = rien changé, 3 = tout changé) ──
    change_score = 0
    if record.get("oeilleres_change") is True:
        change_score += 1
    if record.get("deferre_change") is True:
        change_score += 1
    evol = record.get("evolution_poids")
    if evol is not None:
        try:
            if abs(float(evol)) > 2.0:
                change_score += 1
        except (ValueError, TypeError):
            pass
    record["equipment_change_score"] = change_score

    # ── Première fois aux œillères (signal fort) ──
    if record.get("premiere_oeilleres") is True:
        record["signal_premiere_oeilleres"] = True
    elif record.get("retrait_oeilleres") is True:
        record["signal_retrait_oeilleres"] = True

    # ── Position par rapport au top weight ──
    ecart = record.get("ecart_top_weight")
    if ecart is not None:
        try:
            e = float(ecart)
            record["position_poids"] = (
                "top_weight" if e == 0 else
                "proche_top" if e > -2.0 else
                "moyen" if e > -5.0 else
                "allege"
            )
        except (ValueError, TypeError):
            pass

    return record


def main():
    start = time.time()
    log.info("=" * 60)
    log.info("POST-PROCESSING ÉQUIPEMENTS")
    log.info("=" * 60)

    path = os.path.join(BASE_DIR, "../../data_master", "equipements_master.json")
    log.info(f"Chargement {path}...")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"  → {len(data)} records")

    # Enrichir
    log.info("Enrichissement...")
    for r in data:
        enrich_equipement(r)

    # Stats
    total = len(data)
    new_fields = [
        'poids_category', 'poids_direction', 'poids_change_abs',
        'oeilleres_bool', 'deferre_bool', 'deferre_norm',
        'equipment_change_score', 'signal_premiere_oeilleres', 'signal_retrait_oeilleres',
        'position_poids',
    ]
    for field in new_fields:
        count = sum(1 for r in data if r.get(field) is not None)
        log.info(f"  {field}: {count} ({count*100/total:.1f}%)")

    # Distributions
    for cat_field in ['poids_category', 'poids_direction', 'deferre_norm',
                      'equipment_change_score', 'position_poids']:
        vals = {}
        for r in data:
            v = r.get(cat_field)
            if v is not None:
                vals[v] = vals.get(v, 0) + 1
        log.info(f"  {cat_field}: {dict(sorted(vals.items(), key=lambda x: -x[1]))}")

    # Résumé œillères
    avec = sum(1 for r in data if r.get("oeilleres_bool") is True)
    sans = sum(1 for r in data if r.get("oeilleres_bool") is False)
    log.info(f"  Oeillères: avec={avec}, sans={sans}")

    # Sauvegarder
    log.info("Sauvegarde equipements_master.json enrichi...")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)
    log.info(f"  → {os.path.getsize(path)/1024/1024:.1f} MB")

    log.info(f"TERMINÉ en {time.time()-start:.0f}s")

if __name__ == "__main__":
    main()
