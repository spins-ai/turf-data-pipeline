#!/usr/bin/env python3
"""
40_enrichissement_partants.py
Lit tous les fichiers JSON du cache des reunions (output/02_liste_courses/cache/)
et extrait les champs manquants de partants_normalises.json :
  - gains_victoires_euros, gains_place_euros, gains_annee_precedente_euros
  - cote_tendance, is_favori_direct, grosse_prise
  - cote_ref_tendance, is_favori_ref
Exporte vers output/40_enrichissement_partants/enrichissement_partants.json
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import json
import os
import re
import sys
from multiprocessing import Pool, cpu_count
from pathlib import Path

from utils.normalize import normaliser_texte

BASE_DIR = Path(__file__).resolve().parent
CACHE_DIR = BASE_DIR / "../../output" / "02_liste_courses" / "cache"
OUTPUT_DIR = BASE_DIR / "../../output" / "40_enrichissement_partants"

FILENAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_R(\d+)\.json$")


def safe_get(d, *keys, default=None):
    """Navigue dans un dict imbriqué en toute securite."""
    current = d
    for k in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(k)
        if current is None:
            return default
    return current


def process_file(filepath: str) -> list:
    """Traite un fichier cache et retourne une liste d'enregistrements."""
    fname = os.path.basename(filepath)
    m = FILENAME_RE.match(fname)
    if not m:
        return []

    date_iso = m.group(1)
    num_reunion = int(m.group(2))

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    reunion_data = data.get("reunion_data") or {}
    participants_by_course = data.get("participants") or {}

    # Hippodrome normalise
    hippo_raw = safe_get(reunion_data, "hippodrome", "libelleCourt", default="")
    hippo_norm = normaliser_texte(hippo_raw)

    records = []

    for course_key, course_data in participants_by_course.items():
        try:
            num_course = int(course_key)
        except (ValueError, TypeError):
            continue

        participants = course_data.get("participants") or []

        for p in participants:
            num_pmu = p.get("numPmu")
            if num_pmu is None:
                continue

            cle_partant = f"{date_iso}|{hippo_norm}|R{num_reunion}|C{num_course}|{num_pmu}"

            gains = p.get("gainsParticipant") or {}
            rapport_direct = p.get("dernierRapportDirect") or {}
            rapport_ref = p.get("dernierRapportReference") or {}

            gv = gains.get("gainsVictoires")
            gp = gains.get("gainsPlace")
            gap = gains.get("gainsAnneePrecedente")

            record = {
                "cle_partant": cle_partant,
                "gains_victoires_euros": round(gv / 100, 2) if gv is not None else None,
                "gains_place_euros": round(gp / 100, 2) if gp is not None else None,
                "gains_annee_precedente_euros": round(gap / 100, 2) if gap is not None else None,
                "cote_tendance": rapport_direct.get("nombreIndicateurTendance"),
                "is_favori_direct": rapport_direct.get("favoris"),
                "grosse_prise": rapport_direct.get("grossePrise"),
                "cote_ref_tendance": rapport_ref.get("nombreIndicateurTendance"),
                "is_favori_ref": rapport_ref.get("favoris"),
            }

            records.append(record)

    return records


def main():
    print("=" * 70)
    print("40 - Enrichissement partants depuis le cache des reunions")
    print("=" * 70)

    if not CACHE_DIR.exists():
        print(f"ERREUR: Repertoire cache introuvable: {CACHE_DIR}")
        sys.exit(1)

    # Lister tous les fichiers JSON du cache
    all_files = sorted([
        str(CACHE_DIR / f) for f in os.listdir(CACHE_DIR)
        if FILENAME_RE.match(f)
    ])
    total_files = len(all_files)
    print(f"Fichiers cache trouves: {total_files}")

    if total_files == 0:
        print("Aucun fichier a traiter.")
        sys.exit(0)

    # Traitement multiprocessing par batch
    n_workers = min(cpu_count(), 8)
    print(f"Workers: {n_workers}")

    all_records = []
    batch_size = 500
    processed = 0

    with Pool(processes=n_workers) as pool:
        for i in range(0, total_files, batch_size):
            batch = all_files[i : i + batch_size]
            results = pool.map(process_file, batch)
            for recs in results:
                all_records.extend(recs)
            processed += len(batch)
            if processed % 1000 < batch_size or processed == total_files:
                print(f"  Progres: {processed:>6d} / {total_files} fichiers "
                      f"({processed * 100 / total_files:.1f}%) - "
                      f"{len(all_records)} enregistrements")

    total_records = len(all_records)
    print(f"\nTotal enregistrements extraits: {total_records}")

    # Stats: combien ont des valeurs non-null pour chaque champ
    fields = [
        "gains_victoires_euros", "gains_place_euros",
        "gains_annee_precedente_euros", "cote_tendance",
        "is_favori_direct", "grosse_prise",
        "cote_ref_tendance", "is_favori_ref",
    ]

    print("\n--- Statistiques de remplissage ---")
    for field in fields:
        non_null = sum(1 for r in all_records if r.get(field) is not None)
        pct = (non_null / total_records * 100) if total_records > 0 else 0
        print(f"  {field:35s}: {non_null:>9d} / {total_records} ({pct:.1f}%)")

    # Exporter
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "enrichissement_partants.json"
    print(f"\nExport vers: {out_path}")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False)
    file_size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"Fichier ecrit: {file_size_mb:.1f} MB")
    print("Termine.")


if __name__ == "__main__":
    main()
