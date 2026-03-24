#!/usr/bin/env python3
"""
integrate_new_sources.py  (Etape 8.3)
Integrateur generique pour toute nouvelle source scraper.
  - Prend un repertoire source en argument (ex: output/58_at_the_races)
  - Auto-detecte les fichiers JSONL dans ce repertoire
  - Match avec partants_master par meilleure cle disponible
  - Merge les nouveaux champs avec prefixe derive du nom de source
  - Rapporte le taux de matching

Usage:
  python integrate_new_sources.py output/58_at_the_races
  python integrate_new_sources.py output/58_at_the_races --prefix atr
  python integrate_new_sources.py output/58_at_the_races --dry-run
"""

import argparse
import json

import re
import sys
import time
from collections import defaultdict
from pathlib import Path

from utils.normalize import normalize_date, normalize_name

BASE_DIR = Path(__file__).resolve().parent
PARTANTS_MASTER = BASE_DIR / "data_master" / "partants_master.jsonl"
LOG_DIR = BASE_DIR / "logs"

LOG_DIR.mkdir(exist_ok=True)

from utils.logging_setup import setup_logging
log = setup_logging("integrate_new_sources")


# ── Detection automatique des champs de matching ─────────────────────

# Champs candidats pour le nom du cheval (par priorite)
HORSE_NAME_FIELDS = [
    "nom_cheval", "horse_name", "name", "horse", "nom", "cheval",
    "runner_name", "runner", "participant", "horse_nom",
]

# Champs candidats pour la date
DATE_FIELDS = [
    "date", "date_reunion_iso", "race_date", "date_course",
    "meeting_date", "event_date", "date_iso",
]

# Champs candidats pour l'hippodrome
TRACK_FIELDS = [
    "hippodrome", "hippodrome_normalise", "track", "racecourse",
    "course", "venue", "meeting",
]


def detect_matching_fields(sample_records):
    """
    Analyse un echantillon de records pour detecter
    les meilleurs champs de matching.
    """
    detected = {"horse_field": None, "date_field": None, "track_field": None}
    field_counts = defaultdict(int)

    for record in sample_records:
        for key in record.keys():
            field_counts[key] += 1

    n = len(sample_records) if sample_records else 1

    # Detecter le champ nom du cheval
    for candidate in HORSE_NAME_FIELDS:
        if field_counts.get(candidate, 0) / n > 0.5:
            detected["horse_field"] = candidate
            break

    # Detecter le champ date
    for candidate in DATE_FIELDS:
        if field_counts.get(candidate, 0) / n > 0.5:
            detected["date_field"] = candidate
            break

    # Detecter le champ hippodrome
    for candidate in TRACK_FIELDS:
        if field_counts.get(candidate, 0) / n > 0.3:
            detected["track_field"] = candidate
            break

    return detected


def derive_prefix(source_dir_name):
    """Derive un prefixe court depuis le nom du repertoire source."""
    # Supprimer le numero initial
    name = re.sub(r"^\d+_", "", source_dir_name)
    # Abbrevier
    parts = name.split("_")
    if len(parts) == 1:
        return parts[0][:6]
    # Prendre premieres lettres de chaque mot
    prefix = "".join(p[:3] for p in parts[:3])
    return prefix.lower()


# ── Chargement source ────────────────────────────────────────────────

def find_jsonl_files(source_dir):
    """Trouve tous les .jsonl dans le repertoire source (hors cache)."""
    source_path = Path(source_dir)
    if not source_path.exists():
        return []

    jsonl_files = []
    for f in source_path.iterdir():
        if f.suffix == ".jsonl" and f.is_file():
            jsonl_files.append(f)
    return sorted(jsonl_files)


def load_source_records(jsonl_files, max_sample=1000):
    """Charge tous les records des fichiers JSONL. Retourne records + sample."""
    all_records = []
    sample = []
    n_errors = 0

    for fpath in jsonl_files:
        log.info("  Lecture %s ...", fpath.name)
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    all_records.append(record)
                    if len(sample) < max_sample:
                        sample.append(record)
                except json.JSONDecodeError:
                    n_errors += 1

    log.info("  Total records charges : %d (erreurs: %d)", len(all_records), n_errors)
    return all_records, sample


def build_source_index(records, horse_field, date_field, track_field=None):
    """
    Construit un index (nom_norm, date) -> [records]
    avec fallback sur (nom_norm, date, track) si disponible.
    """
    index_2key = defaultdict(list)  # (nom, date)
    index_3key = defaultdict(list)  # (nom, date, track)
    n_indexed = 0
    n_skipped = 0

    for record in records:
        horse_raw = record.get(horse_field, "") if horse_field else ""
        date_raw = record.get(date_field, "") if date_field else ""

        horse_norm = normalize_name(horse_raw)
        date_norm = normalize_date(date_raw)

        if not horse_norm or not date_norm:
            n_skipped += 1
            continue

        index_2key[(horse_norm, date_norm)].append(record)
        n_indexed += 1

        if track_field:
            track_raw = record.get(track_field, "")
            if track_raw:
                track_norm = normalize_name(track_raw)
                index_3key[(horse_norm, date_norm, track_norm)].append(record)

    log.info(
        "  Index construit : %d records indexes, %d sans cle, %d cles 2-tuple, %d cles 3-tuple",
        n_indexed, n_skipped, len(index_2key), len(index_3key),
    )
    return index_2key, index_3key


# ── Merge ─────────────────────────────────────────────────────────────

# Champs a ne pas copier (meta / cles de matching)
SKIP_FIELDS = {
    "date", "source", "type", "scraped_at", "nom_cheval", "horse_name",
    "name", "horse", "nom", "cheval", "runner_name", "runner",
    "date_reunion_iso", "race_date", "date_course", "meeting_date",
}


def merge_source_into_partant(partant, source_records, prefix):
    """Merge les champs d'un ou plusieurs source records dans le partant."""
    if not source_records:
        return partant, False

    # Prendre le premier record comme base
    best = source_records[0]

    partant[f"{prefix}_source"] = True
    partant[f"{prefix}_nb_records"] = len(source_records)

    # Copier tous les champs non-meta avec prefixe
    n_fields = 0
    for key, value in best.items():
        if key.lower() in SKIP_FIELDS:
            continue
        if value is None or value == "":
            continue
        prefixed_key = f"{prefix}_{key}"
        # Ne pas ecraser un champ existant
        if prefixed_key not in partant:
            partant[prefixed_key] = value
            n_fields += 1

    return partant, n_fields > 0


# ── Pipeline principal ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Integrateur generique de nouvelles sources dans partants_master"
    )
    parser.add_argument(
        "source_dir",
        help="Repertoire source contenant les .jsonl (ex: output/58_at_the_races)",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Prefixe pour les champs (defaut: derive du nom du repertoire)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyse sans ecrire le fichier de sortie",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Fichier de sortie (defaut: data_master/partants_master_enrichi_<prefix>.jsonl)",
    )
    parser.add_argument(
        "--horse-field",
        default=None,
        help="Forcer le champ nom du cheval (defaut: auto-detect)",
    )
    parser.add_argument(
        "--date-field",
        default=None,
        help="Forcer le champ date (defaut: auto-detect)",
    )
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    if not source_dir.is_absolute():
        source_dir = BASE_DIR / source_dir

    start = time.time()
    dir_name = source_dir.name
    prefix = args.prefix or derive_prefix(dir_name)

    log.info("=" * 60)
    log.info("INTEGRATION GENERIQUE : %s", dir_name)
    log.info("  Prefixe : %s_*", prefix)
    log.info("=" * 60)

    # 1. Trouver les JSONL
    jsonl_files = find_jsonl_files(source_dir)
    if not jsonl_files:
        log.warning("Aucun .jsonl trouve dans %s", source_dir)
        # Essayer aussi dans cache/
        cache_dir = source_dir / "cache"
        if cache_dir.exists():
            # Chercher JSON dans cache
            json_files = sorted(cache_dir.glob("*.json"))
            if json_files:
                log.info("  %d fichiers .json trouves dans cache/", len(json_files))
                log.info("  NOTE: ce script attend des .jsonl dans le repertoire principal.")
                log.info("  Les fichiers cache JSON sont des fichiers par date, pas des JSONL consolides.")
        sys.exit(1)

    log.info("Fichiers JSONL trouves : %d", len(jsonl_files))
    for f in jsonl_files:
        size_mb = f.stat().st_size / 1024 / 1024
        log.info("  %s (%.1f MB)", f.name, size_mb)

    # 2. Charger
    records, sample = load_source_records(jsonl_files)
    if not records:
        log.warning("Aucun record valide. Arret.")
        sys.exit(1)

    # 3. Detecter les champs de matching
    if args.horse_field and args.date_field:
        detected = {
            "horse_field": args.horse_field,
            "date_field": args.date_field,
            "track_field": None,
        }
    else:
        detected = detect_matching_fields(sample)
        if args.horse_field:
            detected["horse_field"] = args.horse_field
        if args.date_field:
            detected["date_field"] = args.date_field

    log.info("Champs de matching detectes :")
    log.info("  Nom cheval : %s", detected["horse_field"] or "(non detecte)")
    log.info("  Date       : %s", detected["date_field"] or "(non detecte)")
    log.info("  Hippodrome : %s", detected["track_field"] or "(non detecte)")

    if not detected["horse_field"] or not detected["date_field"]:
        log.warning("Impossible de detecter les champs de matching.")
        log.info("Champs disponibles dans la source :")
        all_fields = defaultdict(int)
        for r in sample:
            for k in r.keys():
                all_fields[k] += 1
        for k, v in sorted(all_fields.items(), key=lambda x: -x[1]):
            log.info("  %-30s : %d/%d records", k, v, len(sample))
        log.info("Utilisez --horse-field et --date-field pour specifier manuellement.")
        sys.exit(1)

    # 4. Construire l'index
    index_2key, index_3key = build_source_index(
        records,
        detected["horse_field"],
        detected["date_field"],
        detected["track_field"],
    )

    if not index_2key:
        log.warning("Index vide (aucun record avec nom+date). Arret.")
        sys.exit(1)

    # 5. Rapport champs disponibles
    log.info("Champs de la source (echantillon de %d records) :", len(sample))
    field_stats = defaultdict(int)
    for r in sample:
        for k in r.keys():
            if k.lower() not in SKIP_FIELDS:
                field_stats[k] += 1
    for k, v in sorted(field_stats.items(), key=lambda x: -x[1])[:20]:
        log.info("  %-30s : %d/%d (%.0f%%)", k, v, len(sample), 100.0 * v / len(sample))

    # 6. Dry-run ou merge
    if args.dry_run:
        log.info("=== DRY RUN : pas d'ecriture ===")
        log.info("  Records source    : %d", len(records))
        log.info("  Cles dans l'index : %d", len(index_2key))
        log.info("  Prefixe           : %s_*", prefix)
        log.info("  Utilisez sans --dry-run pour ecrire le fichier enrichi.")
        return

    if not PARTANTS_MASTER.exists():
        log.error("partants_master.jsonl introuvable : %s", PARTANTS_MASTER)
        sys.exit(1)

    output_file = Path(args.output) if args.output else (
        BASE_DIR / "data_master" / f"partants_master_enrichi_{prefix}.jsonl"
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)

    n_partants = 0
    n_matched = 0
    n_fields_added = 0

    with open(PARTANTS_MASTER, "r", encoding="utf-8", errors="replace") as fin, \
         open(output_file, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                partant = json.loads(line)
            except json.JSONDecodeError:
                continue

            n_partants += 1

            nom = normalize_name(partant.get("nom_cheval", ""))
            date = normalize_date(partant.get("date_reunion_iso", ""))

            source_records_matched = None

            if nom and date:
                # Essayer match 3-cle d'abord (plus precis)
                if index_3key and detected["track_field"]:
                    hippo = normalize_name(
                        partant.get("hippodrome_normalise", "")
                    )
                    if hippo:
                        source_records_matched = index_3key.get(
                            (nom, date, hippo)
                        )

                # Fallback sur match 2-cle
                if not source_records_matched:
                    source_records_matched = index_2key.get((nom, date))

            partant, enriched = merge_source_into_partant(
                partant, source_records_matched, prefix
            )

            if enriched:
                n_matched += 1

            fout.write(json.dumps(partant, ensure_ascii=False) + "\n")

            if n_partants % 200000 == 0:
                log.info(
                    "  ... %d partants, %d matches", n_partants, n_matched
                )

    elapsed = time.time() - start
    match_rate = 100.0 * n_matched / max(1, n_partants)

    log.info("=" * 60)
    log.info("RESULTATS INTEGRATION %s", dir_name.upper())
    log.info("  Source records   : %d", len(records))
    log.info("  Partants traites : %d", n_partants)
    log.info("  Matches          : %d (%.1f%%)", n_matched, match_rate)
    log.info("  Output ecrit     : %s", output_file)
    log.info("  Duree            : %.1f s", elapsed)

    if match_rate < 1.0:
        log.warning(
            "  ATTENTION : taux de match tres bas (%.1f%%). "
            "Verifiez les champs de matching avec --dry-run.", match_rate
        )
    elif match_rate < 10.0:
        log.info(
            "  NOTE : taux de match bas (%.1f%%). "
            "Les sources internationales matchent peu avec les courses PMU francaises.",
            match_rate,
        )

    log.info("=" * 60)


if __name__ == "__main__":
    main()
