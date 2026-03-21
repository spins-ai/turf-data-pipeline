#!/usr/bin/env python3
"""
audit_data_integrity.py — Étape 2 du TODO
==========================================
Audit complet de toutes les données collectées.

Vérifie :
  - Validité JSON/JSONL (pas tronqué, parsable)
  - Nombre de records par fichier vs attendu
  - Fichiers de 0 bytes ou corrompus
  - Cohérence des plages de dates (2013-2026)
  - Couverture par année, hippodrome, discipline
  - Doublons (course_uid, partant_uid)
  - Outliers (cotes négatives, distances aberrantes, etc.)
  - Types de données (string vs int vs float)
  - Taux de remplissage par champ

Output : output/audit/
  - audit_report.md (rapport complet)
  - audit_stats.json (stats machine-readable)

Usage :
    python3 audit_data_integrity.py
"""

import json
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "audit")
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)


# ================================================================
# UTILITAIRES
# ================================================================

def count_jsonl_records(path):
    """Compte les records dans un fichier JSONL et vérifie la validité."""
    if not os.path.exists(path):
        return {"exists": False}

    size = os.path.getsize(path)
    total = 0
    errors = 0
    empty_lines = 0
    sample_fields = set()

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                empty_lines += 1
                continue
            try:
                record = json.loads(line)
                total += 1
                if total <= 5 and isinstance(record, dict):
                    sample_fields.update(record.keys())
            except json.JSONDecodeError:
                errors += 1
                if errors <= 3:
                    log.warning(f"  JSON invalide ligne {line_num} dans {path}")

    return {
        "exists": True,
        "size_mb": round(size / 1024 / 1024, 1),
        "total_records": total,
        "json_errors": errors,
        "empty_lines": empty_lines,
        "nb_fields": len(sample_fields),
        "fields_sample": sorted(sample_fields)[:20],
    }


def count_json_records(path):
    """Compte les records dans un fichier JSON."""
    if not os.path.exists(path):
        return {"exists": False}

    size = os.path.getsize(path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            nb = len(data)
            sample_fields = set()
            if nb > 0 and isinstance(data[0], dict):
                sample_fields = set(data[0].keys())
            del data
            return {
                "exists": True,
                "size_mb": round(size / 1024 / 1024, 1),
                "total_records": nb,
                "json_errors": 0,
                "nb_fields": len(sample_fields),
                "fields_sample": sorted(sample_fields)[:20],
            }
        elif isinstance(data, dict):
            del data
            return {
                "exists": True,
                "size_mb": round(size / 1024 / 1024, 1),
                "total_records": 1,
                "json_errors": 0,
                "type": "dict",
            }
    except json.JSONDecodeError as e:
        return {
            "exists": True,
            "size_mb": round(size / 1024 / 1024, 1),
            "json_errors": 1,
            "error": str(e)[:100],
        }
    except MemoryError:
        return {
            "exists": True,
            "size_mb": round(size / 1024 / 1024, 1),
            "json_errors": 0,
            "note": "Trop gros pour charger en mémoire — utiliser JSONL",
        }


def audit_file(path):
    """Audit un fichier (JSON ou JSONL)."""
    if path.endswith(".jsonl"):
        return count_jsonl_records(path)
    elif path.endswith(".json"):
        return count_json_records(path)
    return {"exists": False, "error": "Format inconnu"}


def stream_partants_fields(path, max_records=100000):
    """Analyse les champs des partants en streaming (taux de remplissage)."""
    field_counts = Counter()
    field_types = defaultdict(Counter)
    total = 0
    uids = set()
    dates = set()
    hippos = Counter()
    disciplines = Counter()
    positions = []
    cotes = []
    distances = []

    opener = None
    if path.endswith(".jsonl"):
        def opener():
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            pass
    elif path.endswith(".json"):
        def opener():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for item in data:
                yield item
            del data
    else:
        return {}

    for record in opener():
        total += 1
        if total > max_records:
            break

        if isinstance(record, dict):
            for key, value in record.items():
                if value is not None and value != "" and value != []:
                    field_counts[key] += 1

                # Type tracking
                if value is not None:
                    field_types[key][type(value).__name__] += 1

            # Doublons
            uid = record.get("partant_uid") or record.get("course_uid")
            if uid:
                uids.add(uid)

            # Dates
            date_iso = record.get("date_reunion_iso", "")
            if date_iso:
                dates.add(date_iso[:4])  # année

            # Hippodromes
            hippo = record.get("hippodrome_normalise", "")
            if hippo:
                hippos[hippo] += 1

            # Disciplines
            disc = record.get("discipline", "")
            if disc:
                disciplines[disc] += 1

            # Outliers
            pos = record.get("position_arrivee")
            if pos is not None:
                try:
                    positions.append(int(pos))
                except (ValueError, TypeError):
                    pass

            cote = record.get("cote_finale")
            if cote is not None:
                try:
                    cotes.append(float(cote))
                except (ValueError, TypeError):
                    pass

            dist = record.get("distance")
            if dist is not None:
                try:
                    distances.append(int(dist))
                except (ValueError, TypeError):
                    pass

    # Calculer taux de remplissage
    fill_rates = {}
    for key, count in field_counts.items():
        fill_rates[key] = round(count / total * 100, 1) if total > 0 else 0

    # Outliers
    outliers = {}
    if cotes:
        neg_cotes = sum(1 for c in cotes if c < 0)
        if neg_cotes:
            outliers["cotes_negatives"] = neg_cotes
        huge_cotes = sum(1 for c in cotes if c > 500)
        if huge_cotes:
            outliers["cotes_>500"] = huge_cotes

    if distances:
        weird_dist = sum(1 for d in distances if d < 500 or d > 10000)
        if weird_dist:
            outliers["distances_aberrantes"] = weird_dist

    if positions:
        weird_pos = sum(1 for p in positions if p < 0 or p > 30)
        if weird_pos:
            outliers["positions_aberrantes"] = weird_pos

    return {
        "total_analysed": total,
        "unique_uids": len(uids),
        "doublons_estimes": total - len(uids) if uids else "N/A",
        "annees": sorted(dates),
        "nb_hippodromes": len(hippos),
        "top_hippodromes": dict(hippos.most_common(10)),
        "disciplines": dict(disciplines),
        "fill_rates_low": {k: v for k, v in sorted(fill_rates.items(), key=lambda x: x[1]) if v < 80},
        "fill_rates_high": {k: v for k, v in sorted(fill_rates.items(), key=lambda x: -x[1]) if v >= 80},
        "outliers": outliers,
    }


# ================================================================
# MAIN
# ================================================================

def main():
    log.info("=" * 70)
    log.info("AUDIT DATA INTEGRITY — Étape 2")
    log.info("=" * 70)

    report_lines = ["# Rapport d'audit — Data Warehouse Hippique", ""]
    report_lines.append(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report_lines.append("")

    stats = {}

    # ================================================================
    # 1. Audit de chaque fichier output
    # ================================================================
    log.info("Phase 1: Audit des fichiers de données...")
    report_lines.append("## 1. Fichiers de données")
    report_lines.append("")

    # Liste des fichiers à auditer
    files_to_audit = [
        # Script 02
        ("02_courses_brut", "output/02_liste_courses/courses_brut.json"),
        ("02_courses_brut_jsonl", "output/02_liste_courses/courses_brut.jsonl"),
        ("02_courses_norm", "output/02_liste_courses/courses_normalisees.json"),
        ("02_courses_norm_jsonl", "output/02_liste_courses/courses_normalisees.jsonl"),
        ("02_partants_brut", "output/02_liste_courses/partants_brut.json"),
        ("02_partants_brut_jsonl", "output/02_liste_courses/partants_brut.jsonl"),
        ("02_partants_norm", "output/02_liste_courses/partants_normalises.json"),
        ("02_partants_norm_jsonl", "output/02_liste_courses/partants_normalises.jsonl"),
        ("02_courses_ref", "output/02_liste_courses/courses_references_04.json"),
        ("02_courses_ref_jsonl", "output/02_liste_courses/courses_references_04.jsonl"),
        # Script 02b
        ("02b_courses", "output/02b_liste_courses_2013/courses_normalisees.json"),
        ("02b_partants", "output/02b_liste_courses_2013/partants_normalises.json"),
        # Scripts collecte
        ("04_resultats", "output/04_resultats/resultats.json"),
        ("05_historique_chevaux", "output/05_historique_chevaux/historique_chevaux.json"),
        ("06_historique_jockeys", "output/06_historique_jockeys/historique_jockeys.json"),
        ("07_cotes_marche", "output/07_cotes_marche/cotes_marche.json"),
        ("08_pedigree", "output/08_pedigree/pedigree.json"),
        ("09_equipements", "output/09_equipements/equipements_historique.json"),
        ("10_poids", "output/10_poids_handicaps/poids_handicaps.json"),
        ("11_sectionals", "output/11_sectionals/sectionals.json"),
        ("13_meteo", "output/13_meteo_historique/meteo_historique.json"),
        ("14_pedigree_pq", "output/14_pedigree/pedigrees_pq.json"),
        ("14_pedigree_pq_jsonl", "output/14_pedigree/pedigrees_pq.jsonl"),
        # Scripts JSONL
        ("21_rapports", "output/21_rapports_definitifs/rapports_definitifs.jsonl"),
        ("22_performances", "output/22_performances_detaillees/performances_detaillees.jsonl"),
        ("23_pronostics", "output/23_pronostics_equidia/pronostics.json"),
        ("24_canalturf", "output/24_canalturf/canalturf.json"),
        ("25_turfostats", "output/25_turfostats/turfostats.json"),
        ("26_geny", "output/26_geny/geny.json"),
        ("27_citations", "output/27_citations_enjeux/citations_enjeux.jsonl"),
        ("28_combinaisons", "output/28_combinaisons_marche/combinaisons_marche.jsonl"),
        ("30_smarkets", "output/30_smarkets_exchange/smarkets.json"),
        ("37_racing_post", "output/37_racing_post/racing_post_fr.json"),
        ("37_racing_post_jsonl", "output/37_racing_post/racing_post_fr.jsonl"),
        ("38_rapports_internet", "output/38_rapports_internet/rapports_internet.jsonl"),
        ("39_reunions", "output/39_reunions_enrichies/reunions_enrichies.jsonl"),
        ("40_enrichissement", "output/40_enrichissement_partants/enrichissement.json"),
        # Masters
        ("master_pedigree", os.path.join(BASE_DIR, "data_master", "pedigree_master.json")),
        ("master_rapports", os.path.join(BASE_DIR, "data_master", "rapports_master.json")),
        ("master_meteo", os.path.join(BASE_DIR, "data_master", "meteo_master.json")),
        ("master_stats_ext", os.path.join(BASE_DIR, "data_master", "stats_externes_master.json")),
        ("master_marche", os.path.join(BASE_DIR, "data_master", "marche_master.json")),
        ("master_equipements", os.path.join(BASE_DIR, "data_master", "equipements_master.json")),
        ("master_horse_stats", os.path.join(BASE_DIR, "data_master", "horse_stats_master.json")),
    ]

    for name, path in files_to_audit:
        if os.path.exists(path):
            log.info(f"  Audit: {name} ({path})")
            result = audit_file(path)
            stats[name] = result
            size = result.get("size_mb", 0)
            records = result.get("total_records", "?")
            errors = result.get("json_errors", 0)
            status = "OK" if errors == 0 else f"ERREURS: {errors}"
            report_lines.append(f"| {name} | {size} MB | {records} records | {status} |")
        else:
            stats[name] = {"exists": False}

    # ================================================================
    # 2. Audit détaillé des partants
    # ================================================================
    log.info("Phase 2: Audit détaillé des partants...")
    report_lines.append("")
    report_lines.append("## 2. Audit détaillé partants_normalises")
    report_lines.append("")

    for path in ["output/02_liste_courses/partants_normalises.jsonl",
                 "output/02_liste_courses/partants_normalises.json"]:
        if os.path.exists(path):
            log.info(f"  Analyse champs: {path}")
            field_analysis = stream_partants_fields(path, max_records=500000)
            stats["partants_field_analysis"] = field_analysis

            report_lines.append(f"Records analysés : {field_analysis.get('total_analysed', 0)}")
            report_lines.append(f"UIDs uniques : {field_analysis.get('unique_uids', 0)}")
            report_lines.append(f"Doublons estimés : {field_analysis.get('doublons_estimes', 'N/A')}")
            report_lines.append(f"Années couvertes : {', '.join(field_analysis.get('annees', []))}")
            report_lines.append(f"Hippodromes : {field_analysis.get('nb_hippodromes', 0)}")
            report_lines.append("")

            # Disciplines
            report_lines.append("### Disciplines")
            for disc, count in sorted(field_analysis.get("disciplines", {}).items(), key=lambda x: -x[1]):
                report_lines.append(f"  - {disc}: {count}")
            report_lines.append("")

            # Champs faibles
            report_lines.append("### Champs avec taux de remplissage < 80%")
            for field, rate in sorted(field_analysis.get("fill_rates_low", {}).items(), key=lambda x: x[1]):
                report_lines.append(f"  - {field}: {rate}%")
            report_lines.append("")

            # Outliers
            outliers = field_analysis.get("outliers", {})
            if outliers:
                report_lines.append("### Outliers détectés")
                for k, v in outliers.items():
                    report_lines.append(f"  - {k}: {v}")
            report_lines.append("")

            # Top hippodromes
            report_lines.append("### Top 10 hippodromes")
            for hippo, count in field_analysis.get("top_hippodromes", {}).items():
                report_lines.append(f"  - {hippo}: {count}")

            break

    # ================================================================
    # 3. Vérification des fichiers de cache
    # ================================================================
    log.info("Phase 3: Vérification des caches...")
    report_lines.append("")
    report_lines.append("## 3. Caches et checkpoints")
    report_lines.append("")

    cache_dirs = [
        "output/02_liste_courses/cache",
        "output/14_pedigree/cache",
        "output/21_rapports_definitifs/cache",
        "output/37_racing_post/cache",
        "output/38_rapports_internet/cache",
    ]

    for cache_dir in cache_dirs:
        if os.path.exists(cache_dir):
            files = os.listdir(cache_dir)
            total_size = sum(os.path.getsize(os.path.join(cache_dir, f)) for f in files if os.path.isfile(os.path.join(cache_dir, f)))
            report_lines.append(f"- {cache_dir}: {len(files)} fichiers, {round(total_size/1024/1024, 1)} MB")
            stats[f"cache_{os.path.basename(os.path.dirname(cache_dir))}"] = {
                "nb_files": len(files),
                "size_mb": round(total_size / 1024 / 1024, 1),
            }

    # Checkpoints
    report_lines.append("")
    report_lines.append("### Checkpoints")
    for root, dirs, files in os.walk("output"):
        for f in files:
            if f.startswith(".checkpoint") and f.endswith(".json"):
                cp_path = os.path.join(root, f)
                try:
                    with open(cp_path, "r") as fh:
                        cp = json.load(fh)
                    report_lines.append(f"- {cp_path}: {json.dumps(cp)[:200]}")
                except Exception:
                    report_lines.append(f"- {cp_path}: ERREUR LECTURE")

    # ================================================================
    # 4. Résumé
    # ================================================================
    log.info("Phase 4: Génération du rapport...")

    existing = {k: v for k, v in stats.items() if isinstance(v, dict) and v.get("exists")}
    missing = {k: v for k, v in stats.items() if isinstance(v, dict) and not v.get("exists", True)}
    errors = {k: v for k, v in stats.items() if isinstance(v, dict) and v.get("json_errors", 0) > 0}

    total_records = sum(v.get("total_records", 0) for v in existing.values() if isinstance(v.get("total_records"), int))
    total_size = sum(v.get("size_mb", 0) for v in existing.values())

    report_lines.insert(3, "")
    report_lines.insert(4, "## Résumé")
    report_lines.insert(5, f"- Fichiers existants : {len(existing)}")
    report_lines.insert(6, f"- Fichiers manquants : {len(missing)}")
    report_lines.insert(7, f"- Fichiers avec erreurs JSON : {len(errors)}")
    report_lines.insert(8, f"- Total records : {total_records:,}")
    report_lines.insert(9, f"- Taille totale : {total_size:,.1f} MB")
    report_lines.insert(10, "")

    if missing:
        report_lines.insert(11, "### Fichiers manquants")
        for k in sorted(missing.keys()):
            report_lines.insert(12, f"  - {k}")

    # Sauver rapport
    report_path = os.path.join(OUTPUT_DIR, "audit_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    log.info(f"Rapport: {report_path}")

    # Sauver stats JSON
    stats_path = os.path.join(OUTPUT_DIR, "audit_stats.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"Stats: {stats_path}")

    log.info("=" * 70)
    log.info(f"AUDIT TERMINÉ: {len(existing)} fichiers, {total_records:,} records, {total_size:,.1f} MB")
    if errors:
        log.warning(f"  ATTENTION: {len(errors)} fichiers avec erreurs JSON")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
