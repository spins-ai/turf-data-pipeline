#!/usr/bin/env python3
"""
audit_html_vs_json.py  (Etape 8.0)
Compare les champs disponibles dans le HTML brut scraped vs le JSON parse
pour chaque scraper du pipeline.
  - Pour chaque dossier output/XX_source avec cache/ contenant des HTML et JSON,
    compare les champs extraits dans le JSON vs les champs detectes dans le HTML
  - Liste les champs disponibles en HTML mais pas extraits
  - Priorise par valeur potentielle pour le modele

Usage:
  python audit_html_vs_json.py
  python audit_html_vs_json.py --source 56_timeform
  python audit_html_vs_json.py --top 20
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from html.parser import HTMLParser

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
AUDIT_DIR = BASE_DIR / "output" / "audit"
LOG_DIR = BASE_DIR / "logs"

LOG_DIR.mkdir(exist_ok=True)
AUDIT_DIR.mkdir(exist_ok=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging

log = setup_logging("audit_html_vs_json")


# ── HTML field extractor ──────────────────────────────────────────────

# Priorite des champs pour le modele predictif
FIELD_PRIORITY = {
    # Haute priorite : champs numeriques / performance
    "rating": 10, "speed": 10, "figure": 10, "timeform": 10,
    "odds": 9, "price": 9, "cote": 9, "sp": 9, "bsp": 9,
    "weight": 8, "poids": 8, "kg": 8,
    "distance": 8, "going": 8, "terrain": 8, "ground": 8,
    "form": 8, "musique": 8, "recent": 8,
    "win": 7, "place": 7, "victoire": 7,
    "jockey": 7, "trainer": 7, "entraineur": 7, "driver": 7,
    "age": 7, "draw": 7, "stall": 7, "corde": 7,
    "class": 6, "grade": 6, "groupe": 6,
    "time": 6, "temps": 6, "clock": 6, "sectional": 6,
    # Moyenne priorite
    "owner": 5, "proprietaire": 5, "silks": 5, "casaque": 5,
    "sire": 5, "dam": 5, "pere": 5, "mere": 5, "pedigree": 5,
    "breeding": 5, "bloodline": 5,
    "comment": 4, "note": 4, "verdict": 4, "tip": 4,
    "selection": 4, "pronostic": 4, "nap": 4,
    # Basse priorite
    "photo": 2, "image": 2, "video": 2,
    "link": 1, "url": 1, "href": 1,
    "ad": 0, "sponsor": 0, "banner": 0, "cookie": 0,
}


def estimate_field_priority(field_name):
    """Estime la priorite d'un champ base sur son nom."""
    field_lower = field_name.lower()
    best_score = 3  # score par defaut
    for keyword, score in FIELD_PRIORITY.items():
        if keyword in field_lower:
            best_score = max(best_score, score)
    return best_score


class HTMLFieldExtractor(HTMLParser):
    """
    Parse HTML et extrait les types de champs/donnees presentes.
    Detecte :
      - Attributs data-*
      - Classes semantiques (odds, rating, form, etc.)
      - Labels de formulaire
      - Noms de colonnes de tableaux
      - Meta tags
      - JSON-LD / structured data
    """

    def __init__(self):
        super().__init__()
        self.fields = defaultdict(int)
        self.current_tag = ""
        self.in_th = False
        self.in_label = False
        self.in_script_jsonld = False
        self.text_buffer = ""

    def handle_starttag(self, tag, attrs):
        self.current_tag = tag
        attrs_dict = dict(attrs)

        # data-* attributes
        for attr_name, attr_val in attrs:
            if attr_name.startswith("data-"):
                field_name = attr_name.replace("data-", "")
                self.fields[f"data-{field_name}"] += 1

            # Classes semantiques
            if attr_name == "class" and attr_val:
                for cls in attr_val.split():
                    cls_lower = cls.lower()
                    # Filtrer classes non-informatives
                    if len(cls_lower) < 3:
                        continue
                    if any(kw in cls_lower for kw in [
                        "odds", "price", "rating", "form", "weight",
                        "speed", "figure", "going", "draw", "stall",
                        "jockey", "trainer", "sire", "dam", "age",
                        "distance", "time", "result", "position",
                        "tip", "comment", "verdict", "selection",
                        "runner", "horse", "card", "race",
                    ]):
                        self.fields[f"class:{cls_lower}"] += 1

            # Input names
            if attr_name == "name" and tag in ("input", "select", "textarea"):
                self.fields[f"input:{attr_val}"] += 1

            # Meta tags
            if tag == "meta":
                meta_name = attrs_dict.get("name", attrs_dict.get("property", ""))
                if meta_name:
                    self.fields[f"meta:{meta_name}"] += 1

        # Table headers
        if tag == "th":
            self.in_th = True
            self.text_buffer = ""
        elif tag == "label":
            self.in_label = True
            self.text_buffer = ""

        # JSON-LD
        if tag == "script":
            script_type = attrs_dict.get("type", "")
            if "json" in script_type.lower():
                self.in_script_jsonld = True
                self.text_buffer = ""

    def handle_data(self, data):
        if self.in_th:
            self.text_buffer += data
        elif self.in_label:
            self.text_buffer += data
        elif self.in_script_jsonld:
            self.text_buffer += data

    def handle_endtag(self, tag):
        if tag == "th" and self.in_th:
            self.in_th = False
            header = self.text_buffer.strip()
            if header and len(header) < 50:
                self.fields[f"th:{header}"] += 1

        elif tag == "label" and self.in_label:
            self.in_label = False
            label = self.text_buffer.strip()
            if label and len(label) < 50:
                self.fields[f"label:{label}"] += 1

        elif tag == "script" and self.in_script_jsonld:
            self.in_script_jsonld = False
            try:
                data = json.loads(self.text_buffer)
                if isinstance(data, dict):
                    for key in data.keys():
                        self.fields[f"jsonld:{key}"] += 1
            except (json.JSONDecodeError, ValueError):
                pass

    def handle_error(self, message):
        pass


def extract_html_fields(html_content):
    """Extrait les champs/donnees detectees dans le HTML."""
    parser = HTMLFieldExtractor()
    try:
        parser.feed(html_content)
    except Exception as e:
        log.debug("Error parsing HTML fields: %s", e)
    return dict(parser.fields)


def extract_json_fields(json_content):
    """Extrait les cles presentes dans un JSON."""
    fields = set()
    try:
        data = json.loads(json_content)
        if isinstance(data, dict):
            _collect_keys(data, "", fields)
        elif isinstance(data, list):
            for item in data[:10]:  # Echantillon
                if isinstance(item, dict):
                    _collect_keys(item, "", fields)
    except (json.JSONDecodeError, ValueError):
        pass
    return fields


def _collect_keys(d, prefix, fields, depth=0):
    """Collecte recursivement les cles d'un dict."""
    if depth > 5:
        return
    for key, value in d.items():
        full_key = f"{prefix}.{key}" if prefix else key
        fields.add(full_key)
        if isinstance(value, dict):
            _collect_keys(value, full_key, fields, depth + 1)
        elif isinstance(value, list) and value:
            if isinstance(value[0], dict):
                _collect_keys(value[0], f"{full_key}[]", fields, depth + 1)


# ── Audit d'un scraper ───────────────────────────────────────────────

def audit_scraper(source_dir, max_html=20, max_json=20):
    """
    Audite un scraper : compare HTML scrape vs JSON parse.
    Retourne un rapport dict.
    """
    cache_dir = source_dir / "cache"
    source_name = source_dir.name

    report = {
        "source": source_name,
        "cache_dir": str(cache_dir),
        "html_files": 0,
        "json_files": 0,
        "jsonl_files": 0,
        "html_fields": {},
        "json_fields": set(),
        "html_only_fields": [],
        "json_only_fields": [],
        "common_fields": [],
    }

    if not cache_dir.exists():
        return report

    # Compter les fichiers
    html_files = sorted(cache_dir.glob("*.html"))
    json_files = sorted(cache_dir.glob("*.json"))
    jsonl_parent = sorted(source_dir.glob("*.jsonl"))

    report["html_files"] = len(html_files)
    report["json_files"] = len(json_files)
    report["jsonl_files"] = len(jsonl_parent)

    if not html_files and not json_files:
        return report

    # Analyser un echantillon de HTML
    html_fields_all = defaultdict(int)
    for html_file in html_files[:max_html]:
        try:
            with open(html_file, "r", encoding="utf-8", errors="replace") as f:
                content = f.read(500_000)  # Max 500KB par fichier
            fields = extract_html_fields(content)
            for k, v in fields.items():
                html_fields_all[k] += v
        except Exception as e:
            log.debug("  Erreur lecture HTML %s: %s", html_file.name, e)

    # Analyser un echantillon de JSON
    json_fields_all = set()
    for json_file in json_files[:max_json]:
        try:
            with open(json_file, "r", encoding="utf-8", errors="replace") as f:
                content = f.read(500_000)
            fields = extract_json_fields(content)
            json_fields_all.update(fields)
        except Exception as e:
            log.debug("  Erreur lecture JSON %s: %s", json_file.name, e)

    # Aussi analyser les JSONL du repertoire parent
    for jsonl_file in jsonl_parent:
        try:
            with open(jsonl_file, "r", encoding="utf-8", errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= 50:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        if isinstance(record, dict):
                            json_fields_all.update(record.keys())
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            log.debug("Error reading JSONL file: %s", e)

    report["html_fields"] = dict(html_fields_all)
    report["json_fields"] = json_fields_all

    # Comparer
    html_field_names = set(html_fields_all.keys())
    # Normaliser pour comparaison
    html_normalized = set()
    for f in html_field_names:
        # Extraire le nom apres le prefixe (data-, class:, th:, etc.)
        parts = f.split(":", 1)
        name = parts[-1] if len(parts) > 1 else f
        name = name.replace("data-", "").replace("-", "_").lower()
        html_normalized.add(name)

    json_normalized = set()
    for f in json_fields_all:
        name = f.split(".")[-1].replace("-", "_").lower()
        json_normalized.add(name)

    html_only = html_normalized - json_normalized
    json_only = json_normalized - html_normalized

    # Scorer les champs HTML-only par priorite
    html_only_scored = []
    for field_name in html_only:
        # Retrouver le nom original
        originals = [
            f for f in html_field_names
            if field_name in f.replace("data-", "").replace("-", "_").lower()
        ]
        original = originals[0] if originals else field_name
        count = html_fields_all.get(original, 0)
        priority = estimate_field_priority(field_name)
        html_only_scored.append({
            "field": original,
            "normalized": field_name,
            "count_in_html": count,
            "priority": priority,
        })

    html_only_scored.sort(key=lambda x: (-x["priority"], -x["count_in_html"]))
    report["html_only_fields"] = html_only_scored
    report["json_only_fields"] = sorted(json_only)
    report["common_fields"] = sorted(html_normalized & json_normalized)

    return report


# ── Pipeline principal ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Audit HTML brut vs JSON parse pour chaque scraper"
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Auditer un seul scraper (ex: 56_timeform)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=15,
        help="Nombre de champs HTML-only a afficher par source (defaut: 15)",
    )
    parser.add_argument(
        "--min-priority",
        type=int,
        default=4,
        help="Priorite minimale pour les champs a afficher (defaut: 4)",
    )
    args = parser.parse_args()

    start = time.time()
    log.info("=" * 60)
    log.info("AUDIT HTML vs JSON — Detection champs non-extraits")
    log.info("=" * 60)

    # Trouver les scrapers a auditer
    if args.source:
        source_dirs = [OUTPUT_DIR / args.source]
    else:
        source_dirs = sorted([
            d for d in OUTPUT_DIR.iterdir()
            if d.is_dir() and (d / "cache").exists()
            and d.name != "audit"
        ])

    log.info("Scrapers a auditer : %d", len(source_dirs))

    all_reports = []
    global_html_only_high = []

    for source_dir in source_dirs:
        if not source_dir.exists():
            log.warning("Repertoire introuvable : %s", source_dir)
            continue

        log.info("--- Audit %s ---", source_dir.name)
        report = audit_scraper(source_dir)
        all_reports.append(report)

        # Afficher le resume
        log.info(
            "  Fichiers : %d HTML, %d JSON, %d JSONL",
            report["html_files"], report["json_files"], report["jsonl_files"],
        )

        n_html = len(report["html_fields"])
        n_json = len(report["json_fields"])
        n_common = len(report["common_fields"])
        n_html_only = len(report["html_only_fields"])
        n_json_only = len(report["json_only_fields"])

        log.info(
            "  Champs detectes : HTML=%d, JSON=%d, communs=%d, HTML-only=%d, JSON-only=%d",
            n_html, n_json, n_common, n_html_only, n_json_only,
        )

        # Afficher les champs HTML-only haute priorite
        high_priority = [
            f for f in report["html_only_fields"]
            if f["priority"] >= args.min_priority
        ]
        if high_priority:
            log.info("  Champs HTML non extraits (priorite >= %d) :", args.min_priority)
            for f in high_priority[:args.top]:
                log.info(
                    "    [P%d] %-35s (x%d dans HTML)",
                    f["priority"], f["field"], f["count_in_html"],
                )
                global_html_only_high.append({
                    "source": source_dir.name,
                    "field": f["field"],
                    "normalized": f["normalized"],
                    "priority": f["priority"],
                    "count": f["count_in_html"],
                })

    # Resume global
    log.info("")
    log.info("=" * 60)
    log.info("RESUME GLOBAL")
    log.info("=" * 60)

    total_html_only = sum(len(r["html_only_fields"]) for r in all_reports)
    total_json_fields = sum(len(r["json_fields"]) for r in all_reports)
    scrapers_with_gaps = sum(
        1 for r in all_reports if len(r["html_only_fields"]) > 0
    )

    log.info("  Scrapers audites     : %d", len(all_reports))
    log.info("  Scrapers avec ecarts : %d", scrapers_with_gaps)
    log.info("  Total champs JSON    : %d", total_json_fields)
    log.info("  Total champs HTML-only : %d", total_html_only)

    # Top champs HTML-only toutes sources confondues
    if global_html_only_high:
        global_html_only_high.sort(key=lambda x: (-x["priority"], -x["count"]))
        log.info("")
        log.info("TOP %d CHAMPS HTML NON EXTRAITS (toutes sources) :", args.top)
        for f in global_html_only_high[:args.top]:
            log.info(
                "  [P%d] %-20s %-35s (x%d)",
                f["priority"], f["source"], f["field"], f["count"],
            )

    # Sauvegarder le rapport complet
    report_file = AUDIT_DIR / "audit_html_vs_json.json"
    serializable_reports = []
    for r in all_reports:
        sr = dict(r)
        sr["json_fields"] = sorted(r["json_fields"])
        serializable_reports.append(sr)

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(serializable_reports, f, indent=2, ensure_ascii=False)
    log.info("")
    log.info("Rapport complet sauvegarde : %s", report_file)

    elapsed = time.time() - start
    log.info("Duree totale : %.1f s", elapsed)


if __name__ == "__main__":
    main()
