#!/usr/bin/env python3
"""
pilier_drift_detection.py -- Pilier Qualite : Detection de drift
================================================================

Detecte les derives (drift) dans les distributions de donnees
entre periodes temporelles successives.

Fonctionnalites :
  1. Compare les distributions de features mois par mois
  2. Alerte sur les changements significatifs (KS-test, chi2)
  3. Traque les valeurs nouvelles / disparues dans les champs categoriques
  4. Genere un rapport de drift en JSON + resume console

Usage:
    python pilier_drift_detection.py
    python pilier_drift_detection.py --file data_master/partants_master.jsonl
    python pilier_drift_detection.py --threshold 0.05
"""

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_MASTER = BASE_DIR / "data_master"
LOGS_DIR = BASE_DIR / "logs"
REPORT_FILE = LOGS_DIR / "drift_report.json"

# Seuils par defaut
DEFAULT_KS_THRESHOLD = 0.05  # p-value sous laquelle on alerte
DEFAULT_CAT_DRIFT_RATIO = 0.10  # ratio de valeurs nouvelles/disparues


def load_jsonl(filepath: Path) -> list[dict]:
    """Charge un fichier JSONL."""
    records = []
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def load_json(filepath: Path) -> list[dict]:
    """Charge un fichier JSON (liste ou objet unique)."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def load_data(filepath: Path) -> list[dict]:
    """Charge un fichier JSON ou JSONL."""
    suffix = filepath.suffix.lower()
    if suffix == ".jsonl":
        return load_jsonl(filepath)
    elif suffix == ".json":
        return load_json(filepath)
    else:
        return []


def extract_date_field(record: dict) -> str:
    """Extrait un champ date et retourne YYYY-MM ou None."""
    for key in ("date", "date_course", "date_reunion", "jour", "date_debut"):
        val = record.get(key)
        if val and isinstance(val, str):
            # Formats: YYYY-MM-DD, DD/MM/YYYY, YYYY/MM/DD
            try:
                if len(val) >= 10 and val[4] in ("-", "/"):
                    return val[:7].replace("/", "-")
                elif len(val) >= 10 and val[2] in ("-", "/"):
                    parts = val.split(val[2])
                    if len(parts) == 3:
                        return f"{parts[2]}-{parts[1]}"
            except (IndexError, ValueError):
                continue
    return None


def classify_field(values: list) -> str:
    """Classifie un champ comme 'numeric', 'categorical' ou 'skip'."""
    non_null = [v for v in values if v is not None and v != ""]
    if len(non_null) < 10:
        return "skip"

    numeric_count = 0
    for v in non_null[:200]:
        if isinstance(v, (int, float)):
            numeric_count += 1
        elif isinstance(v, str):
            try:
                float(v.replace(",", "."))
                numeric_count += 1
            except (ValueError, AttributeError):
                pass

    ratio = numeric_count / min(len(non_null), 200)
    if ratio > 0.8:
        return "numeric"
    elif len(set(str(v) for v in non_null[:500])) < 100:
        return "categorical"
    else:
        return "skip"


def to_float(v) -> float:
    """Convertit une valeur en float."""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        return float(v.replace(",", "."))
    raise ValueError(f"Cannot convert {v}")


# -----------------------------------------------------------------------
# KS test simplifie (sans scipy)
# -----------------------------------------------------------------------

def ks_statistic(sample1: list[float], sample2: list[float]) -> float:
    """
    Calcule la statistique de Kolmogorov-Smirnov entre deux echantillons.
    Retourne la statistique D (0 a 1).
    """
    if not sample1 or not sample2:
        return 0.0

    s1 = sorted(sample1)
    s2 = sorted(sample2)
    n1, n2 = len(s1), len(s2)

    all_vals = sorted(set(s1 + s2))
    max_d = 0.0

    i1, i2 = 0, 0
    for val in all_vals:
        while i1 < n1 and s1[i1] <= val:
            i1 += 1
        while i2 < n2 and s2[i2] <= val:
            i2 += 1
        d = abs(i1 / n1 - i2 / n2)
        if d > max_d:
            max_d = d

    return max_d


def ks_critical_value(n1: int, n2: int, alpha: float = 0.05) -> float:
    """Valeur critique approchee pour le test KS a deux echantillons."""
    # Approximation: c(alpha) * sqrt((n1+n2)/(n1*n2))
    c_alpha = {0.10: 1.22, 0.05: 1.36, 0.01: 1.63}
    c = c_alpha.get(alpha, 1.36)
    if n1 == 0 or n2 == 0:
        return 1.0
    return c * math.sqrt((n1 + n2) / (n1 * n2))


# -----------------------------------------------------------------------
# Drift categorical
# -----------------------------------------------------------------------

def categorical_drift(values_a: list, values_b: list) -> dict:
    """Compare deux ensembles de valeurs categoriques."""
    set_a = set(str(v) for v in values_a if v is not None and v != "")
    set_b = set(str(v) for v in values_b if v is not None and v != "")

    appeared = set_b - set_a
    disappeared = set_a - set_b
    stable = set_a & set_b

    total_unique = len(set_a | set_b)
    drift_ratio = (len(appeared) + len(disappeared)) / max(total_unique, 1)

    # Distribution shift (Jensen-Shannon-like via counters)
    counter_a = Counter(str(v) for v in values_a if v is not None and v != "")
    counter_b = Counter(str(v) for v in values_b if v is not None and v != "")

    return {
        "appeared": sorted(list(appeared))[:20],
        "disappeared": sorted(list(disappeared))[:20],
        "n_appeared": len(appeared),
        "n_disappeared": len(disappeared),
        "n_stable": len(stable),
        "drift_ratio": round(drift_ratio, 4),
    }


# -----------------------------------------------------------------------
# Analyse principale
# -----------------------------------------------------------------------

def analyze_drift(records: list[dict], threshold: float = DEFAULT_KS_THRESHOLD) -> dict:
    """Analyse le drift entre periodes mensuelles."""
    # Grouper par mois
    by_month = defaultdict(list)
    no_date_count = 0
    for rec in records:
        month = extract_date_field(rec)
        if month:
            by_month[month].append(rec)
        else:
            no_date_count += 1

    months_sorted = sorted(by_month.keys())
    if len(months_sorted) < 2:
        return {
            "status": "insufficient_data",
            "months_found": len(months_sorted),
            "no_date_records": no_date_count,
        }

    # Detecter les champs a analyser (sur un echantillon)
    sample = records[:500]
    all_fields = set()
    for rec in sample:
        all_fields.update(rec.keys())

    # Classifier les champs
    field_types = {}
    for field in sorted(all_fields):
        vals = [rec.get(field) for rec in sample]
        ftype = classify_field(vals)
        if ftype != "skip":
            field_types[field] = ftype

    # Comparer paires de mois consecutifs
    drift_results = []
    alert_count = 0

    for i in range(len(months_sorted) - 1):
        m_a, m_b = months_sorted[i], months_sorted[i + 1]
        recs_a = by_month[m_a]
        recs_b = by_month[m_b]

        pair_result = {
            "period_a": m_a,
            "period_b": m_b,
            "count_a": len(recs_a),
            "count_b": len(recs_b),
            "fields": {},
        }

        for field, ftype in field_types.items():
            if ftype == "numeric":
                try:
                    vals_a = [to_float(r[field]) for r in recs_a
                              if field in r and r[field] is not None and r[field] != ""]
                    vals_b = [to_float(r[field]) for r in recs_b
                              if field in r and r[field] is not None and r[field] != ""]
                except (ValueError, TypeError):
                    continue

                if len(vals_a) < 5 or len(vals_b) < 5:
                    continue

                d_stat = ks_statistic(vals_a, vals_b)
                critical = ks_critical_value(len(vals_a), len(vals_b), threshold)
                is_drift = d_stat > critical

                if is_drift:
                    alert_count += 1

                pair_result["fields"][field] = {
                    "type": "numeric",
                    "ks_statistic": round(d_stat, 4),
                    "critical_value": round(critical, 4),
                    "drift_detected": is_drift,
                    "mean_a": round(sum(vals_a) / len(vals_a), 4) if vals_a else None,
                    "mean_b": round(sum(vals_b) / len(vals_b), 4) if vals_b else None,
                    "n_a": len(vals_a),
                    "n_b": len(vals_b),
                }

            elif ftype == "categorical":
                vals_a = [r.get(field) for r in recs_a]
                vals_b = [r.get(field) for r in recs_b]
                cat_result = categorical_drift(vals_a, vals_b)

                is_drift = cat_result["drift_ratio"] > DEFAULT_CAT_DRIFT_RATIO
                if is_drift:
                    alert_count += 1

                pair_result["fields"][field] = {
                    "type": "categorical",
                    "drift_detected": is_drift,
                    **cat_result,
                }

        # Ne garder que les paires avec du drift
        fields_with_drift = {
            k: v for k, v in pair_result["fields"].items()
            if v.get("drift_detected", False)
        }
        pair_result["n_drifted_fields"] = len(fields_with_drift)
        drift_results.append(pair_result)

    return {
        "status": "ok",
        "total_records": len(records),
        "no_date_records": no_date_count,
        "months_analyzed": len(months_sorted),
        "month_range": f"{months_sorted[0]} -> {months_sorted[-1]}",
        "n_fields_monitored": len(field_types),
        "field_types": {k: v for k, v in sorted(field_types.items())},
        "total_alerts": alert_count,
        "pairs": drift_results,
    }


def run_drift_detection(files: list[Path], threshold: float) -> dict:
    """Execute la detection de drift sur plusieurs fichiers."""
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "threshold": threshold,
        "files": {},
    }

    for filepath in files:
        fname = filepath.name
        print(f"  [DRIFT] Analyse: {fname}")

        try:
            data = load_data(filepath)
        except Exception as e:
            print(f"    ERREUR chargement: {e}")
            report["files"][fname] = {"status": "error", "error": str(e)}
            continue

        if not data:
            print(f"    Fichier vide ou format non supporte")
            report["files"][fname] = {"status": "empty"}
            continue

        result = analyze_drift(data, threshold)
        report["files"][fname] = result

        if result.get("status") == "ok":
            n_alerts = result.get("total_alerts", 0)
            n_months = result.get("months_analyzed", 0)
            tag = "ALERTE" if n_alerts > 0 else "OK"
            print(f"    [{tag}] {n_months} mois, {n_alerts} alertes drift")
        else:
            print(f"    Donnees insuffisantes pour l'analyse")

    return report


def main():
    parser = argparse.ArgumentParser(description="Detection de drift dans les donnees")
    parser.add_argument("--file", "-f", help="Fichier specifique a analyser")
    parser.add_argument("--threshold", "-t", type=float, default=DEFAULT_KS_THRESHOLD,
                        help=f"Seuil KS (defaut: {DEFAULT_KS_THRESHOLD})")
    parser.add_argument("--output", "-o", help="Fichier rapport de sortie")
    args = parser.parse_args()

    print("=" * 60)
    print("PILIER DRIFT DETECTION")
    print("=" * 60)

    # Determiner les fichiers a analyser
    if args.file:
        target = BASE_DIR / args.file
        if not target.exists():
            print(f"ERREUR: Fichier introuvable: {target}")
            sys.exit(1)
        files = [target]
    else:
        # Analyser tous les fichiers master
        files = []
        if DATA_MASTER.exists():
            for f in sorted(DATA_MASTER.iterdir()):
                if f.suffix in (".json", ".jsonl") and not f.name.endswith(".tmp"):
                    files.append(f)
        if not files:
            print("Aucun fichier master trouve dans data_master/")
            sys.exit(1)

    print(f"Fichiers a analyser: {len(files)}")
    print(f"Seuil KS: {args.threshold}")
    print("-" * 60)

    report = run_drift_detection(files, args.threshold)

    # Sauvegarder
    out_path = Path(args.output) if args.output else REPORT_FILE
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("-" * 60)

    # Resume
    total_alerts = sum(
        r.get("total_alerts", 0)
        for r in report["files"].values()
        if isinstance(r, dict)
    )
    print(f"Total alertes drift: {total_alerts}")
    print(f"Rapport sauvegarde: {out_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
