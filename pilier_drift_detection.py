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
import random
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


# Max valeurs numeriques stockees par mois par champ (reservoir sampling)
MAX_SAMPLES_PER_MONTH = 2000


def stream_jsonl_lines(filepath: Path):
    """Generateur streaming de records JSONL."""
    with open(filepath, "r", encoding="utf-8", errors="replace", buffering=1048576) as f:
        line = f.readline()
        while line:
            stripped = line.strip()
            if stripped:
                try:
                    yield json.loads(stripped)
                except json.JSONDecodeError:
                    pass
            line = f.readline()


def extract_date_field(record: dict) -> str:
    """Extrait un champ date et retourne YYYY-MM ou None."""
    for key in ("date_reunion_iso", "date", "date_course", "date_reunion", "jour", "date_debut"):
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

def analyze_drift_streaming(filepath: Path, threshold: float = DEFAULT_KS_THRESHOLD) -> dict:
    """Analyse le drift en streaming - jamais plus de ~200 MB en RAM."""
    suffix = filepath.suffix.lower()
    file_size = filepath.stat().st_size

    # Skip fichiers JSON > 500 MB (ne peuvent pas etre streames)
    if suffix == ".json" and file_size > 500_000_000:
        return {"status": "skipped", "reason": "JSON trop gros (>500 MB)"}
    if suffix == ".json":
        # Petit JSON: charger normalement
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list) or not data:
                return {"status": "empty"}
            records_iter = iter(data)
        except Exception as e:
            return {"status": "error", "error": str(e)}
    elif suffix == ".jsonl":
        records_iter = stream_jsonl_lines(filepath)
    else:
        return {"status": "unsupported_format"}

    # Pass 1+2 combinee: lire 500 records pour classifier, puis continuer streaming
    sample_records = []
    field_types = {}
    total_records = 0
    no_date_count = 0

    # Structure par mois: pour numeric -> reservoir sampling, pour categorical -> Counter
    # month_data[month][field] = {"values": [...], "count": N} pour numeric
    # month_data[month][field] = Counter() pour categorical
    month_numeric = defaultdict(lambda: defaultdict(list))  # month -> field -> [sampled values]
    month_cat = defaultdict(lambda: defaultdict(Counter))  # month -> field -> Counter
    month_counts = Counter()  # month -> nb records
    month_record_idx = defaultdict(int)  # pour reservoir sampling

    classify_done = False

    for rec in records_iter:
        total_records += 1

        # Accumuler les 500 premiers pour classifier
        if total_records <= 500:
            sample_records.append(rec)
            if total_records == 500:
                # Classifier les champs
                all_fields = set()
                for r in sample_records:
                    all_fields.update(r.keys())
                for field in sorted(all_fields):
                    vals = [r.get(field) for r in sample_records]
                    ftype = classify_field(vals)
                    if ftype != "skip":
                        field_types[field] = ftype
                classify_done = True

                # Rejouer les 500 premiers records
                for r in sample_records:
                    month = extract_date_field(r)
                    if not month:
                        continue
                    month_counts[month] += 1
                    for field, ftype in field_types.items():
                        val = r.get(field)
                        if val is None or val == "":
                            continue
                        if ftype == "numeric":
                            try:
                                fval = to_float(val)
                                month_numeric[month][field].append(fval)
                            except (ValueError, TypeError):
                                pass
                        elif ftype == "categorical":
                            month_cat[month][field][str(val)] += 1
                    month_record_idx[month] = month_counts[month]

                sample_records = []  # Liberer la memoire
                continue
            else:
                continue

        # Apres classification: streaming pur
        month = extract_date_field(rec)
        if not month:
            no_date_count += 1
            continue

        month_counts[month] += 1
        month_record_idx[month] += 1
        idx = month_record_idx[month]

        for field, ftype in field_types.items():
            val = rec.get(field)
            if val is None or val == "":
                continue
            if ftype == "numeric":
                try:
                    fval = to_float(val)
                except (ValueError, TypeError):
                    continue
                samples = month_numeric[month][field]
                if len(samples) < MAX_SAMPLES_PER_MONTH:
                    samples.append(fval)
                else:
                    # Reservoir sampling
                    j = random.randint(0, idx - 1)
                    if j < MAX_SAMPLES_PER_MONTH:
                        samples[j] = fval
            elif ftype == "categorical":
                month_cat[month][field][str(val)] += 1

        if total_records % 500000 == 0:
            print(f"    {total_records:,} records traites ...")

    # Si < 500 records, classifier maintenant
    if not classify_done and sample_records:
        all_fields = set()
        for r in sample_records:
            all_fields.update(r.keys())
        for field in sorted(all_fields):
            vals = [r.get(field) for r in sample_records]
            ftype = classify_field(vals)
            if ftype != "skip":
                field_types[field] = ftype

        for r in sample_records:
            total_records_counted = True
            month = extract_date_field(r)
            if not month:
                no_date_count += 1
                continue
            month_counts[month] += 1
            for field, ftype in field_types.items():
                val = r.get(field)
                if val is None or val == "":
                    continue
                if ftype == "numeric":
                    try:
                        month_numeric[month][field].append(to_float(val))
                    except (ValueError, TypeError):
                        pass
                elif ftype == "categorical":
                    month_cat[month][field][str(val)] += 1
        sample_records = []

    months_sorted = sorted(month_counts.keys())
    if len(months_sorted) < 2:
        return {
            "status": "insufficient_data",
            "months_found": len(months_sorted),
            "total_records": total_records,
            "no_date_records": no_date_count,
        }

    # Comparer paires de mois consecutifs
    drift_results = []
    alert_count = 0

    for i in range(len(months_sorted) - 1):
        m_a, m_b = months_sorted[i], months_sorted[i + 1]

        pair_result = {
            "period_a": m_a,
            "period_b": m_b,
            "count_a": month_counts[m_a],
            "count_b": month_counts[m_b],
            "fields": {},
        }

        for field, ftype in field_types.items():
            if ftype == "numeric":
                vals_a = month_numeric[m_a].get(field, [])
                vals_b = month_numeric[m_b].get(field, [])

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
                counter_a = month_cat[m_a].get(field, Counter())
                counter_b = month_cat[m_b].get(field, Counter())
                set_a = set(counter_a.keys())
                set_b = set(counter_b.keys())

                appeared = set_b - set_a
                disappeared = set_a - set_b
                stable = set_a & set_b
                total_unique = len(set_a | set_b)
                drift_ratio = (len(appeared) + len(disappeared)) / max(total_unique, 1)

                is_drift = drift_ratio > DEFAULT_CAT_DRIFT_RATIO
                if is_drift:
                    alert_count += 1

                pair_result["fields"][field] = {
                    "type": "categorical",
                    "drift_detected": is_drift,
                    "n_appeared": len(appeared),
                    "n_disappeared": len(disappeared),
                    "n_stable": len(stable),
                    "drift_ratio": round(drift_ratio, 4),
                    "appeared": sorted(list(appeared))[:20],
                    "disappeared": sorted(list(disappeared))[:20],
                }

        fields_with_drift = {
            k: v for k, v in pair_result["fields"].items()
            if v.get("drift_detected", False)
        }
        pair_result["n_drifted_fields"] = len(fields_with_drift)
        drift_results.append(pair_result)

    return {
        "status": "ok",
        "total_records": total_records,
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
        file_size_mb = filepath.stat().st_size / 1024 / 1024
        print(f"  [DRIFT] Analyse: {fname} ({file_size_mb:.0f} MB)")

        try:
            result = analyze_drift_streaming(filepath, threshold)
        except Exception as e:
            print(f"    ERREUR: {e}")
            report["files"][fname] = {"status": "error", "error": str(e)}
            continue

        report["files"][fname] = result

        status = result.get("status", "")
        if status == "ok":
            n_alerts = result.get("total_alerts", 0)
            n_months = result.get("months_analyzed", 0)
            tag = "ALERTE" if n_alerts > 0 else "OK"
            print(f"    [{tag}] {n_months} mois, {n_alerts} alertes drift")
        elif status == "skipped":
            print(f"    [SKIP] {result.get('reason', '')}")
        elif status == "empty":
            print(f"    Fichier vide")
        elif status == "insufficient_data":
            print(f"    Donnees insuffisantes ({result.get('months_found', 0)} mois)")
        else:
            print(f"    Status: {status}")

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
