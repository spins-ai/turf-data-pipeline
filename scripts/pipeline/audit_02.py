#!/usr/bin/env python3
"""
audit_02.py — Audit qualite des donnees produites par 02_liste_courses.py.

Lit partants_normalises.json et courses_normalisees.json, calcule des
statistiques par champ (taux de remplissage, distribution, anomalies)
et imprime un rapport formate.
"""

import sys as _sys, os as _os  # auto-added by organize_project.py
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', '..'))  # project root

import argparse
import json
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
import sys
from collections import Counter
from datetime import datetime
from statistics import mean, median


# ── Categories de champs partants ────────────────────────────────────────────

CATEGORIES_PARTANTS = {
    "identifiants": [
        "partant_uid", "course_uid", "reunion_uid", "cle_partant",
        "source", "date_reunion_iso", "hippodrome_normalise",
        "numero_reunion", "numero_course", "timestamp_collecte",
    ],
    "cheval": [
        "nom_cheval", "num_pmu", "age", "sexe", "race", "robe",
        "pays_cheval", "pays_entrainement",
    ],
    "jockey": [
        "jockey_driver", "jockey_driver_change", "entraineur",
        "proprietaire", "eleveur",
    ],
    "pedigree": [
        "pere", "mere", "pere_mere",
    ],
    "equipement": [
        "oeilleres", "deferre", "allure",
    ],
    "statut": [
        "statut", "engagement", "supplement_euros",
        "is_inedit", "jument_pleine", "poids_monte_change",
    ],
    "performance": [
        "musique", "nb_courses_carriere", "nb_victoires_carriere",
        "nb_places_carriere", "nb_places_2eme", "nb_places_3eme",
        "gains_carriere_euros", "gains_annee_euros",
    ],
    "course_specifique": [
        "distance", "discipline", "place_corde",
        "poids_porte_kg", "handicap_valeur", "handicap_distance_m",
    ],
    "resultat": [
        "position_arrivee", "temps_ms", "reduction_km_ms",
        "is_gagnant", "is_place", "is_disqualifie",
    ],
    "cotes": [
        "cote_finale", "cote_reference", "proba_implicite",
    ],
    "infos_supplementaires": [
        "incident", "ecart_precedent", "commentaire_apres_course",
        "avis_entraineur",
    ],
}

CATEGORIES_COURSES = {
    "identifiants": [
        "course_uid", "reunion_uid", "cle_course", "source",
        "date_reunion_iso", "hippodrome_normalise", "hippodrome",
        "pays", "numero_reunion", "numero_course", "timestamp_collecte",
    ],
    "description": [
        "libelle", "distance", "parcours", "corde", "discipline",
        "specialite", "conditions_texte", "condition_sexe",
    ],
    "organisation": [
        "nombre_partants", "heure_depart",
        "allocation_totale", "allocation_1er",
    ],
    "piste": [
        "type_piste", "penetrometre",
    ],
    "resultat": [
        "statut", "ordre_arrivee", "duree_course_ms", "incidents",
    ],
    "media_paris": [
        "paris_types", "replay_disponible", "course_trackee", "url_source",
    ],
}

# Champs mis en avant par l'utilisateur
HIGHLIGHT_FIELDS = {
    "nom_cheval", "num_pmu", "age", "sexe", "robe",
    "place_corde", "poids_porte_kg", "handicap_valeur", "musique",
    "oeilleres", "deferre", "jockey_driver", "entraineur",
    "proprietaire", "eleveur", "pere", "mere", "pere_mere",
    "position_arrivee", "temps_ms", "reduction_km_ms",
    "cote_finale", "proba_implicite",
    "incident", "ecart_precedent", "commentaire_apres_course",
    "avis_entraineur", "pays_cheval", "pays_entrainement",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def is_filled(value):
    """Renvoie True si la valeur est consideree comme remplie."""
    if value is None:
        return False
    if isinstance(value, bool):
        return True  # un booleen est toujours "rempli" (True ou False)
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


def fmt_pct(num, den):
    if den == 0:
        return "  —  "
    return f"{100 * num / den:5.1f}%"


def fmt_num(v):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def pad(s, width):
    s = str(s)
    return s[:width].ljust(width)


def table_line(cols, widths):
    return "  ".join(pad(c, w) for c, w in zip(cols, widths))


def separator(widths):
    return "  ".join("-" * w for w in widths)


def compute_field_stats(records, field):
    """Calcule les statistiques pour un champ donne."""
    values = [r.get(field) for r in records]
    total = len(values)
    filled_values = [v for v in values if is_filled(v)]
    filled = len(filled_values)

    stats = {
        "total": total,
        "filled": filled,
        "fill_rate": filled / total if total else 0,
    }

    # Determiner le type dominant
    numeric_vals = [v for v in filled_values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    str_vals = [v for v in filled_values if isinstance(v, str) and v.strip()]

    if len(numeric_vals) > len(str_vals) and numeric_vals:
        stats["type"] = "numeric"
        stats["min"] = min(numeric_vals)
        stats["max"] = max(numeric_vals)
        stats["mean"] = mean(numeric_vals)
        stats["median"] = median(numeric_vals)
    elif str_vals:
        stats["type"] = "string"
        counter = Counter(str_vals)
        stats["unique"] = len(counter)
        stats["top5"] = counter.most_common(5)
    else:
        # Booleans, listes, etc.
        bool_vals = [v for v in filled_values if isinstance(v, bool)]
        if bool_vals:
            stats["type"] = "boolean"
            stats["true_count"] = sum(1 for v in bool_vals if v)
            stats["false_count"] = sum(1 for v in bool_vals if not v)
        else:
            stats["type"] = "other"

    return stats


# ── Rapport ──────────────────────────────────────────────────────────────────

def print_field_table(records, fields, all_fields_in_data, out):
    """Imprime un tableau de stats par champ."""
    widths = [30, 6, 6, 7, 40]
    header = table_line(["Champ", "Total", "Rempl", "Taux", "Details"], widths)
    out.write("  " + header + "\n")
    out.write("  " + separator(widths) + "\n")

    for field in fields:
        if field not in all_fields_in_data:
            out.write("  " + table_line([field, "—", "—", "ABSENT", ""], widths) + "\n")
            continue

        s = compute_field_stats(records, field)
        marker = " *" if field in HIGHLIGHT_FIELDS else ""
        rate_str = fmt_pct(s["filled"], s["total"])

        details = ""
        if s["type"] == "numeric":
            details = f"min={fmt_num(s['min'])}  max={fmt_num(s['max'])}  moy={fmt_num(s['mean'])}  med={fmt_num(s['median'])}"
        elif s["type"] == "string":
            top = ", ".join(f"{v}({c})" for v, c in s["top5"][:3])
            details = f"{s['unique']} uniq | top: {top}"
        elif s["type"] == "boolean":
            details = f"True={s['true_count']}  False={s['false_count']}"

        row = table_line(
            [field + marker, str(s["total"]), str(s["filled"]), rate_str, details],
            widths,
        )
        out.write("  " + row + "\n")


def print_category_summary(records, categories, all_fields, out):
    """Resume par categorie."""
    widths = [25, 8, 8, 7]
    header = table_line(["Categorie", "Champs", "Moy.rem", "Taux"], widths)
    out.write("  " + header + "\n")
    out.write("  " + separator(widths) + "\n")

    for cat, fields in categories.items():
        rates = []
        for f in fields:
            if f in all_fields:
                s = compute_field_stats(records, f)
                rates.append(s["fill_rate"])
        if rates:
            avg = mean(rates)
            out.write("  " + table_line(
                [cat, str(len(fields)), f"{avg * 100:.1f}%", ""],
                widths,
            ) + "\n")
        else:
            out.write("  " + table_line([cat, str(len(fields)), "—", ""], widths) + "\n")


def detect_anomalies(courses, partants, out):
    """Detecte les anomalies dans les donnees."""
    issues = []

    # --- Courses avec 0 partants ---
    courses_0 = [c for c in courses if c.get("nombre_partants", 0) == 0]
    if courses_0:
        issues.append(f"  - {len(courses_0)} course(s) avec 0 partants")
        for c in courses_0[:5]:
            issues.append(f"      {c.get('cle_course')}")

    # Construire lookup partants par course_uid
    partants_par_course = {}
    for p in partants:
        uid = p.get("course_uid")
        partants_par_course.setdefault(uid, []).append(p)

    # --- Courses terminees sans aucun gagnant ---
    courses_finies = [c for c in courses if c.get("statut", "").lower() in ("fin course", "fin_course", "terminee")]
    no_winner = []
    for c in courses_finies:
        uid = c.get("course_uid")
        parts = partants_par_course.get(uid, [])
        if parts and not any(p.get("is_gagnant") for p in parts):
            no_winner.append(c)
    if no_winner:
        issues.append(f"  - {len(no_winner)} course(s) terminee(s) sans gagnant")
        for c in no_winner[:5]:
            issues.append(f"      {c.get('cle_course')}")

    # --- Courses terminees dont les partants n'ont pas de position_arrivee ---
    missing_pos = []
    for c in courses_finies:
        uid = c.get("course_uid")
        parts = partants_par_course.get(uid, [])
        missing = [p for p in parts if p.get("position_arrivee") is None and p.get("statut", "") == "partant"]
        if missing:
            missing_pos.append((c, len(missing), len(parts)))
    if missing_pos:
        issues.append(f"  - {len(missing_pos)} course(s) terminee(s) avec partants sans position_arrivee")
        for c, n_miss, n_tot in missing_pos[:5]:
            issues.append(f"      {c.get('cle_course')}: {n_miss}/{n_tot} partants sans position")

    # --- is_gagnant=True mais position_arrivee != 1 ---
    gagnant_wrong = [
        p for p in partants
        if p.get("is_gagnant") and p.get("position_arrivee") is not None and p.get("position_arrivee") != 1
    ]
    if gagnant_wrong:
        issues.append(f"  - {len(gagnant_wrong)} partant(s) avec is_gagnant=True mais position_arrivee != 1")
        for p in gagnant_wrong[:5]:
            issues.append(f"      {p.get('cle_partant')} pos={p.get('position_arrivee')}")

    # --- Courses dans courses.json sans partants dans partants.json ---
    course_uids_with_partants = set(partants_par_course.keys())
    orphan_courses = [c for c in courses if c.get("course_uid") not in course_uids_with_partants]
    if orphan_courses:
        issues.append(f"  - {len(orphan_courses)} course(s) sans aucun partant dans le fichier partants")
        for c in orphan_courses[:5]:
            issues.append(f"      {c.get('cle_course')}")

    # --- Couverture temporelle ---
    dates_str = sorted(set(
        c.get("date_reunion_iso") for c in courses if c.get("date_reunion_iso")
    ))
    if dates_str:
        dates = [datetime.strptime(d, "%Y-%m-%d") for d in dates_str]
        issues.append(f"  - Couverture: {dates_str[0]} -> {dates_str[-1]} ({len(dates)} dates)")
        gaps = []
        for i in range(1, len(dates)):
            delta = (dates[i] - dates[i - 1]).days
            if delta > 7:
                gaps.append((dates_str[i - 1], dates_str[i], delta))
        if gaps:
            issues.append(f"  - {len(gaps)} trou(s) > 7 jours:")
            for d1, d2, delta in gaps[:10]:
                issues.append(f"      {d1} -> {d2} ({delta} jours)")
        else:
            issues.append("  - Aucun trou > 7 jours")

    if not issues:
        out.write("  Aucune anomalie detectee.\n")
    else:
        for line in issues:
            out.write(line + "\n")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Audit qualite des donnees issues de 02_liste_courses.py"
    )
    parser.add_argument(
        "--input-dir",
        default=os.path.join(BASE_DIR, "../../output", "02_liste_courses"),
        help="Repertoire contenant les fichiers JSON (defaut: output/02_liste_courses)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Chemin du rapport texte (defaut: <input-dir>/audit_02_rapport.txt)",
    )
    args = parser.parse_args()

    input_dir = args.input_dir
    output_path = args.output or os.path.join(input_dir, "audit_02_rapport.txt")

    partants_path = os.path.join(input_dir, "partants_normalises.json")
    courses_path = os.path.join(input_dir, "courses_normalisees.json")

    for path in (partants_path, courses_path):
        if not os.path.exists(path):
            print(f"ERREUR: fichier introuvable: {path}", file=sys.stderr)
            sys.exit(1)

    with open(partants_path, encoding="utf-8") as f:
        partants = json.load(f)
    with open(courses_path, encoding="utf-8") as f:
        courses = json.load(f)

    all_partant_fields = set()
    for p in partants:
        all_partant_fields.update(p.keys())

    all_course_fields = set()
    for c in courses:
        all_course_fields.update(c.keys())

    # Construire le rapport dans un buffer
    lines = []

    class Writer:
        def write(self, s):
            lines.append(s)

    out = Writer()

    out.write("=" * 90 + "\n")
    out.write("  AUDIT QUALITE — 02_liste_courses.py\n")
    out.write("  Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
    out.write("=" * 90 + "\n\n")

    out.write(f"  Fichiers:\n")
    out.write(f"    partants : {partants_path} ({len(partants)} enregistrements)\n")
    out.write(f"    courses  : {courses_path} ({len(courses)} enregistrements)\n\n")

    # ── PARTANTS ─────────────────────────────────────────────────────────────

    out.write("-" * 90 + "\n")
    out.write("  PARTANTS — Resume par categorie\n")
    out.write("-" * 90 + "\n\n")
    print_category_summary(partants, CATEGORIES_PARTANTS, all_partant_fields, out)
    out.write("\n")

    for cat, fields in CATEGORIES_PARTANTS.items():
        out.write(f"\n  [{cat.upper()}]\n")
        print_field_table(partants, fields, all_partant_fields, out)
        out.write("\n")

    # Champs presents dans les donnees mais pas dans les categories
    categorized = set()
    for fields in CATEGORIES_PARTANTS.values():
        categorized.update(fields)
    uncategorized = sorted(all_partant_fields - categorized)
    if uncategorized:
        out.write(f"\n  [NON CATEGORISES]\n")
        print_field_table(partants, uncategorized, all_partant_fields, out)
        out.write("\n")

    # ── COURSES ──────────────────────────────────────────────────────────────

    out.write("-" * 90 + "\n")
    out.write("  COURSES — Resume par categorie\n")
    out.write("-" * 90 + "\n\n")
    print_category_summary(courses, CATEGORIES_COURSES, all_course_fields, out)
    out.write("\n")

    for cat, fields in CATEGORIES_COURSES.items():
        out.write(f"\n  [{cat.upper()}]\n")
        print_field_table(courses, fields, all_course_fields, out)
        out.write("\n")

    categorized_c = set()
    for fields in CATEGORIES_COURSES.values():
        categorized_c.update(fields)
    uncategorized_c = sorted(all_course_fields - categorized_c)
    if uncategorized_c:
        out.write(f"\n  [NON CATEGORISES]\n")
        print_field_table(courses, uncategorized_c, all_course_fields, out)
        out.write("\n")

    # ── CHAMPS PRIORITAIRES ──────────────────────────────────────────────────

    out.write("-" * 90 + "\n")
    out.write("  CHAMPS PRIORITAIRES (marques * dans les tableaux)\n")
    out.write("-" * 90 + "\n\n")

    highlight_in_data = sorted(HIGHLIGHT_FIELDS & all_partant_fields)
    highlight_missing = sorted(HIGHLIGHT_FIELDS - all_partant_fields)

    print_field_table(partants, highlight_in_data, all_partant_fields, out)
    if highlight_missing:
        out.write(f"\n  Champs prioritaires absents des donnees: {', '.join(highlight_missing)}\n")
    out.write("\n")

    # ── ANOMALIES ────────────────────────────────────────────────────────────

    out.write("-" * 90 + "\n")
    out.write("  ANOMALIES DETECTEES\n")
    out.write("-" * 90 + "\n\n")
    detect_anomalies(courses, partants, out)
    out.write("\n")

    out.write("=" * 90 + "\n")
    out.write("  Fin du rapport\n")
    out.write("=" * 90 + "\n")

    # Assembler et ecrire
    report = "".join(lines)

    print(report)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n  Rapport sauvegarde: {output_path}")


if __name__ == "__main__":
    main()
