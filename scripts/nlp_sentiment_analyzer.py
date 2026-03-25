#!/usr/bin/env python3
"""
NLP Sentiment Analyzer for Horse Racing Comments.

Analyzes:
- commentaire_apres_course: post-race comments (currently ~0.5% filled)
- avis_entraineur: trainer opinion (currently ~9.2% filled, mostly 'NEUTRE')

Uses a rule-based French sentiment lexicon (no external NLP library needed).
Produces sentiment scores and keyword extraction for each comment field.

Output: output/nlp/sentiment_analysis.jsonl
        output/nlp/sentiment_report.json
"""

import json
import os
import re
import sys
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parent.parent
MASTER_FILE = ROOT / "data_master" / "partants_master_enrichi.jsonl"
if not MASTER_FILE.exists():
    MASTER_FILE = ROOT / "data_master" / "partants_master.jsonl"
OUTPUT_DIR = ROOT / "output" / "nlp"
OUTPUT_FILE = OUTPUT_DIR / "sentiment_analysis.jsonl"
REPORT_FILE = OUTPUT_DIR / "sentiment_report.json"

# French horse racing sentiment lexicon
POSITIVE_WORDS = {
    # Performance positive
    "bien", "bon", "bonne", "brillant", "brillante", "excellent", "excellente",
    "facile", "facilement", "fort", "forte", "gagnant", "gagne", "impressionnant",
    "impressionnante", "magnifique", "meilleur", "meilleure", "parfait", "parfaite",
    "performant", "performante", "puissant", "puissante", "rapide", "remarquable",
    "reussi", "reussie", "solide", "superbe", "victoire", "victorieux",
    # Racing specific
    "attaque", "deborde", "decroche", "detache", "domine", "emporte", "enleve",
    "finit", "galope", "progresse", "remonte", "reprend", "resiste", "s'impose",
    "termine", "tient", "devance", "ecrase", "survole",
    # Condition positive
    "confiance", "forme", "pleine", "pret", "recupere", "sain", "affute",
    # Odds/value
    "favori", "surprise", "valeur", "chance", "espoir",
}

NEGATIVE_WORDS = {
    # Performance negative
    "abandon", "abandonne", "arrete", "blesse", "boite", "boiteux", "chute",
    "decu", "decevant", "decevante", "defaillance", "dernier", "derniere",
    "difficile", "distanc", "echoue", "elimine", "faible", "fatigue", "faute",
    "irregulier", "irreguliere", "lent", "lente", "mauvais", "mauvaise",
    "mediocre", "mou", "molle", "nul", "nulle", "pire", "rate", "recule",
    # Racing specific
    "decroche", "deporte", "desuni", "devisse", "encombre", "gene",
    "lache", "manque", "perd", "recule", "stoppe", "tombe",
    # Condition negative
    "blessure", "boiterie", "douleur", "fatigue", "probleme", "souci",
    "incident", "disqualifie", "non-partant",
}

NEUTRAL_WORDS = {
    "neutre", "normal", "moyen", "moyenne", "correct", "correcte",
    "passable", "ordinaire", "standard", "classique", "habituel",
}

INTENSITY_MULTIPLIERS = {
    "tres": 1.5, "vraiment": 1.5, "extremement": 2.0, "particulierement": 1.3,
    "assez": 0.7, "plutot": 0.7, "un peu": 0.5, "leger": 0.5, "legere": 0.5,
    "nettement": 1.5, "largement": 1.5, "completement": 1.8,
}

NEGATION_WORDS = {"pas", "ne", "ni", "jamais", "aucun", "aucune", "sans", "non"}


def normalize_text(text):
    """Normalize French text for analysis."""
    if not text or not isinstance(text, str):
        return ""
    text = text.lower().strip()
    # Remove accents for matching (keep original for display)
    replacements = {
        "\u00e9": "e", "\u00e8": "e", "\u00ea": "e", "\u00eb": "e",
        "\u00e0": "a", "\u00e2": "a", "\u00e4": "a",
        "\u00f4": "o", "\u00f6": "o",
        "\u00ee": "i", "\u00ef": "i",
        "\u00fb": "u", "\u00fc": "u", "\u00f9": "u",
        "\u00e7": "c",
    }
    normalized = text
    for src, dst in replacements.items():
        normalized = normalized.replace(src, dst)
    return normalized


def analyze_sentiment(text):
    """
    Analyze sentiment of a French horse racing comment.
    Returns dict with score (-1 to +1), label, keywords, confidence.
    """
    if not text or not isinstance(text, str) or text.strip() in ("", "NEUTRE", "null", "N/A", "None"):
        return {
            "score": 0.0,
            "label": "NEUTRE",
            "confidence": 0.0,
            "positive_keywords": [],
            "negative_keywords": [],
            "word_count": 0,
        }

    normalized = normalize_text(text)
    words = re.findall(r'\b\w+\b', normalized)

    if not words:
        return {
            "score": 0.0, "label": "NEUTRE", "confidence": 0.0,
            "positive_keywords": [], "negative_keywords": [], "word_count": 0,
        }

    positive_hits = []
    negative_hits = []
    pos_score = 0.0
    neg_score = 0.0

    for i, word in enumerate(words):
        # Check for negation in preceding 2 words
        is_negated = False
        for j in range(max(0, i - 2), i):
            if words[j] in NEGATION_WORDS:
                is_negated = True
                break

        # Check intensity multiplier in preceding word
        multiplier = 1.0
        if i > 0 and words[i - 1] in INTENSITY_MULTIPLIERS:
            multiplier = INTENSITY_MULTIPLIERS[words[i - 1]]

        if word in POSITIVE_WORDS:
            if is_negated:
                neg_score += 0.5 * multiplier
                negative_hits.append(f"pas_{word}")
            else:
                pos_score += 1.0 * multiplier
                positive_hits.append(word)
        elif word in NEGATIVE_WORDS:
            if is_negated:
                pos_score += 0.3 * multiplier  # "pas mauvais" is mildly positive
                positive_hits.append(f"pas_{word}")
            else:
                neg_score += 1.0 * multiplier
                negative_hits.append(word)

    # Calculate normalized score
    total = pos_score + neg_score
    if total == 0:
        score = 0.0
        confidence = 0.1  # Low confidence (no keywords matched)
    else:
        score = (pos_score - neg_score) / total  # -1 to +1
        # Confidence based on how many keywords we found
        confidence = min(1.0, total / max(len(words) * 0.3, 1))

    # Determine label
    if score > 0.2:
        label = "POSITIF"
    elif score < -0.2:
        label = "NEGATIF"
    else:
        label = "NEUTRE"

    return {
        "score": round(score, 3),
        "label": label,
        "confidence": round(confidence, 3),
        "positive_keywords": positive_hits[:5],
        "negative_keywords": negative_hits[:5],
        "word_count": len(words),
    }


def main():
    print("=" * 60)
    print("NLP Sentiment Analyzer - Horse Racing Comments")
    print("=" * 60)

    if not MASTER_FILE.exists():
        print(f"ERROR: Master file not found: {MASTER_FILE}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    stats = {
        "total_records": 0,
        "commentaire_apres_course": {"total": 0, "non_empty": 0, "positif": 0, "negatif": 0, "neutre": 0},
        "avis_entraineur": {"total": 0, "non_empty": 0, "positif": 0, "negatif": 0, "neutre": 0},
        "top_positive_keywords": Counter(),
        "top_negative_keywords": Counter(),
        "sample_comments": [],
    }

    analyzed_count = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        with open(MASTER_FILE, "r", encoding="utf-8") as in_f:
            for line_num, line in enumerate(in_f):
                stats["total_records"] += 1

                try:
                    rec = json.loads(line.strip())
                except json.JSONDecodeError:
                    continue

                partant_uid = rec.get("partant_uid", rec.get("id", f"line_{line_num}"))
                comment = rec.get("commentaire_apres_course", "")
                avis = rec.get("avis_entraineur", "")

                has_data = False

                # Analyze commentaire_apres_course
                stats["commentaire_apres_course"]["total"] += 1
                if comment and comment not in ("", "null", "None", "N/A"):
                    stats["commentaire_apres_course"]["non_empty"] += 1
                    result = analyze_sentiment(comment)
                    stats["commentaire_apres_course"][result["label"].lower()] += 1
                    for kw in result["positive_keywords"]:
                        stats["top_positive_keywords"][kw] += 1
                    for kw in result["negative_keywords"]:
                        stats["top_negative_keywords"][kw] += 1
                    has_data = True

                    if len(stats["sample_comments"]) < 20:
                        stats["sample_comments"].append({
                            "partant_uid": str(partant_uid),
                            "field": "commentaire_apres_course",
                            "text": comment[:200],
                            "sentiment": result,
                        })

                    output_rec = {
                        "partant_uid": partant_uid,
                        "comment_sentiment_score": result["score"],
                        "comment_sentiment_label": result["label"],
                        "comment_sentiment_confidence": result["confidence"],
                        "comment_positive_keywords": result["positive_keywords"],
                        "comment_negative_keywords": result["negative_keywords"],
                        "comment_word_count": result["word_count"],
                    }
                else:
                    output_rec = {"partant_uid": partant_uid}

                # Analyze avis_entraineur
                stats["avis_entraineur"]["total"] += 1
                if avis and avis not in ("", "null", "None", "N/A", "NEUTRE"):
                    stats["avis_entraineur"]["non_empty"] += 1
                    result = analyze_sentiment(avis)
                    stats["avis_entraineur"][result["label"].lower()] += 1
                    has_data = True

                    output_rec.update({
                        "avis_sentiment_score": result["score"],
                        "avis_sentiment_label": result["label"],
                        "avis_sentiment_confidence": result["confidence"],
                    })

                if has_data:
                    out_f.write(json.dumps(output_rec, ensure_ascii=False) + "\n")
                    analyzed_count += 1

                # Progress
                if stats["total_records"] % 500000 == 0:
                    print(f"  Processed {stats['total_records']:,} records... "
                          f"({stats['commentaire_apres_course']['non_empty']} comments, "
                          f"{stats['avis_entraineur']['non_empty']} avis)")

    # Prepare report
    report = {
        "generated_at": datetime.now().isoformat(),
        "master_file": str(MASTER_FILE),
        "total_records_scanned": stats["total_records"],
        "records_with_sentiment": analyzed_count,
        "commentaire_apres_course": {
            "total": stats["commentaire_apres_course"]["total"],
            "non_empty": stats["commentaire_apres_course"]["non_empty"],
            "fill_rate": f"{100 * stats['commentaire_apres_course']['non_empty'] / max(stats['commentaire_apres_course']['total'], 1):.2f}%",
            "positif": stats["commentaire_apres_course"]["positif"],
            "negatif": stats["commentaire_apres_course"]["negatif"],
            "neutre": stats["commentaire_apres_course"]["neutre"],
        },
        "avis_entraineur": {
            "total": stats["avis_entraineur"]["total"],
            "non_empty": stats["avis_entraineur"]["non_empty"],
            "fill_rate": f"{100 * stats['avis_entraineur']['non_empty'] / max(stats['avis_entraineur']['total'], 1):.2f}%",
            "positif": stats["avis_entraineur"]["positif"],
            "negatif": stats["avis_entraineur"]["negatif"],
            "neutre": stats["avis_entraineur"]["neutre"],
        },
        "top_positive_keywords": stats["top_positive_keywords"].most_common(20),
        "top_negative_keywords": stats["top_negative_keywords"].most_common(20),
        "sample_comments": stats["sample_comments"][:10],
        "output_file": str(OUTPUT_FILE),
    }

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\nResults:")
    print(f"  Total records scanned:     {stats['total_records']:,}")
    print(f"  commentaire_apres_course:  {stats['commentaire_apres_course']['non_empty']:,} non-empty "
          f"({100 * stats['commentaire_apres_course']['non_empty'] / max(stats['total_records'], 1):.2f}%)")
    print(f"  avis_entraineur:           {stats['avis_entraineur']['non_empty']:,} non-empty "
          f"({100 * stats['avis_entraineur']['non_empty'] / max(stats['total_records'], 1):.2f}%)")
    print(f"  Records with sentiment:    {analyzed_count:,}")
    print(f"\nOutput:  {OUTPUT_FILE}")
    print(f"Report:  {REPORT_FILE}")
    print(f"\nNote: commentaire_apres_course is currently ~0.5% filled.")
    print(f"This analyzer is ready for when more comment data is collected")
    print(f"(e.g., via France Galop Playwright scraping or PMU detail pages).")


if __name__ == "__main__":
    main()
