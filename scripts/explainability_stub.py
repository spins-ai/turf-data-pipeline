#!/usr/bin/env python3
"""
explainability_stub.py — Pilier 18 : Explainability (SHAP / LIME).

Stub pour l'interpretabilite des predictions. Fournit :
  - explain_prediction()  : calcule les importances de features (placeholder SHAP)
  - generate_explanation_report() : genere un rapport Markdown lisible

Ce fichier definit les interfaces completes mais utilise des valeurs aleatoires
tant que les modeles ne sont pas entraines et que SHAP/LIME ne sont pas integres.

Usage :
    python scripts/explainability_stub.py
"""

from __future__ import annotations

import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# ---------------------------------------------------------------------------
# Canonical feature names used across the pipeline
# ---------------------------------------------------------------------------
CANONICAL_FEATURES: List[str] = [
    "elo_rating",
    "jockey_win_rate_30j",
    "trainer_win_rate_90j",
    "going_match_score",
    "distance_aptitude",
    "weight_carried_kg",
    "days_since_last_run",
    "class_drop_indicator",
    "pace_early_speed",
    "pace_late_speed",
    "track_bias_score",
    "field_strength_index",
    "odds_implied_prob",
    "odds_movement_12h",
    "jockey_trainer_synergy",
    "pedigree_sire_win_pct",
    "pedigree_broodmare_sire_score",
    "sectional_last_600m",
    "rolling_position_avg_5",
    "age_factor",
]

# ---------------------------------------------------------------------------
# Natural-language templates for top features
# ---------------------------------------------------------------------------
_NL_TEMPLATES: Dict[str, str] = {
    "elo_rating": "Cheval {direction} (elo={value:.0f})",
    "jockey_win_rate_30j": "jockey {direction} (win_rate_30j={value:.0%})",
    "trainer_win_rate_90j": "entraineur {direction} (win_rate_90j={value:.0%})",
    "going_match_score": "terrain {direction} (going_match={value:.2f})",
    "distance_aptitude": "distance {direction} (aptitude={value:.2f})",
    "weight_carried_kg": "poids {direction} ({value:.1f} kg)",
    "days_since_last_run": "fraicheur {direction} ({value:.0f}j depuis dernier run)",
    "class_drop_indicator": "drop de classe {direction} (indicator={value:.2f})",
    "pace_early_speed": "early speed {direction} (pace={value:.2f})",
    "pace_late_speed": "late speed {direction} (pace={value:.2f})",
    "track_bias_score": "biais de piste {direction} (score={value:.2f})",
    "field_strength_index": "niveau du lot {direction} (index={value:.2f})",
    "odds_implied_prob": "cote {direction} (implied={value:.2%})",
    "odds_movement_12h": "mouvement de cote {direction} (12h={value:+.2f})",
    "jockey_trainer_synergy": "synergie jockey-entraineur {direction} (score={value:.2f})",
    "pedigree_sire_win_pct": "lignee paternelle {direction} (sire_win={value:.1%})",
    "pedigree_broodmare_sire_score": "broodmare sire {direction} (score={value:.2f})",
    "sectional_last_600m": "finish {direction} (last_600m={value:.2f}s)",
    "rolling_position_avg_5": "regularite {direction} (pos_avg_5={value:.1f})",
    "age_factor": "age {direction} (factor={value:.2f})",
}


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def explain_prediction(
    model: Any,
    features: dict,
    partant_uid: str,
    top_k: int = 10,
) -> dict:
    """Compute feature importances for a single prediction.

    Parameters
    ----------
    model : Any
        Trained model object. Currently unused (placeholder).
    features : dict
        Feature vector for the partant (``feature_name -> value``).
    partant_uid : str
        Unique identifier for the runner being explained.
    top_k : int
        Number of top features to return.

    Returns
    -------
    dict
        Keys:
        - ``partant_uid``: the runner UID
        - ``shap_values``: dict of feature_name -> SHAP value (placeholder: random)
        - ``top_features``: list of (feature, importance, value) sorted by |importance|
        - ``explanation_nl``: natural-language explanation string
    """
    # ------------------------------------------------------------------
    # Placeholder: generate random SHAP-like importances
    # TODO: replace with real SHAP / LIME computation
    #   import shap
    #   explainer = shap.TreeExplainer(model)
    #   shap_values = explainer.shap_values(feature_vector)
    # ------------------------------------------------------------------
    feature_names = list(features.keys()) if features else CANONICAL_FEATURES
    shap_values: Dict[str, float] = {}

    for feat in feature_names:
        shap_values[feat] = round(random.uniform(-0.15, 0.15), 4)

    # Sort by absolute importance
    sorted_feats: List[Tuple[str, float]] = sorted(
        shap_values.items(), key=lambda x: abs(x[1]), reverse=True
    )
    top_features = sorted_feats[:top_k]

    # Build natural-language explanation
    explanation_parts: List[str] = []
    for feat_name, importance in top_features[:5]:
        feat_value = features.get(feat_name, random.random())
        direction = "favorable" if importance > 0 else "defavorable"
        template = _NL_TEMPLATES.get(feat_name)
        if template:
            explanation_parts.append(template.format(direction=direction, value=feat_value))
        else:
            explanation_parts.append(f"{feat_name} {direction} ({feat_value:.3f})")

    explanation_nl = ", ".join(explanation_parts)

    return {
        "partant_uid": partant_uid,
        "shap_values": shap_values,
        "top_features": [
            {"feature": f, "importance": imp, "value": features.get(f, None)}
            for f, imp in top_features
        ],
        "explanation_nl": explanation_nl,
    }


def generate_explanation_report(
    predictions: dict,
    explanations: List[dict],
    race_label: str = "Course inconnue",
) -> str:
    """Generate a Markdown report combining predictions and explanations.

    Parameters
    ----------
    predictions : dict
        Output of ``InferencePipeline.format_output()`` — must contain
        a ``predictions`` key with a list of ranked partants.
    explanations : list[dict]
        List of outputs from :func:`explain_prediction`, one per partant.
    race_label : str
        Human-readable label for the race header.

    Returns
    -------
    str
        A Markdown-formatted report string.
    """
    lines: List[str] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines.append(f"# Rapport d'explicabilite — {race_label}")
    lines.append(f"*Genere le {now}*\n")

    # Map explanations by partant UID for easy lookup
    expl_map: Dict[str, dict] = {e["partant_uid"]: e for e in explanations}

    ranked = predictions.get("predictions", [])
    if not ranked:
        lines.append("> Aucune prediction disponible.\n")
        return "\n".join(lines)

    # Summary table
    lines.append("## Classement predit\n")
    lines.append("| Rang | N. | Proba | Confiance | Explication courte |")
    lines.append("|------|----|-------|-----------|--------------------|")

    for p in ranked:
        uid = str(p.get("partant", "?"))
        expl = expl_map.get(uid, {})
        short_expl = expl.get("explanation_nl", "—")
        # Truncate for table readability
        if len(short_expl) > 80:
            short_expl = short_expl[:77] + "..."
        lines.append(
            f"| {p.get('rank', '?')} "
            f"| {uid} "
            f"| {p.get('probability', 0):.2%} "
            f"| {p.get('confidence_label', '?')} "
            f"| {short_expl} |"
        )

    lines.append("")

    # Detailed explanations for top 5
    lines.append("## Details — Top 5\n")
    for p in ranked[:5]:
        uid = str(p.get("partant", "?"))
        expl = expl_map.get(uid)
        lines.append(f"### Partant {uid} (rang {p.get('rank', '?')})\n")

        if not expl:
            lines.append("> Pas d'explication disponible.\n")
            continue

        lines.append(f"**Resume** : {expl.get('explanation_nl', '—')}\n")
        lines.append("**Top features** :\n")
        for tf in expl.get("top_features", []):
            sign = "+" if tf["importance"] > 0 else ""
            lines.append(
                f"- `{tf['feature']}` : SHAP = {sign}{tf['importance']:.4f}"
                f"  (valeur = {tf.get('value', '?')})"
            )
        lines.append("")

    # Footer
    lines.append("---")
    lines.append("*Stub — les valeurs SHAP sont aleatoires tant que les modeles "
                  "ne sont pas entraines.*\n")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Demo / CLI
# ---------------------------------------------------------------------------

def _demo() -> None:
    """Run a quick demo to verify the stub works end-to-end."""
    print("=== Explainability Stub — Demo ===\n")

    # Fake features for 5 partants
    fake_features: Dict[str, dict] = {}
    for i in range(1, 6):
        uid = str(i)
        fake_features[uid] = {
            feat: round(random.uniform(0, 1), 3) if "rate" in feat or "score" in feat
            else round(random.uniform(1, 100), 1)
            for feat in CANONICAL_FEATURES
        }

    # Fake predictions
    fake_predictions = {
        "predictions": [
            {
                "rank": rank,
                "partant": str(rank),
                "probability": round(random.uniform(0.05, 0.40), 4),
                "expected_value": round(random.uniform(0.5, 4.0), 2),
                "confidence": round(random.uniform(0.3, 0.95), 4),
                "confidence_label": random.choice(["haute", "moyenne", "faible"]),
            }
            for rank in range(1, 6)
        ],
    }

    # Explain each partant
    explanations = []
    for uid, feats in fake_features.items():
        expl = explain_prediction(model=None, features=feats, partant_uid=uid)
        explanations.append(expl)
        print(f"Partant {uid}: {expl['explanation_nl']}\n")

    # Generate report
    report = generate_explanation_report(
        predictions=fake_predictions,
        explanations=explanations,
        race_label="R1C1 Longchamp — 2026-03-24",
    )
    print(report)


if __name__ == "__main__":
    _demo()
