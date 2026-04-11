#!/usr/bin/env python3
"""
feature_builders.commentaire_deep_nlp_builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
NLP features extracted from post-race comments (commentaire_apres_course)
and trainer opinions (avis_entraineur) using pure Python regex + keyword
matching -- no external NLP libraries required.

Reads partants_master.jsonl in streaming mode, processes all records
chronologically, and computes per-partant NLP features.

Temporal integrity: for any partant at date D, only prior races contribute
to historical features -- no future leakage.

Produces:
  - commentaire_deep_nlp.jsonl  in output/commentaire_deep_nlp/

Features per partant (21):
  - nlp_comment_len_chars        : number of characters in comment
  - nlp_comment_len_words        : number of words in comment
  - nlp_sentiment_score          : positive keyword count minus negative count
  - nlp_effort_score             : count of effort indicators
  - nlp_incident_score           : count of incident mentions
  - nlp_position_keywords        : count of position-related phrases
  - nlp_fitness_concerns         : count of fitness/condition keywords
  - nlp_ground_mentions          : count of ground/surface keywords
  - nlp_running_style            : extracted running style (meneur/attentiste/finisseur/regulier)
  - nlp_avis_sentiment           : trainer opinion sentiment score
  - nlp_avis_confident           : 1 if trainer sounds confident, else 0
  - nlp_hist_avg_sentiment_5     : horse's avg sentiment over last 5 comments
  - nlp_hist_incident_last5      : count of races with incidents in last 5
  - nlp_hist_incident_last10     : count of races with incidents in last 10
  - nlp_hist_comment_len_trend   : trend (recent - older) of comment length
  - nlp_hist_trainer_opinion_hit : historical rate: trainer confident AND horse won
  - nlp_positive_ratio           : positive / (positive + negative) keywords
  - nlp_has_comment              : 1 if comment is non-empty, else 0
  - nlp_has_avis                 : 1 if avis_entraineur is non-empty, else 0
  - nlp_effort_vs_incident       : effort_score - incident_score (grit vs trouble)
  - nlp_comment_richness         : unique words / total words ratio

Usage:
    python feature_builders/commentaire_deep_nlp_builder.py
    python feature_builders/commentaire_deep_nlp_builder.py --input path/to/partants_master.jsonl
"""

from __future__ import annotations

import argparse
import gc
import json
import re
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

# ===========================================================================
# CONFIG
# ===========================================================================

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/commentaire_deep_nlp")

_LOG_EVERY = 500_000
_GC_EVERY = 500_000

# ===========================================================================
# KEYWORD DICTIONARIES
# ===========================================================================

_POSITIVE_WORDS = re.compile(
    r"\b(?:bien|fort|bon|bonne|facile|facilement|brillant|brillante|"
    r"impressionnant|impressionnante|domin[eé]|ais[eé]ment|superbe|"
    r"remarquable|excellent|excellente|parfait|parfaite|"
    r"convaincant|convaincante|autoritaire|nette|nettement)\b",
    re.IGNORECASE,
)

_NEGATIVE_WORDS = re.compile(
    r"\b(?:fatigu[eé]|fatigu[eé]e|d[eé][cç]u|d[eé][cç]ue|g[eê]n[eé]|g[eê]n[eé]e|"
    r"boiteux|boiteuse|irr[eé]gulier|irr[eé]guli[eè]re|d[eé]cevant|d[eé]cevante|"
    r"mauvais|mauvaise|m[eé]diocre|insuffisant|insuffisante|"
    r"limit[eé]|limit[eé]e|laborieux|laborieuse|pénible)\b",
    re.IGNORECASE,
)

_EFFORT_PATTERNS = re.compile(
    r"(?:a\s+lutt[eé]|a\s+r[eé]sist[eé]|s['\u2019]est\s+battu|au\s+courage|"
    r"a\s+tout\s+donn[eé]|a\s+tenu|s['\u2019]est\s+accroché|vaillant|"
    r"combatif|combative|courageux|courageuse)",
    re.IGNORECASE,
)

_INCIDENT_PATTERNS = re.compile(
    r"(?:g[eê]n[eé]|bousculé|d[eé]ferr[eé]|a\s+but[eé]|cass[eé]|tomb[eé]|"
    r"accident|faux[- ]d[eé]part|a\s+failli|perdu\s+(?:un\s+)?fer|"
    r"fautif|fautive|a\s+cass[eé]|a\s+accroch[eé]|galopad[eé]|"
    r"dist(?:anci[eé]|qualifi[eé])|arr[eê]t[eé]|a\s+coul[eé])",
    re.IGNORECASE,
)

_POSITION_PATTERNS = re.compile(
    r"(?:en\s+t[eê]te|men[eé]|a\s+men[eé]|attendu|attentiste|"
    r"derni[eè]re\s+ligne|remont[eé]e|sprint\s+final|pointe\s+de\s+vitesse|"
    r"sur\s+la\s+fin|a\s+termin[eé]|en\s+queue|a\s+d[eé]bord[eé]|"
    r"au\s+large|[aà]\s+la\s+corde|en\s+pointe|en\s+retrait)",
    re.IGNORECASE,
)

_FITNESS_PATTERNS = re.compile(
    r"(?:manque\s+de\s+condition|pas\s+en\s+forme|besoin\s+de\s+course|"
    r"premi[eè]re\s+sortie|rentr[eé]e|de\s+rentr[eé]e|reprend|"
    r"manque\s+de\s+comp[eé]tition|court\s+de\s+course|"
    r"manque\s+de\s+moyens|pas\s+au\s+mieux|en\s+m[eé]forme)",
    re.IGNORECASE,
)

_GROUND_PATTERNS = re.compile(
    r"(?:terrain\s+lourd|terrain\s+souple|terrain\s+coll|bon\s+terrain|"
    r"tenu|terrain\s+l[eé]ger|terrain\s+sec|terrain\s+profond|"
    r"piste\s+lourde|piste\s+souple|sol\s+lourd|sol\s+souple)",
    re.IGNORECASE,
)

# Running style extraction: order matters (first match wins)
_STYLE_MENEUR = re.compile(
    r"(?:en\s+t[eê]te|a\s+men[eé]|meneu[rx]|a\s+pris\s+la\s+t[eê]te|"
    r"a\s+fait\s+la\s+course\s+en\s+t[eê]te)", re.IGNORECASE,
)
_STYLE_ATTENTISTE = re.compile(
    r"(?:attendu|attentiste|a\s+attendu|plac[eé]\s+(?:en\s+)?(?:milieu|"
    r"arri[eè]re)|en\s+retrait|couvert)", re.IGNORECASE,
)
_STYLE_FINISSEUR = re.compile(
    r"(?:finisseur|sprint\s+final|pointe\s+de\s+vitesse|termin[eé]\s+fort|"
    r"(?:forte|belle)\s+remont[eé]e|a\s+remont[eé]|d[eé]bord[eé]\s+sur\s+la\s+fin)",
    re.IGNORECASE,
)
_STYLE_REGULIER = re.compile(
    r"(?:r[eé]gulier|r[eé]guli[eè]re|sans\s+briller|m[eê]me\s+allure|"
    r"a\s+tenu\s+son\s+rang)", re.IGNORECASE,
)

# Trainer opinion keywords
_AVIS_POSITIVE = re.compile(
    r"(?:confiant|confiance|en\s+forme|objectif\s+victoire|"
    r"bien\s+pr[eé]par[eé]|pr[eê]t|en\s+progr[eè]s|"
    r"devrait\s+bien|am[eé]lior[eé]|ambitieux|ambition)",
    re.IGNORECASE,
)
_AVIS_NEGATIVE = re.compile(
    r"(?:reprend|manque|besoin\s+de\s+course|pas\s+(?:encore\s+)?pr[eê]t|"
    r"en\s+qu[eê]te|d[eé]couverte|sans\s+pr[eé]tention|"
    r"prudent|prudente|observation|en\s+rodage)",
    re.IGNORECASE,
)


# ===========================================================================
# TEXT ANALYSIS FUNCTIONS
# ===========================================================================


def _normalize_text(text: Any) -> str:
    """Normalize comment text to a clean string."""
    if not text or not isinstance(text, str):
        return ""
    return text.strip()


def _count_matches(pattern: re.Pattern, text: str) -> int:
    """Count non-overlapping matches of a pattern in text."""
    return len(pattern.findall(text))


def _extract_running_style(text: str) -> Optional[str]:
    """Extract running style from comment. Returns first match."""
    if not text:
        return None
    if _STYLE_MENEUR.search(text):
        return "meneur"
    if _STYLE_FINISSEUR.search(text):
        return "finisseur"
    if _STYLE_ATTENTISTE.search(text):
        return "attentiste"
    if _STYLE_REGULIER.search(text):
        return "regulier"
    return None


def _word_count(text: str) -> int:
    """Count words in text."""
    if not text:
        return 0
    return len(text.split())


def _unique_word_ratio(text: str) -> Optional[float]:
    """Ratio of unique words to total words."""
    if not text:
        return None
    words = text.lower().split()
    if not words:
        return None
    return round(len(set(words)) / len(words), 4)


def _analyze_comment(text: str) -> dict[str, Any]:
    """Extract all NLP features from a single comment."""
    result: dict[str, Any] = {}

    if not text:
        result["nlp_comment_len_chars"] = 0
        result["nlp_comment_len_words"] = 0
        result["nlp_sentiment_score"] = None
        result["nlp_effort_score"] = 0
        result["nlp_incident_score"] = 0
        result["nlp_position_keywords"] = 0
        result["nlp_fitness_concerns"] = 0
        result["nlp_ground_mentions"] = 0
        result["nlp_running_style"] = None
        result["nlp_positive_ratio"] = None
        result["nlp_has_comment"] = 0
        result["nlp_comment_richness"] = None
        result["nlp_effort_vs_incident"] = None
        return result

    n_pos = _count_matches(_POSITIVE_WORDS, text)
    n_neg = _count_matches(_NEGATIVE_WORDS, text)
    effort = _count_matches(_EFFORT_PATTERNS, text)
    incident = _count_matches(_INCIDENT_PATTERNS, text)

    result["nlp_comment_len_chars"] = len(text)
    result["nlp_comment_len_words"] = _word_count(text)
    result["nlp_sentiment_score"] = n_pos - n_neg
    result["nlp_effort_score"] = effort
    result["nlp_incident_score"] = incident
    result["nlp_position_keywords"] = _count_matches(_POSITION_PATTERNS, text)
    result["nlp_fitness_concerns"] = _count_matches(_FITNESS_PATTERNS, text)
    result["nlp_ground_mentions"] = _count_matches(_GROUND_PATTERNS, text)
    result["nlp_running_style"] = _extract_running_style(text)
    result["nlp_has_comment"] = 1
    result["nlp_comment_richness"] = _unique_word_ratio(text)

    total_sentiment = n_pos + n_neg
    result["nlp_positive_ratio"] = round(n_pos / total_sentiment, 4) if total_sentiment > 0 else None
    result["nlp_effort_vs_incident"] = effort - incident

    return result


def _analyze_avis(text: str) -> dict[str, Any]:
    """Extract features from trainer opinion."""
    if not text:
        return {
            "nlp_avis_sentiment": None,
            "nlp_avis_confident": None,
            "nlp_has_avis": 0,
        }

    n_pos = _count_matches(_AVIS_POSITIVE, text)
    n_neg = _count_matches(_AVIS_NEGATIVE, text)

    return {
        "nlp_avis_sentiment": n_pos - n_neg,
        "nlp_avis_confident": 1 if n_pos > n_neg else 0,
        "nlp_has_avis": 1,
    }


# ===========================================================================
# HORSE HISTORY TRACKER
# ===========================================================================


class _HorseCommentHistory:
    """Tracks rolling comment history per horse for historical features.

    Uses deque(maxlen=10) for bounded memory usage.
    """

    __slots__ = ("sentiments", "incidents", "comment_lens", "trainer_confident", "trainer_won")

    def __init__(self) -> None:
        self.sentiments: deque[float] = deque(maxlen=10)
        self.incidents: deque[int] = deque(maxlen=10)      # 1 if incident, else 0
        self.comment_lens: deque[int] = deque(maxlen=10)
        self.trainer_confident: deque[int] = deque(maxlen=10)  # 1 if confident
        self.trainer_won: deque[int] = deque(maxlen=10)        # 1 if won that race

    def snapshot_features(self) -> dict[str, Any]:
        """Return historical features BEFORE updating with current race."""
        feats: dict[str, Any] = {}

        # Avg sentiment last 5
        sent_list = list(self.sentiments)
        last5_sent = sent_list[-5:] if len(sent_list) >= 1 else []
        feats["nlp_hist_avg_sentiment_5"] = (
            round(sum(last5_sent) / len(last5_sent), 4) if last5_sent else None
        )

        # Incident count last 5 and last 10
        inc_list = list(self.incidents)
        feats["nlp_hist_incident_last5"] = sum(inc_list[-5:]) if inc_list else None
        feats["nlp_hist_incident_last10"] = sum(inc_list) if inc_list else None

        # Comment length trend: avg of recent 3 minus avg of older entries
        cl_list = list(self.comment_lens)
        if len(cl_list) >= 4:
            recent = cl_list[-3:]
            older = cl_list[:-3]
            avg_recent = sum(recent) / len(recent)
            avg_older = sum(older) / len(older)
            if avg_older > 0:
                feats["nlp_hist_comment_len_trend"] = round(
                    (avg_recent - avg_older) / avg_older, 4
                )
            else:
                feats["nlp_hist_comment_len_trend"] = None
        else:
            feats["nlp_hist_comment_len_trend"] = None

        # Trainer opinion vs actual result correlation
        tc_list = list(self.trainer_confident)
        tw_list = list(self.trainer_won)
        if tc_list:
            confident_races = [(c, w) for c, w in zip(tc_list, tw_list) if c == 1]
            if confident_races:
                feats["nlp_hist_trainer_opinion_hit"] = round(
                    sum(w for _, w in confident_races) / len(confident_races), 4
                )
            else:
                feats["nlp_hist_trainer_opinion_hit"] = None
        else:
            feats["nlp_hist_trainer_opinion_hit"] = None

        return feats

    def update(
        self,
        sentiment: Optional[int],
        has_incident: bool,
        comment_len: int,
        trainer_confident: Optional[int],
        is_winner: bool,
    ) -> None:
        """Update history with current race data."""
        if sentiment is not None:
            self.sentiments.append(float(sentiment))
        self.incidents.append(1 if has_incident else 0)
        self.comment_lens.append(comment_len)
        if trainer_confident is not None:
            self.trainer_confident.append(trainer_confident)
            self.trainer_won.append(1 if is_winner else 0)


# ===========================================================================
# STREAMING READER
# ===========================================================================


def _iter_jsonl(path: Path, logger):
    """Yield dicts from a JSONL file, one line at a time (streaming)."""
    count = 0
    errors = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                errors += 1
                if errors <= 10:
                    logger.warning("Ligne JSON invalide ignoree (erreur %d)", errors)
    logger.info("Lecture terminee: %d records, %d erreurs JSON", count, errors)


def _resolve_input(cli_arg: Optional[str]) -> Path:
    """Find the input file."""
    if cli_arg:
        p = Path(cli_arg)
        if p.exists():
            return p
    if INPUT_PARTANTS.exists():
        return INPUT_PARTANTS
    raise FileNotFoundError(
        f"Input not found: {INPUT_PARTANTS}; pass --input explicitly."
    )


# ===========================================================================
# MAIN BUILDER
# ===========================================================================


def _process_nlp_course(course_group, fout, horse_history, feature_names,
                        fill_counts, n_written, n_processed, t0, logger):
    """Process one course: snapshot features, write, then update history."""
    course_features: list[tuple[dict, dict]] = []

    for rec in course_group:
        partant_uid = rec.get("partant_uid", "")
        nom_cheval = rec.get("nom_cheval") or ""

        commentaire = _normalize_text(
            rec.get("commentaire_apres_course") or rec.get("commentaire")
        )
        avis = _normalize_text(rec.get("avis_entraineur"))

        features: dict[str, Any] = {"partant_uid": partant_uid}
        features.update(_analyze_comment(commentaire))
        features.update(_analyze_avis(avis))

        if nom_cheval:
            hist = horse_history[nom_cheval]
            features.update(hist.snapshot_features())
        else:
            features["nlp_hist_avg_sentiment_5"] = None
            features["nlp_hist_incident_last5"] = None
            features["nlp_hist_incident_last10"] = None
            features["nlp_hist_comment_len_trend"] = None
            features["nlp_hist_trainer_opinion_hit"] = None

        course_features.append((rec, features))

    for rec, features in course_features:
        fout.write(json.dumps(features, ensure_ascii=False) + "\n")
        n_written += 1

        for fname in feature_names:
            val = features.get(fname)
            if val is not None and val != "" and val != 0:
                fill_counts[fname] += 1

        nom_cheval = rec.get("nom_cheval") or ""
        if nom_cheval:
            commentaire = _normalize_text(
                rec.get("commentaire_apres_course") or rec.get("commentaire")
            )
            is_winner = bool(rec.get("is_gagnant"))
            horse_history[nom_cheval].update(
                sentiment=features.get("nlp_sentiment_score"),
                has_incident=features.get("nlp_incident_score", 0) > 0,
                comment_len=features.get("nlp_comment_len_chars", 0),
                trainer_confident=features.get("nlp_avis_confident"),
                is_winner=is_winner,
            )

    n_processed += len(course_group)
    if n_processed % _LOG_EVERY < len(course_group):
        elapsed = time.time() - t0
        rate = n_processed / elapsed if elapsed > 0 else 0
        logger.info("  processed %d records (%.0f rec/s)", n_processed, rate)
    if n_processed % _GC_EVERY < len(course_group):
        gc.collect()

    return n_written, n_processed


def build_commentaire_deep_nlp(input_path: Path, output_dir: Path) -> None:
    """Build all NLP features from comments and trainer opinions."""
    logger = setup_logging("commentaire_deep_nlp_builder")
    logger.info("=== Commentaire Deep NLP Builder ===")
    logger.info("Input: %s", input_path)
    logger.info("Output dir: %s", output_dir)
    t0 = time.time()

    # ── Streaming mode: data is already sorted by course_uid ──────────
    logger.info("Streaming mode: processing course by course...")

    horse_history: dict[str, _HorseCommentHistory] = defaultdict(_HorseCommentHistory)

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "commentaire_deep_nlp.jsonl"
    tmp_out = output_file.with_suffix(".tmp")

    n_processed = 0
    n_written = 0

    # Fill rate counters
    feature_names = [
        "nlp_comment_len_chars", "nlp_comment_len_words",
        "nlp_sentiment_score", "nlp_effort_score", "nlp_incident_score",
        "nlp_position_keywords", "nlp_fitness_concerns", "nlp_ground_mentions",
        "nlp_running_style", "nlp_avis_sentiment", "nlp_avis_confident",
        "nlp_hist_avg_sentiment_5", "nlp_hist_incident_last5",
        "nlp_hist_incident_last10", "nlp_hist_comment_len_trend",
        "nlp_hist_trainer_opinion_hit", "nlp_positive_ratio",
        "nlp_has_comment", "nlp_has_avis", "nlp_effort_vs_incident",
        "nlp_comment_richness",
    ]
    fill_counts: dict[str, int] = {name: 0 for name in feature_names}

    with open(tmp_out, "w", encoding="utf-8", newline="\n") as fout:
        current_course = None
        course_group: list[dict[str, Any]] = []

        for rec in _iter_jsonl(input_path, logger):
            cuid = str(rec.get("course_uid", "") or "")

            if cuid != current_course and course_group:

                # Process the completed course group
                n_written, n_processed = _process_nlp_course(
                    course_group, fout, horse_history, feature_names,
                    fill_counts, n_written, n_processed, t0, logger,
                )
                course_group = []

            current_course = cuid
            course_group.append(rec)

        # Process last course group
        if course_group:
            n_written, n_processed = _process_nlp_course(
                course_group, fout, horse_history, feature_names,
                fill_counts, n_written, n_processed, t0, logger,
            )

    gc.collect()

    # Atomic rename
    tmp_out.rename(output_file)

    elapsed = time.time() - t0
    logger.info("=== Done: %d features written in %.1fs ===", n_written, elapsed)

    # ── Fill rate report ─────────────────────────────────────────────
    logger.info("--- Fill rates ---")
    for fname in feature_names:
        count = fill_counts[fname]
        rate = count / n_written * 100 if n_written > 0 else 0
        logger.info("  %-40s %8d / %d  (%5.1f%%)", fname, count, n_written, rate)

    logger.info(
        "Unique horses tracked: %d", len(horse_history)
    )
    logger.info("Output: %s", output_file)


# ===========================================================================
# CLI
# ===========================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build NLP features from post-race comments and trainer opinions."
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=None,
        help="Path to partants_master.jsonl (auto-detected if omitted).",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default=None,
        help=f"Output directory (default: {OUTPUT_DIR}).",
    )
    args = parser.parse_args()

    input_path = _resolve_input(args.input)
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    build_commentaire_deep_nlp(input_path, output_dir)


if __name__ == "__main__":
    main()
