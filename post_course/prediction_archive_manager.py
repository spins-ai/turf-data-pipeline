#!/usr/bin/env python3
"""
post_course/prediction_archive_manager.py
==========================================
Archive chaque prediction emise par le systeme.

Stocke : date_course, course_uid, partant_uid, model_name,
         predicted_proba, predicted_rank, cote_marche, ticket_propose, mise.

Les predictions sont sauvegardees dans des fichiers JSON quotidiens
sous output/predictions/YYYY-MM-DD.json.

Interface de requete : par plage de dates, course, modele.
Stats : nombre total de predictions, hit rate vs predictions.

Aucun appel API : traitement 100 % local.

Usage :
    from post_course.prediction_archive_manager import PredictionArchiveManager

    mgr = PredictionArchiveManager()
    mgr.archive_prediction(pred)
    preds = mgr.query(date_from="2025-01-01", date_to="2025-01-31", model_name="xgb_v2")
    stats = mgr.compute_stats(preds)
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# ===========================================================================
# CONFIG
# ===========================================================================

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
OUTPUT_DIR = _PROJECT_ROOT / "output" / "predictions"


# ===========================================================================
# LOGGING
# ===========================================================================

from utils.logging_setup import setup_logging


# ===========================================================================
# DATACLASS
# ===========================================================================

@dataclass
class PredictionRecord:
    """Enregistrement unitaire d'une prediction."""
    date_course: str                     # YYYY-MM-DD
    course_uid: str
    partant_uid: str
    model_name: str
    predicted_proba: float               # probabilite estimee (0-1)
    predicted_rank: int                  # rang predit (1 = favori du modele)
    cote_marche: Optional[float] = None  # cote du marche au moment du pari
    ticket_propose: Optional[str] = None # type de pari propose (simple_gagnant, couple, etc.)
    mise: Optional[float] = None         # montant mise en euros
    timestamp: Optional[str] = None      # ISO timestamp de creation

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


# ===========================================================================
# MANAGER
# ===========================================================================

class PredictionArchiveManager:
    """Gere l'archivage et la requete des predictions."""

    def __init__(self, output_dir: Optional[Path] = None) -> None:
        self.output_dir = output_dir or OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logging("prediction_archive_manager")

    # ---- Ecriture --------------------------------------------------------

    def _daily_path(self, date_str: str) -> Path:
        """Chemin du fichier JSON quotidien."""
        return self.output_dir / f"{date_str}.json"

    def _load_daily(self, date_str: str) -> list[dict]:
        path = self._daily_path(date_str)
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_daily(self, date_str: str, records: list[dict]) -> None:
        path = self._daily_path(date_str)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)
        tmp.replace(path)

    def archive_prediction(self, pred: PredictionRecord) -> None:
        """Ajoute une prediction a l'archive quotidienne."""
        records = self._load_daily(pred.date_course)
        records.append(asdict(pred))
        self._save_daily(pred.date_course, records)
        self.logger.info(
            "Archive: %s / %s / %s (p=%.3f)",
            pred.date_course, pred.course_uid, pred.partant_uid, pred.predicted_proba,
        )

    def archive_batch(self, preds: list[PredictionRecord]) -> int:
        """Archive un lot de predictions. Retourne le nombre archive."""
        by_date: dict[str, list[dict]] = {}
        for pred in preds:
            by_date.setdefault(pred.date_course, []).append(asdict(pred))

        total = 0
        for date_str, new_records in by_date.items():
            existing = self._load_daily(date_str)
            existing.extend(new_records)
            self._save_daily(date_str, existing)
            total += len(new_records)

        self.logger.info("Archive batch: %d predictions sur %d jours", total, len(by_date))
        return total

    # ---- Requete ---------------------------------------------------------

    def query(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        course_uid: Optional[str] = None,
        model_name: Optional[str] = None,
        partant_uid: Optional[str] = None,
    ) -> list[dict]:
        """
        Interroge les archives avec des filtres optionnels.

        Args:
            date_from: date de debut (YYYY-MM-DD), incluse
            date_to: date de fin (YYYY-MM-DD), incluse
            course_uid: filtre par course
            model_name: filtre par modele
            partant_uid: filtre par partant

        Returns:
            Liste de dicts correspondant aux predictions filtrees.
        """
        results: list[dict] = []

        # Lister les fichiers JSON quotidiens
        daily_files = sorted(self.output_dir.glob("????-??-??.json"))

        for fpath in daily_files:
            file_date = fpath.stem  # YYYY-MM-DD
            if date_from and file_date < date_from:
                continue
            if date_to and file_date > date_to:
                continue

            with open(fpath, "r", encoding="utf-8") as f:
                records = json.load(f)

            for rec in records:
                if course_uid and rec.get("course_uid") != course_uid:
                    continue
                if model_name and rec.get("model_name") != model_name:
                    continue
                if partant_uid and rec.get("partant_uid") != partant_uid:
                    continue
                results.append(rec)

        self.logger.info("Query: %d predictions trouvees", len(results))
        return results

    # ---- Stats -----------------------------------------------------------

    def compute_stats(
        self,
        predictions: list[dict],
        actual_results: Optional[list[dict]] = None,
    ) -> dict:
        """
        Calcule des statistiques sur les predictions.

        Args:
            predictions: liste de predictions (dicts)
            actual_results: resultats reels optionnels (liste de dicts avec
                            partant_uid, course_uid, position_arrivee, is_gagnant)

        Returns:
            dict avec total_predictions, par modele, hit_rate si resultats fournis.
        """
        total = len(predictions)
        by_model: dict[str, int] = {}
        for p in predictions:
            m = p.get("model_name", "inconnu")
            by_model[m] = by_model.get(m, 0) + 1

        stats: dict = {
            "total_predictions": total,
            "predictions_par_modele": by_model,
        }

        if actual_results:
            # Indexer les resultats par (course_uid, partant_uid)
            results_idx: dict[tuple[str, str], dict] = {}
            for r in actual_results:
                key = (r.get("course_uid", ""), r.get("partant_uid", ""))
                results_idx[key] = r

            hits_win = 0
            hits_place = 0
            matched = 0

            for p in predictions:
                key = (p.get("course_uid", ""), p.get("partant_uid", ""))
                actual = results_idx.get(key)
                if actual is None:
                    continue
                matched += 1
                if actual.get("is_gagnant") or actual.get("position_arrivee") == 1:
                    if p.get("predicted_rank") == 1:
                        hits_win += 1
                pos = actual.get("position_arrivee")
                if pos is not None and pos <= 3:
                    if p.get("predicted_rank", 99) <= 3:
                        hits_place += 1

            stats["matched"] = matched
            stats["hit_rate_win"] = round(hits_win / matched, 4) if matched else 0.0
            stats["hit_rate_place_top3"] = round(hits_place / matched, 4) if matched else 0.0

        return stats


# ===========================================================================
# MAIN (CLI)
# ===========================================================================

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Gestionnaire d'archives de predictions")
    parser.add_argument("--date-from", type=str, default=None)
    parser.add_argument("--date-to", type=str, default=None)
    parser.add_argument("--model", type=str, default=None)
    parser.add_argument("--course", type=str, default=None)
    args = parser.parse_args()

    mgr = PredictionArchiveManager()
    preds = mgr.query(
        date_from=args.date_from,
        date_to=args.date_to,
        model_name=args.model,
        course_uid=args.course,
    )
    stats = mgr.compute_stats(preds)
    print(json.dumps(stats, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
