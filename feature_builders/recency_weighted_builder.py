#!/usr/bin/env python3
"""Recency-weighted statistics: exponentially weighted averages where recent races
count more than older ones. Critical for GBM/XGBoost — standard in sports ML.
Uses decay factor alpha=0.15 (half-life ~4.3 races)."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/recency_weighted")
_LOG_EVERY = 500_000
_ALPHA = 0.15  # Decay factor: higher = more weight on recent


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _EWMAState:
    """Exponentially Weighted Moving Average tracker."""
    __slots__ = ("ew_pos", "ew_speed", "ew_win", "ew_place", "ew_cote",
                 "ew_field_rank", "total", "initialized")

    def __init__(self):
        self.ew_pos = None       # EWMA of normalized position
        self.ew_speed = None     # EWMA of reduction_km_ms
        self.ew_win = None       # EWMA of win (binary)
        self.ew_place = None     # EWMA of top-3 (binary)
        self.ew_cote = None      # EWMA of cote_finale
        self.ew_field_rank = None  # EWMA of rank/field_size
        self.total = 0
        self.initialized = False

    def update(self, norm_pos, speed, is_win, is_place, cote, field_rank):
        alpha = _ALPHA
        if not self.initialized:
            self.ew_pos = norm_pos
            self.ew_speed = speed
            self.ew_win = float(is_win) if is_win is not None else None
            self.ew_place = float(is_place) if is_place is not None else None
            self.ew_cote = cote
            self.ew_field_rank = field_rank
            self.initialized = True
        else:
            if norm_pos is not None:
                self.ew_pos = alpha * norm_pos + (1 - alpha) * (self.ew_pos or norm_pos)
            if speed is not None:
                self.ew_speed = alpha * speed + (1 - alpha) * (self.ew_speed or speed)
            if is_win is not None:
                self.ew_win = alpha * float(is_win) + (1 - alpha) * (self.ew_win or 0.0)
            if is_place is not None:
                self.ew_place = alpha * float(is_place) + (1 - alpha) * (self.ew_place or 0.0)
            if cote is not None:
                self.ew_cote = alpha * cote + (1 - alpha) * (self.ew_cote or cote)
            if field_rank is not None:
                self.ew_field_rank = alpha * field_rank + (1 - alpha) * (self.ew_field_rank or field_rank)
        self.total += 1


def main():
    logger = setup_logging("recency_weighted_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "recency_weighted_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _EWMAState] = {}
    jockey_states: dict[str, _EWMAState] = {}
    trainer_states: dict[str, _EWMAState] = {}

    t0 = time.perf_counter()
    written = 0
    fills = defaultdict(int)

    with open(tmp_path, "w", encoding="utf-8") as fout:
        current_course = None
        course_records: list[dict] = []

        with open(INPUT_PARTANTS, "r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, 1):
                rec = json.loads(line)
                cuid = rec.get("course_uid", "")

                if cuid != current_course and course_records:
                    _process_course(course_records, fout, horse_states, jockey_states, trainer_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states, jockey_states, trainer_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_st, jockey_st, trainer_st, fills):
    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = rec.get("jockey_driver") or rec.get("nom_jockey") or ""
        trainer = rec.get("entraineur") or rec.get("nom_entraineur") or ""

        # Horse EWMA features
        hs = horse_st.get(hid)
        if hs and hs.total >= 2:
            if hs.ew_pos is not None:
                feat["rw_horse_ewma_pos"] = round(hs.ew_pos, 5)
                fills["rw_horse_ewma_pos"] += 1
            if hs.ew_speed is not None:
                feat["rw_horse_ewma_speed"] = round(hs.ew_speed, 4)
                fills["rw_horse_ewma_speed"] += 1
            if hs.ew_win is not None:
                feat["rw_horse_ewma_wr"] = round(hs.ew_win, 5)
                fills["rw_horse_ewma_wr"] += 1
            if hs.ew_place is not None:
                feat["rw_horse_ewma_place"] = round(hs.ew_place, 5)
                fills["rw_horse_ewma_place"] += 1
            if hs.ew_cote is not None:
                feat["rw_horse_ewma_cote"] = round(hs.ew_cote, 3)
                fills["rw_horse_ewma_cote"] += 1
            if hs.ew_field_rank is not None:
                feat["rw_horse_ewma_rank"] = round(hs.ew_field_rank, 5)
                fills["rw_horse_ewma_rank"] += 1

            # Momentum: compare EWMA to simple average (divergence = trend)
            if hs.ew_win is not None and hs.total > 0:
                # Can't compute simple average without storing all values, use ewma vs current implied
                cote = _safe(rec.get("cote_finale"))
                if cote and cote > 0:
                    implied = 1.0 / cote
                    feat["rw_horse_ewma_vs_market"] = round(hs.ew_win - implied, 5)
                    fills["rw_horse_ewma_vs_market"] += 1

        # Jockey EWMA features
        js = jockey_st.get(jockey)
        if js and js.total >= 10:
            if js.ew_win is not None:
                feat["rw_jockey_ewma_wr"] = round(js.ew_win, 5)
                fills["rw_jockey_ewma_wr"] += 1
            if js.ew_place is not None:
                feat["rw_jockey_ewma_place"] = round(js.ew_place, 5)
                fills["rw_jockey_ewma_place"] += 1
            if js.ew_pos is not None:
                feat["rw_jockey_ewma_pos"] = round(js.ew_pos, 5)
                fills["rw_jockey_ewma_pos"] += 1

        # Trainer EWMA features
        ts = trainer_st.get(trainer)
        if ts and ts.total >= 10:
            if ts.ew_win is not None:
                feat["rw_trainer_ewma_wr"] = round(ts.ew_win, 5)
                fills["rw_trainer_ewma_wr"] += 1
            if ts.ew_place is not None:
                feat["rw_trainer_ewma_place"] = round(ts.ew_place, 5)
                fills["rw_trainer_ewma_place"] += 1

        # Composite EWMA: weighted combination
        weights = []
        if hs and hs.total >= 2 and hs.ew_win is not None:
            weights.append((hs.ew_win, 3.0))
        if js and js.total >= 10 and js.ew_win is not None:
            weights.append((js.ew_win, 1.5))
        if ts and ts.total >= 10 and ts.ew_win is not None:
            weights.append((ts.ew_win, 1.0))
        if weights:
            wsum = sum(v * w for v, w in weights)
            wtotal = sum(w for _, w in weights)
            feat["rw_composite_ewma_wr"] = round(wsum / wtotal, 5)
            fills["rw_composite_ewma_wr"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jockey = rec.get("jockey_driver") or rec.get("nom_jockey") or ""
        trainer = rec.get("entraineur") or rec.get("nom_entraineur") or ""

        pos = _safe(rec.get("position_arrivee"))
        field = _safe(rec.get("nombre_partants"))
        speed = _safe(rec.get("reduction_km_ms"))
        cote = _safe(rec.get("cote_finale"))
        is_winner = bool(rec.get("is_gagnant"))
        is_placed = False
        try:
            is_placed = int(rec.get("position_arrivee", 99)) <= 3
        except (TypeError, ValueError):
            pass

        norm_pos = pos / field if pos is not None and field and field > 0 else None
        field_rank = norm_pos  # same as normalized position

        for entity_key, states_dict in [(hid, horse_st), (jockey, jockey_st), (trainer, trainer_st)]:
            if not entity_key:
                continue
            if entity_key not in states_dict:
                states_dict[entity_key] = _EWMAState()
            states_dict[entity_key].update(norm_pos, speed, is_winner, is_placed, cote, field_rank)


if __name__ == "__main__":
    main()
