#!/usr/bin/env python3
"""Owner/breeder deep features: historical performance stats for proprietaire and eleveur,
plus owner-trainer and owner-jockey affinity metrics."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/proprietaire_eleveur_deep")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


class _EntityState:
    __slots__ = ("wins", "places", "total", "recent_pos", "ewma_pos")

    def __init__(self):
        self.wins = 0
        self.places = 0
        self.total = 0
        self.recent_pos = deque(maxlen=30)
        self.ewma_pos = None


class _PairState:
    __slots__ = ("wins", "total")

    def __init__(self):
        self.wins = 0
        self.total = 0


def main():
    logger = setup_logging("proprietaire_eleveur_deep_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "proprietaire_eleveur_deep_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    owner_states: dict[str, _EntityState] = {}
    breeder_states: dict[str, _EntityState] = {}
    owner_trainer: dict[str, _PairState] = {}
    owner_jockey: dict[str, _PairState] = {}

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
                    _process_course(course_records, fout, owner_states, breeder_states,
                                    owner_trainer, owner_jockey, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, owner_states, breeder_states,
                            owner_trainer, owner_jockey, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, owner_states, breeder_states,
                    owner_trainer, owner_jockey, fills):
    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        owner = (rec.get("proprietaire") or "").strip()
        breeder = (rec.get("eleveur") or "").strip()
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()

        # Owner features
        if owner:
            ost = owner_states.get(owner)
            if ost and ost.total >= 5:
                feat["ped_owner_wr"] = round(ost.wins / ost.total, 5)
                feat["ped_owner_pr"] = round(ost.places / ost.total, 5)
                feat["ped_owner_total"] = ost.total
                fills["ped_owner_wr"] += 1
                fills["ped_owner_pr"] += 1
                fills["ped_owner_total"] += 1

                if ost.recent_pos:
                    rp = list(ost.recent_pos)
                    feat["ped_owner_avg_pos_recent"] = round(sum(rp) / len(rp), 2)
                    fills["ped_owner_avg_pos_recent"] += 1

                if ost.ewma_pos is not None:
                    feat["ped_owner_ewma_pos"] = round(ost.ewma_pos, 3)
                    fills["ped_owner_ewma_pos"] += 1

        # Breeder features
        if breeder:
            bst = breeder_states.get(breeder)
            if bst and bst.total >= 10:
                feat["ped_breeder_wr"] = round(bst.wins / bst.total, 5)
                feat["ped_breeder_pr"] = round(bst.places / bst.total, 5)
                feat["ped_breeder_total"] = bst.total
                fills["ped_breeder_wr"] += 1
                fills["ped_breeder_pr"] += 1
                fills["ped_breeder_total"] += 1

        # Owner-trainer pair
        if owner and trainer:
            pair_key = f"{owner}|{trainer}"
            pt = owner_trainer.get(pair_key)
            if pt and pt.total >= 5:
                feat["ped_owner_trainer_wr"] = round(pt.wins / pt.total, 5)
                feat["ped_owner_trainer_runs"] = pt.total
                fills["ped_owner_trainer_wr"] += 1
                fills["ped_owner_trainer_runs"] += 1

        # Owner-jockey pair
        if owner and jockey:
            pair_key = f"{owner}|{jockey}"
            pj = owner_jockey.get(pair_key)
            if pj and pj.total >= 5:
                feat["ped_owner_jockey_wr"] = round(pj.wins / pj.total, 5)
                feat["ped_owner_jockey_runs"] = pj.total
                fills["ped_owner_jockey_wr"] += 1
                fills["ped_owner_jockey_runs"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    alpha = 0.15
    for rec in records:
        owner = (rec.get("proprietaire") or "").strip()
        breeder = (rec.get("eleveur") or "").strip()
        jockey = (rec.get("jockey") or rec.get("driver") or "").strip()
        trainer = (rec.get("entraineur") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))
        pos = _safe(rec.get("place_arrivee"))
        is_place = pos is not None and pos <= 3

        if owner:
            if owner not in owner_states:
                owner_states[owner] = _EntityState()
            ost = owner_states[owner]
            ost.wins += int(is_winner)
            ost.places += int(is_place)
            ost.total += 1
            if pos is not None:
                ost.recent_pos.append(pos)
                if ost.ewma_pos is None:
                    ost.ewma_pos = pos
                else:
                    ost.ewma_pos = alpha * pos + (1 - alpha) * ost.ewma_pos

        if breeder:
            if breeder not in breeder_states:
                breeder_states[breeder] = _EntityState()
            bst = breeder_states[breeder]
            bst.wins += int(is_winner)
            bst.places += int(is_place)
            bst.total += 1

        if owner and trainer:
            pk = f"{owner}|{trainer}"
            if pk not in owner_trainer:
                owner_trainer[pk] = _PairState()
            owner_trainer[pk].wins += int(is_winner)
            owner_trainer[pk].total += 1

        if owner and jockey:
            pk = f"{owner}|{jockey}"
            if pk not in owner_jockey:
                owner_jockey[pk] = _PairState()
            owner_jockey[pk].wins += int(is_winner)
            owner_jockey[pk].total += 1


if __name__ == "__main__":
    main()
