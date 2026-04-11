#!/usr/bin/env python3
"""Conditional win probability: Bayesian P(win | discipline, distance_bucket, age, surface, etc.)
Multi-dimensional conditional probabilities from historical data. Critical for all models."""
from __future__ import annotations
import gc, json, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/conditional_winprob")
_LOG_EVERY = 500_000
_MIN_OBS = 20  # Minimum observations before emitting a conditional probability
_SHRINK_K = 15  # Bayesian shrinkage factor


def _safe(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _dist_bucket(d):
    if d is None:
        return None
    if d < 1400:
        return "sprint"
    if d < 1800:
        return "mile"
    if d < 2200:
        return "inter"
    if d < 2800:
        return "stay"
    return "long"


def _age_bucket(a):
    if a is None:
        return None
    if a <= 2:
        return "2yo"
    if a == 3:
        return "3yo"
    if a <= 5:
        return "4-5yo"
    return "6+yo"


def _shrunk(wins, total, global_rate):
    if total < 3:
        return None
    raw = wins / total
    return (total * raw + _SHRINK_K * global_rate) / (total + _SHRINK_K)


class _CondCounter:
    __slots__ = ("wins", "total")

    def __init__(self):
        self.wins = 0
        self.total = 0


def main():
    logger = setup_logging("conditional_winprob_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "conditional_winprob_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    # Multi-dimensional conditional counters
    # Key = tuple of condition values
    cond_1d: dict[str, _CondCounter] = defaultdict(_CondCounter)   # single dimension
    cond_2d: dict[str, _CondCounter] = defaultdict(_CondCounter)   # pairs
    cond_3d: dict[str, _CondCounter] = defaultdict(_CondCounter)   # triples
    global_wins = 0
    global_total = 0

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
                    global_wins, global_total = _process_course(
                        course_records, fout, cond_1d, cond_2d, cond_3d,
                        global_wins, global_total, fills,
                    )
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            global_wins, global_total = _process_course(
                course_records, fout, cond_1d, cond_2d, cond_3d,
                global_wins, global_total, fills,
            )
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, cond_1d, cond_2d, cond_3d, gw, gt, fills):
    gr = gw / gt if gt > 0 else 0.08  # global rate

    features_list = []
    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}

        disc = rec.get("discipline", "")
        distance = _safe(rec.get("distance"))
        dist_b = _dist_bucket(distance)
        age = _safe(rec.get("age"))
        age_b = _age_bucket(age)
        surface = rec.get("type_piste", "")
        hippo = rec.get("hippodrome_normalise", "")
        sexe = rec.get("sexe", "")

        # 1D conditional probabilities
        if disc:
            c = cond_1d.get(f"d:{disc}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_disc_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_disc_wr"] += 1

        if dist_b:
            c = cond_1d.get(f"db:{dist_b}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_dist_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_dist_wr"] += 1

        if age_b:
            c = cond_1d.get(f"ab:{age_b}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_age_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_age_wr"] += 1

        if surface:
            c = cond_1d.get(f"s:{surface}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_surface_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_surface_wr"] += 1

        if hippo:
            c = cond_1d.get(f"h:{hippo}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_hippo_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_hippo_wr"] += 1

        # 2D conditional probabilities
        if disc and dist_b:
            c = cond_2d.get(f"dd:{disc}|{dist_b}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_disc_dist_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_disc_dist_wr"] += 1

        if disc and age_b:
            c = cond_2d.get(f"da:{disc}|{age_b}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_disc_age_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_disc_age_wr"] += 1

        if dist_b and surface:
            c = cond_2d.get(f"ds:{dist_b}|{surface}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_dist_surface_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_dist_surface_wr"] += 1

        if hippo and dist_b:
            c = cond_2d.get(f"hd:{hippo}|{dist_b}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_hippo_dist_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_hippo_dist_wr"] += 1

        if age_b and sexe:
            c = cond_2d.get(f"as:{age_b}|{sexe}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_age_sex_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_age_sex_wr"] += 1

        # 3D conditional probabilities
        if disc and dist_b and age_b:
            c = cond_3d.get(f"dda:{disc}|{dist_b}|{age_b}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_disc_dist_age_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_disc_dist_age_wr"] += 1

        if disc and dist_b and surface:
            c = cond_3d.get(f"dds:{disc}|{dist_b}|{surface}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_disc_dist_surf_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_disc_dist_surf_wr"] += 1

        if hippo and disc and dist_b:
            c = cond_3d.get(f"hdd:{hippo}|{disc}|{dist_b}")
            if c and c.total >= _MIN_OBS:
                feat["cwp_hippo_disc_dist_wr"] = round(_shrunk(c.wins, c.total, gr), 5)
                fills["cwp_hippo_disc_dist_wr"] += 1

        features_list.append(feat)

    # Write features
    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state AFTER features emitted
    for rec in records:
        is_winner = bool(rec.get("is_gagnant"))
        disc = rec.get("discipline", "")
        distance = _safe(rec.get("distance"))
        dist_b = _dist_bucket(distance)
        age = _safe(rec.get("age"))
        age_b = _age_bucket(age)
        surface = rec.get("type_piste", "")
        hippo = rec.get("hippodrome_normalise", "")
        sexe = rec.get("sexe", "")

        gw += int(is_winner)
        gt += 1

        # 1D updates
        for key in [f"d:{disc}", f"db:{dist_b}", f"ab:{age_b}", f"s:{surface}", f"h:{hippo}"]:
            if key.split(":", 1)[1]:
                c = cond_1d[key]
                c.wins += int(is_winner)
                c.total += 1

        # 2D updates
        pairs = [
            (f"dd:{disc}|{dist_b}", disc and dist_b),
            (f"da:{disc}|{age_b}", disc and age_b),
            (f"ds:{dist_b}|{surface}", dist_b and surface),
            (f"hd:{hippo}|{dist_b}", hippo and dist_b),
            (f"as:{age_b}|{sexe}", age_b and sexe),
        ]
        for key, valid in pairs:
            if valid:
                c = cond_2d[key]
                c.wins += int(is_winner)
                c.total += 1

        # 3D updates
        triples = [
            (f"dda:{disc}|{dist_b}|{age_b}", disc and dist_b and age_b),
            (f"dds:{disc}|{dist_b}|{surface}", disc and dist_b and surface),
            (f"hdd:{hippo}|{disc}|{dist_b}", hippo and disc and dist_b),
        ]
        for key, valid in triples:
            if valid:
                c = cond_3d[key]
                c.wins += int(is_winner)
                c.total += 1

    return gw, gt


if __name__ == "__main__":
    main()
