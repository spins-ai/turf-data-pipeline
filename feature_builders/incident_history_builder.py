#!/usr/bin/env python3
"""Incident history features: track disqualifications, falls, retrogrades,
non-partant events, and DNF patterns per horse/jockey."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/incident_history")
_LOG_EVERY = 500_000


class _IncidentState:
    __slots__ = ("disq", "tombe", "retro", "arrete", "np", "total",
                 "recent_incidents")

    def __init__(self):
        self.disq = 0
        self.tombe = 0
        self.retro = 0
        self.arrete = 0
        self.np = 0  # non-partant
        self.total = 0
        self.recent_incidents = deque(maxlen=10)  # 1=incident, 0=clean


def _detect_incident(rec):
    """Return incident type or None."""
    # Check multiple fields
    statut = (rec.get("statut") or rec.get("statut_partant") or "").lower()
    comment = (rec.get("commentaire_course") or "").lower()

    if any(x in statut for x in ("disq", "dq")):
        return "disq"
    if any(x in statut for x in ("tomb", "chut")):
        return "tombe"
    if "retro" in statut:
        return "retro"
    if any(x in statut for x in ("arret", "arr")):
        return "arrete"
    if any(x in statut for x in ("np", "non-part", "non part")):
        return "np"

    # Check from musique last char
    musique = rec.get("musique") or ""
    if musique:
        first = musique[0].upper()
        if first == "D": return "disq"
        if first == "T": return "tombe"
        if first == "R": return "retro"
        if first == "A": return "arrete"

    # Check comment
    if "disqualif" in comment: return "disq"
    if "tomb" in comment: return "tombe"

    return None


def main():
    logger = setup_logging("incident_history_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "incident_history_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _IncidentState] = {}
    jockey_states: dict[str, _IncidentState] = {}

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
                    _process_course(course_records, fout, horse_states,
                                    jockey_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states,
                            jockey_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, jockey_states, fills):
    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jid = (rec.get("jockey") or rec.get("driver") or "").strip()

        # Horse incident history
        if hid:
            hst = horse_states.get(hid)
            if hst and hst.total >= 3:
                total_inc = hst.disq + hst.tombe + hst.retro + hst.arrete
                feat["ih_horse_incident_rate"] = round(total_inc / hst.total, 5)
                feat["ih_horse_disq_count"] = hst.disq
                feat["ih_horse_fall_count"] = hst.tombe
                fills["ih_horse_incident_rate"] += 1
                fills["ih_horse_disq_count"] += 1
                fills["ih_horse_fall_count"] += 1

                # Recent incident streak
                recent = list(hst.recent_incidents)
                if len(recent) >= 3:
                    feat["ih_horse_recent_inc_rate"] = round(sum(recent[-5:]) / min(5, len(recent)), 4)
                    fills["ih_horse_recent_inc_rate"] += 1

                    # Consecutive clean races
                    clean = 0
                    for v in reversed(recent):
                        if v == 0:
                            clean += 1
                        else:
                            break
                    feat["ih_horse_clean_streak"] = clean
                    fills["ih_horse_clean_streak"] += 1

        # Jockey incident history
        if jid:
            jst = jockey_states.get(jid)
            if jst and jst.total >= 10:
                total_inc = jst.disq + jst.tombe + jst.retro
                feat["ih_jockey_incident_rate"] = round(total_inc / jst.total, 5)
                feat["ih_jockey_fall_rate"] = round(jst.tombe / jst.total, 5)
                fills["ih_jockey_incident_rate"] += 1
                fills["ih_jockey_fall_rate"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        jid = (rec.get("jockey") or rec.get("driver") or "").strip()
        incident = _detect_incident(rec)
        has_inc = 1 if incident else 0

        if hid:
            if hid not in horse_states:
                horse_states[hid] = _IncidentState()
            hst = horse_states[hid]
            if incident == "disq": hst.disq += 1
            elif incident == "tombe": hst.tombe += 1
            elif incident == "retro": hst.retro += 1
            elif incident == "arrete": hst.arrete += 1
            elif incident == "np": hst.np += 1
            hst.total += 1
            hst.recent_incidents.append(has_inc)

        if jid:
            if jid not in jockey_states:
                jockey_states[jid] = _IncidentState()
            jst = jockey_states[jid]
            if incident == "disq": jst.disq += 1
            elif incident == "tombe": jst.tombe += 1
            elif incident == "retro": jst.retro += 1
            elif incident == "arrete": jst.arrete += 1
            jst.total += 1
            jst.recent_incidents.append(has_inc)


if __name__ == "__main__":
    main()
