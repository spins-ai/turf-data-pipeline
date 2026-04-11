#!/usr/bin/env python3
"""Equipment impact features: oeilleres, deferre, jument_pleine change effects.
Cross equipment with performance history and detect equipment change signals."""
from __future__ import annotations
import gc, json, math, sys, time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.logging_setup import setup_logging

INPUT_PARTANTS = Path("D:/turf-data-pipeline/03_DONNEES_MASTER/partants_master.jsonl")
OUTPUT_DIR = Path("D:/turf-data-pipeline/02_DONNEES_BRUTES/builder_outputs/equipement_impact")
_LOG_EVERY = 500_000


def _safe(val):
    try:
        v = float(val)
        return v if math.isfinite(v) and v > 0 else None
    except (TypeError, ValueError):
        return None


class _HorseEquipState:
    __slots__ = ("prev_oeilleres", "prev_deferre", "total",
                 "wins_with_oeil", "total_with_oeil",
                 "wins_without_oeil", "total_without_oeil")

    def __init__(self):
        self.prev_oeilleres = None
        self.prev_deferre = None
        self.total = 0
        self.wins_with_oeil = 0
        self.total_with_oeil = 0
        self.wins_without_oeil = 0
        self.total_without_oeil = 0


# Global stats for equipment
_oeil_stats = defaultdict(lambda: {"wins": 0, "total": 0})


def main():
    logger = setup_logging("equipement_impact_builder")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "equipement_impact_features.jsonl"
    tmp_path = out_path.with_suffix(".tmp")

    horse_states: dict[str, _HorseEquipState] = {}

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
                    _process_course(course_records, fout, horse_states, fills)
                    written += len(course_records)
                    course_records = []

                current_course = cuid
                course_records.append(rec)

                if lineno % _LOG_EVERY == 0:
                    logger.info(f"Read {lineno:,}, written {written:,}")
                    gc.collect()

        if course_records:
            _process_course(course_records, fout, horse_states, fills)
            written += len(course_records)

    tmp_path.rename(out_path)
    elapsed = time.perf_counter() - t0
    logger.info(f"Done: {written:,} rows in {elapsed:.0f}s")
    total = written or 1
    for k, v in sorted(fills.items()):
        logger.info(f"  fill {k}: {v / total * 100:.1f}%")


def _process_course(records, fout, horse_states, fills):
    features_list = []

    for rec in records:
        feat = {"partant_uid": rec.get("partant_uid", "")}
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        oeilleres = (rec.get("oeilleres") or "").strip().lower()
        deferre = (rec.get("deferre") or "").strip()
        jument = rec.get("jument_pleine")

        # 1. Oeilleres encoding
        has_oeil = oeilleres and oeilleres != "sans" and oeilleres != ""
        if oeilleres:
            feat["ei_has_oeilleres"] = 1 if has_oeil else 0
            fills["ei_has_oeilleres"] += 1

            # Type d'oeillères
            if "austral" in oeilleres:
                feat["ei_oeil_type"] = 1
            elif "avec" in oeilleres or "oui" in oeilleres:
                feat["ei_oeil_type"] = 2
            else:
                feat["ei_oeil_type"] = 0
            fills["ei_oeil_type"] += 1

        # 2. Déferré encoding
        if deferre:
            feat["ei_has_deferre"] = 1 if deferre and deferre != "0" else 0
            fills["ei_has_deferre"] += 1

            # Decode deferre pattern (e.g., "F4" = front 4, "PA" = postérieur anti)
            deferre_score = 0
            if "4" in deferre or "P" in deferre.upper():
                deferre_score = 2  # Heavily deferre
            elif deferre and deferre != "0":
                deferre_score = 1
            feat["ei_deferre_score"] = deferre_score
            fills["ei_deferre_score"] += 1

        # 3. Jument pleine
        if jument is not None:
            feat["ei_jument_pleine"] = 1 if jument else 0
            fills["ei_jument_pleine"] += 1

        # Horse-specific equipment change detection
        if hid:
            st = horse_states.get(hid)
            if st and st.total >= 2:
                # 4. Equipment change
                if st.prev_oeilleres is not None:
                    oeil_changed = (has_oeil != st.prev_oeilleres)
                    feat["ei_oeil_changed"] = 1 if oeil_changed else 0
                    fills["ei_oeil_changed"] += 1

                    # First time with oeilleres (positive signal)
                    if has_oeil and not st.prev_oeilleres:
                        feat["ei_first_oeilleres"] = 1
                        fills["ei_first_oeilleres"] += 1

                if st.prev_deferre is not None:
                    def_changed = (deferre != st.prev_deferre)
                    feat["ei_deferre_changed"] = 1 if def_changed else 0
                    fills["ei_deferre_changed"] += 1

                # 5. Performance with vs without oeilleres
                if st.total_with_oeil >= 3 and st.total_without_oeil >= 3:
                    wr_with = st.wins_with_oeil / st.total_with_oeil
                    wr_without = st.wins_without_oeil / st.total_without_oeil
                    feat["ei_oeil_wr_diff"] = round(wr_with - wr_without, 5)
                    fills["ei_oeil_wr_diff"] += 1

            # 6. Global oeilleres win rate
            oeil_key = "with" if has_oeil else "without"
            gs = _oeil_stats[oeil_key]
            if gs["total"] >= 100:
                feat["ei_global_oeil_wr"] = round(gs["wins"] / gs["total"], 5)
                fills["ei_global_oeil_wr"] += 1

        features_list.append(feat)

    for feat in features_list:
        fout.write(json.dumps(feat, ensure_ascii=False) + "\n")

    # Update state
    for rec in records:
        hid = rec.get("horse_id") or rec.get("nom_cheval") or ""
        oeilleres = (rec.get("oeilleres") or "").strip().lower()
        deferre = (rec.get("deferre") or "").strip()
        is_winner = bool(rec.get("is_gagnant"))
        has_oeil = oeilleres and oeilleres != "sans" and oeilleres != ""

        if hid:
            if hid not in horse_states:
                horse_states[hid] = _HorseEquipState()
            st = horse_states[hid]
            st.prev_oeilleres = has_oeil
            st.prev_deferre = deferre
            st.total += 1
            if has_oeil:
                st.wins_with_oeil += int(is_winner)
                st.total_with_oeil += 1
            else:
                st.wins_without_oeil += int(is_winner)
                st.total_without_oeil += 1

        oeil_key = "with" if has_oeil else "without"
        _oeil_stats[oeil_key]["wins"] += int(is_winner)
        _oeil_stats[oeil_key]["total"] += 1


if __name__ == "__main__":
    main()
