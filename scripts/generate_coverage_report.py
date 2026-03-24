"""
Generate a data coverage report from partants_master.jsonl.
Streams line-by-line to keep RAM under 2GB.
"""

import json
import os
from collections import Counter
from datetime import datetime

INPUT = os.path.join(os.path.dirname(__file__), "..", "data_master", "partants_master.jsonl")
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "docs", "COVERAGE_REPORT.md")

def main():
    # Counters
    total_records = 0
    total_fields_sum = 0
    records_per_year = Counter()
    records_per_discipline = Counter()
    records_per_hippo = Counter()
    unique_courses = set()
    unique_chevaux = set()
    unique_jockeys = set()
    min_date = None
    max_date = None

    print(f"Streaming {INPUT} ...")
    with open(INPUT, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            total_records += 1
            total_fields_sum += len(rec)

            # Date
            date_str = rec.get("date_reunion_iso") or rec.get("date") or ""
            if date_str:
                year = date_str[:4]
                records_per_year[year] += 1
                if min_date is None or date_str < min_date:
                    min_date = date_str
                if max_date is None or date_str > max_date:
                    max_date = date_str

            # Discipline
            disc = rec.get("discipline", "UNKNOWN")
            if disc:
                records_per_discipline[disc.upper()] += 1

            # Hippodrome
            hippo = rec.get("hippodrome_normalise", "")
            if hippo:
                records_per_hippo[hippo] += 1

            # Unique sets
            cuid = rec.get("course_uid")
            if cuid:
                unique_courses.add(cuid)
            hid = rec.get("horse_id") or rec.get("nom_cheval")
            if hid:
                unique_chevaux.add(hid)
            jid = rec.get("jockey_driver")
            if jid:
                unique_jockeys.add(jid)

            # Progress every 2M lines
            if (i + 1) % 2_000_000 == 0:
                print(f"  ... {i+1:,} lines processed")

    # Build report
    avg_fields = total_fields_sum / total_records if total_records else 0
    top20_hippo = records_per_hippo.most_common(20)

    report_lines = []
    report_lines.append("# Data Coverage Report - partants_master.jsonl")
    report_lines.append("")
    report_lines.append(f"*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    report_lines.append("")

    report_lines.append("## Summary")
    report_lines.append("")
    report_lines.append(f"| Metric | Value |")
    report_lines.append(f"|---|---|")
    report_lines.append(f"| Total records | {total_records:,} |")
    report_lines.append(f"| Average fields per record | {avg_fields:.1f} |")
    report_lines.append(f"| Date range | {min_date} to {max_date} |")
    report_lines.append(f"| Unique courses | {len(unique_courses):,} |")
    report_lines.append(f"| Unique chevaux (horses) | {len(unique_chevaux):,} |")
    report_lines.append(f"| Unique jockeys/drivers | {len(unique_jockeys):,} |")
    report_lines.append("")

    report_lines.append("## Records per Year")
    report_lines.append("")
    report_lines.append("| Year | Records | % |")
    report_lines.append("|---|---|---|")
    for year in sorted(records_per_year.keys()):
        cnt = records_per_year[year]
        pct = cnt / total_records * 100 if total_records else 0
        report_lines.append(f"| {year} | {cnt:,} | {pct:.1f}% |")
    report_lines.append("")

    report_lines.append("## Records per Discipline")
    report_lines.append("")
    report_lines.append("| Discipline | Records | % |")
    report_lines.append("|---|---|---|")
    for disc, cnt in records_per_discipline.most_common():
        pct = cnt / total_records * 100 if total_records else 0
        report_lines.append(f"| {disc} | {cnt:,} | {pct:.1f}% |")
    report_lines.append("")

    report_lines.append("## Top 20 Hippodromes")
    report_lines.append("")
    report_lines.append("| Rank | Hippodrome | Records | % |")
    report_lines.append("|---|---|---|---|")
    for rank, (hippo, cnt) in enumerate(top20_hippo, 1):
        pct = cnt / total_records * 100 if total_records else 0
        report_lines.append(f"| {rank} | {hippo} | {cnt:,} | {pct:.1f}% |")
    report_lines.append("")

    report_text = "\n".join(report_lines)

    # Print to console
    print("\n" + report_text)

    # Save to file
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(report_text + "\n")
    print(f"\nReport saved to {OUTPUT}")

if __name__ == "__main__":
    main()
