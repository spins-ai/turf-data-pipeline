#!/usr/bin/env python3
"""
scripts/scraping_compliance.py
===============================
Checks scraping legality and best-practice compliance across all scrapers.

For each scraper file matching [0-9]*_*scraper*.py:
  - Extracts target URL, User-Agent header, rate limiting (smart_pause params)
  - Checks robots.txt compliance mentions
  - Verifies: rate limiting (smart_pause call), proper user-agent, try/except around requests
  - Reports scrapers without rate limiting as HIGH RISK
  - Checks if cache exists (avoids re-scraping)

Output: quality/scraping_compliance_report.md

Memory budget: < 1 GB (reads files as text, no data loading).

Usage:
    python scripts/scraping_compliance.py
    python scripts/scraping_compliance.py --output quality/scraping_compliance_report.md
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config import QUALITY_DIR  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("scraping_compliance")

# ---------------------------------------------------------------------------
# Regex patterns for analysis
# ---------------------------------------------------------------------------

# Scraper filename pattern
SCRAPER_FILE_RE = re.compile(r"^\d+_.*scraper.*\.py$", re.IGNORECASE)

# URL patterns
URL_RE = re.compile(
    r"""(?:https?://[^\s"'<>\\]+)""",
    re.IGNORECASE,
)
BASE_URL_RE = re.compile(
    r"""(?:BASE_URL|base_url|BASE)\s*=\s*["'](https?://[^"']+)["']""",
)

# User-Agent patterns
UA_HEADER_RE = re.compile(
    r"""USER_AGENT|user.agent|User-Agent|"User-Agent"|'User-Agent'""",
    re.IGNORECASE,
)

# Rate limiting patterns
SMART_PAUSE_RE = re.compile(r"smart_pause\s*\(([^)]*)\)")
TIME_SLEEP_RE = re.compile(r"time\.sleep\s*\(([^)]*)\)")
RATE_LIMIT_RE = re.compile(
    r"smart_pause|time\.sleep|rate.limit|throttle|delay",
    re.IGNORECASE,
)

# Error handling patterns
TRY_EXCEPT_RE = re.compile(r"^\s*try\s*:", re.MULTILINE)
EXCEPT_RE = re.compile(
    r"^\s*except\s+.*(?:Exception|Error|requests\.|HTTPError|ConnectionError|Timeout)",
    re.MULTILINE,
)

# Cache patterns
CACHE_RE = re.compile(
    r"cache_file|CACHE_DIR|cache_dir|html_cache|\.cache|load_cache|from_cache",
    re.IGNORECASE,
)

# Robots.txt patterns
ROBOTS_RE = re.compile(r"robots\.txt|robots_txt|robotparser|RobotFileParser", re.IGNORECASE)

# Session / requests patterns
SESSION_RE = re.compile(
    r"create_session|requests\.Session|session\.get|session\.post|fetch_with_retry",
    re.IGNORECASE,
)


@dataclass
class ScraperReport:
    """Compliance report for a single scraper."""
    filename: str
    filepath: str
    target_urls: list[str] = field(default_factory=list)
    has_user_agent: bool = False
    has_rate_limiting: bool = False
    rate_limit_details: list[str] = field(default_factory=list)
    has_try_except: bool = False
    has_cache: bool = False
    has_robots_check: bool = False
    has_session: bool = False
    risk_level: str = "LOW"
    issues: list[str] = field(default_factory=list)
    smart_pause_params: list[str] = field(default_factory=list)
    line_count: int = 0


def analyze_scraper(filepath: Path) -> ScraperReport:
    """Analyze a single scraper file for compliance."""
    report = ScraperReport(
        filename=filepath.name,
        filepath=str(filepath),
    )

    try:
        content = filepath.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        report.issues.append(f"Cannot read file: {exc}")
        report.risk_level = "UNKNOWN"
        return report

    report.line_count = content.count("\n") + 1

    # --- Extract target URLs ---
    base_urls = BASE_URL_RE.findall(content)
    all_urls = URL_RE.findall(content)
    # Filter out common non-target URLs
    target_urls = set()
    for url in base_urls:
        target_urls.add(url.rstrip("/"))
    for url in all_urls:
        clean = url.rstrip("/'\")")
        # Skip common non-scraping URLs
        if any(skip in clean for skip in [
            "github.com", "python.org", "mozilla.org",
            "w3.org", "schema.org", "example.com",
            "googleapis.com/auth", "creativecommons.org",
        ]):
            continue
        # Keep plausible target domains
        if len(clean) > 15:
            target_urls.add(clean)
    # Limit to 5 most relevant
    report.target_urls = sorted(target_urls)[:5]

    # --- User-Agent ---
    report.has_user_agent = bool(UA_HEADER_RE.search(content))
    if not report.has_user_agent:
        report.issues.append("No User-Agent header found")

    # --- Rate limiting ---
    smart_pauses = SMART_PAUSE_RE.findall(content)
    time_sleeps = TIME_SLEEP_RE.findall(content)
    has_other_rate = bool(
        re.search(r"rate.limit|throttle", content, re.IGNORECASE)
    )

    report.has_rate_limiting = bool(smart_pauses or time_sleeps or has_other_rate)
    if smart_pauses:
        report.smart_pause_params = [p.strip() for p in smart_pauses[:5]]
        report.rate_limit_details.append(
            f"smart_pause: {len(smart_pauses)} call(s)"
        )
    if time_sleeps:
        report.rate_limit_details.append(
            f"time.sleep: {len(time_sleeps)} call(s)"
        )
    if not report.has_rate_limiting:
        report.issues.append("NO RATE LIMITING -- HIGH RISK")

    # --- Error handling ---
    has_try = bool(TRY_EXCEPT_RE.search(content))
    has_except = bool(EXCEPT_RE.search(content))
    report.has_try_except = has_try and has_except
    if not report.has_try_except:
        report.issues.append("Missing try/except around requests")

    # --- Cache ---
    report.has_cache = bool(CACHE_RE.search(content))

    # --- Robots.txt ---
    report.has_robots_check = bool(ROBOTS_RE.search(content))

    # --- Session ---
    report.has_session = bool(SESSION_RE.search(content))

    # --- Risk level ---
    if not report.has_rate_limiting:
        report.risk_level = "HIGH"
    elif not report.has_user_agent or not report.has_try_except:
        report.risk_level = "MEDIUM"
    else:
        report.risk_level = "LOW"

    return report


def discover_scrapers(project_root: Path) -> list[Path]:
    """Find all scraper files matching the naming convention."""
    scrapers = []

    # Root-level scrapers
    for f in sorted(project_root.iterdir()):
        if f.is_file() and SCRAPER_FILE_RE.match(f.name):
            scrapers.append(f)

    # Playwright scrapers
    pw_dir = project_root / "scrapers_playwright"
    if pw_dir.is_dir():
        for f in sorted(pw_dir.iterdir()):
            if f.is_file() and SCRAPER_FILE_RE.match(f.name):
                scrapers.append(f)

    return scrapers


def generate_report(reports: list[ScraperReport]) -> str:
    """Generate Markdown compliance report."""
    now = datetime.now().isoformat(timespec="seconds")
    total = len(reports)
    high_risk = [r for r in reports if r.risk_level == "HIGH"]
    medium_risk = [r for r in reports if r.risk_level == "MEDIUM"]
    low_risk = [r for r in reports if r.risk_level == "LOW"]

    with_rate = sum(1 for r in reports if r.has_rate_limiting)
    with_ua = sum(1 for r in reports if r.has_user_agent)
    with_cache = sum(1 for r in reports if r.has_cache)
    with_try = sum(1 for r in reports if r.has_try_except)
    with_robots = sum(1 for r in reports if r.has_robots_check)

    lines: list[str] = []
    w = lines.append

    w(f"# Scraping Compliance Report")
    w(f"")
    w(f"> Generated: {now}")
    w(f"> Generator: `scripts/scraping_compliance.py`")
    w(f"> Total scrapers analyzed: {total}")
    w(f"")

    # --- Executive summary ---
    w(f"## Executive Summary")
    w(f"")
    w(f"| Metric | Count | Percentage |")
    w(f"|--------|-------|------------|")
    w(f"| Rate limiting (smart_pause/sleep) | {with_rate} | {_pct(with_rate, total)} |")
    w(f"| User-Agent header | {with_ua} | {_pct(with_ua, total)} |")
    w(f"| Error handling (try/except) | {with_try} | {_pct(with_try, total)} |")
    w(f"| Response caching | {with_cache} | {_pct(with_cache, total)} |")
    w(f"| Robots.txt check | {with_robots} | {_pct(with_robots, total)} |")
    w(f"")

    # --- Risk distribution ---
    w(f"## Risk Distribution")
    w(f"")
    w(f"| Risk Level | Count | Scrapers |")
    w(f"|------------|-------|----------|")
    w(f"| HIGH | {len(high_risk)} | {', '.join(r.filename for r in high_risk) or 'None'} |")
    w(f"| MEDIUM | {len(medium_risk)} | {', '.join(r.filename for r in medium_risk) or 'None'} |")
    w(f"| LOW | {len(low_risk)} | {', '.join(r.filename for r in low_risk) or 'None'} |")
    w(f"")

    # --- HIGH RISK details ---
    if high_risk:
        w(f"## HIGH RISK Scrapers (No Rate Limiting)")
        w(f"")
        w(f"These scrapers have NO rate limiting and risk being blocked or causing legal issues.")
        w(f"")
        for r in high_risk:
            w(f"### {r.filename}")
            w(f"")
            if r.target_urls:
                w(f"- **Target URLs**: {', '.join(r.target_urls[:3])}")
            w(f"- **User-Agent**: {'Yes' if r.has_user_agent else 'MISSING'}")
            w(f"- **Error handling**: {'Yes' if r.has_try_except else 'MISSING'}")
            w(f"- **Cache**: {'Yes' if r.has_cache else 'No'}")
            w(f"- **Issues**: {'; '.join(r.issues)}")
            w(f"")

    # --- Full scraper table ---
    w(f"## Full Compliance Matrix")
    w(f"")
    w(f"| Scraper | Risk | Rate Limit | User-Agent | Try/Except | Cache | Robots | Lines |")
    w(f"|---------|------|------------|------------|------------|-------|--------|-------|")
    for r in reports:
        risk_badge = r.risk_level
        w(
            f"| {r.filename} "
            f"| {risk_badge} "
            f"| {'Yes' if r.has_rate_limiting else 'NO'} "
            f"| {'Yes' if r.has_user_agent else 'No'} "
            f"| {'Yes' if r.has_try_except else 'No'} "
            f"| {'Yes' if r.has_cache else 'No'} "
            f"| {'Yes' if r.has_robots_check else 'No'} "
            f"| {r.line_count} |"
        )
    w(f"")

    # --- Rate limiting details ---
    w(f"## Rate Limiting Details")
    w(f"")
    w(f"| Scraper | smart_pause Params | Other |")
    w(f"|---------|-------------------|-------|")
    for r in reports:
        params = "; ".join(r.smart_pause_params[:3]) if r.smart_pause_params else "-"
        details = "; ".join(r.rate_limit_details) if r.rate_limit_details else "None"
        w(f"| {r.filename} | `{params}` | {details} |")
    w(f"")

    # --- Recommendations ---
    w(f"## Recommendations")
    w(f"")
    if high_risk:
        w(f"1. **URGENT**: Add `smart_pause()` calls to {len(high_risk)} HIGH RISK scraper(s):")
        for r in high_risk:
            w(f"   - `{r.filename}`")
        w(f"")
    if not all(r.has_user_agent for r in reports):
        missing_ua = [r.filename for r in reports if not r.has_user_agent]
        w(f"2. Add User-Agent rotation to: {', '.join(missing_ua[:10])}")
        w(f"")
    if not all(r.has_cache for r in reports):
        missing_cache = [r.filename for r in reports if not r.has_cache]
        w(f"3. Add response caching to {len(missing_cache)} scraper(s) to avoid redundant requests.")
        w(f"")
    if with_robots == 0:
        w(f"4. Consider adding robots.txt checking (none of the scrapers currently check it).")
        w(f"")
    w(f"---")
    w(f"*End of report.*")

    return "\n".join(lines)


def _pct(count: int, total: int) -> str:
    """Format percentage string."""
    if total == 0:
        return "0%"
    return f"{count * 100 / total:.0f}%"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check scraping compliance across all scrapers"
    )
    parser.add_argument(
        "--output", type=Path, default=QUALITY_DIR / "scraping_compliance_report.md",
        help="Output path for compliance report",
    )
    parser.add_argument(
        "--project-root", type=Path, default=_PROJECT_ROOT,
        help="Project root directory",
    )
    args = parser.parse_args()

    scrapers = discover_scrapers(args.project_root)
    log.info("Discovered %d scraper files.", len(scrapers))

    if not scrapers:
        log.warning("No scraper files found matching pattern [0-9]*_*scraper*.py")
        sys.exit(1)

    reports: list[ScraperReport] = []
    for scraper_path in scrapers:
        log.info("Analyzing: %s", scraper_path.name)
        report = analyze_scraper(scraper_path)
        reports.append(report)

    # Sort: HIGH risk first, then MEDIUM, then LOW
    risk_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "UNKNOWN": 3}
    reports.sort(key=lambda r: (risk_order.get(r.risk_level, 9), r.filename))

    # Generate and write report
    report_text = generate_report(reports)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report_text, encoding="utf-8")
    log.info("Wrote compliance report: %s", args.output)

    # Print summary to stdout
    high = sum(1 for r in reports if r.risk_level == "HIGH")
    medium = sum(1 for r in reports if r.risk_level == "MEDIUM")
    low = sum(1 for r in reports if r.risk_level == "LOW")

    print(f"\n{'='*60}")
    print(f"Scraping Compliance Summary ({len(reports)} scrapers)")
    print(f"{'='*60}")
    print(f"  HIGH risk:   {high}")
    print(f"  MEDIUM risk: {medium}")
    print(f"  LOW risk:    {low}")
    if high > 0:
        print(f"\n  HIGH RISK scrapers (no rate limiting):")
        for r in reports:
            if r.risk_level == "HIGH":
                print(f"    - {r.filename}")
    print(f"\n  Report: {args.output}")


if __name__ == "__main__":
    main()
