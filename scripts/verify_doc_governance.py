#!/usr/bin/env python3
"""Read-only consistency checks for Boaz Health governing documents."""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
import csv
import re
import sys
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_DOCS = [
    ROOT / "AGENTS.md",
    ROOT / "CLAUDE.md",
    ROOT / "agent.md",
    ROOT / "design.md",
    ROOT / "memory.md",
    ROOT / "README.md",
    ROOT / "Server/README.md",
    ROOT / "docs/TEST_CASES.md",
]
HTML_DOCS = [ROOT / "docs/prd.html", ROOT / "prd.html"]
EVIDENCE_DIR = ROOT / "docs/evidence/2026-09-19"
EVIDENCE_README = EVIDENCE_DIR / "README.md"
CASE_RESULTS = EVIDENCE_DIR / "case-results.csv"
SOURCE_MANIFEST = EVIDENCE_DIR / "source-manifest.md"


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in {"a", "img", "script", "link"}:
            return
        key = "href" if tag in {"a", "link"} else "src"
        for name, value in attrs:
            if name == key and value:
                self.links.append(value)


def local_target(source: Path, raw: str) -> Path | None:
    parsed = urlsplit(raw)
    if parsed.scheme or parsed.netloc or not parsed.path:
        return None
    if raw.startswith(("#", "mailto:", "javascript:")):
        return None
    return (source.parent / unquote(parsed.path)).resolve()


def main() -> int:
    failures: list[str] = []

    for path in [
        *MARKDOWN_DOCS,
        *HTML_DOCS,
        EVIDENCE_README,
        CASE_RESULTS,
        SOURCE_MANIFEST,
    ]:
        if not path.is_file():
            failures.append(f"missing governed document: {path.relative_to(ROOT)}")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    texts = {path: path.read_text(encoding="utf-8") for path in [*MARKDOWN_DOCS, *HTML_DOCS]}
    agents = texts[ROOT / "AGENTS.md"]
    claude = texts[ROOT / "CLAUDE.md"]
    compatibility_agent = texts[ROOT / "agent.md"]
    canonical_prd = texts[ROOT / "docs/prd.html"]
    compatibility_prd = texts[ROOT / "prd.html"]
    cases = texts[ROOT / "docs/TEST_CASES.md"]
    governed = "\n".join(texts[path] for path in MARKDOWN_DOCS + HTML_DOCS)

    if "repository-level engineering contract" not in agents:
        failures.append("AGENTS.md does not identify itself as the repository contract")
    if len(claude.splitlines()) > 40 or "canonical repository engineering contract is [AGENTS.md]" not in claude:
        failures.append("CLAUDE.md is not a thin entry to AGENTS.md")
    if len(compatibility_agent.splitlines()) > 8 or "canonical repository engineering contract is [AGENTS.md]" not in compatibility_agent:
        failures.append("agent.md is not a thin compatibility entry")
    if "url=docs/prd.html" not in compatibility_prd or "唯一" not in compatibility_prd:
        failures.append("root prd.html is not a compatibility entry to docs/prd.html")
    if "comment-rail" in compatibility_prd or compatibility_prd == canonical_prd:
        failures.append("root prd.html duplicates the canonical PRD")

    forbidden = {
        "127.0.0.1:8080": "stale receiver port",
        "6-digit": "stale pairing-code shape",
        "6 digit": "stale pairing-code shape",
        "待创建的验收报告": "stale missing-evidence claim",
    }
    for token, description in forbidden.items():
        if token in governed:
            failures.append(f"{description}: found {token!r}")

    required = {
        "127.0.0.1:8787": "receiver port 8787",
        "64-hex": "64-hex pairing secret",
        "dm-crypt": "implemented encryption verifier boundary",
        "provider-managed": "provider-managed encryption boundary",
        "label hashing": "metric label hashing boundary",
        "docs/evidence/2026-09-18": "dated evidence pointer",
        "AI-NATIVE GATE: INTERCEPT": "release gate",
    }
    for token, description in required.items():
        if token not in governed:
            failures.append(f"missing {description}: {token!r}")

    specification_ids = re.findall(
        r"^\| ((?:BLD|HK|DATA|SLP|SYNC|API|OPS|ERA|BAK|UX|PERF)-\d{2}) / P[012] \|",
        cases,
        flags=re.MULTILINE,
    )
    status_rows = re.findall(
        r"^\| ((?:BLD|HK|DATA|SLP|SYNC|API|OPS|ERA|BAK|UX|PERF)-\d{2}) \| (PASS|FAIL|BLOCKED|NOT_RUN) \|",
        cases,
        flags=re.MULTILINE,
    )
    status_ids = [case_id for case_id, _ in status_rows]
    if len(specification_ids) != 77 or len(set(specification_ids)) != 77:
        failures.append(f"expected 77 unique acceptance specifications, found {len(specification_ids)} rows/{len(set(specification_ids))} unique")
    if len(status_ids) != 77 or set(status_ids) != set(specification_ids):
        failures.append(f"77-case status register mismatch: {len(status_ids)} rows, {len(set(status_ids))} unique")

    with CASE_RESULTS.open(newline="", encoding="utf-8") as handle:
        evidence_rows = list(csv.DictReader(handle))
    evidence_ids = [row.get("case_id", "") for row in evidence_rows]
    evidence_status = {row.get("case_id", ""): row.get("status", "") for row in evidence_rows}
    register_status = dict(status_rows)
    if len(evidence_rows) != 77 or len(set(evidence_ids)) != 77:
        failures.append(
            f"expected 77 unique dated evidence rows, found {len(evidence_rows)} rows/{len(set(evidence_ids))} unique"
        )
    if set(evidence_ids) != set(specification_ids):
        failures.append("dated case evidence IDs do not match the acceptance specification")
    if evidence_status != register_status:
        failures.append("dated case evidence statuses do not match the TEST_CASES register")
    evidence_counts = {
        status: sum(row.get("status") == status for row in evidence_rows)
        for status in ("PASS", "FAIL", "BLOCKED", "NOT_RUN")
    }
    expected_counts = {"PASS": 20, "FAIL": 0, "BLOCKED": 42, "NOT_RUN": 15}
    if evidence_counts != expected_counts:
        failures.append(f"unexpected dated evidence counts: {evidence_counts!r}")

    evidence_readme = EVIDENCE_README.read_text(encoding="utf-8")
    for token in [
        "77 executed, 77 passed, 0 failed, 0 skipped, 0 runtime warnings",
        "AI-NATIVE GATE: INTERCEPT",
        "The PRD was not visually rendered",
        "ERA-04",
        "BAK-03",
        "BAK-05",
    ]:
        if token not in evidence_readme:
            failures.append(f"dated evidence README is missing required boundary: {token!r}")

    xctest_count = 0
    for source in (ROOT / "Tests").glob("*.swift"):
        xctest_count += len(re.findall(r"^\s*func test", source.read_text(encoding="utf-8"), flags=re.MULTILINE))
    if xctest_count != 78:
        failures.append(f"expected 78 discovered XCTest methods, found {xctest_count}")

    package_resolution = ROOT / "boazapp.xcodeproj/project.xcworkspace/xcshareddata/swiftpm/Package.resolved"
    if not package_resolution.is_file():
        failures.append("missing reviewed Swift package resolution")
    simulator_script = ROOT / "scripts/test_ios_simulator.sh"
    if not simulator_script.is_file():
        failures.append("missing governed iOS simulator runner")
    else:
        runner = simulator_script.read_text(encoding="utf-8")
        for token in ["Debug", "parallel-testing-enabled", "xcresult"]:
            if token not in runner:
                failures.append(f"simulator runner is missing required contract token: {token}")
    generator = (ROOT / "scripts/generate_xcode_project.py").read_text(encoding="utf-8")
    for token in ["--check", "SWIFT_ACTIVE_COMPILATION_CONDITIONS", "DEBUG"]:
        if token not in generator:
            failures.append(f"project generator is missing required contract token: {token}")

    deployment_env = (ROOT / "Server/deploy/boaz-health.env.example").read_text(encoding="utf-8")
    for token in [
        "BOAZ_HEALTH_DATA_ROOT",
        "BOAZ_HEALTH_DB",
        "BOAZ_HEALTH_CONTROL_DB",
        "BOAZ_HEALTH_CONTROL_MIRROR_DIR",
        "BOAZ_HEALTH_BACKUP_DIR",
        "BOAZ_HEALTH_CONTROL_VOLUME_ENCRYPTED",
    ]:
        if token not in deployment_env:
            failures.append(f"deployment environment template is missing: {token}")

    for source in MARKDOWN_DOCS:
        for raw in re.findall(r"\[[^\]]+\]\(([^)]+)\)", texts[source]):
            target = local_target(source, raw)
            if target is not None and not target.exists():
                failures.append(f"broken Markdown link in {source.relative_to(ROOT)}: {raw}")

    for source in HTML_DOCS:
        parser = LinkParser()
        try:
            parser.feed(texts[source])
            parser.close()
        except Exception as error:  # pragma: no cover - defensive parser boundary
            failures.append(f"HTML parse failed for {source.relative_to(ROOT)}: {error}")
            continue
        for raw in parser.links:
            target = local_target(source, raw)
            if target is not None and not target.exists():
                failures.append(f"broken HTML link in {source.relative_to(ROOT)}: {raw}")

    expected_source_contracts = [ROOT / "Server/src/database.rs", ROOT / "Server/src/control.rs"]
    for path in expected_source_contracts:
        if not path.is_file():
            failures.append(f"governed source contract is absent: {path.relative_to(ROOT)}")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    print("PASS: documentation governance is internally consistent")
    print(f"PASS: {len(specification_ids)} acceptance cases, {len(status_rows)} status rows and {xctest_count} discovered XCTest methods")
    print(f"PASS: dated evidence dispositions {evidence_counts}")
    print("PASS: AGENTS.md is canonical; CLAUDE.md, agent.md and root prd.html are thin entries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
