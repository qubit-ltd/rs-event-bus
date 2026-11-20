#!/usr/bin/env python3
"""Check consolidated performance evidence links and Cargo package distribution."""

import argparse
import re
import sys
from pathlib import Path


FIXED_COMMIT = "7299e7d19b1889655e28625a6ca3e47af926e6d4"
RAW_URL = re.compile(
    r"https://raw\.githubusercontent\.com/qubit-ltd/rs-event-bus/([0-9a-f]{40})/([^\s)]+)"
)
TREE_URL = re.compile(
    r"https://github\.com/qubit-ltd/rs-event-bus/tree/([0-9a-f]{40})/([^\s)]+)"
)
ARCHIVES = (
    (
        "doc/ordering_performance_artifacts/README.md",
        "doc/ordering_performance_artifacts",
        "doc/ordering_performance_artifacts/paired_samples.txt",
    ),
    (
        "doc/capacity_performance_artifacts/20261104-review/README.md",
        "doc/capacity_performance_artifacts/20261104-review",
        "doc/capacity_performance_artifacts/20261104-review/manifest.json",
    ),
)
PERFORMANCE_DOC = "doc/performance.md"
SHUTDOWN_DOC = "doc/local-shutdown-measurements-20261109.md"
PUBLIC_EVIDENCE = (
    "doc/cancellation-causal-probe-20261109.csv",
    "doc/cancellation-causal-probe-20261109.raw.txt",
)


def fail(path: str, message: str) -> None:
    print(f"{path}: {message}", file=sys.stderr)


def read_text(path: Path, label: str) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        fail(label, f"cannot read file: {error}")
        return None


def check(package_list: Path) -> bool:
    root = Path(__file__).resolve().parents[1]
    listed = read_text(package_list, str(package_list))
    if listed is None:
        return False
    packaged = {
        line.strip().replace("\\", "/").removeprefix("./")
        for line in listed.splitlines()
        if line.strip()
    }

    ok = True
    if PERFORMANCE_DOC not in packaged:
        fail(PERFORMANCE_DOC, "consolidated performance document must be included in the Cargo package list")
        ok = False
    if SHUTDOWN_DOC not in packaged:
        fail(SHUTDOWN_DOC, "shutdown measurement document must be included in the Cargo package list")
        ok = False
    performance = read_text(root / PERFORMANCE_DOC, PERFORMANCE_DOC)
    if performance is None:
        ok = False
    else:
        for _, _, evidence in ARCHIVES:
            found = [
                sha
                for sha, target in RAW_URL.findall(performance)
                if target == evidence
            ]
            if found != [FIXED_COMMIT]:
                fail(PERFORMANCE_DOC, f"must link exactly once to fixed raw evidence file {evidence} at {FIXED_COMMIT}")
                ok = False

    shutdown = read_text(root / SHUTDOWN_DOC, SHUTDOWN_DOC)
    if shutdown is None:
        ok = False
    for source_doc, content in ((PERFORMANCE_DOC, performance), (SHUTDOWN_DOC, shutdown)):
        if content is None:
            continue
        for evidence in PUBLIC_EVIDENCE:
            found = [
                sha
                for sha, target in RAW_URL.findall(content)
                if target == evidence
            ]
            if found != [FIXED_COMMIT]:
                fail(source_doc, f"must link exactly once to fixed raw evidence file {evidence} at {FIXED_COMMIT}")
                ok = False
            if evidence in packaged:
                fail(evidence, "public raw evidence must remain outside the Cargo package")
                ok = False

    for readme, archive, evidence in ARCHIVES:
        if readme not in packaged:
            fail(readme, "evidence navigation README must be included in the Cargo package list")
            ok = False
        content = read_text(root / readme, readme)
        if content is None:
            ok = False
            continue

        links = [(sha, target) for sha, target in TREE_URL.findall(content) if target == archive]
        if len(links) != 1 or links[0][0] != FIXED_COMMIT:
            fail(readme, f"must contain one fixed snapshot link for {archive} at {FIXED_COMMIT}")
            ok = False
        if "distribution and complete evidence" not in content.lower():
            fail(readme, "must explain distribution and how to consult complete evidence online")
            ok = False
        if "raw evidence is not included in the current cargo package" not in content.lower():
            fail(readme, "must state that raw evidence is outside the Cargo package")
            ok = False
        if evidence in packaged:
            fail(evidence, "raw evidence must remain outside the Cargo package and use its fixed raw file URL")
            ok = False

    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-list", required=True, type=Path)
    args = parser.parse_args()
    return 0 if check(args.package_list) else 1


if __name__ == "__main__":
    raise SystemExit(main())
