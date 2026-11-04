#!/usr/bin/env python3
"""Check the performance evidence links and Cargo package distribution."""

import argparse
import re
import sys
from pathlib import Path


SNAPSHOT_URL = re.compile(
    r"https://github\.com/qubit-ltd/rs-event-bus/tree/([0-9a-f]{40})/([^\s)]+)"
)
ARCHIVES = (
    (
        "doc/ordering_performance_artifacts/README.md",
        "doc/ordering_performance_artifacts",
        "doc/ordering_performance_artifacts/paired_samples.txt",
        True,
    ),
    (
        "doc/capacity_performance_artifacts/20261104-review/README.md",
        "doc/capacity_performance_artifacts/20261104-review",
        "doc/capacity_performance_artifacts/20261104-review/manifest.json",
        False,
    ),
)
PACKAGED_CAPACITY_DOC = "doc/sync_capacity.md"
EXPECTED_SNAPSHOT = "e8a1ed724954756d48c3d5216cbd13ac39a0cb81"


def fail(path: str, message: str) -> None:
    print(f"{path}: {message}", file=sys.stderr)


def check(package_list: Path) -> bool:
    root = Path(__file__).resolve().parents[1]
    try:
        lines = package_list.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        fail(str(package_list), f"cannot read package list: {error}")
        return False

    packaged = {line.strip().replace("\\", "/").removeprefix("./") for line in lines if line.strip()}
    ok = True
    snapshots = []
    if PACKAGED_CAPACITY_DOC not in packaged:
        fail(PACKAGED_CAPACITY_DOC, "capacity performance document must be included in the Cargo package list")
        ok = False
    else:
        try:
            content = (root / PACKAGED_CAPACITY_DOC).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            fail(PACKAGED_CAPACITY_DOC, f"cannot read packaged performance document: {error}")
            ok = False
        else:
            links = SNAPSHOT_URL.findall(content)
            if not any(sha == EXPECTED_SNAPSHOT and target == ARCHIVES[1][1] for sha, target in links):
                fail(PACKAGED_CAPACITY_DOC, "must link directly to the fixed complete capacity evidence snapshot")
                ok = False

    for readme, archive, representative, must_be_packaged in ARCHIVES:
        path = root / readme
        if must_be_packaged and readme not in packaged:
            fail(readme, "README must be included in the Cargo package list")
            ok = False
        try:
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            fail(readme, f"cannot read archive entry: {error}")
            ok = False
            continue

        links = SNAPSHOT_URL.findall(content)
        matching = [(sha, target) for sha, target in links if target == archive]
        if len(matching) != 1:
            fail(readme, f"must contain exactly one fixed snapshot link to {archive}")
            ok = False
        else:
            snapshots.append((readme, matching[0][0]))
            if matching[0][0] != EXPECTED_SNAPSHOT:
                fail(readme, f"snapshot SHA must be the reviewed fixed commit {EXPECTED_SNAPSHOT}")
                ok = False
        if "distribution and complete evidence" not in content.lower():
            fail(readme, "must explain distribution and how to consult complete evidence online")
            ok = False
        if representative in packaged:
            fail(representative, "representative non-Markdown evidence must remain outside the Cargo package")
            ok = False

    if len(snapshots) == len(ARCHIVES) and snapshots[0][1] != snapshots[1][1]:
        fail(ARCHIVES[1][0], f"snapshot SHA must match {ARCHIVES[0][0]} ({snapshots[0][1]})")
        ok = False
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-list", required=True, type=Path)
    args = parser.parse_args()
    return 0 if check(args.package_list) else 1


if __name__ == "__main__":
    raise SystemExit(main())
