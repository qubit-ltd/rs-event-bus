#!/usr/bin/env python3
import argparse
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL = "examples/local_delivery.rs"
LIB = "src/lib.rs"
ASYNC = "tests/fixtures/documentation_consumer/src/bin/async_local.rs"
SPEC = "tests/fixtures/documentation_consumer/src/provider_spec.rs"
PROVIDER_CONTRACT = "tests/fixtures/documentation_consumer/src/provider_contract.rs"
CODEC = "tests/fixtures/documentation_consumer/src/order_created_codec.rs"
RECEIPT = "tests/fixtures/documentation_consumer/src/receipt_safety.rs"
SHUTDOWN = "tests/fixtures/documentation_consumer/src/bounded_shutdown.rs"
CAPACITY = "tests/fixtures/documentation_consumer/src/local_capacity.rs"
REPUBLISH = "tests/fixtures/documentation_consumer/src/republish_action.rs"
RETRY = "tests/fixtures/documentation_consumer/src/retry_policy.rs"
README_ORDERS = "tests/fixtures/documentation_consumer/src/readme_orders.rs"
MONITOR = "tests/fixtures/documentation_consumer/src/run_state_monitor.rs"
DEAD_LETTER = "tests/fixtures/documentation_consumer/src/dead_letter_codec.rs"
REQUIRED = {
    LIB: {LOCAL},
    "README.md": {LOCAL, README_ORDERS},
    "README.zh_CN.md": {LOCAL, README_ORDERS},
    "doc/user_guide.md": {LOCAL, ASYNC, CODEC, DEAD_LETTER, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY, MONITOR},
    "doc/user_guide.zh_CN.md": {LOCAL, ASYNC, CODEC, DEAD_LETTER, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY, MONITOR},
    "doc/design.md": {SPEC, PROVIDER_CONTRACT, DEAD_LETTER},
    "doc/design.zh_CN.md": {SPEC, PROVIDER_CONTRACT, DEAD_LETTER},
}
PATTERN = re.compile(
    r"<!-- event-bus-source: (?P<path>[^\n]+?) -->\s*"
    r"(?P<fence>[\x60~]{3})rust\s*\n(?P<code>.*?)\n(?P=fence)",
    re.DOTALL,
)
FILE_HEADER = re.compile(
    r"\A// =+\n"
    r"//    Copyright \(c\) [^\n]+\n"
    r"//\n"
    r"//    SPDX-License-Identifier: Apache-2\.0\n"
    r"//\n"
    r"//    Licensed under the Apache License, Version 2\.0\.\n"
    r"// =+\n"
)
EXAMPLE_DOCUMENTS = {LIB, "README.md", "README.zh_CN.md", "doc/user_guide.md", "doc/user_guide.zh_CN.md"}


def read_document_text(path):
    text = path.read_text(encoding="utf-8")
    if path.suffix != ".rs":
        return text
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if line.startswith("//!")), None)
    if start is None:
        return ""
    result = []
    for line in lines[start:]:
        if not line.startswith("//!"):
            break
        result.append(line[3:].removeprefix(" "))
    return "\n".join(result)


DOCUMENT_LINKS = {
    "User guide": "doc/user_guide.md",
    "用户指南": "doc/user_guide.zh_CN.md",
    "Design": "doc/design.md",
    "设计文档": "doc/design.zh_CN.md",
}


def check_snapshot_content(snapshot_sha, fetch_snapshot):
    errors = []
    try:
        if fetch_snapshot:
            present = subprocess.run(
                ["git", "cat-file", "-e", f"{snapshot_sha}^{{commit}}"],
                cwd=ROOT,
                capture_output=True,
                check=False,
            )
            if present.returncode != 0:
                fetched = subprocess.run(
                    ["git", "fetch", "--no-tags", "--depth=1", "origin", snapshot_sha],
                    cwd=ROOT,
                    capture_output=True,
                    check=False,
                )
                if fetched.returncode != 0:
                    detail = fetched.stderr.decode("utf-8", errors="replace").strip()
                    errors.append(f"snapshot {snapshot_sha} fetch failed: {detail or 'git fetch failed'}")
                    return errors
        for document_path in DOCUMENT_LINKS.values():
            result = subprocess.run(
                ["git", "show", f"{snapshot_sha}:{document_path}"],
                cwd=ROOT,
                capture_output=True,
                check=False,
            )
            if result.returncode != 0:
                detail = result.stderr.decode("utf-8", errors="replace").strip()
                errors.append(
                    f"{document_path}: snapshot {snapshot_sha} unavailable"
                    f"{': ' + detail if detail else ''}"
                )
            elif result.stdout != (ROOT / document_path).read_bytes():
                errors.append(f"{document_path}: snapshot {snapshot_sha} content drift")
    except OSError as error:
        errors.append(f"snapshot {snapshot_sha}: unable to run git: {error}")
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-snapshot-content", action="store_true")
    parser.add_argument("--fetch-snapshot", action="store_true")
    args = parser.parse_args()
    if args.fetch_snapshot and not args.check_snapshot_content:
        parser.error("--fetch-snapshot requires --check-snapshot-content")

    errors = []
    document_text = read_document_text(ROOT / LIB)
    document_shas = {}
    for label, path in DOCUMENT_LINKS.items():
        label_pattern = re.escape(label)
        matches = re.findall(rf"\[{label_pattern}\]\(([^)]*)\)", document_text)
        if len(matches) != 1:
            errors.append(f"{LIB}: [{label}] must appear exactly once (found {len(matches)})")
            continue
        url = matches[0]
        expected = re.fullmatch(
            rf"https://github\.com/qubit-ltd/rs-event-bus/blob/([0-9a-f]{{40}})/{re.escape(path)}",
            url,
        )
        if expected is None:
            errors.append(
                f"{LIB}: [{label}] has invalid snapshot link {url!r}; expected "
                f"https://github.com/qubit-ltd/rs-event-bus/blob/<40-lowercase-hex-sha>/{path}"
            )
        else:
            document_shas[label] = expected.group(1)
    if len(set(document_shas.values())) > 1:
        for label, sha in document_shas.items():
            errors.append(f"{LIB}: [{label}] uses snapshot SHA {sha}, but all four links must use the same SHA")

    for document, required in REQUIRED.items():
        text = read_document_text(ROOT / document)
        found = set()
        for match in PATTERN.finditer(text):
            name = match.group("path")
            source = (ROOT / name).resolve()
            if not source.is_relative_to(ROOT) or not source.is_file():
                errors.append(f"{document}: invalid source path {name}")
                continue
            found.add(name)
            source_code = source.read_text(encoding="utf-8")
            if document in EXAMPLE_DOCUMENTS:
                source_code = FILE_HEADER.sub("", source_code)
            if match.group("code").strip() != source_code.strip():
                errors.append(f"{document}: example drift from {name}")
        for missing in sorted(required - found):
            errors.append(f"{document}: missing checked example {missing}")

    if args.check_snapshot_content and not errors and len(set(document_shas.values())) == 1:
        errors.extend(check_snapshot_content(next(iter(document_shas.values())), args.fetch_snapshot))
    if errors:
        raise SystemExit("\n".join(errors))
    print("documentation examples match their compiled sources and snapshot links")


if __name__ == "__main__":
    main()
