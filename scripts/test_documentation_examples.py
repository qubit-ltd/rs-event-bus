#!/usr/bin/env python3
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts/check_documentation_examples.py"
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
WEIGHTED = "tests/fixtures/documentation_consumer/src/weighted_notifications.rs"
README_ORDERS = "tests/fixtures/documentation_consumer/src/readme_orders.rs"
MONITOR = "tests/fixtures/documentation_consumer/src/run_state_monitor.rs"
DEAD_LETTER = "tests/fixtures/documentation_consumer/src/dead_letter_codec.rs"
REQUIRED = {
    LIB: {LOCAL},
    "README.md": {LOCAL, README_ORDERS},
    "README.zh_CN.md": {LOCAL, README_ORDERS},
    "doc/user_guide.md": {LOCAL, ASYNC, CODEC, DEAD_LETTER, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY, MONITOR, WEIGHTED},
    "doc/user_guide.zh_CN.md": {LOCAL, ASYNC, CODEC, DEAD_LETTER, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY, MONITOR, WEIGHTED},
    "doc/design.md": {SPEC, PROVIDER_CONTRACT, DEAD_LETTER},
    "doc/design.zh_CN.md": {SPEC, PROVIDER_CONTRACT, DEAD_LETTER},
}
FILE_HEADER = (
    "// =============================================================================\n"
    "//    Copyright (c) 2026 Haixing Hu.\n"
    "//\n"
    "//    SPDX-License-Identifier: Apache-2.0\n"
    "//\n"
    "//    Licensed under the Apache License, Version 2.0.\n"
    "// =============================================================================\n"
)
DOC_SHA = "6e8db1c737e4cd9d98acccce9631a6f5a2690ca1"
DOC_LINKS = (
    ("User guide", "doc/user_guide.md"),
    ("用户指南", "doc/user_guide.zh_CN.md"),
    ("Design", "doc/design.md"),
    ("设计文档", "doc/design.zh_CN.md"),
)
DOCUMENT_PATHS = tuple(path for _, path in DOC_LINKS)


case_count = 0

def run_case(mutator, args=(), snapshot=False, git_env=None, snapshot_missing=None):
    global case_count
    case_count += 1
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory) / "repository"
        root.mkdir()
        (root.parent / "outside.rs").write_text("fn nested() {}\n", encoding="utf-8")
        (root / "scripts").mkdir()
        shutil.copy(CHECKER, root / "scripts/check_documentation_examples.py")
        for source in (LOCAL, README_ORDERS, ASYNC, SPEC, PROVIDER_CONTRACT, CODEC, DEAD_LETTER, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY, MONITOR, WEIGHTED):
            path = root / source
            path.parent.mkdir(parents=True, exist_ok=True)
            header = FILE_HEADER if source in {LOCAL, README_ORDERS} else ""
            path.write_text(f"{header}fn example() {{ /* {source} */ }}\n", encoding="utf-8")
        for document, required in REQUIRED.items():
            if document == LIB:
                continue
            path = root / document
            path.parent.mkdir(parents=True, exist_ok=True)
            blocks = []
            for source in sorted(required):
                code = (root / source).read_text(encoding="utf-8").removeprefix(FILE_HEADER).strip()
                blocks.append(f"<!-- event-bus-source: {source} -->\n```rust\n{code}\n```")
            path.write_text("\n\n".join(blocks) + "\n", encoding="utf-8")
        lib_path = root / LIB
        lib_path.parent.mkdir(parents=True, exist_ok=True)
        source_code = (root / LOCAL).read_text(encoding="utf-8").removeprefix(FILE_HEADER).strip()
        lib_document = (
            "//! Example crate.\n//!\n"
            f"//! <!-- event-bus-source: {LOCAL} -->\n"
            "//! ```rust\n"
            + "".join(f"//! {line}\n" for line in source_code.splitlines())
            + "//! ```\n"
            + "//!\n//! ## Further reading\n//!\n"
            + "".join(
                f"//! - [{label}](https://github.com/qubit-ltd/rs-event-bus/blob/{DOC_SHA}/{path})\n"
                for label, path in DOC_LINKS
            )
            + "pub mod example;\n"
        )
        lib_path.write_text(lib_document, encoding="utf-8")
        if snapshot:
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.name", "Documentation Test"], cwd=root, check=True)
            tracked_documents = [path for path in DOCUMENT_PATHS if path != snapshot_missing]
            subprocess.run(["git", "add", *tracked_documents], cwd=root, check=True)
            subprocess.run(["git", "commit", "-qm", "snapshot documents"], cwd=root, check=True)
            snapshot_sha = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
            ).stdout.strip()
            lib_path.write_text(lib_path.read_text(encoding="utf-8").replace(DOC_SHA, snapshot_sha), encoding="utf-8")
        mutator(root)
        return subprocess.run(
            [sys.executable, "scripts/check_documentation_examples.py", *args],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
            env=git_env,
        )


def expect_failure(mutator, expected):
    result = run_case(mutator)
    assert result.returncode != 0, result.stdout
    assert expected in result.stdout + result.stderr, result.stdout + result.stderr


def missing_marker(root):
    (root / "README.md").write_text("# missing\n", encoding="utf-8")


def drift(root):
    path = root / "README.md"
    path.write_text(path.read_text(encoding="utf-8").replace("/* examples", "/* changed"), encoding="utf-8")


def remove_lib_marker(root):
    path = root / LIB
    path.write_text(path.read_text(encoding="utf-8").replace(f"//! <!-- event-bus-source: {LOCAL} -->", "//! <!-- omitted -->"), encoding="utf-8")


def drift_lib(root):
    path = root / LIB
    path.write_text(path.read_text(encoding="utf-8").replace("//! fn example()", "//! fn changed_example()", 1), encoding="utf-8")


def normalize_lib_blank_lines(root):
    path = root / LIB
    text = path.read_text(encoding="utf-8").replace("//! \n", "//!\n", 1)
    path.write_text(text, encoding="utf-8")


def remove_monitor_marker(root, document):
    remove_safety_marker(root, MONITOR, document)


def drift_monitor(root, document):
    path = root / document
    text = path.read_text(encoding="utf-8")
    marker = f"<!-- event-bus-source: {MONITOR} -->"
    before, block = text.split(marker, 1)
    block = block.replace("fn example()", "fn changed_example()", 1)
    path.write_text(before + marker + block, encoding="utf-8")


def add_file_header(root):
    path = root / "README.md"
    path.write_text(path.read_text(encoding="utf-8").replace("```rust\n", "```rust\n" + FILE_HEADER), encoding="utf-8")


def traversal(root):
    path = root / "README.md"
    path.write_text(path.read_text(encoding="utf-8").replace(LOCAL, "../../outside.rs"), encoding="utf-8")


def remove_safety_marker(root, source, document):
    path = root / document
    text = path.read_text(encoding="utf-8")
    marker = f"<!-- event-bus-source: {source} -->"
    path.write_text(text.replace(marker, "<!-- omitted -->"), encoding="utf-8")


def safety_drift(root, source):
    path = root / source
    path.write_text("fn changed() {}\n", encoding="utf-8")


def add_source(root, source):
    path = root / "README.md"
    path.write_text(path.read_text(encoding="utf-8") +
                    f"\n<!-- event-bus-source: {source} -->\n```rust\nfn nested() {{}}\n```\n",
                    encoding="utf-8")


def legal_nested_path(root):
    source = "tests/nested/examples/demo.rs"
    path = root / source
    path.parent.mkdir(parents=True)
    path.write_text("fn nested() {}\n", encoding="utf-8")
    add_source(root, source)


def existing_escape(root):
    # The target exists outside the repository: failure must come from the root
    # boundary, not merely from a missing file.
    add_source(root, "../outside.rs")


def symlink_escape(root):
    (root / "examples/escape.rs").symlink_to(root.parent / "outside.rs")
    add_source(root, "examples/escape.rs")


def mutate_doc_link(root, label, replacement):
    path = root / LIB
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    index = next(i for i, line in enumerate(lines) if f"[{label}]" in line)
    lines[index] = replacement
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def branch_ref(root):
    mutate_doc_link(root, "User guide", "//! - [User guide](https://github.com/qubit-ltd/rs-event-bus/blob/main/doc/user_guide.md)")


def missing_doc_link(root):
    path = root / LIB
    path.write_text("\n".join(line for line in path.read_text(encoding="utf-8").splitlines() if "[User guide]" not in line) + "\n", encoding="utf-8")


def duplicate_doc_label(root):
    path = root / LIB
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "pub mod example;\n",
        f"//! - [User guide](https://github.com/qubit-ltd/rs-event-bus/blob/{DOC_SHA}/doc/user_guide.md)\n"
        "pub mod example;\n",
    )
    path.write_text(text, encoding="utf-8")


def wrong_doc_repository(root):
    mutate_doc_link(root, "用户指南", f"//! - [用户指南](https://github.com/other/rs-event-bus/blob/{DOC_SHA}/doc/user_guide.zh_CN.md)")


def wrong_doc_path(root):
    mutate_doc_link(root, "Design", f"//! - [Design](https://github.com/qubit-ltd/rs-event-bus/blob/{DOC_SHA}/doc/other.md)")


def mixed_doc_snapshot(root):
    mutate_doc_link(root, "设计文档", "//! - [设计文档](https://github.com/qubit-ltd/rs-event-bus/blob/1111111111111111111111111111111111111111/doc/design.zh_CN.md)")


for source, documents in (
    *((source, ("doc/user_guide.md", "doc/user_guide.zh_CN.md")) for source in (RECEIPT, SHUTDOWN, REPUBLISH, WEIGHTED)),
    (PROVIDER_CONTRACT, ("doc/design.md", "doc/design.zh_CN.md")),
):
    for document in documents:
        expect_failure(lambda root: remove_safety_marker(root, source, document),
                       f"missing checked example {source}")
    expect_failure(lambda root: safety_drift(root, source), f"example drift from {source}")

for document in ("doc/user_guide.md", "doc/user_guide.zh_CN.md"):
    expect_failure(lambda root, document=document: remove_monitor_marker(root, document),
                   f"missing checked example {MONITOR}")
    expect_failure(lambda root, document=document: drift_monitor(root, document),
                   f"example drift from {MONITOR}")

for document in ("doc/user_guide.md", "doc/user_guide.zh_CN.md", "doc/design.md", "doc/design.zh_CN.md"):
    expect_failure(lambda root, document=document: remove_safety_marker(root, DEAD_LETTER, document),
                   f"missing checked example {DEAD_LETTER}")
    expect_failure(lambda root: safety_drift(root, DEAD_LETTER), f"example drift from {DEAD_LETTER}")

expect_failure(existing_escape, "invalid source path ../outside.rs")
expect_failure(symlink_escape, "invalid source path examples/escape.rs")
for mutator, label in (
    (branch_ref, "User guide"),
    (missing_doc_link, "User guide"),
    (duplicate_doc_label, "User guide"),
    (wrong_doc_repository, "用户指南"),
    (wrong_doc_path, "Design"),
    (mixed_doc_snapshot, "设计文档"),
):
    try:
        expect_failure(mutator, label)
    except AssertionError as error:
        raise AssertionError(f"snapshot-link regression case failed for {mutator.__name__}: {error}") from error
result = run_case(legal_nested_path)
assert result.returncode == 0, result.stdout + result.stderr

expect_failure(missing_marker, f"missing checked example {LOCAL}")
expect_failure(drift, f"example drift from {LOCAL}")
expect_failure(add_file_header, f"example drift from {LOCAL}")
expect_failure(traversal, "invalid source path ../../outside.rs")
expect_failure(remove_lib_marker, f"src/lib.rs: missing checked example {LOCAL}")
expect_failure(drift_lib, f"src/lib.rs: example drift from {LOCAL}")
result = run_case(normalize_lib_blank_lines)
assert result.returncode == 0, result.stdout + result.stderr
result = run_case(lambda _root: None)
assert result.returncode == 0, result.stdout + result.stderr

SNAPSHOT_ARGS = ("--check-snapshot-content",)
result = run_case(lambda _root: None, args=SNAPSHOT_ARGS, snapshot=True)
assert result.returncode == 0, result.stdout + result.stderr

def change_user_guide(root):
    path = root / "doc/user_guide.md"
    path.write_text(path.read_text(encoding="utf-8") + "\nchanged after snapshot\n", encoding="utf-8")


result = run_case(change_user_guide, args=SNAPSHOT_ARGS, snapshot=True)
assert result.returncode != 0 and "doc/user_guide.md: snapshot " in result.stdout + result.stderr
assert "content drift" in result.stdout + result.stderr

result = run_case(lambda root: None, args=SNAPSHOT_ARGS, snapshot=True,
                  snapshot_missing="doc/design.zh_CN.md")
assert result.returncode != 0 and "doc/design.zh_CN.md: snapshot " in result.stdout + result.stderr
assert "unavailable" in result.stdout + result.stderr

def unknown_snapshot(root):
    path = root / LIB
    text = path.read_text(encoding="utf-8")
    path.write_text(re.sub(r"blob/[0-9a-f]{40}/", "blob/0000000000000000000000000000000000000000/", text), encoding="utf-8")


result = run_case(unknown_snapshot, args=SNAPSHOT_ARGS, snapshot=True)
assert result.returncode != 0 and "snapshot 0000000000000000000000000000000000000000 unavailable" in result.stdout + result.stderr

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory) / "repository"
    root.mkdir()
    git_bin = root.parent / "bin"
    git_bin.mkdir()
    env = os.environ.copy()
    env["PATH"] = str(git_bin)
    result = run_case(lambda _root: None, args=SNAPSHOT_ARGS, snapshot=True, git_env=env)
    assert result.returncode != 0 and "unable to run git" in result.stdout + result.stderr

# The fetch flag is explicit: a wrapper simulates a missing object, records the
# exact fetch arguments, and delegates content reads to the real Git binary.
with tempfile.TemporaryDirectory() as directory:
    wrapper_dir = Path(directory) / "bin"
    wrapper_dir.mkdir()
    git_real = shutil.which("git")
    log_path = Path(directory) / "git-args.log"
    wrapper = wrapper_dir / "git"
    wrapper.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> '{log_path}'\n"
        "if [ \"$1\" = cat-file ]; then exit 1; fi\n"
        "if [ \"$1\" = fetch ]; then\n"
        "  if [ \"$MOCK_FETCH_FAIL\" = 1 ]; then echo 'mock fetch failed' >&2; exit 9; fi\n"
        "  exit 0\n"
        "fi\n"
        f"exec '{git_real}' \"$@\"\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = str(wrapper_dir) + os.pathsep + env.get("PATH", "")
    fetch_args = ("--check-snapshot-content", "--fetch-snapshot")
    result = run_case(lambda _root: None, args=fetch_args, snapshot=True, git_env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    commands = log_path.read_text(encoding="utf-8").splitlines()
    fetch = next((line for line in commands if line.startswith("fetch ")), "")
    assert fetch.startswith("fetch --no-tags --depth=1 origin "), commands
    env["MOCK_FETCH_FAIL"] = "1"
    result = run_case(lambda _root: None, args=fetch_args, snapshot=True, git_env=env)
    assert result.returncode != 0 and "snapshot " in result.stdout + result.stderr
    assert "fetch failed: mock fetch failed" in result.stdout + result.stderr

print(f"documentation example checker: {case_count} cases passed (examples, snapshot links/content, safety drift, nested paths, traversal, symlink escape and explicit fetch)")
