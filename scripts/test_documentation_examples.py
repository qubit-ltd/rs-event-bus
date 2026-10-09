#!/usr/bin/env python3
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts/check_documentation_examples.py"
LOCAL = "examples/local_delivery.rs"
ASYNC = "tests/fixtures/documentation_consumer/src/bin/async_local.rs"
SPEC = "tests/fixtures/documentation_consumer/src/provider_spec.rs"
CODEC = "tests/fixtures/documentation_consumer/src/order_created_codec.rs"
RECEIPT = "tests/fixtures/documentation_consumer/src/receipt_safety.rs"
SHUTDOWN = "tests/fixtures/documentation_consumer/src/bounded_shutdown.rs"
CAPACITY = "tests/fixtures/documentation_consumer/src/local_capacity.rs"
REPUBLISH = "tests/fixtures/documentation_consumer/src/republish_action.rs"
RETRY = "tests/fixtures/documentation_consumer/src/retry_policy.rs"
REQUIRED = {
    "README.md": {LOCAL},
    "README.zh_CN.md": {LOCAL},
    "doc/user_guide.md": {LOCAL, ASYNC, CODEC, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY},
    "doc/user_guide.zh_CN.md": {LOCAL, ASYNC, CODEC, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY},
    "doc/design.md": {SPEC},
    "doc/design.zh_CN.md": {SPEC},
}


def run_case(mutator):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory) / "repository"
        root.mkdir()
        (root.parent / "outside.rs").write_text("fn nested() {}\n", encoding="utf-8")
        (root / "scripts").mkdir()
        shutil.copy(CHECKER, root / "scripts/check_documentation_examples.py")
        for source in (LOCAL, ASYNC, SPEC, CODEC, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY):
            path = root / source
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"fn example() {{ /* {source} */ }}\n", encoding="utf-8")
        for document, required in REQUIRED.items():
            path = root / document
            path.parent.mkdir(parents=True, exist_ok=True)
            blocks = []
            for source in sorted(required):
                code = (root / source).read_text(encoding="utf-8").strip()
                blocks.append(f"<!-- event-bus-source: {source} -->\n```rust\n{code}\n```")
            path.write_text("\n\n".join(blocks) + "\n", encoding="utf-8")
        mutator(root)
        return subprocess.run(
            ["python3", "scripts/check_documentation_examples.py"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
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


for source in (RECEIPT, SHUTDOWN, REPUBLISH):
    for document in ("doc/user_guide.md", "doc/user_guide.zh_CN.md"):
        expect_failure(lambda root: remove_safety_marker(root, source, document),
                       f"missing checked example {source}")
    expect_failure(lambda root: safety_drift(root, source), f"example drift from {source}")

expect_failure(existing_escape, "invalid source path ../outside.rs")
expect_failure(symlink_escape, "invalid source path examples/escape.rs")
result = run_case(legal_nested_path)
assert result.returncode == 0, result.stdout + result.stderr

expect_failure(missing_marker, f"missing checked example {LOCAL}")
expect_failure(drift, f"example drift from {LOCAL}")
expect_failure(traversal, "invalid source path ../../outside.rs")
result = run_case(lambda _root: None)
assert result.returncode == 0, result.stdout + result.stderr
print("documentation example checker: 16 cases passed (both guides, safety drift, nested paths, traversal and symlink escape)")
