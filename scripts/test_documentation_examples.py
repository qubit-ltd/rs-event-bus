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
REQUIRED = {
    "README.md": {LOCAL},
    "README.zh_CN.md": {LOCAL},
    "doc/user_guide.md": {LOCAL, ASYNC, CODEC},
    "doc/user_guide.zh_CN.md": {LOCAL, ASYNC, CODEC},
    "doc/design.md": {SPEC},
    "doc/design.zh_CN.md": {SPEC},
}


def run_case(mutator):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "scripts").mkdir()
        shutil.copy(CHECKER, root / "scripts/check_documentation_examples.py")
        for source in (LOCAL, ASYNC, SPEC, CODEC):
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


expect_failure(missing_marker, f"missing checked example {LOCAL}")
expect_failure(drift, f"example drift from {LOCAL}")
expect_failure(traversal, "invalid source path ../../outside.rs")
result = run_case(lambda _root: None)
assert result.returncode == 0, result.stdout + result.stderr
print("documentation example checker rejects missing, drifted, and escaping sources")
