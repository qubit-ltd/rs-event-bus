#!/usr/bin/env python3
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
LOCAL = "examples/local_delivery.rs"
ASYNC = "tests/fixtures/documentation_consumer/src/bin/async_local.rs"
SPEC = "tests/fixtures/documentation_consumer/src/provider_spec.rs"
CODEC = "tests/fixtures/documentation_consumer/src/order_created_codec.rs"
RECEIPT = "tests/fixtures/documentation_consumer/src/receipt_safety.rs"
SHUTDOWN = "tests/fixtures/documentation_consumer/src/bounded_shutdown.rs"
CAPACITY = "tests/fixtures/documentation_consumer/src/local_capacity.rs"
REPUBLISH = "tests/fixtures/documentation_consumer/src/republish_action.rs"
REQUIRED = {
    "README.md": {LOCAL},
    "README.zh_CN.md": {LOCAL},
    "doc/user_guide.md": {LOCAL, ASYNC, CODEC, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH},
    "doc/user_guide.zh_CN.md": {LOCAL, ASYNC, CODEC, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH},
    "doc/design.md": {SPEC},
    "doc/design.zh_CN.md": {SPEC},
}
PATTERN = re.compile(
    r"<!-- event-bus-source: (?P<path>[^\n]+?) -->\s*"
    r"(?P<fence>[\x60~]{3})rust\s*\n(?P<code>.*?)\n(?P=fence)",
    re.DOTALL,
)
errors = []
for document, required in REQUIRED.items():
    text = (ROOT / document).read_text(encoding="utf-8")
    found = set()
    for match in PATTERN.finditer(text):
        name = match.group("path")
        source = (ROOT / name).resolve()
        if not source.is_relative_to(ROOT) or not source.is_file():
            errors.append(f"{document}: invalid source path {name}")
            continue
        found.add(name)
        if match.group("code").strip() != source.read_text(encoding="utf-8").strip():
            errors.append(f"{document}: example drift from {name}")
    for missing in sorted(required - found):
        errors.append(f"{document}: missing checked example {missing}")
if errors:
    raise SystemExit("\n".join(errors))
print("documentation examples match their compiled sources")
