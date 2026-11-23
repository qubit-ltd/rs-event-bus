#!/usr/bin/env python3
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
HOOK = ROOT / ".infra/project-ci-check.local.sh"
BOOTSTRAP = ROOT / ".infra/bootstrap.sh"


class ProjectHookGraphTests(unittest.TestCase):
    def test_real_hook_exists_and_is_executable(self):
        self.assertTrue(HOOK.is_file(), "missing project hook")
        self.assertTrue(os.access(HOOK, os.X_OK), "project hook is not executable")

    def test_bootstrap_dispatch_contract(self):
        self.assertTrue(HOOK.is_file(), "missing project hook")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "project"
            shared = Path(directory) / "shared"
            cache = Path(directory) / "cache"
            (root / ".infra").mkdir(parents=True)
            (root / "scripts").mkdir()
            shutil.copy(HOOK, root / ".infra/project-ci-check.local.sh")
            (root / ".infra/project-ci-check.local.sh").chmod(0o755)
            record = Path(directory) / "argv"
            stub = root / "scripts/project-ci-check.sh"
            stub.write_text(
                '#!/usr/bin/env bash\nset -euo pipefail\nfor argument in "$@"; do printf \'%s\\0\' "$argument"; done > "$ARG_RECORD"\nprintf \'hook-marker\\n\'\nexit "${HOOK_EXIT:-0}"\n',
                encoding="utf-8",
            )
            stub.chmod(0o755)
            true = shutil.which("true")
            self.assertIsNotNone(true, "test environment must provide the true executable")
            environment = os.environ.copy()
            environment.update({
                "RS_INFRA_PROJECT_ROOT": str(root),
                "RS_INFRA_SHARED_ROOT": str(shared),
                "RS_INFRA_TOOLS_BIN": true,
                "RS_INFRA_CACHE_DIR": str(cache),
                "ARG_RECORD": str(record),
            })

            def invoke(arguments=(), exit_code="0"):
                environment["HOOK_EXIT"] = exit_code
                record.unlink(missing_ok=True)
                return subprocess.run(
                    ["bash", str(BOOTSTRAP), ".infra/bin/project-ci-check.sh", *arguments],
                    env=environment,
                    capture_output=True,
                    text=True,
                    check=False,
                )

            default = invoke()
            self.assertEqual(default.returncode, 0, default.stderr)
            self.assertIn("hook-marker", default.stdout)
            self.assertEqual(record.read_bytes(), b"")

            failing = invoke(exit_code="23")
            self.assertEqual(failing.returncode, 23, failing.stderr)
            self.assertIn("hook-marker", failing.stdout)

    def test_hook_rejects_python_below_311_before_tests_and_accepts_supported_python(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            fake_bin = base / "bin"
            fake_bin.mkdir()
            hook = base / "project-ci-check.sh"
            shutil.copy(ROOT / "scripts/project-ci-check.sh", hook)
            hook.chmod(0o755)
            log = base / "python-invocations"
            fake_python = fake_bin / "python3"
            fake_python.write_text(
                '#!/usr/bin/env bash\n'
                'if [[ "$1" == "-c" ]]; then\n'
                '  printf "%s\\n" "${FAKE_PYTHON_VERSION}"\n'
                '  [[ "${FAKE_PYTHON_VERSION}" != 3.10.14 ]]\n'
                'else\n'
                '  printf "%s\\n" "$*" >> "$PYTHON_LOG"\n'
                '  exit 23\n'
                'fi\n',
                encoding="utf-8",
            )
            fake_python.chmod(0o755)
            environment = os.environ.copy()
            environment.update({"PATH": f"{fake_bin}{os.pathsep}{environment['PATH']}",
                                "PYTHON_LOG": str(log),
                                "FAKE_PYTHON_VERSION": "3.10.14"})
            unsupported = subprocess.run(["bash", str(hook)], env=environment,
                                         capture_output=True, text=True, check=False)
            self.assertEqual(unsupported.returncode, 1)
            self.assertIn("requires Python 3.11 or newer; found 3.10.14", unsupported.stderr)
            self.assertFalse(log.exists(), "low Python version must not start any test")

            environment["FAKE_PYTHON_VERSION"] = "3.12.3"
            supported = subprocess.run(["bash", str(hook)], env=environment,
                                       capture_output=True, text=True, check=False)
            self.assertEqual(supported.returncode, 23)
            self.assertEqual(log.read_text().splitlines(), [
                "-B -m unittest discover -s "
                f"{base.parent / 'scripts/tests'} -p performance_evidence_tests.py",
            ])

if __name__ == "__main__":
    unittest.main()
