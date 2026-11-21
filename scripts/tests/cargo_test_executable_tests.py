import json
import os
import tempfile
import unittest

from scripts.cargo_test_executable import select_executable


class CargoTestExecutableTests(unittest.TestCase):
    def artifact(self, path, *, target="conformance_watchdog_tests", kind=None):
        return json.dumps(
            {
                "reason": "compiler-artifact",
                "target": {"name": target, "kind": kind or ["test"]},
                "executable": path,
            }
        )

    def test_selects_matching_test_artifact_and_ignores_other_messages(self):
        with tempfile.NamedTemporaryFile() as executable:
            os.chmod(executable.name, 0o700)
            output = select_executable(
                json.dumps({"reason": "build-finished", "success": True})
                + "\n"
                + self.artifact("/not/the/target", target="other_tests")
                + "\n"
                + self.artifact(executable.name),
                "conformance_watchdog_tests",
            )
            self.assertEqual(output, executable.name)

    def test_rejects_missing_target(self):
        with self.assertRaisesRegex(ValueError, "no executable"):
            select_executable(self.artifact("/unused", target="other_tests"), "conformance_watchdog_tests")

    def test_rejects_ambiguous_target(self):
        with tempfile.NamedTemporaryFile() as first, tempfile.NamedTemporaryFile() as second:
            messages = self.artifact(first.name) + "\n" + self.artifact(second.name)
            with self.assertRaisesRegex(ValueError, "2 executables"):
                select_executable(messages, "conformance_watchdog_tests")

    def test_rejects_non_test_target(self):
        with self.assertRaisesRegex(ValueError, "no executable"):
            select_executable(
                self.artifact("/unused", kind=["bin"]), "conformance_watchdog_tests"
            )

    def test_selects_example_without_matching_same_named_test_target(self):
        with tempfile.NamedTemporaryFile() as executable:
            os.chmod(executable.name, 0o700)
            messages = self.artifact("/unused", target="sample", kind=["test"])
            messages += "\n" + self.artifact(executable.name, target="sample", kind=["example"])
            self.assertEqual(select_executable(messages, "sample", "example"), executable.name)

    def test_rejects_ambiguous_example_target(self):
        with tempfile.NamedTemporaryFile() as first, tempfile.NamedTemporaryFile() as second:
            messages = self.artifact(first.name, target="sample", kind=["example"])
            messages += "\n" + self.artifact(second.name, target="sample", kind=["example"])
            with self.assertRaisesRegex(ValueError, "2 executables"):
                select_executable(messages, "sample", "example")

    def test_rejects_missing_executable(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            select_executable(self.artifact("/missing/test-binary"), "conformance_watchdog_tests")


if __name__ == "__main__":
    unittest.main()
