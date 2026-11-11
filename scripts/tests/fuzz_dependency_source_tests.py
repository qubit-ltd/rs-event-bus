"""Tests for the fuzz dependency source gate."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).parents[1] / "check_fuzz_dependency_source.py"
spec = importlib.util.spec_from_file_location("fuzz_source_gate", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def metadata(source=None, manifest="/project/Cargo.toml", root="/project"):
    return {
        "packages": [
            {"id": "fuzz", "name": "fuzz",
             "manifest_path": str(Path(root) / "fuzz" / "Cargo.toml")},
            {"id": "bus", "name": "qubit-event-bus",
             "version": "0.21.0", "source": source, "manifest_path": manifest},
        ],
        "resolve": {"nodes": [
            {"id": "fuzz", "deps": [
                {"name": "qubit_event_bus", "pkg": "bus"}],
             "dependencies": ["bus"]},
            {"id": "bus", "deps": [], "dependencies": []},
        ]},
    }


class MetadataTests(unittest.TestCase):
    def test_accepts_direct_edge_to_local_root_package(self):
        self.assertEqual(gate.validate_metadata(metadata(), "/project"),
                         ("0.21.0", Path("/project/Cargo.toml")))

    def test_rejects_registry_edge_even_if_local_same_name_exists(self):
        graph = metadata("registry+https://example.invalid", "/registry/Cargo.toml")
        graph["packages"].append({"id": "local", "name": "qubit-event-bus",
                                  "version": "0.21.0", "source": None,
                                  "manifest_path": "/project/Cargo.toml"})
        with self.assertRaisesRegex(ValueError, "expected local"):
            gate.validate_metadata(graph, "/project")

    def test_rejects_wrong_local_manifest(self):
        with self.assertRaisesRegex(ValueError, "expected local"):
            gate.validate_metadata(metadata(manifest="/other/Cargo.toml"), "/project")

    def test_rejects_missing_edge_resolve_package_and_malformed_fields(self):
        graph = metadata()
        graph["resolve"]["nodes"][0]["deps"] = []
        with self.assertRaisesRegex(ValueError, "dependency edge"):
            gate.validate_metadata(graph, "/project")
        for graph in ({"packages": []}, {"packages": metadata()["packages"], "resolve": {"nodes": []}},
                      {"packages": metadata()["packages"], "resolve": {"nodes": [{"id": "x"}]}}):
            with self.assertRaises(ValueError):
                gate.validate_metadata(graph, "/project")


class ManifestTests(unittest.TestCase):
    def test_requires_path_and_version_with_correct_resolution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "project with spaces"
            (root / "fuzz").mkdir(parents=True)
            manifest = root / "fuzz" / "Cargo.toml"
            manifest.write_text('[package]\nname = "fuzz"\nversion = "0.0.0"\n'
                '[dependencies]\nqubit-event-bus = { version = "^0.21", path = ".." }\n')
            gate.validate_manifest(manifest, root)
            manifest.write_text('[package]\nname = "fuzz"\nversion = "0.0.0"\n'
                '[dependencies]\nqubit-event-bus = { version = "^0.21" }\n')
            with self.assertRaisesRegex(ValueError, "path dependency"):
                gate.validate_manifest(manifest, root)
            manifest.write_text('[package]\nname = "fuzz"\nversion = "0.0.0"\n'
                '[dependencies]\nqubit-event-bus = { path = ".." }\n')
            with self.assertRaisesRegex(ValueError, "version constraint"):
                gate.validate_manifest(manifest, root)
            manifest.write_text('[package]\nname = "fuzz"\nversion = "0.0.0"\n'
                '[dependencies]\nqubit-event-bus = { version = "^0.21", path = "../wrong" }\n')
            with self.assertRaisesRegex(ValueError, "expected qubit-event-bus path"):
                gate.validate_manifest(manifest, root)


class CliTests(unittest.TestCase):
    def test_subprocess_arguments_cwd_and_space_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "project with spaces"
            (root / "fuzz").mkdir(parents=True)
            (root / "fuzz" / "Cargo.toml").write_text(
                '[package]\nname = "fuzz"\nversion = "0.0.0"\n'
                '[dependencies]\nqubit-event-bus = { version = "^0.21", path = ".." }\n')
            completed = subprocess.CompletedProcess([], 0, json.dumps(metadata(manifest=str(root / "Cargo.toml"), root=str(root))), "")
            with mock.patch.object(gate.subprocess, "run", return_value=completed) as run:
                self.assertEqual(gate.check_project(root), ("0.21.0", root / "Cargo.toml"))
            args, kwargs = run.call_args
            self.assertEqual(args[0], ["cargo", "metadata", "--locked", "--format-version", "1",
                                       "--manifest-path", str(root / "fuzz" / "Cargo.toml")])
            self.assertEqual(kwargs["cwd"], root)
            self.assertFalse(kwargs.get("shell", False))

    def test_cli_from_outside_cwd_and_cargo_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "project with spaces"
            (root / "fuzz").mkdir(parents=True)
            (root / "fuzz" / "Cargo.toml").write_text(
                '[package]\nname = "fuzz"\nversion = "0.0.0"\n'
                '[dependencies]\nqubit-event-bus = { version = "^0.21", path = ".." }\n')
            stub = "#!/bin/sh\nprintf '%s' \"$FAKE_CARGO_OUTPUT\"\nif [ \"${FAKE_CARGO_STATUS:-0}\" -ne 0 ]; then printf '%s\n' \"$FAKE_CARGO_OUTPUT\" >&2; fi\nexit \"${FAKE_CARGO_STATUS:-0}\"\n"
            cargo_dir = root / "bin"
            cargo_dir.mkdir()
            cargo = cargo_dir / "cargo"
            cargo.write_text(stub)
            cargo.chmod(0o755)
            env = dict(os.environ, PATH=f"{cargo_dir}:{os.environ['PATH']}",
                       FAKE_CARGO_OUTPUT="not json", FAKE_CARGO_STATUS="0")
            with mock.patch.dict(os.environ, env):
                result = subprocess.run(["python3", "-B", str(SCRIPT), "--project-root", str(root)],
                                        cwd=temporary, capture_output=True, text=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("invalid JSON", result.stderr)
            env["FAKE_CARGO_OUTPUT"] = "cargo failure"
            env["FAKE_CARGO_STATUS"] = "7"
            with mock.patch.dict(os.environ, env):
                result = subprocess.run(["python3", "-B", str(SCRIPT), "--project-root", str(root)],
                                        cwd=temporary, capture_output=True, text=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("cargo failure", result.stderr)
            self.assertIn("status 7", result.stderr)


if __name__ == "__main__":
    unittest.main()
