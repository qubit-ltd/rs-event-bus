"""Regression tests for the standalone crate dependency graph gate."""

import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).parents[1] / "check_event_bus_graph.py"
spec = importlib.util.spec_from_file_location("graph_gate", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def metadata(*versions):
    packages = [{"id": "crate", "name": "crate", "version": "1.0.0"}]
    nodes = [{"id": "crate", "dependencies": []}]
    for index, version in enumerate(versions):
        identity = f"bus-{index}"
        packages.append({"id": identity, "name": "qubit-event-bus", "version": version,
                         "source": None, "manifest_path": "/project/Cargo.toml"})
        nodes[0]["dependencies"].append(identity)
        nodes.append({"id": identity, "dependencies": []})
    return {"packages": packages, "workspace_members": ["crate"],
            "resolve": {"nodes": nodes}}


class GraphTests(unittest.TestCase):
    def test_accepts_current_minor(self):
        gate.validate_graph(metadata("0.21.0"), require_bus=True, expected_core=Path("/project"))

    def test_rejects_old_minor(self):
        with self.assertRaisesRegex(ValueError, "0.21"):
            gate.validate_graph(metadata("0.20.1"), require_bus=True)

    def test_rejects_duplicate_core_packages(self):
        with self.assertRaisesRegex(ValueError, "multiple"):
            gate.validate_graph(metadata("0.21.0", "0.21.0"), require_bus=True)

    def test_rejects_registry_core_package(self):
        graph = metadata("0.21.0")
        graph["packages"][1]["source"] = "registry+https://example.invalid"
        with self.assertRaisesRegex(ValueError, "local"):
            gate.validate_graph(graph, require_bus=True, expected_core=Path("/project"))

    def test_requires_core_package(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            gate.validate_graph(metadata(), require_bus=True)

    def test_only_reachable_packages_form_dependency_graph(self):
        graph = metadata("0.21.0", "0.20.1")
        graph["resolve"]["nodes"][0]["dependencies"].pop()
        gate.validate_graph(graph, require_bus=True)


if __name__ == "__main__":
    unittest.main()
