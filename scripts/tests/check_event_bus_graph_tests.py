"""Regression tests for the event-bus release graph gate."""

import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).parents[1] / "check_event_bus_graph.py"
spec = importlib.util.spec_from_file_location("graph_gate", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def metadata(*versions):
    packages = [{"id": "consumer", "name": "consumer", "version": "1.0.0"}]
    nodes = [{"id": "consumer", "dependencies": []}]
    for index, version in enumerate(versions):
        identity = f"bus-{index}"
        packages.append({"id": identity, "name": "qubit-event-bus",
                         "version": version, "source": None,
                         "manifest_path": "/layout/rs-event-bus/Cargo.toml"})
        nodes[0]["dependencies"].append(identity)
        nodes.append({"id": identity, "dependencies": []})
    return {"packages": packages, "workspace_members": ["consumer"],
            "resolve": {"nodes": nodes}}


class GraphTests(unittest.TestCase):
    def test_accepts_new_minor(self):
        gate.validate_graph(metadata("0.18.0"), require_bus=True)

    def test_rejects_old_minor(self):
        with self.assertRaisesRegex(ValueError, "0.18"):
            gate.validate_graph(metadata("0.17.0"), require_bus=True)

    def test_rejects_duplicate_registry_and_path_packages(self):
        graph = metadata("0.18.0", "0.18.0")
        graph["packages"][2]["source"] = "registry+https://example.invalid"
        with self.assertRaisesRegex(ValueError, "multiple"):
            gate.validate_graph(graph, require_bus=True)

    def test_rejects_registry_in_controlled_layout(self):
        graph = metadata("0.18.0")
        graph["packages"][1]["source"] = "registry+https://example.invalid"
        with self.assertRaisesRegex(ValueError, "local"):
            gate.validate_graph(graph, require_bus=True,
                                expected_core=Path("/layout/rs-event-bus"))

    def test_rejects_wrong_local_core(self):
        with self.assertRaisesRegex(ValueError, "local"):
            gate.validate_graph(metadata("0.18.0"), require_bus=True,
                                expected_core=Path("/other/rs-event-bus"))

    def test_non_bus_root_does_not_require_bus(self):
        gate.validate_graph(metadata(), require_bus=False)
        with self.assertRaisesRegex(ValueError, "missing"):
            gate.validate_graph(metadata(), require_bus=True)

    def test_only_reachable_packages_form_dependency_graph(self):
        graph = metadata("0.18.0", "0.17.0")
        graph["resolve"]["nodes"][0]["dependencies"].pop()
        gate.validate_graph(graph, require_bus=True)

    def test_explicit_layout_requires_every_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "rs-event-bus.*rs-task"):
                gate.layout_manifests(Path(directory))


if __name__ == "__main__":
    unittest.main()
