import unittest
from pathlib import Path

from scripts.check_downstream_compatibility import cargo_config
from scripts.check_downstream_compatibility import validate_graph_for_downstream


class DownstreamCompatibilityTests(unittest.TestCase):
    def test_cargo_patch_points_at_requested_core_and_redis_checkouts(self):
        args = cargo_config(Path("/core checkout"), Path("/redis checkout"))
        self.assertEqual(args, [
            "--config", 'patch.crates-io.qubit-event-bus.path="/core checkout"',
            "--config", 'patch.crates-io.qubit-event-bus-redis.path="/redis checkout"',
        ])

    def test_graph_requires_exactly_the_requested_local_core_manifest(self):
        graph = {
            "workspace_members": ["consumer 1.0.0 (path+file:///consumer)"],
            "resolve": {"nodes": [{
                "id": "consumer 1.0.0 (path+file:///consumer)",
                "dependencies": ["qubit-event-bus 0.21.0 (path+file:///core)",
                                 "qubit-event-bus-redis 0.7.0 (path+file:///redis)"],
            }, {
                "id": "qubit-event-bus 0.21.0 (path+file:///core)",
                "dependencies": [],
            }, {
                "id": "qubit-event-bus-redis 0.7.0 (path+file:///redis)",
                "dependencies": [],
            }]},
            "packages": [{
                "id": "qubit-event-bus 0.21.0 (path+file:///core)",
                "name": "qubit-event-bus",
                "version": "0.21.0",
                "source": None,
                "manifest_path": "/core/Cargo.toml",
            }, {
                "id": "qubit-event-bus-redis 0.7.0 (path+file:///redis)",
                "name": "qubit-event-bus-redis",
                "version": "0.7.0",
                "source": None,
                "manifest_path": "/redis/Cargo.toml",
            }],
        }
        validate_graph_for_downstream(graph, expected_core=Path("/core"), expected_redis=Path("/redis"))


if __name__ == "__main__":
    unittest.main()
