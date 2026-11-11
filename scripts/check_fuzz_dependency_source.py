"""Ensure the fuzz package resolves qubit-event-bus from this checkout."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tomllib


DEPENDENCY = "qubit-event-bus"


def validate_metadata(metadata, project_root):
    """Validate the fuzz package's direct resolved dependency edge."""
    root_manifest = (Path(project_root) / "Cargo.toml").resolve()
    fuzz_manifest = (Path(project_root) / "fuzz" / "Cargo.toml").resolve()
    try:
        packages = metadata["packages"]
        nodes = metadata["resolve"]["nodes"]
        fuzz_packages = [package for package in packages
                         if Path(package["manifest_path"]).resolve() == fuzz_manifest]
        if len(fuzz_packages) != 1:
            raise ValueError(f"expected one fuzz package, found {len(fuzz_packages)}")
        fuzz_id = fuzz_packages[0]["id"]
        fuzz_nodes = [node for node in nodes if node["id"] == fuzz_id]
        if len(fuzz_nodes) != 1:
            raise ValueError(f"expected one resolve node for fuzz package {fuzz_id}")
        matching_edges = [dependency for dependency in fuzz_nodes[0]["deps"]
                          if dependency["name"] == "qubit_event_bus"]
        if len(matching_edges) != 1:
            raise ValueError(f"expected one direct {DEPENDENCY} dependency edge, found {len(matching_edges)}")
        package_id = matching_edges[0]["pkg"]
        resolved = [package for package in packages if package["id"] == package_id]
        if len(resolved) != 1:
            raise ValueError(f"dependency edge points to missing or ambiguous package {package_id}")
        package = resolved[0]
        actual_manifest = Path(package["manifest_path"]).resolve()
        if package["name"] != DEPENDENCY:
            raise ValueError(f"dependency edge resolves to {package['name']}, expected {DEPENDENCY}")
        if package["source"] is not None or actual_manifest != root_manifest:
            raise ValueError(f"expected local {root_manifest}, got source={package['source']!r} "
                             f"manifest_path={actual_manifest}")
        return package["version"], actual_manifest
    except (KeyError, TypeError, OSError) as error:
        raise ValueError(f"invalid cargo metadata structure: {error}") from error


def validate_manifest(manifest_path, project_root):
    """Require an explicit path and retained version constraint in fuzz manifest."""
    try:
        with Path(manifest_path).open("rb") as stream:
            manifest = tomllib.load(stream)
        dependency = manifest["dependencies"][DEPENDENCY]
        if not isinstance(dependency, dict):
            raise ValueError(f"{DEPENDENCY} must use a detailed dependency declaration")
        if "version" not in dependency:
            raise ValueError(f"{DEPENDENCY} must retain a version constraint")
        if not isinstance(dependency["version"], str) or not dependency["version"]:
            raise ValueError(f"{DEPENDENCY} version constraint must be a non-empty string")
        if "path" not in dependency:
            raise ValueError(f"{DEPENDENCY} must declare a path dependency")
        actual = (Path(manifest_path).parent / dependency["path"]).resolve()
        expected = Path(project_root).resolve()
        if actual != expected:
            raise ValueError(f"expected {DEPENDENCY} path {expected}, got {actual}")
    except (KeyError, TypeError, tomllib.TOMLDecodeError, OSError) as error:
        raise ValueError(f"invalid fuzz manifest: {error}") from error


def check_project(project_root):
    root = Path(project_root).resolve()
    manifest = root / "fuzz" / "Cargo.toml"
    validate_manifest(manifest, root)
    command = ["cargo", "metadata", "--locked", "--format-version", "1",
               "--manifest-path", str(manifest)]
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
    if result.returncode:
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise ValueError(f"cargo metadata exited with status {result.returncode}")
    try:
        metadata = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError(f"cargo metadata returned invalid JSON: {error}") from error
    return validate_metadata(metadata, root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path,
                        default=Path(__file__).resolve().parents[1],
                        help="project root (defaults to the repository containing this script)")
    args = parser.parse_args()
    version, manifest = check_project(args.project_root)
    print(f"fuzz dependency source: passed qubit-event-bus {version} at {manifest}")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.SubprocessError) as error:
        print(f"fuzz dependency source: {error}", file=sys.stderr)
        sys.exit(1)
