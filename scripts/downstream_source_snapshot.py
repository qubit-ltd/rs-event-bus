"""Create an isolated copy of the tracked state of a Git worktree."""

import os
from pathlib import Path
import shutil
import stat
import subprocess


def _inside(path, root):
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _tracked_entries(source):
    result = subprocess.run(
        ["git", "-C", str(source), "ls-files", "--cached", "--stage", "-z"],
        check=True,
        stdout=subprocess.PIPE,
    )
    entries = []
    for record in result.stdout.split(b"\0"):
        if not record:
            continue
        metadata, raw_path = record.split(b"\t", 1)
        mode, _object_id, stage = metadata.split(b" ", 2)
        relative = Path(os.fsdecode(raw_path))
        label = relative.as_posix()
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"tracked path escapes source tree: {label}")
        if stage != b"0":
            raise ValueError(f"cannot snapshot unmerged tracked path: {label}")
        if mode == b"160000":
            raise ValueError(f"cannot snapshot submodule: {label}")
        if mode not in (b"100644", b"100755", b"120000"):
            raise ValueError(f"unsupported tracked path mode {mode.decode('ascii')}: {label}")
        entries.append(relative)
    return entries


def tracked_source_files(source):
    """Return the validated tracked paths that a source snapshot would copy."""
    source = Path(source).resolve(strict=True)
    if not source.is_dir() or not (source / ".git").exists():
        raise ValueError(f"source is not a Git worktree: {source}")
    return _tracked_entries(source)


def _snapshot_members(source, tracked):
    """Return copyable tracked leaves and their snapshot-created parent directories."""
    leaves = set()
    directories = {Path(".")}
    for relative in tracked:
        try:
            info = (source / relative).lstat()
        except (FileNotFoundError, NotADirectoryError):
            continue
        if not (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
            continue
        leaves.add(relative)
        for parent in relative.parents:
            directories.add(parent)
    return leaves, directories


def tracked_snapshot_directory_identity(source, declaration, tracked=None):
    """Resolve a declaration's parent by walking only directories visible in a snapshot."""
    source = Path(source).resolve(strict=True)
    declaration = Path(declaration)
    if not declaration.is_absolute():
        declaration = source / declaration
    try:
        relative = declaration.parent.relative_to(source)
    except ValueError as error:
        raise ValueError(f"declaration is outside owning repository: {declaration}") from error
    tracked = set(tracked_source_files(source)) if tracked is None else set(tracked)
    _leaves, directories = _snapshot_members(source, tracked)
    current = source
    for part in relative.parts:
        if part in ("", "."):
            continue
        if part == "..":
            current = current.parent
            if not _inside(current, source):
                raise ValueError(f"link escapes owning repository at {current}")
            continue
        candidate = current / part
        rel = candidate.relative_to(source)
        if candidate.is_symlink():
            if rel not in tracked:
                raise ValueError(f"untracked symlink hop: {rel.as_posix()}")
            current = candidate.resolve(strict=True)
            if not _inside(current, source):
                raise ValueError(f"link escapes owning repository at {rel.as_posix()}")
        else:
            if rel not in directories:
                raise FileNotFoundError(f"missing path component at {rel.as_posix()}")
            current = candidate
        try:
            if not stat.S_ISDIR(current.stat().st_mode):
                raise NotADirectoryError(f"non-directory path component at {rel.as_posix()}")
        except FileNotFoundError as error:
            raise FileNotFoundError(f"missing path component at {rel.as_posix()}") from error
    return current.resolve(strict=True)


def is_tracked_snapshot_file(source, declaration, tracked=None):
    """Return whether a declared file would be available in a tracked snapshot."""
    try:
        validate_tracked_snapshot_file(source, declaration, tracked)
    except (OSError, ValueError):
        return False
    return True


def validate_tracked_snapshot_file(source, declaration, tracked=None):
    """Validate every link traversed by a file declaration for snapshot safety."""
    return _validate_tracked_snapshot_file(source, declaration, tracked)


def validate_tracked_config_snapshot_file(source, declaration, tracked=None, *, require_directory=False):
    """Validate a Cargo config path, returning None when its final file is absent from a snapshot."""
    return _validate_tracked_snapshot_file(
        source, declaration, tracked, allow_missing_final=True,
        require_directory=require_directory)


def _validate_tracked_snapshot_file(source, declaration, tracked=None, *, allow_missing_final=False,
                                    require_directory=False):
    """Validate a path component by component as it would appear in a snapshot."""
    source = Path(source).resolve(strict=True)
    declaration = Path(declaration)
    if not declaration.is_absolute():
        declaration = source / declaration
    try:
        relative = declaration.relative_to(source)
    except ValueError:
        raise ValueError(f"declaration is outside owning repository: {declaration}")
    tracked = set(tracked_source_files(source)) if tracked is None else tracked
    leaves, directories = _snapshot_members(source, tracked)
    if relative not in tracked and not allow_missing_final:
        raise ValueError(f"declaration is not tracked: {relative.as_posix()}")
    pending = list(relative.parts)
    if require_directory:
        pending.append(("require-directory",))
    current = source
    active_links = set()
    link_expansions = 0
    max_link_expansions = 40
    while pending:
        part = pending.pop(0)
        if isinstance(part, tuple):
            if part[0] == "require-directory":
                try:
                    info = current.stat()
                except FileNotFoundError:
                    info = None
                if info is None or not stat.S_ISDIR(info.st_mode):
                    if allow_missing_final:
                        return None
                    raise FileNotFoundError(f"non-directory path component at {current}")
                continue
            active_links.remove(part[1])
            continue
        if part in ("", "."):
            continue
        if part == "..":
            current = current.parent
            if not _inside(current, source):
                raise ValueError(f"link escapes owning repository at {current}")
            continue
        candidate = current / part
        rel = candidate.relative_to(source)
        label = rel.as_posix()
        if not _inside(candidate, source):
            raise ValueError(f"link escapes owning repository at {label}")
        if candidate.is_symlink():
            if rel not in tracked:
                raise ValueError(f"untracked symlink hop: {label}")
            if candidate in active_links:
                raise ValueError(f"symlink cycle at {label}")
            link_expansions += 1
            if link_expansions > max_link_expansions:
                raise ValueError(f"symlink expansion limit exceeded ({max_link_expansions} hops) at {label}")
            active_links.add(candidate)
            raw_target = os.readlink(candidate)
            target = Path(raw_target)
            if target.is_absolute():
                raise ValueError(f"absolute symlink target at {label}: {target}")
            target_parts = list(target.parts)
            if raw_target.endswith(("/", "/.")):
                target_parts.append(("require-directory",))
            pending = target_parts + [("end-link", candidate)] + pending
            current = candidate.parent.resolve(strict=True)
            continue
        is_final = not any(not isinstance(next_part, tuple) for next_part in pending)
        visible = rel in leaves or rel in directories
        try:
            candidate_stat = candidate.stat()
        except FileNotFoundError:
            candidate_stat = None
        if candidate_stat is None or not visible:
            if allow_missing_final:
                return None
            raise FileNotFoundError(f"missing path component at {label}")
        if is_final:
            current = candidate
        elif not stat.S_ISDIR(candidate_stat.st_mode):
            if allow_missing_final:
                return None
            raise FileNotFoundError(f"non-directory path component at {label}")
        else:
            current = candidate.resolve(strict=True)
            if not _inside(current, source):
                raise ValueError(f"link escapes owning repository at {label}")
    try:
        target_stat = current.stat()
    except FileNotFoundError as error:
        if allow_missing_final:
            return None
        raise FileNotFoundError(f"final file is missing: {current}") from error
    if not stat.S_ISREG(target_stat.st_mode):
        raise ValueError(f"final target is not a regular file: {current}")
    target_rel = current.relative_to(source)
    if target_rel not in tracked:
        if allow_missing_final:
            return None
        raise ValueError(f"final file is not tracked: {target_rel.as_posix()}")
    return current


def copy_tracked_source(source: Path, destination: Path) -> None:
    """Copy current tracked files, including edits and lockfiles, without untracked files."""
    source = Path(source).resolve(strict=True)
    destination = Path(destination).absolute()
    if source == destination or _inside(destination, source) or _inside(source, destination):
        raise ValueError("source and destination must be separate directory trees")
    if not source.is_dir() or not (source / ".git").exists():
        raise ValueError(f"source is not a Git worktree: {source}")
    if destination.exists():
        if not destination.is_dir() or any(destination.iterdir()):
            raise ValueError(f"destination must be an empty directory: {destination}")
    else:
        destination.mkdir(parents=True)

    tracked_entries = tracked_source_files(source)
    tracked = set(tracked_entries)
    copyable_leaves, _directories = _snapshot_members(source, tracked)
    for relative in tracked_entries:
        source_path = source / relative
        try:
            info = source_path.lstat()
        except (FileNotFoundError, NotADirectoryError):
            # A tracked deletion or a path hidden behind a replaced parent is absent in the snapshot.
            continue
        if not (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
            raise ValueError(f"unsupported tracked filesystem object: {relative.as_posix()}")
        if relative not in copyable_leaves:
            continue
        label = relative.as_posix()
        try:
            source_resolved = source_path.resolve(strict=False)
            source_resolved.relative_to(source)
        except ValueError as error:
            raise ValueError(f"tracked path escapes source tree: {label}") from error
        except OSError as error:
            raise ValueError(f"cannot resolve tracked path {label}: {error}") from error
        try:
            info = source_path.lstat()
        except FileNotFoundError:
            # A tracked deletion in the worktree remains deleted in the snapshot.
            continue
        except OSError as error:
            raise ValueError(f"cannot inspect tracked path {label}: {error}") from error

        is_cargo_input = (relative.name == "Cargo.toml" or
                          relative.parts[-2:] in ((".cargo", "config"),
                                                  (".cargo", "config.toml")))
        if is_cargo_input:
            if stat.S_ISLNK(info.st_mode) and Path(os.readlink(source_path)).is_absolute():
                raise ValueError(f"tracked symlink escapes snapshot: {label}")
            try:
                if relative.parts[-2:] in ((".cargo", "config"), (".cargo", "config.toml")):
                    validate_tracked_config_snapshot_file(source, source_path, tracked)
                else:
                    validate_tracked_snapshot_file(source, source_path, tracked)
            except (OSError, ValueError) as error:
                raise ValueError(f"tracked Cargo input is not snapshot-safe: {label}: {error}") from error

        destination_path = destination / relative
        parent = destination_path.parent
        parent.mkdir(parents=True, exist_ok=True)
        try:
            parent.resolve(strict=True).relative_to(destination.resolve(strict=True))
        except ValueError as error:
            raise ValueError(f"destination path escapes snapshot: {label}") from error
        current = destination
        for component in relative.parts[:-1]:
            current = current / component
            if current.is_symlink() or not current.is_dir():
                raise ValueError(f"destination path crosses a non-directory: {label}")

        if stat.S_ISLNK(info.st_mode):
            try:
                target = os.readlink(source_path)
            except OSError as error:
                raise ValueError(f"cannot read tracked symlink {label}: {error}") from error
            copied_target = Path(os.path.normpath(os.path.join(destination_path.parent, target)))
            try:
                copied_target.resolve(strict=False).relative_to(destination.resolve(strict=True))
            except ValueError as error:
                raise ValueError(f"tracked symlink escapes snapshot: {label}") from error
            if os.path.lexists(destination_path):
                raise ValueError(f"duplicate tracked destination path: {label}")
            destination_path.symlink_to(target)
        elif stat.S_ISREG(info.st_mode):
            if os.path.lexists(destination_path):
                raise ValueError(f"duplicate tracked destination path: {label}")
            shutil.copyfile(source_path, destination_path)
            destination_path.chmod(stat.S_IMODE(info.st_mode))
        else:
            raise ValueError(f"unsupported tracked filesystem object: {label}")
