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


def is_tracked_snapshot_file(source, declaration, tracked=None):
    """Return whether a declared file would be available in a tracked snapshot."""
    try:
        validate_tracked_snapshot_file(source, declaration, tracked)
    except (OSError, ValueError):
        return False
    return True


def validate_tracked_snapshot_file(source, declaration, tracked=None):
    """Validate every link traversed by a file declaration for snapshot safety."""
    source = Path(source).resolve(strict=True)
    declaration = Path(os.path.abspath(declaration))
    try:
        relative = declaration.relative_to(source)
    except ValueError:
        raise ValueError(f"declaration is outside owning repository: {declaration}")
    tracked = set(tracked_source_files(source)) if tracked is None else tracked
    if relative not in tracked:
        raise ValueError(f"declaration is not tracked: {relative.as_posix()}")
    pending = list(relative.parts)
    current = source
    visited = set()
    while pending:
        part = pending.pop(0)
        if part in ("", "."):
            continue
        if part == "..":
            current = current.parent
            if not _inside(current, source):
                raise ValueError(f"link escapes owning repository at {current}")
            continue
        current = current / part
        rel = current.relative_to(source)
        label = rel.as_posix()
        if not _inside(current, source):
            raise ValueError(f"link escapes owning repository at {label}")
        if current.is_symlink():
            if rel not in tracked:
                raise ValueError(f"untracked symlink hop: {label}")
            if current in visited:
                raise ValueError(f"symlink cycle at {label}")
            visited.add(current)
            target = Path(os.readlink(current))
            if target.is_absolute():
                raise ValueError(f"absolute symlink target at {label}: {target}")
            pending = list(target.parts) + pending
            current = current.parent
            continue
        if pending and not current.is_dir():
            raise FileNotFoundError(f"missing path component at {label}")
    try:
        target_stat = current.stat()
    except FileNotFoundError as error:
        raise FileNotFoundError(f"final file is missing: {current}") from error
    if not stat.S_ISREG(target_stat.st_mode):
        raise ValueError(f"final target is not a regular file: {current}")
    target_rel = current.relative_to(source)
    if target_rel not in tracked:
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

    for relative in tracked_source_files(source):
        label = relative.as_posix()
        source_path = source / relative
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
