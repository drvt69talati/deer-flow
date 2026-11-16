"""Mirror a kosa8 sandbox's ``/mnt/user-data/outputs`` to the owner's host thread directory.

The Gateway serves artifacts (``present_files`` previews and downloads) from the
host thread directory. kosa8 sandboxes share no mount with the Gateway, so the
provider copies output files there before it parks a sandbox, following the
same rules as ``community/opensandbox``:

- The listing is NUL-framed ``find -print0`` over the exec stream, whose frames
  carry raw bytes, so a file name may hold any byte but NUL.
- Names must be portable host paths: traversal, backslashes, colons, Windows
  reserved components and components ending in a space or dot are skipped.
- Names that differ only by case or Unicode normalization are skipped, with all
  outputs below them, because they collide on some host file systems.
- Each file is published through an exclusive temporary file and an atomic
  replace, and a linked destination is never written through.
- It is best effort and bounded: failures keep the previous complete copy, and
  a file deleted in the sandbox stays on the host.
"""

from __future__ import annotations

import logging
import os
import shlex
import stat
import tempfile
import time
import unicodedata
from pathlib import Path

from .sandbox import Kosa8Sandbox

logger = logging.getLogger(__name__)

MAX_SYNC_FILES = 2000
MAX_SYNC_TOTAL_BYTES = 512 * 1024 * 1024
MAX_SYNC_FILE_BYTES = 100 * 1024 * 1024
MAX_SYNC_LISTING_BYTES = 512 * 1024
SYNC_DEADLINE_SECONDS = 120.0

_WINDOWS_RESERVED_NAMES = frozenset({"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"} | {f"{prefix}{digit}" for prefix in ("COM", "LPT") for digit in "123456789¹²³"})


class _ListingTooLarge(Exception):
    pass


def _parts(relative: str) -> tuple[str, ...]:
    if "\\" in relative:
        raise ValueError("Unsafe output path")
    parts = tuple(relative.split("/"))
    if any(part in {"", ".", ".."} or ":" in part or part.partition(".")[0].rstrip(" ").upper() in _WINDOWS_RESERVED_NAMES or part.endswith((" ", ".")) for part in parts):
        raise ValueError("Unsafe output path")
    return parts


def _key(name: str) -> str:
    return unicodedata.normalize("NFC", name.casefold())


def _ambiguous(relatives: list[str]) -> set[str]:
    """Paths with a component that another path spells differently under the same key."""
    spelling: dict[tuple[str, ...], str] = {}
    bad_prefixes: set[tuple[str, ...]] = set()
    for relative in relatives:
        try:
            parts = _parts(relative)
        except ValueError:
            continue
        keys: tuple[str, ...] = ()
        for part in parts:
            keys = (*keys, _key(part))
            seen = spelling.setdefault(keys, part)
            if seen != part:
                bad_prefixes.add(keys)
    result: set[str] = set()
    for relative in relatives:
        try:
            keys = tuple(_key(part) for part in _parts(relative))
        except ValueError:
            continue
        if any(keys[:n] in bad_prefixes for n in range(1, len(keys) + 1)):
            result.add(relative)
    return result


def _destination(root: Path, relative: str) -> Path:
    parts = _parts(relative)
    target = root.joinpath(*parts)
    for component in reversed((target, *target.parents)):
        if component.is_symlink() or component.is_junction():
            raise ValueError("Symlinked output destination")
    parent = root
    for part in parts:
        if parent.is_dir():
            key = _key(part)
            with os.scandir(parent) as entries:
                if any(entry.name != part and _key(entry.name) == key for entry in entries):
                    raise ValueError("Output destination aliases an existing path")
        parent = parent / part
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not stat.S_ISREG(target.stat().st_mode):
        raise ValueError("Output destination is not a regular file")
    return target


def _list_outputs(sandbox: Kosa8Sandbox, remote_root: str) -> list[str | None]:
    received: list[bytes] = []
    size = 0

    def collect(stream: str, chunk: bytes) -> None:
        nonlocal size
        if stream != "stdout":
            return
        size += len(chunk)
        if size > MAX_SYNC_LISTING_BYTES:
            raise _ListingTooLarge
        received.append(chunk)

    quoted = shlex.quote(remote_root)
    # No outputs directory (or a linked one) is "nothing to mirror", not an error.
    script = f"[ -d {quoted} ] && [ ! -L {quoted} ] || exit 0; cd {quoted} && find . -type f -print0"
    result = sandbox._exec(["sh", "-c", script], timeout=SYNC_DEADLINE_SECONDS, on_output=collect)
    if result.timed_out or result.exit_code not in (0, 1):
        raise OSError(f"kosa8 output listing failed with exit code {result.exit_code}")
    raw = b"".join(received)
    if raw and not raw.endswith(b"\0"):
        raise OSError("kosa8 output listing was incomplete")
    if result.exit_code == 1:
        logger.warning("kosa8 output listing had traversal errors; mirroring complete records")
    names: list[str | None] = []
    for record in filter(None, raw.split(b"\0")):
        try:
            name = record.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("Skipping non-UTF-8 kosa8 output %r", record)
            names.append(None)
            continue
        names.append(name[2:] if name.startswith("./") else name)
    return names


def sync_outputs_to_host(sandbox: Kosa8Sandbox, root: Path, *, remote_root: str) -> None:
    """Best-effort bounded mirror of ``remote_root`` into the host directory ``root``."""
    deadline = time.monotonic() + SYNC_DEADLINE_SECONDS
    try:
        names = _list_outputs(sandbox, remote_root)
    except _ListingTooLarge:
        logger.warning("kosa8 output listing exceeded %d bytes; skipping synchronization", MAX_SYNC_LISTING_BYTES)
        return
    # Checked over the whole listing, including records beyond the file cap.
    ambiguous = _ambiguous([name for name in names if name is not None])
    total = 0
    for index, relative in enumerate(names):
        if index >= MAX_SYNC_FILES or total >= MAX_SYNC_TOTAL_BYTES or time.monotonic() >= deadline:
            logger.warning("kosa8 output synchronization stopped at its file, byte or time limit")
            break
        if relative is None:
            continue
        if relative in ambiguous:
            logger.warning("Skipping ambiguous kosa8 output %r", relative)
            continue
        staged: Path | None = None
        transferring = False
        try:
            target = _destination(root, relative)
            transferring = True
            remote_path = f"{remote_root.rstrip('/')}/{relative}"
            data = sandbox._read_bytes(remote_path, min(MAX_SYNC_FILE_BYTES, MAX_SYNC_TOTAL_BYTES - total), remote_path)
            transferring = False
            total += len(data)
            with tempfile.NamedTemporaryFile(prefix=".kosa8-sync-", dir=target.parent, delete=False) as output:
                staged = Path(output.name)
                output.write(data)
            # Recheck confinement after the network call, before publishing.
            _destination(root, relative)
            staged.replace(target)
            staged = None
        except (OSError, ValueError):
            logger.warning("Could not synchronize kosa8 output %r", relative, exc_info=True)
            # A failed read may have consumed budget that cannot be accounted for.
            if transferring:
                break
        finally:
            if staged is not None:
                staged.unlink(missing_ok=True)
