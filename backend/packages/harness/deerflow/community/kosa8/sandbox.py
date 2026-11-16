"""``Kosa8Sandbox`` — DeerFlow :class:`Sandbox` backed by a kosa8 Cloud sandbox.

Each command is a fresh ``sh -lc`` over kosa8's streamed exec. File transport
uses kosa8's files API where it can (``/workspace``, up to 8 MiB a request) and
the exec stream elsewhere, so every path the agent can name stays reachable:

- Reads under ``/workspace`` stream from the files API; other paths, and files
  above the files API's per-request limit, stream through ``cat`` over exec,
  whose frames carry raw bytes. The byte cap is enforced as bytes arrive.
- Writes under ``/workspace`` of at most 8 MiB are one upload. Larger writes,
  appends and writes elsewhere upload in parts to a staging directory under
  ``/workspace`` and are assembled with one ``cat``, because an exec argument is
  at most 8 KiB.
- A command longer than 8 KiB is staged as a script file the same way.

``list_dir`` / ``glob`` / ``grep`` shell out to ``find`` / ``grep`` and parse with
the shared ``deerflow.sandbox`` helpers, as ``community/tenki`` does.
"""

from __future__ import annotations

import errno
import logging
import posixpath
import re
import shlex
import threading
import uuid
from collections.abc import Callable
from typing import TypeVar

from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.sandbox.download import DEFAULT_DOWNLOAD_LIMIT as _MAX_DOWNLOAD_SIZE
from deerflow.sandbox.download import download_limit, read_download_stream
from deerflow.sandbox.read_file_contract import split_file_lines
from deerflow.sandbox.remote_list_dir import parse_remote_list_dir_output, remote_list_dir_command
from deerflow.sandbox.remote_search import parse_remote_search_output, remote_search_command
from deerflow.sandbox.sandbox import Sandbox, _validate_extra_env
from deerflow.sandbox.search import GrepMatch, path_matches, should_ignore_path_under_root, truncate_line

from .client import FILES_ROOT, MAX_ARG_BYTES, MAX_FILE_REQUEST_BYTES, ExecResult, Kosa8APIError, Kosa8Client, under_files_root

T = TypeVar("T")

logger = logging.getLogger(__name__)

# The directory behind DeerFlow's /mnt/user-data. It sits under /workspace so the
# files API reaches it, in a subdirectory so kosa8's own /workspace entries (such
# as background-command logs) stay out of the agent's listings.
DEFAULT_KOSA8_ROOT_DIR = f"{FILES_ROOT}/user-data"
# Staged scripts and upload parts. Outside the root, so the agent never lists them.
STAGING_DIR = f"{FILES_ROOT}/.deer-flow-staging"
# Leave headroom under the files API's request limit.
_UPLOAD_PART_BYTES = MAX_FILE_REQUEST_BYTES - 64 * 1024
# Commands up to this many UTF-8 bytes go inline as one argv entry.
_INLINE_COMMAND_BYTES = MAX_ARG_BYTES - 256

# Run the staged parts in "$@" into the target ("$1"), then remove them. The
# exit status is cat's, so a failed write is never reported as success.
_ASSEMBLE_SCRIPT = 'set -u; t=$1; op=$2; shift 2; mkdir -p -- "$(dirname -- "$t")" || exit $?; if [ "$op" = append ]; then cat -- "$@" >> "$t"; else cat -- "$@" > "$t"; fi; rc=$?; rm -f -- "$@"; exit $rc'
_RUN_STAGED_SCRIPT = 's=$1; sh -l "$s"; rc=$?; rm -f -- "$s"; exit $rc'


class Kosa8Sandbox(Sandbox):
    """DeerFlow Sandbox adapter that delegates to a live kosa8 sandbox.

    Args:
        id: DeerFlow-side sandbox id (the provider's cache key).
        client: The provider's REST client.
        remote_id: The kosa8 sandbox id (``sb_…``).
        root_dir: Directory that backs ``VIRTUAL_PATH_PREFIX`` in the sandbox.
        on_terminal_failure: Called ``(sandbox_id, reason)`` when kosa8 reports
            the sandbox gone, so the provider can evict it.
    """

    #: Every call is a fresh ``sh -lc`` exec — no shell state survives into the next command.
    persistent_shell_sessions = False

    def __init__(
        self,
        id: str,
        client: Kosa8Client,
        remote_id: str,
        *,
        root_dir: str = DEFAULT_KOSA8_ROOT_DIR,
        on_terminal_failure: Callable[[str, str], None] | None = None,
    ) -> None:
        super().__init__(id)
        self._client = client
        self.remote_id = remote_id
        self._root_dir = root_dir.rstrip("/") or "/"
        self._on_terminal_failure = on_terminal_failure
        self._lock = threading.Lock()
        # Serialises same-sandbox writes so two appends cannot interleave their parts.
        self._write_lock = threading.Lock()
        self._closed = False

    @property
    def is_closed(self) -> bool:
        with self._lock:
            return self._closed

    @property
    def artifact_root(self) -> str:
        return self._root_dir

    def close(self) -> None:
        """Delete the kosa8 sandbox (idempotent).

        Mark closed only once kosa8 accepted the delete or reports the sandbox
        already gone. Any other failure stays retryable, so a running (billed)
        sandbox is never silently abandoned.
        """
        with self._lock:
            if self._closed:
                return
            try:
                self._client.delete_sandbox(self.remote_id)
            except Kosa8APIError as e:
                if e.status != 404:
                    logger.error("Error deleting kosa8 sandbox %s: %s", self.remote_id, e)
                    raise
                logger.info("kosa8 sandbox %s was already gone at close", self.remote_id)
            self._closed = True

    # ── bridge helpers ──────────────────────────────────────────────────

    def _call(self, op: Callable[[], T]) -> T:
        """Run one API call, evicting this sandbox when kosa8 reports it gone."""
        if self.is_closed:
            raise RuntimeError("sandbox has been closed")
        try:
            return op()
        except Kosa8APIError as e:
            if e.sandbox_gone and self._on_terminal_failure is not None:
                try:
                    self._on_terminal_failure(self.id, str(e))
                except Exception:
                    logger.exception("Terminal kosa8 failure callback errored for %s", self.id)
            raise

    def _exec(self, argv: list[str], *, env: dict[str, str] | None = None, timeout: float | None = None, on_output: Callable[[str, bytes], None] | None = None) -> ExecResult:
        # No auto-retry: exec is not idempotent, so a lost answer must not run the command twice.
        return self._call(lambda: self._client.exec(self.remote_id, argv, env=env, timeout=timeout, on_output=on_output))

    def _sh(self, script: str, *, env: dict[str, str] | None = None, timeout: float | None = None) -> ExecResult:
        """Run ``script`` through ``sh -lc``, staging it as a file when it is too long for one argument."""
        if len(script.encode("utf-8")) <= _INLINE_COMMAND_BYTES:
            return self._exec(["sh", "-lc", script], env=env, timeout=timeout)
        staged = f"{STAGING_DIR}/cmd-{uuid.uuid4().hex}.sh"
        self._upload(staged, script.encode("utf-8"))
        return self._exec(["sh", "-c", _RUN_STAGED_SCRIPT, "sh", staged], env=env, timeout=timeout)

    def _upload(self, path: str, data: bytes) -> None:
        parent = posixpath.dirname(path)
        self._call(lambda: self._client.mkdir(self.remote_id, parent))
        self._call(lambda: self._client.write_file(self.remote_id, path, data))

    # ── path safety ─────────────────────────────────────────────────────

    @staticmethod
    def _guard_traversal(path: str) -> str:
        if not path:
            raise ValueError("path must be a non-empty string")
        normalized = path.replace("\\", "/")
        if not normalized.startswith("/"):
            raise PermissionError(f"Access denied: path must be absolute: '{path}'")
        for segment in normalized.split("/"):
            if segment == "..":
                raise PermissionError(f"Access denied: path traversal detected in '{path}'")
        return normalized

    def _resolve_path(self, path: str) -> str:
        """Map ``VIRTUAL_PATH_PREFIX`` onto the root directory; other absolute paths pass through."""
        normalized = self._guard_traversal(path)
        if normalized == VIRTUAL_PATH_PREFIX or normalized.startswith(f"{VIRTUAL_PATH_PREFIX}/"):
            tail = normalized[len(VIRTUAL_PATH_PREFIX) :].lstrip("/")
            return f"{self._root_dir}/{tail}".rstrip("/") if tail else self._root_dir
        return normalized

    def _virtual_path(self, resolved: str) -> str:
        """Inverse of :meth:`_resolve_path`, so returned paths feed back into the file APIs."""
        if resolved == self._root_dir:
            return VIRTUAL_PATH_PREFIX
        if resolved.startswith(f"{self._root_dir}/"):
            return f"{VIRTUAL_PATH_PREFIX}/{resolved[len(self._root_dir) :].lstrip('/')}"
        return resolved

    # ── command execution ───────────────────────────────────────────────

    def execute_command(
        self,
        command: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        """Run ``command`` through ``sh -lc`` and return its output as text.

        Per-call ``env`` rides in the exec's own ``env`` field, never in the
        command string, and wins over the sandbox's environment per name.
        """
        _validate_extra_env(env)
        if self.is_closed:
            return "Error: sandbox has been closed"
        try:
            result = self._sh(command, env=env or None, timeout=timeout)
        except Exception as e:
            logger.error("Failed to execute command in kosa8 sandbox %s: %s", self.id, e)
            return f"Error: {e}"

        stdout = result.stdout_text
        stderr = result.stderr_text
        output = f"{stdout}\n{stderr}" if stdout and stderr else (stdout or stderr)
        if result.timed_out:
            message = "Error: command timed out\nExit Code: 124"
            return f"{output}\n{message}" if output else message
        if result.exit_code not in (0, None):
            output = f"{output}\nExit Code: {result.exit_code}" if output else f"Command exited with code {result.exit_code}"
        return output if output else "(no output)"

    # ── file operations ─────────────────────────────────────────────────

    def read_file(
        self,
        path: str,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        resolved = self._resolve_path(path)
        try:
            content = self._read_bytes(resolved, _MAX_DOWNLOAD_SIZE, path).decode("utf-8", errors="replace")
        except Exception as e:
            logger.error("read_file %s failed: %s", resolved, e)
            return f"Error: {e}"
        if start_line is None and end_line is None:
            return content
        lines = split_file_lines(content)
        start = max(start_line or 1, 1)
        end = max(end_line, 0) if end_line is not None else len(lines)
        return "\n".join(lines[start - 1 : end])

    def write_file(self, path: str, content: str, append: bool = False) -> None:
        self._write_bytes(self._resolve_path(path), content.encode("utf-8"), append=append)

    def update_file(self, path: str, content: bytes) -> None:
        self._write_bytes(self._resolve_path(path), content, append=False)

    def _write_bytes(self, resolved: str, data: bytes, *, append: bool) -> None:
        with self._write_lock:
            if not append and under_files_root(resolved) and len(data) <= _UPLOAD_PART_BYTES:
                self._upload(resolved, data)
                return
            # Stage the bytes in parts under /workspace, then assemble them at the
            # target with one cat: a target outside /workspace, a large file and
            # an append all go this way, and an append never rewrites the file.
            batch = uuid.uuid4().hex
            parts = [f"{STAGING_DIR}/up-{batch}.{i}" for i in range(max(1, -(-len(data) // _UPLOAD_PART_BYTES)))]
            try:
                for i, part in enumerate(parts):
                    self._upload(part, data[i * _UPLOAD_PART_BYTES : (i + 1) * _UPLOAD_PART_BYTES])
                result = self._exec(["sh", "-c", _ASSEMBLE_SCRIPT, "sh", resolved, "append" if append else "write", *parts])
            except BaseException:
                self._discard_parts(parts)
                raise
            if result.exit_code != 0:
                raise OSError(f"could not write '{resolved}' in sandbox: {result.stderr_text.strip() or f'exit code {result.exit_code}'}")

    def _discard_parts(self, parts: list[str]) -> None:
        for part in parts:
            try:
                self._client.delete_file(self.remote_id, part)
            except Exception:
                logger.debug("Could not remove staged kosa8 upload %s", part, exc_info=True)

    def _read_bytes(self, resolved: str, limit: int, path: str) -> bytes:
        """Read a file, stopping at the first byte past ``limit``.

        Raises:
            FileNotFoundError: The file does not exist.
            OSError: ``errno.EFBIG`` past the limit, or any other read failure.
        """
        if under_files_root(resolved):
            try:
                return self._call(lambda: read_download_stream(self._client.read_file_stream(self.remote_id, resolved), limit, path))
            except Kosa8APIError as e:
                if e.status == 404 and not e.sandbox_gone:
                    raise FileNotFoundError(errno.ENOENT, "No such file", path) from e
                if e.status != 413:
                    raise OSError(f"cannot read '{path}' from sandbox: {e}") from e
                # Above the files API's request limit: stream it over exec instead.
        return self._read_via_exec(resolved, limit, path)

    def _read_via_exec(self, resolved: str, limit: int, path: str) -> bytes:
        received: list[bytes] = []
        total = 0

        def collect(stream: str, chunk: bytes) -> None:
            nonlocal total
            if stream != "stdout":
                return
            total += len(chunk)
            if total > limit:
                raise OSError(errno.EFBIG, f"File exceeds maximum download size of {limit} bytes", path)
            received.append(chunk)

        # [ -f ] first: cat of a directory or a missing path must not read as an empty file.
        script = f"if [ ! -e {shlex.quote(resolved)} ]; then echo missing >&2; exit 2; fi; [ -f {shlex.quote(resolved)} ] || {{ echo 'not a regular file' >&2; exit 1; }}; exec cat -- {shlex.quote(resolved)}"
        try:
            result = self._exec(["sh", "-c", script], on_output=collect)
        except Kosa8APIError as e:
            raise OSError(f"cannot read '{path}' from sandbox: {e}") from e
        if result.exit_code == 2 and result.stderr_text.strip() == "missing":
            raise FileNotFoundError(errno.ENOENT, "No such file", path)
        if result.exit_code != 0 or result.timed_out:
            raise OSError(f"cannot read '{path}' from sandbox: {result.stderr_text.strip() or f'exit code {result.exit_code}'}")
        return b"".join(received)

    def download_file(self, path: str, *, max_bytes: int | None = None) -> bytes:
        limit = download_limit(max_bytes, _MAX_DOWNLOAD_SIZE)
        normalized = self._guard_traversal(path)
        if normalized != VIRTUAL_PATH_PREFIX and not normalized.startswith(f"{VIRTUAL_PATH_PREFIX}/"):
            raise PermissionError(f"Access denied: path must be under '{VIRTUAL_PATH_PREFIX}': '{path}'")
        try:
            return self._read_bytes(self._resolve_path(path), limit, path)
        except OSError:
            raise
        except Exception as e:
            raise OSError(f"cannot read '{path}' from sandbox: {e}") from e

    def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
        resolved = self._resolve_path(path)
        r = self._sh(remote_list_dir_command(resolved, max_depth))
        entries = parse_remote_list_dir_output(r.stdout_text, resolved, pipeline_exit_code=r.exit_code)
        return [self._virtual_path(line) for line in entries]

    def glob(
        self,
        path: str,
        pattern: str,
        *,
        include_dirs: bool = False,
        max_results: int = 200,
    ) -> tuple[list[str], bool]:
        resolved = self._resolve_path(path)
        types = ("f", "d") if include_dirs else ("f",)
        type_expr = " -o ".join(f"-type {t}" for t in types)
        hard_limit = max(max_results * 4, max_results + 50)
        search = f"find -H {shlex.quote(resolved)} \\( {type_expr} \\) -print 2>/dev/null"
        r = self._sh(remote_search_command(search, resolved, limit=hard_limit))
        output = parse_remote_search_output(r.stdout_text, resolved, tool="find", limit=hard_limit)

        matches: list[str] = []
        root = resolved.rstrip("/") or "/"
        root_prefix = root if root == "/" else f"{root}/"
        for entry in output.text.split("\n"):
            if not entry or (entry != root and not entry.startswith(root_prefix)):
                continue
            if should_ignore_path_under_root(entry, root):
                continue
            rel_path = entry[len(root) :].lstrip("/")
            if not rel_path:
                continue
            if path_matches(pattern, rel_path):
                matches.append(self._virtual_path(entry))
                if len(matches) > max_results:
                    return matches[:max_results], True
        return matches, output.truncated

    def grep(
        self,
        path: str,
        pattern: str,
        *,
        glob: str | None = None,
        literal: bool = False,
        case_sensitive: bool = False,
        max_results: int = 100,
    ) -> tuple[list[GrepMatch], bool]:
        if not literal:
            re.compile(pattern, 0 if case_sensitive else re.IGNORECASE)

        resolved = self._resolve_path(path)
        # busybox+GNU-portable flags; -H keeps the file:line:text shape for a single-file root.
        flags = ["-r", "-H", "-n", "-I"]
        if not case_sensitive:
            flags.append("-i")
        flags.append("-F" if literal else "-E")
        total_cap = max(max_results * 4, max_results + 50)
        search = "grep " + " ".join(flags) + f" -e {shlex.quote(pattern)} {shlex.quote(resolved)} 2>/dev/null"
        r = self._sh(remote_search_command(search, resolved, limit=total_cap))
        output = parse_remote_search_output(r.stdout_text, resolved, tool="grep", limit=total_cap)

        root = resolved.rstrip("/") or "/"
        root_prefix = root if root == "/" else f"{root}/"
        matches: list[GrepMatch] = []
        for raw in output.text.split("\n"):
            try:
                file_path, line_no_str, line_text = raw.split(":", 2)
                line_number = int(line_no_str)
            except ValueError:
                continue
            if should_ignore_path_under_root(file_path, root):
                continue
            if glob is not None:
                if file_path != root and not file_path.startswith(root_prefix):
                    continue
                rel_path = posixpath.basename(file_path) if file_path == root else file_path[len(root) :].lstrip("/")
                if not path_matches(glob, rel_path):
                    continue
            matches.append(GrepMatch(path=self._virtual_path(file_path), line_number=line_number, line=truncate_line(line_text)))
            if len(matches) > max_results:
                return matches[:max_results], True
        return matches, output.truncated
