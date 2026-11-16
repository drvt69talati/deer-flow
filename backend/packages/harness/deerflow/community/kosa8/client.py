"""Small synchronous client for the kosa8 Cloud REST API.

kosa8 has no Python SDK that fits this provider (the published one is sync
only and its exec takes no timeout), so the provider talks to the REST API with
the ``httpx`` client the harness already depends on. Only the calls the
provider uses are here: sandbox create/get/delete, streamed exec, and the
files API.

Two limits of the API shape the callers:

- The files API reaches ``/workspace`` only, and moves at most 8 MiB per
  request.
- Each ``cmd`` entry of an exec is at most 8 KiB. Callers stage longer scripts
  and payloads through the files API.
"""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

DEFAULT_BASE_URL = "https://api.kosa8.com"
# The directory the files API can reach.
FILES_ROOT = "/workspace"
# The files API answers 413 above this; larger transfers go in parts.
MAX_FILE_REQUEST_BYTES = 8 * 1024 * 1024
# Each exec argv entry is capped by the API.
MAX_ARG_BYTES = 8 * 1024
# A buffered exec (the fallback when a host has no streamed timeout) runs at most this long.
MAX_BUFFERED_TIMEOUT_S = 90
_REQUEST_TIMEOUT_S = 30.0
# A streamed exec sends a heartbeat frame every 15 s while the command is quiet.
_STREAM_READ_TIMEOUT_S = 120.0
_USER_AGENT = "deer-flow"


class Kosa8APIError(RuntimeError):
    """A kosa8 API call failed. The message never contains the API key."""

    def __init__(self, message: str, *, status: int | None = None, code: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code

    @property
    def sandbox_gone(self) -> bool:
        """Whether kosa8 reports the sandbox itself as absent or no longer running.

        A file-path 404 also answers ``not_found``; only the sandbox lookup says
        "no such sandbox", so the message tells the two apart.
        """
        if self.status == 404 and self.code == "not_found":
            return "no such sandbox" in str(self).lower()
        return self.status == 409 and self.code == "not_running"


@dataclass
class ExecResult:
    """The outcome of one command."""

    exit_code: int | None
    stdout: bytes
    stderr: bytes
    timed_out: bool = False

    @property
    def stdout_text(self) -> str:
        return self.stdout.decode("utf-8", errors="replace")

    @property
    def stderr_text(self) -> str:
        return self.stderr.decode("utf-8", errors="replace")


class Kosa8Client:
    """Thread-safe REST client (``httpx.Client`` is safe to share across threads)."""

    def __init__(self, *, api_key: str, base_url: str = DEFAULT_BASE_URL, transport: httpx.BaseTransport | None = None) -> None:
        self._api_key = api_key
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": _USER_AGENT},
            timeout=_REQUEST_TIMEOUT_S,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    # ── sandboxes ───────────────────────────────────────────────────────

    def create_sandbox(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _sandbox_of(self._request("POST", "/v1/sandboxes", json=payload))

    def get_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return _sandbox_of(self._request("GET", f"/v1/sandboxes/{_seg(sandbox_id)}"))

    def delete_sandbox(self, sandbox_id: str) -> None:
        self._request("DELETE", f"/v1/sandboxes/{_seg(sandbox_id)}")

    # ── files (``/workspace`` only) ──────────────────────────────────────

    def mkdir(self, sandbox_id: str, path: str) -> None:
        self._request("POST", f"/v1/sandboxes/{_seg(sandbox_id)}/files/mkdir", json={"path": path})

    def write_file(self, sandbox_id: str, path: str, data: bytes) -> None:
        self._request(
            "PUT",
            f"/v1/sandboxes/{_seg(sandbox_id)}/files",
            params={"path": path},
            content=data,
            headers={"Content-Type": "application/octet-stream"},
        )

    def delete_file(self, sandbox_id: str, path: str) -> None:
        self._request("DELETE", f"/v1/sandboxes/{_seg(sandbox_id)}/files", params={"path": path})

    def read_file_stream(self, sandbox_id: str, path: str) -> Iterator[bytes]:
        """Yield a file's bytes as they arrive, so callers can stop at a byte cap.

        Raises :class:`Kosa8APIError` before the first chunk when kosa8 refuses
        the read (404 for a missing file, 413 for one above 8 MiB).
        """
        try:
            with self._http.stream("GET", f"/v1/sandboxes/{_seg(sandbox_id)}/files", params={"path": path}) as response:
                if response.is_error:
                    response.read()
                    raise self._error_for(response)
                yield from response.iter_bytes()
        except httpx.HTTPError as exc:
            raise self._transport_error("read", exc) from exc

    # ── exec ────────────────────────────────────────────────────────────

    def exec(
        self,
        sandbox_id: str,
        argv: list[str],
        *,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
        on_output: Callable[[str, bytes], None] | None = None,
    ) -> ExecResult:
        """Run ``argv`` over the streamed exec and return its outcome.

        ``on_output(stream, data)`` sees each stdout/stderr chunk as it arrives;
        raising from it stops reading and closes the stream. A timeout comes back
        as ``timed_out=True`` with the output printed before it.

        A host without a streamed timeout answers 501 before the command starts;
        the call then falls back to a buffered exec, which kosa8 holds to 90 s.
        """
        body: dict[str, Any] = {"cmd": list(argv), "stream": True}
        if env:
            body["env"] = [f"{name}={value}" for name, value in env.items()]
        if timeout is not None and timeout > 0:
            body["timeout_s"] = max(1, min(86400, math.ceil(timeout)))
        try:
            return self._exec_stream(sandbox_id, body, on_output)
        except Kosa8APIError as exc:
            if exc.status != 501:
                raise
        body.pop("stream")
        if "timeout_s" in body:
            body["timeout_s"] = min(body["timeout_s"], MAX_BUFFERED_TIMEOUT_S)
        return self._exec_buffered(sandbox_id, body, on_output)

    def _exec_stream(self, sandbox_id: str, body: dict[str, Any], on_output: Callable[[str, bytes], None] | None) -> ExecResult:
        timeout = httpx.Timeout(_REQUEST_TIMEOUT_S, read=_STREAM_READ_TIMEOUT_S)
        try:
            with self._http.stream("POST", f"/v1/sandboxes/{_seg(sandbox_id)}/exec", json=body, timeout=timeout) as response:
                if response.is_error:
                    response.read()
                    raise self._error_for(response)
                return _read_frames(response.iter_lines(), on_output)
        except httpx.HTTPError as exc:
            raise self._transport_error("exec", exc) from exc

    def _exec_buffered(self, sandbox_id: str, body: dict[str, Any], on_output: Callable[[str, bytes], None] | None) -> ExecResult:
        timeout = httpx.Timeout(_REQUEST_TIMEOUT_S, read=MAX_BUFFERED_TIMEOUT_S + _REQUEST_TIMEOUT_S)
        try:
            response = self._http.post(f"/v1/sandboxes/{_seg(sandbox_id)}/exec", json=body, timeout=timeout)
        except httpx.HTTPError as exc:
            raise self._transport_error("exec", exc) from exc
        data = _json_or_empty(response)
        timed_out = response.status_code == 504 and data.get("error") == "exec_timeout"
        if response.is_error and not timed_out:
            raise self._error_for(response)
        stdout = str(data.get("stdout") or "").encode()
        stderr = str(data.get("stderr") or "").encode()
        if on_output is not None:
            for name, chunk in (("stdout", stdout), ("stderr", stderr)):
                if chunk:
                    on_output(name, chunk)
        exit_code = data.get("exit")
        return ExecResult(exit_code if isinstance(exit_code, int) else None, stdout, stderr, timed_out)

    # ── plumbing ────────────────────────────────────────────────────────

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = self._http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise self._transport_error(method, exc) from exc
        if response.is_error:
            raise self._error_for(response)
        return _json_or_empty(response)

    def _error_for(self, response: httpx.Response) -> Kosa8APIError:
        data = _json_or_empty(response)
        code = data.get("error") if isinstance(data.get("error"), str) else None
        message = data.get("message") if isinstance(data.get("message"), str) else (response.reason_phrase or "request failed")
        text = self._redact(f"kosa8: {message} (HTTP {response.status_code}{f', {code}' if code else ''})")
        return Kosa8APIError(text, status=response.status_code, code=code)

    def _transport_error(self, what: str, exc: Exception) -> Kosa8APIError:
        return Kosa8APIError(self._redact(f"kosa8 {what} failed: {type(exc).__name__}: {exc}"))

    def _redact(self, text: str) -> str:
        from deerflow.community.provider_credentials import redact_secret

        return redact_secret(text, self._api_key)


def _read_frames(lines: Iterator[str], on_output: Callable[[str, bytes], None] | None) -> ExecResult:
    """Read kosa8's NDJSON exec frames.

    Frames are ``{"stream": "stdout"|"stderr", "data": <base64>}`` (an empty
    ``heartbeat`` frame while the command is quiet), then ``{"done": true,
    "exit": <code>}`` or ``{"done": true, "error": <text>, "code": <code>}``.
    """
    out: dict[str, list[bytes]] = {"stdout": [], "stderr": []}
    for raw in lines:
        if not raw.strip():
            continue
        try:
            frame = json.loads(raw)
        except ValueError as exc:
            raise Kosa8APIError("kosa8 exec stream sent an unreadable frame") from exc
        if not isinstance(frame, dict):
            raise Kosa8APIError("kosa8 exec stream sent an unreadable frame")
        if frame.get("done"):
            stdout, stderr = b"".join(out["stdout"]), b"".join(out["stderr"])
            if frame.get("error"):
                if frame.get("code") == "exec_timeout":
                    return ExecResult(None, stdout, stderr, timed_out=True)
                raise Kosa8APIError(f"kosa8: command did not finish: {frame['error']}", code=frame.get("code") or None)
            exit_code = frame.get("exit")
            return ExecResult(exit_code if isinstance(exit_code, int) else None, stdout, stderr)
        stream = frame.get("stream")
        data = frame.get("data")
        if stream in out and isinstance(data, str) and data:
            chunk = base64.b64decode(data)
            if on_output is not None:
                on_output(stream, chunk)
            out[stream].append(chunk)
    raise Kosa8APIError("kosa8 exec stream ended without a final frame")


def _json_or_empty(response: httpx.Response) -> dict[str, Any]:
    if not response.content:
        return {}
    try:
        data = response.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _sandbox_of(data: dict[str, Any]) -> dict[str, Any]:
    sandbox = data.get("sandbox")
    if not isinstance(sandbox, dict) or not isinstance(sandbox.get("id"), str):
        raise Kosa8APIError("kosa8 answered without a sandbox")
    return sandbox


def _seg(value: str) -> str:
    return quote(value, safe="")


def under_files_root(path: str) -> bool:
    """Whether the files API can reach ``path``."""
    return path == FILES_ROOT or path.startswith(f"{FILES_ROOT}/")
