"""Tests for the kosa8 community sandbox provider.

The provider's real ``httpx`` client talks to an in-memory kosa8 API mounted as
an ``httpx.MockTransport``: sandbox create/get/delete, the files API (limited to
``/workspace``, like kosa8's), and the streamed NDJSON exec. The fake runs each
exec with a real local ``sh`` inside a temporary directory that stands in for
the sandbox's ``/workspace`` and ``/mnt``, so file, search and output-mirroring
round-trips run through the production command strings.

``test_live_kosa8_sandbox`` exercises a real kosa8 sandbox when
``KOSA8_API_KEY`` is set (``-m live``).
"""

from __future__ import annotations

import base64
import errno
import json
import os
import re
import subprocess
import threading
import types
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from deerflow.community.kosa8 import provider as provider_module
from deerflow.community.kosa8.client import Kosa8APIError, Kosa8Client
from deerflow.community.kosa8.provider import Kosa8SandboxProvider
from deerflow.community.kosa8.sandbox import Kosa8Sandbox

posix_only = pytest.mark.skipif(os.name != "posix", reason="the fake kosa8 runs commands with a local POSIX sh")


# ── Fake kosa8 API ────────────────────────────────────────────────────


class FakeKosa8:
    """An in-memory kosa8 Cloud API over a local directory."""

    FRAME = 64  # small, so multi-frame output is exercised

    def __init__(self, base: Path, *, max_file_bytes: int = 8 * 1024 * 1024) -> None:
        self.base = base
        self.max_file_bytes = max_file_bytes
        self.sandboxes: dict[str, dict] = {}
        self.requests: list[tuple[str, str, dict | None]] = []
        self.exec_bodies: list[dict] = []
        self.create_state = "running"
        self.polls_before_running = 0
        self.stream_status: int | None = None  # force an HTTP status on streamed exec
        self.exec_hook = None  # callable(argv) -> optional (status, body) override
        self.delete_status: int | None = None
        self._seq = 0
        self._lock = threading.Lock()

    # Map sandbox paths onto the local stand-in and back.
    def _root(self) -> Path:
        return self.base

    def _local(self, path: str) -> Path:
        return self.base / path.lstrip("/")

    def _sub_in(self, text: str) -> str:
        return re.sub(r"(?<![\w./-])/(workspace|mnt)(?=/|\b)", lambda m: f"{self.base}/{m.group(1)}", text)

    def _sub_out(self, data: bytes) -> bytes:
        return data.replace(f"{self.base}/".encode(), b"/")

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(lambda request: self.handle(request))

    def handle(self, request: httpx.Request) -> httpx.Response:
        url = urlsplit(str(request.url))
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        body = None
        if request.headers.get("content-type", "").startswith("application/json") and request.content:
            body = json.loads(request.content)
        self.requests.append((request.method, url.path, body))
        assert request.headers["authorization"] == "Bearer test-key"
        parts = url.path.strip("/").split("/")
        if parts[:2] != ["v1", "sandboxes"]:
            return _err(404, "not_found", "no such route")
        if len(parts) == 2 and request.method == "POST":
            return self._create(body or {})
        sid = parts[2]
        if sid not in self.sandboxes or self.sandboxes[sid]["state"] == "deleted":
            return _err(404, "not_found", "no such sandbox")
        rest = parts[3:]
        if not rest and request.method == "GET":
            box = self.sandboxes[sid]
            if box["state"] == "creating":
                box["polls"] -= 1
                if box["polls"] <= 0:
                    box["state"] = "running"
            return httpx.Response(200, json={"sandbox": dict(box)})
        if not rest and request.method == "DELETE":
            if self.delete_status:
                return _err(self.delete_status, "host_failed", "host down")
            self.sandboxes[sid]["state"] = "deleted"
            return httpx.Response(200, json={"ok": True})
        if rest == ["exec"]:
            return self._exec(body or {})
        if rest[0] == "files":
            return self._files(request, rest[1:], query)
        return _err(404, "not_found", "no such route")

    def _create(self, body: dict) -> httpx.Response:
        with self._lock:
            self._seq += 1
            sid = f"sb_{self._seq:04d}"
        state = self.create_state
        self.sandboxes[sid] = {"id": sid, "state": state, "template": "base", "name": body.get("name"), "polls": self.polls_before_running, "create": body}
        return httpx.Response(201 if state == "running" else 202, json={"sandbox": {"id": sid, "state": state, "template": "base"}})

    def _files(self, request: httpx.Request, rest: list[str], query: dict) -> httpx.Response:
        path = query.get("path") or (json.loads(request.content).get("path") if request.content else None)
        if not path or not (path == "/workspace" or path.startswith("/workspace/")):
            return _err(400, "bad_request", "path must be a workspace path")
        local = self._local(path)
        if rest == ["mkdir"]:
            local.mkdir(parents=True, exist_ok=True)
            return httpx.Response(200, json={"file": {"path": path, "type": "dir"}})
        if request.method == "PUT":
            if len(request.content) > self.max_file_bytes:
                return _err(413, "too_large", "file too large")
            if not local.parent.is_dir():
                return _err(404, "not_found", f"could not write {path}: no such directory")
            local.write_bytes(request.content)
            return httpx.Response(200, json={"file": {"path": path, "size": len(request.content)}})
        if request.method == "DELETE":
            local.unlink(missing_ok=True)
            return httpx.Response(200, json={"removed": path})
        if request.method == "GET":
            if not local.is_file():
                return _err(404, "not_found", f"could not read {path}: no such file")
            data = local.read_bytes()
            if len(data) > self.max_file_bytes:
                return _err(413, "too_large", f"could not read {path}: too large")
            return httpx.Response(200, content=data)
        return _err(405, "bad_request", "method")

    def _exec(self, body: dict) -> httpx.Response:
        self.exec_bodies.append(body)
        argv = body["cmd"]
        assert all(len(a.encode()) <= 8192 for a in argv), "kosa8 caps each argv entry at 8 KiB"
        if self.exec_hook is not None:
            override = self.exec_hook(argv)
            if override is not None:
                return override
        if body.get("stream") and self.stream_status:
            return _err(self.stream_status, "not_available", "streamed timeout is not available on this host")
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(self.base)}
        env.update(dict(item.split("=", 1) for item in body.get("env", [])))
        (self.base / "workspace").mkdir(parents=True, exist_ok=True)
        local_argv = [self._sub_in(a) for a in argv]
        timed_out = False
        try:
            proc = subprocess.run(local_argv, cwd=self.base / "workspace", env=env, capture_output=True, timeout=body.get("timeout_s"))
            stdout, stderr, code = proc.stdout, proc.stderr, proc.returncode
        except subprocess.TimeoutExpired as e:
            stdout, stderr, code, timed_out = e.stdout or b"", e.stderr or b"", None, True
        stdout, stderr = self._sub_out(stdout), self._sub_out(stderr)
        if not body.get("stream"):
            if timed_out:
                return httpx.Response(504, json={"error": "exec_timeout", "message": "timed out", "stdout": stdout.decode(), "stderr": stderr.decode()})
            return httpx.Response(200, json={"exit": code, "stdout": stdout.decode(), "stderr": stderr.decode()})
        frames = [{"stream": "stdout", "data": "", "heartbeat": True}]
        for name, data in (("stdout", stdout), ("stderr", stderr)):
            frames += [{"stream": name, "data": base64.b64encode(data[i : i + self.FRAME]).decode()} for i in range(0, len(data), self.FRAME)]
        frames.append({"done": True, "error": "timed out", "code": "exec_timeout"} if timed_out else {"done": True, "exit": code})
        return httpx.Response(200, content="".join(json.dumps(f) + "\n" for f in frames).encode(), headers={"content-type": "application/x-ndjson"})


def _err(status: int, code: str, message: str) -> httpx.Response:
    return httpx.Response(status, json={"error": code, "message": message})


def _stub_config(**attrs):
    attrs.setdefault("kosa8_api_key", "test-key")
    attrs.setdefault("idle_timeout", 0)
    return types.SimpleNamespace(sandbox=types.SimpleNamespace(**attrs))


@pytest.fixture
def fake(tmp_path: Path) -> FakeKosa8:
    return FakeKosa8(tmp_path / "box")


@pytest.fixture
def make_provider(monkeypatch, fake):
    providers: list[Kosa8SandboxProvider] = []

    def build(**attrs) -> Kosa8SandboxProvider:
        monkeypatch.setattr(provider_module, "get_app_config", lambda: _stub_config(**attrs))
        monkeypatch.setattr(Kosa8SandboxProvider, "_build_client", lambda self, key, url: Kosa8Client(api_key=key, base_url=url, transport=fake.transport()))
        monkeypatch.setattr(provider_module, "_READY_POLL_S", 0)
        p = Kosa8SandboxProvider()
        providers.append(p)
        return p

    yield build
    for p in providers:
        try:
            p.shutdown()
        except Exception:
            pass


@pytest.fixture
def box(make_provider) -> Kosa8Sandbox:
    provider = make_provider()
    sid = provider.acquire("thread-1", user_id="user-1")
    sandbox = provider.get(sid)
    assert isinstance(sandbox, Kosa8Sandbox)
    return sandbox


# ── Config ────────────────────────────────────────────────────────────


def test_missing_api_key_fails_on_acquire_with_actionable_error(make_provider, monkeypatch):
    monkeypatch.delenv("KOSA8_API_KEY", raising=False)
    provider = make_provider(kosa8_api_key=None)
    with pytest.raises(ValueError, match="KOSA8_API_KEY"):
        provider.acquire("t", user_id="u")


def test_generic_api_key_of_another_provider_is_never_used(make_provider, monkeypatch, fake):
    monkeypatch.delenv("KOSA8_API_KEY", raising=False)
    provider = make_provider(kosa8_api_key=None, api_key="other-vendor-key")
    with pytest.raises(ValueError, match="KOSA8_API_KEY"):
        provider.acquire("t", user_id="u")
    assert fake.requests == []


def test_env_api_key_is_used(make_provider, monkeypatch, fake):
    monkeypatch.setenv("KOSA8_API_KEY", " test-key\n")
    provider = make_provider(kosa8_api_key=None)
    provider.acquire("t", user_id="u")
    assert fake.sandboxes


@pytest.mark.parametrize(
    "attrs,match",
    [
        ({"environment": {"bad-key": "x"}}, "POSIX"),
        ({"size": "3x5"}, "size"),
        ({"persistent": "yes"}, "persistent"),
        ({"root_dir": "/home/user"}, "/workspace"),
    ],
)
def test_invalid_config_fails_at_startup(make_provider, attrs, match):
    with pytest.raises(ValueError, match=match):
        make_provider(**attrs)


# ── Create ────────────────────────────────────────────────────────────


@posix_only
def test_create_payload(make_provider, fake):
    provider = make_provider(image="python:3.12", size="4x8", region="eu", persistent=True, environment={"PYTHONUNBUFFERED": "1"}, sleep_after_s=600)
    sid = provider.acquire("thread-1", user_id="user-1")
    created = next(iter(fake.sandboxes.values()))["create"]
    assert created == {
        "name": f"deer-flow-{sid}",
        "image": "python:3.12",
        "size": "4x8",
        "region": "eu",
        "sleep_after_s": 600,
        "persistent": True,
        "ttl_s": provider_module.DEFAULT_TTL_S,
        "env": {"PYTHONUNBUFFERED": "1"},
    }


@posix_only
def test_ttl_zero_omits_lifetime(make_provider, fake):
    make_provider(ttl_s=0).acquire("t", user_id="u")
    assert "ttl_s" not in next(iter(fake.sandboxes.values()))["create"]


@posix_only
def test_cold_boot_polls_until_running_then_bootstraps(make_provider, fake):
    fake.create_state = "creating"
    fake.polls_before_running = 3
    provider = make_provider()
    sid = provider.acquire("t", user_id="u")
    gets = [r for r in fake.requests if r[0] == "GET" and r[1].endswith(next(iter(fake.sandboxes)))]
    assert len(gets) == 3
    assert (fake.base / "workspace/user-data/outputs").is_dir()
    assert (fake.base / "mnt/user-data").resolve() == (fake.base / "workspace/user-data").resolve()
    assert provider.get(sid) is not None


@posix_only
def test_failed_bootstrap_deletes_the_remote(make_provider, fake):
    fake.exec_hook = lambda argv: httpx.Response(200, content=b'{"done": true, "exit": 1}\n') if "BOOTSTRAP_OK" in argv[-1] else None
    provider = make_provider()
    with pytest.raises(RuntimeError, match="bootstrap"):
        provider.acquire("t", user_id="u")
    assert [b["state"] for b in fake.sandboxes.values()] == ["deleted"]


def test_sandbox_that_fails_to_start_is_deleted(make_provider, fake):
    fake.create_state = "creating"
    fake.polls_before_running = 99

    def fail_on_get(request, original=fake.handle):
        response = original(request)
        if request.method == "GET" and "/files" not in request.url.path:
            for box in fake.sandboxes.values():
                if box["state"] == "creating":
                    box["state"] = "error"
        return response

    fake.handle = fail_on_get
    provider = make_provider()
    with pytest.raises(RuntimeError, match="did not start"):
        provider.acquire("t", user_id="u")
    assert [b["state"] for b in fake.sandboxes.values()] == ["deleted"]


# ── Commands ──────────────────────────────────────────────────────────


@posix_only
def test_execute_command_sends_env_and_timeout_structurally(box, fake):
    out = box.execute_command('printf %s "$TOKEN"', env={"TOKEN": "s3cr$t 'quoted'"}, timeout=5.2)
    assert out == "s3cr$t 'quoted'"
    body = fake.exec_bodies[-1]
    assert body["cmd"] == ["sh", "-lc", 'printf %s "$TOKEN"']
    assert body["env"] == ["TOKEN=s3cr$t 'quoted'"]
    assert body["timeout_s"] == 6
    assert body["stream"] is True


@posix_only
def test_execute_command_reports_nonzero_exit_with_output(box):
    assert box.execute_command("echo out; echo err >&2; exit 3") == "out\n\nerr\n\nExit Code: 3"
    assert box.execute_command("exit 4") == "Command exited with code 4"
    assert box.execute_command("true") == "(no output)"


@posix_only
def test_execute_command_timeout_keeps_partial_output(box):
    out = box.execute_command("echo partial; sleep 5", timeout=1)
    assert out.startswith("partial")
    assert out.endswith("Error: command timed out\nExit Code: 124")


@posix_only
def test_streamed_timeout_unavailable_falls_back_to_buffered_exec(box, fake):
    fake.stream_status = 501
    assert box.execute_command("echo hi", timeout=500) == "hi\n"
    assert "stream" not in fake.exec_bodies[-1]
    assert fake.exec_bodies[-1]["timeout_s"] == 90


@posix_only
def test_long_command_is_staged_as_a_script(box, fake):
    payload = "x" * 20000
    out = box.execute_command(f"printf %s '{payload}' | wc -c")
    assert out.strip() == "20000"
    assert fake.exec_bodies[-1]["cmd"][:2] == ["sh", "-c"]
    assert not list((fake.base / "workspace/.deer-flow-staging").glob("cmd-*"))


def test_execute_command_rejects_invalid_env_key(box):
    with pytest.raises(ValueError):
        box.execute_command("true", env={"bad-key": "x"})


def test_api_error_is_returned_as_text_without_the_key(box, fake):
    fake.exec_hook = lambda argv: _err(402, "payment_required", "add a payment method; key test-key")
    out = box.execute_command("true")
    assert out.startswith("Error: kosa8: add a payment method")
    assert "test-key" not in out


# ── Files ─────────────────────────────────────────────────────────────


@posix_only
def test_file_round_trip_under_virtual_prefix(box, fake):
    box.write_file("/mnt/user-data/workspace/a/b.txt", "héllo\n")
    assert (fake.base / "workspace/user-data/workspace/a/b.txt").read_text(encoding="utf-8") == "héllo\n"
    assert box.read_file("/mnt/user-data/workspace/a/b.txt") == "héllo\n"
    box.write_file("/mnt/user-data/workspace/a/b.txt", "more\n", append=True)
    assert box.read_file("/mnt/user-data/workspace/a/b.txt") == "héllo\nmore\n"
    assert box.read_file("/mnt/user-data/workspace/a/b.txt", start_line=2, end_line=2) == "more"


@posix_only
def test_append_to_missing_file_creates_it(box):
    box.write_file("/mnt/user-data/outputs/new.log", "first", append=True)
    assert box.read_file("/mnt/user-data/outputs/new.log") == "first"


@posix_only
def test_large_binary_write_and_read_go_in_parts(box, fake, monkeypatch):
    monkeypatch.setattr("deerflow.community.kosa8.sandbox._UPLOAD_PART_BYTES", 1000)
    fake.max_file_bytes = 1000
    data = bytes(range(256)) * 20
    box.update_file("/mnt/user-data/outputs/blob.bin", data)
    puts = [r for r in fake.requests if r[0] == "PUT"]
    assert len(puts) >= 6
    assert box.download_file("/mnt/user-data/outputs/blob.bin") == data
    assert not list((fake.base / "workspace/.deer-flow-staging").glob("up-*"))


@posix_only
def test_paths_outside_workspace_use_exec(box, fake):
    target = "/mnt/scratch/notes.txt"
    box.write_file(target, "outside")
    assert (fake.base / "mnt/scratch/notes.txt").read_text() == "outside"
    assert box.read_file(target) == "outside"


@posix_only
def test_missing_file(box):
    assert box.read_file("/mnt/user-data/workspace/nope.txt").startswith("Error:")
    with pytest.raises(FileNotFoundError):
        box.download_file("/mnt/user-data/workspace/nope.txt")


def test_download_guards_reject_before_any_request(box, fake):
    before = len(fake.requests)
    with pytest.raises(PermissionError):
        box.download_file("/etc/passwd")
    with pytest.raises(PermissionError):
        box.download_file("/mnt/user-data/../etc/passwd")
    assert len(fake.requests) == before


@posix_only
def test_download_cap_is_enforced_while_reading(box):
    box.write_file("/mnt/user-data/outputs/big.txt", "x" * 5000)
    with pytest.raises(OSError) as info:
        box.download_file("/mnt/user-data/outputs/big.txt", max_bytes=100)
    assert info.value.errno == errno.EFBIG


@posix_only
def test_download_cap_over_exec_stream(box, fake):
    fake.max_file_bytes = 10  # forces the exec fallback
    box.write_file("/mnt/user-data/outputs/big.txt", "x" * 5)
    (fake.base / "workspace/user-data/outputs/big.txt").write_text("x" * 5000)
    with pytest.raises(OSError) as info:
        box.download_file("/mnt/user-data/outputs/big.txt", max_bytes=100)
    assert info.value.errno == errno.EFBIG


# ── Search ────────────────────────────────────────────────────────────


@posix_only
def test_list_glob_grep_report_virtual_paths(box):
    box.write_file("/mnt/user-data/workspace/src/app.py", "import os\nprint('needle')\n")
    box.write_file("/mnt/user-data/workspace/src/readme.md", "needle here\n")
    listing = box.list_dir("/mnt/user-data/workspace")
    assert "/mnt/user-data/workspace/src/app.py" in listing
    matches, truncated = box.glob("/mnt/user-data/workspace", "**/*.py")
    assert matches == ["/mnt/user-data/workspace/src/app.py"] and not truncated
    hits, _ = box.grep("/mnt/user-data/workspace", "needle", glob="**/*.py")
    assert [(h.path, h.line_number) for h in hits] == [("/mnt/user-data/workspace/src/app.py", 2)]


@posix_only
def test_list_dir_missing_raises(box):
    with pytest.raises(FileNotFoundError):
        box.list_dir("/mnt/user-data/missing")


# ── Terminal failures ─────────────────────────────────────────────────


def test_sandbox_gone_evicts_and_next_acquire_rebuilds(make_provider, fake):
    provider = make_provider()
    fake.exec_hook = None
    sid = provider.acquire("t", user_id="u")
    sandbox = provider.get(sid)
    remote = sandbox.remote_id
    fake.sandboxes[remote]["state"] = "deleted"
    assert sandbox.execute_command("true").startswith("Error:")
    assert provider.get(sid) is None
    provider.acquire("t", user_id="u")
    assert provider.get(sid).remote_id != remote


def test_missing_file_404_does_not_evict(box, make_provider):
    with pytest.raises(OSError):
        box.download_file("/mnt/user-data/workspace/none")
    assert not box.is_closed


def test_sandbox_gone_is_told_apart_from_missing_file():
    assert Kosa8APIError("kosa8: no such sandbox (HTTP 404, not_found)", status=404, code="not_found").sandbox_gone
    assert not Kosa8APIError("kosa8: could not read /workspace/x: no such file (HTTP 404, not_found)", status=404, code="not_found").sandbox_gone
    assert Kosa8APIError("kosa8: the sandbox is deleted, not running (HTTP 409, not_running)", status=409, code="not_running").sandbox_gone


# ── Lifecycle ─────────────────────────────────────────────────────────


def test_sandbox_id_is_the_shared_scope_token():
    from deerflow.sandbox.identity import derive_sandbox_scope_token

    assert Kosa8SandboxProvider._sandbox_id("t-1", "u-1") == derive_sandbox_scope_token(user_id="u-1", thread_id="t-1")


@posix_only
def test_release_parks_and_same_scope_reclaims(make_provider, fake):
    provider = make_provider()
    sid = provider.acquire("t", user_id="u")
    provider.release(sid)
    assert provider.get(sid) is None and sid in provider._warm_pool
    assert provider.acquire("t", user_id="u") == sid
    assert len(fake.sandboxes) == 1


@posix_only
def test_other_scope_never_reclaims(make_provider, fake):
    provider = make_provider()
    sid = provider.acquire("t", user_id="u")
    provider.release(sid)
    other = provider.acquire("t", user_id="someone-else")
    assert other != sid and len(fake.sandboxes) == 2


@posix_only
def test_get_scoped_fails_closed_for_another_identity(make_provider):
    provider = make_provider()
    sid = provider.acquire("t", user_id="u")
    assert provider.get_scoped(sid, thread_id="t", user_id="u") is not None
    assert provider.get_scoped(sid, thread_id="t", user_id="v") is None


@posix_only
def test_unhealthy_warm_sandbox_is_deleted_and_replaced(make_provider, fake):
    provider = make_provider()
    sid = provider.acquire("t", user_id="u")
    old = provider.get(sid).remote_id
    provider.release(sid)
    fake.exec_hook = lambda argv: httpx.Response(200, content=b'{"done": true, "exit": 1}\n') if argv[-1] == "echo ok" else None
    provider.acquire("t", user_id="u")
    assert fake.sandboxes[old]["state"] == "deleted"
    assert provider.get(sid).remote_id != old


@posix_only
def test_shutdown_deletes_active_and_warm(make_provider, fake):
    provider = make_provider()
    a = provider.acquire("a", user_id="u")
    provider.acquire("b", user_id="u")
    provider.release(a)
    provider.shutdown()
    assert {b["state"] for b in fake.sandboxes.values()} == {"deleted"}
    with pytest.raises(RuntimeError):
        provider.acquire("c", user_id="u")


@posix_only
def test_failed_delete_stays_pending_and_shutdown_can_retry(make_provider, fake):
    provider = make_provider()
    provider.acquire("a", user_id="u")
    fake.delete_status = 502
    with pytest.raises(RuntimeError, match="incomplete"):
        provider.shutdown()
    fake.delete_status = None
    provider.shutdown()
    assert {b["state"] for b in fake.sandboxes.values()} == {"deleted"}


@posix_only
def test_sandbox_created_after_shutdown_is_deleted_not_published(make_provider, fake):
    provider = make_provider()
    started, proceed = threading.Event(), threading.Event()
    original = fake.handle

    def gated(request):
        if request.method == "POST" and request.url.path == "/v1/sandboxes":
            started.set()
            assert proceed.wait(5)
        return original(request)

    fake.handle = gated
    errors: list[BaseException] = []

    def run():
        try:
            provider.acquire("t", user_id="u")
        except BaseException as e:  # noqa: BLE001 - recorded for the assertion below
            errors.append(e)

    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert started.wait(5)
        with pytest.raises(RuntimeError, match="incomplete"):
            provider.shutdown()
    finally:
        proceed.set()
        worker.join(5)
    assert errors and "retired" in str(errors[0])
    assert {b["state"] for b in fake.sandboxes.values()} == {"deleted"}
    provider.shutdown()


# ── Output mirroring ──────────────────────────────────────────────────


@posix_only
def test_release_mirrors_outputs_to_the_owner_thread_dir(make_provider, fake, tmp_path, monkeypatch):
    host_outputs = tmp_path / "host" / "outputs"
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: types.SimpleNamespace(sandbox_outputs_dir=lambda thread_id, user_id=None: host_outputs))
    provider = make_provider()
    sid = provider.acquire("t", user_id="u")
    box = provider.get(sid)
    box.write_file("/mnt/user-data/outputs/report.md", "# report\n")
    box.update_file("/mnt/user-data/outputs/charts/plot.png", b"\x89PNG\x00\xff")
    box.write_file("/mnt/user-data/outputs/bad:name.txt", "skipped")
    box.write_file("/mnt/user-data/workspace/not-an-output.txt", "x")
    provider.release(sid)
    mirrored = sorted(p.relative_to(host_outputs).as_posix() for p in host_outputs.rglob("*") if p.is_file())
    assert mirrored == ["charts/plot.png", "report.md"]
    assert (host_outputs / "charts/plot.png").read_bytes() == b"\x89PNG\x00\xff"
    assert sid in provider._warm_pool


def test_names_that_collide_on_some_hosts_are_ambiguous():
    from deerflow.community.kosa8.outputs import _ambiguous

    names = ["Report/a.txt", "report/b.txt", "caf\u00e9.txt", "cafe\u0301.txt", "plain.txt"]
    assert _ambiguous(names) == {"Report/a.txt", "report/b.txt", "caf\u00e9.txt", "cafe\u0301.txt"}


@posix_only
def test_mirroring_never_writes_through_a_linked_destination(make_provider, tmp_path, monkeypatch):
    host_outputs = tmp_path / "host" / "outputs"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    host_outputs.mkdir(parents=True)
    (host_outputs / "charts").symlink_to(elsewhere)
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: types.SimpleNamespace(sandbox_outputs_dir=lambda thread_id, user_id=None: host_outputs))
    provider = make_provider()
    sid = provider.acquire("t", user_id="u")
    provider.get(sid).write_file("/mnt/user-data/outputs/charts/x.txt", "x")
    provider.release(sid)
    assert list(elsewhere.iterdir()) == []


# ── Live ──────────────────────────────────────────────────────────────


@pytest.mark.live
@pytest.mark.skipif(not os.getenv("KOSA8_API_KEY"), reason="requires a real kosa8 API key (KOSA8_API_KEY); calls kosa8 Cloud")
def test_live_kosa8_sandbox(monkeypatch, tmp_path):
    monkeypatch.setattr(provider_module, "get_app_config", lambda: types.SimpleNamespace(sandbox=types.SimpleNamespace(idle_timeout=0, ttl_s=900)))
    host_outputs = tmp_path / "outputs"
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: types.SimpleNamespace(sandbox_outputs_dir=lambda thread_id, user_id=None: host_outputs))
    provider = Kosa8SandboxProvider()
    try:
        sid = provider.acquire("live-thread", user_id="live-user")
        box = provider.get(sid)
        assert box is not None
        assert "42" in box.execute_command("python3 -c 'print(6 * 7)' 2>/dev/null || echo 42")
        assert box.execute_command('printf %s "$SECRET"', env={"SECRET": "s3"}) == "s3"
        assert box.execute_command("sleep 5", timeout=2).endswith("Exit Code: 124")
        box.write_file("/mnt/user-data/workspace/it.txt", "kosa8-e2e\n")
        box.write_file("/mnt/user-data/workspace/it.txt", "more\n", append=True)
        assert box.read_file("/mnt/user-data/workspace/it.txt") == "kosa8-e2e\nmore\n"
        assert "kosa8-e2e" in box.execute_command("cat /mnt/user-data/workspace/it.txt")
        long = "y" * 20000
        assert box.execute_command(f"printf %s '{long}' | wc -c").strip() == "20000"
        hits, _ = box.grep("/mnt/user-data/workspace", "kosa8-e2e")
        assert [h.path for h in hits] == ["/mnt/user-data/workspace/it.txt"]
        box.update_file("/mnt/user-data/outputs/out.bin", b"\x00\x01binary")
        provider.release(sid)
        assert (host_outputs / "out.bin").read_bytes() == b"\x00\x01binary"
        assert provider.acquire("live-thread", user_id="live-user") == sid
    finally:
        provider.shutdown()
