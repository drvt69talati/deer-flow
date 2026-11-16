"""``Kosa8SandboxProvider`` — DeerFlow :class:`SandboxProvider` backed by kosa8 Cloud.

Each DeerFlow sandbox is a `kosa8 <https://kosa8.com>`_ microVM created through
the REST API, one per ``(user, thread)``, reused within the process and parked
in a warm pool (the shared :class:`WarmPoolLifecycleMixin`) when released.
Before a user/thread sandbox is parked, its ``/mnt/user-data/outputs`` files are
mirrored to the owner's host thread directory so the Gateway can serve them.

Config is read off :class:`SandboxConfig` (``extra="allow"``); see this
package's README for the keys. The credential keys carry a ``kosa8_`` prefix so
that a key left in ``config.yaml`` for another provider is never sent to kosa8.
"""

from __future__ import annotations

import atexit
import logging
import os
import shlex
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from typing import Any

from deerflow.config import get_app_config
from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.sandbox.acquire_serialization import AcquireSerializer, AcquireSerializerClosed
from deerflow.sandbox.identity import derive_sandbox_scope_token
from deerflow.sandbox.sandbox import Sandbox, _validate_extra_env
from deerflow.sandbox.sandbox_provider import SandboxProvider

from ..provider_credentials import resolve_api_key
from ..warm_pool_lifecycle import WarmPoolLifecycleMixin
from .client import DEFAULT_BASE_URL, Kosa8Client, under_files_root
from .outputs import sync_outputs_to_host
from .sandbox import DEFAULT_KOSA8_ROOT_DIR, Kosa8Sandbox

logger = logging.getLogger(__name__)

API_KEY_ENV_VAR = "KOSA8_API_KEY"
BASE_URL_ENV_VAR = "KOSA8_API_URL"
_SANDBOX_NAME_PREFIX = "deer-flow-"
# kosa8 sandboxes live until deleted. DeerFlow's warm pool deletes the ones it
# parks, but a Gateway that crashes cannot, so give each a lifetime that
# outlives a long research thread. sandbox.ttl_s overrides it; 0 means none.
DEFAULT_TTL_S = 24 * 60 * 60
# A cold-booted sandbox (one with its own image or size) answers 202 and boots.
DEFAULT_READY_TIMEOUT = 300.0
_READY_POLL_S = 2.0
_BOOTSTRAP_TIMEOUT = 30.0
_SIZES = frozenset({"2x4", "4x8", "8x16", "16x32"})
_GONE_STATES = frozenset({"deleted", "stopped", "error", "host_removed", "host_lost"})


def _bootstrap_script(root_dir: str) -> str:
    """Create DeerFlow's directories under the root and link ``/mnt/user-data`` to it.

    The directories are required: without them the first write fails. The link
    is best effort, so commands that name the literal ``/mnt/user-data`` path
    work wherever the image lets the sandbox user create it; the file APIs use
    the root directly either way.
    """
    root = shlex.quote(root_dir)
    link = shlex.quote(VIRTUAL_PATH_PREFIX)
    return (
        f"mkdir -p {root}/workspace {root}/uploads {root}/outputs || exit 1; "
        f"if [ ! -e {link} ] || [ -L {link} ]; then "
        f"mkdir -p {shlex.quote(os.path.dirname(VIRTUAL_PATH_PREFIX))} 2>/dev/null; ln -sfn {root} {link} 2>/dev/null || true; "
        f"fi; echo BOOTSTRAP_OK"
    )


class _ShutdownIncompleteError(RuntimeError):
    """Cleanup still owns acquisitions or remote deletions that need retry."""


class Kosa8SandboxProvider(WarmPoolLifecycleMixin[Kosa8Sandbox], SandboxProvider):
    """Run each DeerFlow sandbox as a kosa8 Cloud microVM."""

    uses_thread_data_mounts = False
    needs_upload_permission_adjustment = True
    _idle_checker_thread_name = "kosa8-idle-reaper"

    @staticmethod
    def _sandbox_id(thread_id: str, user_id: str) -> str:
        return derive_sandbox_scope_token(user_id=user_id, thread_id=thread_id)

    # ── Provider lifecycle ───────────────────────────────────────────────

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sandboxes: dict[str, Kosa8Sandbox] = {}
        self._thread_sandboxes: dict[tuple[str, str], str] = {}
        self._warm_pool: dict[str, tuple[Kosa8Sandbox, float]] = {}
        self._acquire_serializer: AcquireSerializer[str] = AcquireSerializer(thread_name_prefix="kosa8-acquire-wait")
        self._idle_checker_stop = threading.Event()
        self._idle_checker_thread: threading.Thread | None = None
        self._shutdown_called = False
        self._shutdown_complete = False
        self._shutdown_lock = threading.Lock()
        self._accepting = True
        self._acquires = 0
        # Keyed by object identity: a replacement can share the logical id while
        # an older remote still needs deleting.
        self._pending_closes: dict[int, Kosa8Sandbox] = {}
        self._closing: set[int] = set()
        self._client: Kosa8Client | None = None
        self._config = self._load_config()
        atexit.register(self._shutdown_at_exit)
        self._start_idle_checker()

    def _load_config(self) -> dict[str, Any]:
        sandbox_config = get_app_config().sandbox

        def _opt(name: str, default: Any = None) -> Any:
            return getattr(sandbox_config, name, default)

        environment = dict(_opt("environment") or {})
        # Fail at startup on a bad name rather than on the first create.
        _validate_extra_env(environment)
        size = _opt("size")
        if size is not None and size not in _SIZES:
            raise ValueError(f"sandbox.size must be one of {sorted(_SIZES)}, got {size!r}")
        persistent = _opt("persistent")
        if persistent is not None and not isinstance(persistent, bool):
            raise ValueError("sandbox.persistent must be true or false")
        root_dir = (_opt("root_dir") or DEFAULT_KOSA8_ROOT_DIR).rstrip("/")
        if not under_files_root(root_dir):
            raise ValueError(f"sandbox.root_dir must be under /workspace (the kosa8 files API reaches only /workspace), got {root_dir!r}")
        ttl_s = _opt("ttl_s")
        replicas = _opt("replicas")
        idle_timeout = _opt("idle_timeout")
        return {
            "api_key": _opt("kosa8_api_key"),
            "base_url": _opt("kosa8_base_url") or os.environ.get(BASE_URL_ENV_VAR) or DEFAULT_BASE_URL,
            "template": _opt("template"),
            "image": _opt("image"),
            "size": size,
            "region": _opt("region"),
            "persistent": bool(persistent),
            "sleep_after_s": _opt("sleep_after_s"),
            "ttl_s": int(ttl_s if ttl_s is not None else DEFAULT_TTL_S),
            "ready_timeout": float(_opt("ready_timeout") or DEFAULT_READY_TIMEOUT),
            "root_dir": root_dir,
            "environment": environment,
            "replicas": replicas if replicas is not None else self.DEFAULT_REPLICAS,
            "idle_timeout": idle_timeout if idle_timeout is not None else self.DEFAULT_IDLE_TIMEOUT,
        }

    def _get_client(self) -> Kosa8Client:
        with self._lock:
            if self._client is not None:
                return self._client
        api_key = resolve_api_key(self._config["api_key"]) or resolve_api_key(os.environ.get(API_KEY_ENV_VAR))
        if not api_key:
            raise ValueError(f"Kosa8SandboxProvider needs an API key: set sandbox.kosa8_api_key in config.yaml or the {API_KEY_ENV_VAR} environment variable (create one at https://app.kosa8.com).")
        # Built on the acquire worker, never on the event loop: TLS setup is synchronous.
        client = self._build_client(api_key, self._config["base_url"])
        with self._lock:
            if self._client is None:
                self._client = client
                return client
        client.close()
        with self._lock:
            return self._client  # type: ignore[return-value]

    def _build_client(self, api_key: str, base_url: str) -> Kosa8Client:
        """Seam for tests, which pass an in-memory transport."""
        return Kosa8Client(api_key=api_key, base_url=base_url)

    @staticmethod
    def _thread_key(thread_id: str, user_id: str | None) -> tuple[str, str]:
        return (user_id or "", thread_id)

    def _start_idle_checker(self) -> None:
        """Start idle cleanup when enabled; idle_timeout=0 keeps it disabled."""
        if self._config["idle_timeout"] <= 0:
            return
        super()._start_idle_checker()

    def _active_count_locked(self) -> int:
        return len(self._sandboxes) + len(self._pending_closes)

    def _cleanup_idle_resources(self, idle_timeout: float) -> None:
        self._retry_pending_closes()
        super()._cleanup_idle_resources(idle_timeout)

    def _destroy_warm_entry(self, sandbox_id: str, entry: Kosa8Sandbox, *, reason: str) -> None:
        self._close_quietly(entry, context=f"warm pool, reason={reason}")

    def _invalidate_sandbox(self, sandbox_id: str, reason: str, *, expected_sandbox: Kosa8Sandbox | None = None) -> None:
        """Deregister and delete a sandbox kosa8 reports gone, so the next acquire rebuilds it."""
        with self._lock:
            active = self._sandboxes.get(sandbox_id)
            warm_entry = self._warm_pool.get(sandbox_id)
            current = active or (warm_entry[0] if warm_entry is not None else None)
            if expected_sandbox is not None and current is not expected_sandbox:
                # A late failure from a replaced adapter must not evict its healthy replacement.
                logger.debug("Ignoring late terminal failure from replaced kosa8 sandbox %s: %s", sandbox_id, reason)
                return
            active = self._sandboxes.pop(sandbox_id, None)
            warm_entry = self._warm_pool.pop(sandbox_id, None)
            for key in [k for k, sid in self._thread_sandboxes.items() if sid == sandbox_id]:
                self._thread_sandboxes.pop(key, None)
            to_close = active or (warm_entry[0] if warm_entry is not None else None)
            if to_close is not None:
                self._pending_closes[id(to_close)] = to_close
        if to_close is None:
            logger.warning("kosa8 sandbox %s failed terminally but was not tracked: %s", sandbox_id, reason)
            return
        logger.warning("Invalidating kosa8 sandbox %s after terminal failure: %s", sandbox_id, reason)
        self._close_quietly(to_close, context="terminal failure")

    # ── Acquire / release ────────────────────────────────────────────────

    @contextmanager
    def _acquiring(self) -> Iterator[None]:
        with self._lock:
            if not self._accepting:
                raise RuntimeError("kosa8 provider is retired or shutting down")
            self._acquires += 1
        try:
            yield
        finally:
            with self._lock:
                self._acquires -= 1

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        with self._acquiring():
            if thread_id is None:
                sandbox = self._create_sandbox(uuid.uuid4().hex[:16])
                return self._publish_sandbox(sandbox)
            key = self._thread_key(thread_id, user_id)
            sandbox_id = self._sandbox_id(thread_id, user_id or "")
            with self._acquire_serializer.hold(sandbox_id):
                return self._acquire_scope_locked(key, sandbox_id)

    async def acquire_async(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        """Run the whole synchronous acquire on the serializer's executor.

        A cancelled awaiter abandons the worker, which runs to completion and
        either publishes its sandbox or deletes it when the provider retired.
        """
        return await self._acquire_serializer.run_on_executor(partial(self.acquire, thread_id, user_id=user_id))

    def _publish_sandbox(self, sandbox: Kosa8Sandbox, key: tuple[str, str] | None = None) -> str:
        with self._lock:
            if self._accepting:
                self._sandboxes[sandbox.id] = sandbox
                if key is not None:
                    self._thread_sandboxes[key] = sandbox.id
                return sandbox.id
        # Shutdown started while this worker was creating: never publish a late sandbox.
        self._close_quietly(sandbox, context="acquired after provider retirement")
        raise RuntimeError("kosa8 provider retired or shut down during acquire")

    def _acquire_scope_locked(self, key: tuple[str, str], sandbox_id: str) -> str:
        with self._lock:
            if not self._accepting:
                raise RuntimeError("kosa8 provider is retired or shutting down")
            existing = self._thread_sandboxes.get(key)
            if existing is not None and existing in self._sandboxes:
                return existing

        reclaimed = self._reclaim_warm_pool(sandbox_id)
        if reclaimed is not None:
            with self._lock:
                if not self._accepting:
                    raise RuntimeError("kosa8 provider retired or shut down during acquire")
                self._thread_sandboxes[key] = reclaimed
            return reclaimed

        return self._publish_sandbox(self._create_sandbox(sandbox_id), key)

    def _create_payload(self, sandbox_id: str) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": f"{_SANDBOX_NAME_PREFIX}{sandbox_id}"}
        for key in ("template", "image", "size", "region", "sleep_after_s"):
            if self._config[key] is not None:
                payload[key] = self._config[key]
        if self._config["persistent"]:
            payload["persistent"] = True
        if self._config["ttl_s"] > 0:
            payload["ttl_s"] = self._config["ttl_s"]
        if self._config["environment"]:
            # Kept on the sandbox's host and given to every command; never logged or returned by kosa8.
            payload["env"] = self._config["environment"]
        return payload

    def _create_sandbox(self, sandbox_id: str) -> Kosa8Sandbox:
        replicas, total = self._replica_count()
        if total >= replicas:
            evicted = self._evict_oldest_warm()
            self._log_replicas_soft_cap(replicas, sandbox_id, evicted)

        client = self._get_client()
        remote = client.create_sandbox(self._create_payload(sandbox_id))
        sandbox = Kosa8Sandbox(
            sandbox_id,
            client,
            remote["id"],
            root_dir=self._config["root_dir"],
            on_terminal_failure=lambda failed_id, reason: self._invalidate_sandbox(failed_id, reason, expected_sandbox=sandbox),
        )
        try:
            self._wait_running(client, remote)
            result = sandbox._exec(["sh", "-c", _bootstrap_script(self._config["root_dir"])], timeout=_BOOTSTRAP_TIMEOUT)
            if result.timed_out or result.exit_code != 0 or "BOOTSTRAP_OK" not in result.stdout_text:
                raise RuntimeError(f"kosa8 bootstrap for {sandbox_id} failed: exit={result.exit_code} timed_out={result.timed_out} stderr={result.stderr_text.strip()[:500]}")
        except BaseException:
            # The remote exists and is billed: delete it rather than leak it.
            self._close_quietly(sandbox, context="create failure")
            raise
        logger.info("Created kosa8 sandbox %s (remote=%s)", sandbox_id, remote["id"])
        return sandbox

    def _wait_running(self, client: Kosa8Client, remote: dict[str, Any]) -> None:
        deadline = time.monotonic() + self._config["ready_timeout"]
        state = remote.get("state")
        while state != "running":
            if state in _GONE_STATES:
                raise RuntimeError(f"kosa8 sandbox {remote['id']} did not start: state {state}, {remote.get('last_error') or 'no error given'}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"kosa8 sandbox {remote['id']} was not running after {self._config['ready_timeout']:.0f} s (state {state})")
            time.sleep(_READY_POLL_S)
            remote = client.get_sandbox(remote["id"])
            state = remote.get("state")

    def _close_quietly(self, sandbox: Kosa8Sandbox, *, context: str) -> None:
        """Own deletion until kosa8 accepts it, keeping failures for a later sweep."""
        token = id(sandbox)
        with self._lock:
            self._pending_closes[token] = sandbox
            if token in self._closing:
                return
            self._closing.add(token)
        closed = False
        try:
            sandbox.close()
        except Exception as e:
            logger.warning("Error deleting kosa8 sandbox %s (%s): %s", sandbox.id, context, e)
        else:
            closed = True
        finally:
            with self._lock:
                if closed:
                    self._pending_closes.pop(token, None)
                self._closing.discard(token)

    def _retry_pending_closes(self) -> None:
        with self._lock:
            pending = list(self._pending_closes.values())
        for sandbox in pending:
            self._close_quietly(sandbox, context="retry deletion")

    def get(self, sandbox_id: str) -> Sandbox | None:
        with self._lock:
            return self._sandboxes.get(sandbox_id)

    def get_scoped(self, sandbox_id: str, *, thread_id: str, user_id: str) -> Sandbox | None:
        with self._lock:
            if self._thread_sandboxes.get(self._thread_key(thread_id, user_id)) != sandbox_id:
                return None
            return self._sandboxes.get(sandbox_id)

    def release(self, sandbox_id: str) -> None:
        """Mirror outputs to the host, then park the sandbox in the warm pool (it keeps running).

        Holds the same scope serializer as acquire, so the next turn cannot reuse
        the sandbox while its outputs are still being copied, and keeps it active
        during the copy so the idle reaper cannot delete it.
        """
        with self._lock:
            if sandbox_id not in self._sandboxes:
                return
        try:
            with self._acquire_serializer.hold(sandbox_id):
                self._release_scope_locked(sandbox_id)
        except AcquireSerializerClosed:
            with self._lock:
                untracked = sandbox_id not in self._sandboxes
            if not untracked:
                raise

    def _release_scope_locked(self, sandbox_id: str) -> None:
        with self._lock:
            sandbox = self._sandboxes.get(sandbox_id)
            scope = next((key for key, value in self._thread_sandboxes.items() if value == sandbox_id), None)
        if sandbox is None:
            return
        if scope is not None and scope[0]:
            try:
                from deerflow.config.paths import get_paths

                root = get_paths().sandbox_outputs_dir(scope[1], user_id=scope[0])
                sync_outputs_to_host(sandbox, root, remote_root=f"{self._config['root_dir']}/outputs")
            except Exception:
                logger.warning("Failed to synchronize kosa8 outputs for %s", sandbox_id, exc_info=True)
        close_after_unlock = False
        with self._lock:
            # A terminal failure or shutdown during the copy may have untracked it; never resurrect it.
            if self._sandboxes.get(sandbox_id) is not sandbox:
                return
            self._sandboxes.pop(sandbox_id)
            for key in [key for key, value in self._thread_sandboxes.items() if value == sandbox_id]:
                self._thread_sandboxes.pop(key, None)
            if self._shutdown_called:
                close_after_unlock = True
                self._pending_closes[id(sandbox)] = sandbox
            else:
                self._warm_pool[sandbox_id] = (sandbox, time.monotonic())
        if close_after_unlock:
            self._close_quietly(sandbox, context="released during shutdown")
        else:
            logger.info("Released kosa8 sandbox %s to warm pool", sandbox_id)

    def _reclaim_warm_pool(self, sandbox_id: str) -> str | None:
        """Reclaim a warm sandbox after a liveness probe; a failed probe deletes it."""
        with self._lock:
            if sandbox_id not in self._warm_pool:
                return None
            sandbox, _ = self._warm_pool[sandbox_id]
        try:
            result = sandbox._exec(["sh", "-c", "echo ok"], timeout=10)
            healthy = not result.timed_out and result.exit_code == 0 and "ok" in result.stdout_text.splitlines()
            if not healthy:
                logger.warning("kosa8 warm-pool sandbox %s health check failed: exit=%s", sandbox_id, result.exit_code)
        except Exception as e:
            logger.warning("kosa8 warm-pool sandbox %s health check error: %s", sandbox_id, e)
            healthy = False

        if not healthy:
            with self._lock:
                warm_entry = self._warm_pool.pop(sandbox_id, None)
            if warm_entry is not None:
                self._destroy_warm_entry(sandbox_id, warm_entry[0], reason="health_check_failed")
            return None

        with self._lock:
            warm_entry = self._warm_pool.pop(sandbox_id, None)
            if warm_entry is None:
                return None
            admitted = self._accepting
            if admitted:
                self._sandboxes[sandbox_id] = warm_entry[0]
        if not admitted:
            self._close_quietly(warm_entry[0], context="reclaimed after provider retirement")
            raise RuntimeError("kosa8 provider retired or shut down during acquire")
        logger.info("Reclaimed warm-pool kosa8 sandbox %s", sandbox_id)
        return sandbox_id

    def reset(self) -> None:
        """Retire the provider and park its sandboxes for this instance's reaper and shutdown."""
        with self._lock:
            self._accepting = False
            now = time.monotonic()
            for sandbox_id, sandbox in self._sandboxes.items():
                self._warm_pool.setdefault(sandbox_id, (sandbox, now))
            self._sandboxes.clear()
            self._thread_sandboxes.clear()
            self._acquire_serializer.close()

    def _shutdown_at_exit(self) -> None:
        try:
            self.shutdown()
        except _ShutdownIncompleteError as e:
            logger.warning("%s", e)

    def shutdown(self) -> None:
        """Fence admission and delete every sandbox; raise while work remains so callers retry.

        A failure to stop the idle reaper does not skip the deletions: they
        still run, and the error is raised after them.
        """
        with self._shutdown_lock:
            with self._lock:
                if self._shutdown_complete:
                    return
                self._shutdown_called = True
                self._accepting = False
            self._acquire_serializer.close()
            reaper_error: Exception | None = None
            try:
                self._stop_idle_checker()
            except Exception as e:
                reaper_error = e
            with self._lock:
                active = list(self._sandboxes.values())
                warm = [sandbox for sandbox, _ in self._warm_pool.values()]
                self._sandboxes.clear()
                self._warm_pool.clear()
                self._thread_sandboxes.clear()
                previous_pending = list(self._pending_closes.values())
            for sandbox in {id(box): box for box in active + warm + previous_pending}.values():
                self._close_quietly(sandbox, context="shutdown")
            with self._lock:
                outstanding = self._acquires or self._pending_closes
                if not outstanding and reaper_error is None:
                    self._shutdown_complete = True
                    client, self._client = self._client, None
                else:
                    client = None
            if client is not None:
                client.close()
            if reaper_error is not None:
                raise _ShutdownIncompleteError(f"kosa8 shutdown incomplete: {reaper_error}; retry shutdown") from reaper_error
            if outstanding:
                raise _ShutdownIncompleteError(f"kosa8 shutdown incomplete: {self._acquires} acquires and {len(self._pending_closes)} deletions pending; retry shutdown")


__all__ = ["Kosa8SandboxProvider"]
