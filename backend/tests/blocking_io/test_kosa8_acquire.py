"""Anchor: contended kosa8 acquire_async must not block the event loop.

Two concurrent acquire_async calls for the same scope serialize on the
AcquireSerializer. The fake kosa8 API below performs REAL file IO when it
creates a sandbox, so a regression that moves creation (or the HTTP client's
construction) back onto the loop trips the Blockbuster gate.
"""

from __future__ import annotations

import asyncio
import types
from pathlib import Path

import httpx
import pytest
from blockbuster import BlockingError

from deerflow.community.kosa8 import provider as provider_module
from deerflow.community.kosa8.client import Kosa8Client
from deerflow.community.kosa8.provider import Kosa8SandboxProvider

pytestmark = pytest.mark.asyncio


def _install(monkeypatch: pytest.MonkeyPatch, probe_dir: Path) -> Kosa8SandboxProvider:
    created: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/sandboxes":
            probe = probe_dir / f"create-{len(created) + 1}.probe"
            probe.write_text("x" * 4096)  # real blocking file IO at the API boundary
            created.append(probe.name)
            return httpx.Response(201, json={"sandbox": {"id": f"sb_{len(created)}", "state": "running", "template": "base"}})
        if request.url.path.endswith("/exec"):
            return httpx.Response(200, content=b'{"stream":"stdout","data":"Qk9PVFNUUkFQX09LCg=="}\n{"done":true,"exit":0}\n')
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(provider_module, "get_app_config", lambda: types.SimpleNamespace(sandbox=types.SimpleNamespace(kosa8_api_key="k", idle_timeout=0)))
    monkeypatch.setattr(Kosa8SandboxProvider, "_build_client", lambda self, key, url: Kosa8Client(api_key=key, base_url=url, transport=httpx.MockTransport(handle)))
    return Kosa8SandboxProvider()


async def test_concurrent_acquire_async_stays_off_event_loop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    provider = _install(monkeypatch, tmp_path)
    try:
        first, second = await asyncio.gather(
            provider.acquire_async("thread-anchor", user_id="u-anchor"),
            provider.acquire_async("thread-anchor", user_id="u-anchor"),
        )
        assert first == second  # same scope serialized, second caller reuses
    finally:
        await asyncio.to_thread(provider.shutdown)


async def test_sync_acquire_on_loop_trips_the_gate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Meta-check (teeth): a synchronous acquire on the loop MUST be caught."""
    provider = _install(monkeypatch, tmp_path)
    try:
        with pytest.raises(BlockingError):
            provider.acquire("thread-anchor", user_id="u-anchor")
    finally:
        await asyncio.to_thread(provider.shutdown)
