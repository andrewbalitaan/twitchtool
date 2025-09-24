from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from twitchtool.poller import _probe_user_live


class FakeProc:
    def __init__(self, rc: int):
        self.returncode = None
        self._rc = rc

    async def wait(self):
        await asyncio.sleep(0.01)
        self.returncode = self._rc
        return self._rc

    def kill(self):
        self.returncode = -9


@pytest.mark.asyncio
async def test_probe_user_live(monkeypatch):
    async def fake_cpe(*args, **kwargs):
        # Determine rc by substring match against the constructed command
        joined = " ".join(str(a) for a in args)
        rc = 0 if "gooduser" in joined else 1
        return FakeProc(rc)

    monkeypatch.setattr("twitchtool.poller.asyncio.create_subprocess_exec", fake_cpe)
    # force which("streamlink") True
    monkeypatch.setattr("twitchtool.poller.which", lambda _: "/usr/bin/streamlink")
    assert await _probe_user_live("gooduser", "best", 1) is True
    assert await _probe_user_live("baduser", "best", 1) is False
