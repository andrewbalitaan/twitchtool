from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from twitchtool.poller import PollerOptions


class FakeProc:
    def __init__(self, rc: int = 0):
        self.returncode = None
        self._rc = rc

    async def wait(self):
        await asyncio.sleep(0.001)
        self.returncode = self._rc
        return self._rc

    def kill(self):
        self.returncode = -9


@pytest.mark.asyncio
async def test_poller_passes_config_to_recorders(monkeypatch, tmp_path: Path):
    # Import inside test to ensure monkeypatches apply to the module state
    from twitchtool import poller as pol

    # Avoid touching real state files
    monkeypatch.setattr(pol, "_register_poller_process", lambda interval, logger: "now")
    monkeypatch.setattr(pol, "_clear_pid_file", lambda: None)
    monkeypatch.setattr(pol, "_write_status", lambda update: None)

    # Make probe fast and successful
    monkeypatch.setattr(pol, "which", lambda _: "/usr/bin/streamlink")

    async def fake_cpe(*args, **kwargs):
        return FakeProc(0)

    monkeypatch.setattr(pol.asyncio, "create_subprocess_exec", fake_cpe)

    # Ensure the download command resolves to an absolute path
    monkeypatch.setattr(pol.shutil, "which", lambda exe: "/usr/bin/twitchtool" if exe == "twitchtool" else None)

    # Simulate one user, live, not locked
    monkeypatch.setattr(pol, "_load_users", lambda path: ["gooduser"])
    monkeypatch.setattr(pol.PerUserLock, "is_user_locked", staticmethod(lambda user: False))

    class DummyGSM:
        def __init__(self, *args, **kwargs):
            pass

        def active_count(self):
            return 0

    monkeypatch.setattr(pol, "GlobalSlotManager", DummyGSM)

    launched = {}

    def fake_popen(cmd, *, logfile):
        launched["cmd"] = list(cmd)
        # Stop after first launch by raising an exception to break out
        raise RuntimeError("stop after first launch")

    monkeypatch.setattr(pol, "_detached_popen", fake_popen)

    cfg_path = (tmp_path / "config.toml").resolve()
    cfg_path.write_text("record = {}\n", encoding="utf-8")

    opts = PollerOptions(
        users_file=tmp_path / "users.txt",
        interval=1,
        quality="best",
        download_cmd="twitchtool record",
        timeout=1,
        probe_concurrency=1,
        record_limit=1,
        logs_dir=tmp_path / "logs",
        json_logs=False,
        config_path=cfg_path,
    )

    with pytest.raises(RuntimeError):
        await pol.poller(opts)

    # Verify --config is passed through to the recorder invocation
    cmd = launched.get("cmd")
    assert cmd is not None
    assert "--config" in cmd
    idx = cmd.index("--config")
    assert cmd[idx + 1] == str(cfg_path)

