from __future__ import annotations

import asyncio
import os
import shlex
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import shutil

from .locks import GlobalSlotManager, PerUserLock
from .utils import (
    abspath,
    ensure_dir,
    is_process_alive,
    is_valid_twitch_username,
    now_utc_iso,
    read_json,
    setup_logging,
    which,
    atomic_write_json,
)


@dataclass
class PollerOptions:
    users_file: Path
    interval: int = 300
    quality: str = "best"
    download_cmd: str = "twitchtool record"
    timeout: int = 15
    probe_concurrency: int = 10
    record_limit: int = 6
    logs_dir: Path = Path("~/twitch-logs")
    json_logs: bool = False


STATE_DIR = Path("~/.local/state/twitchtool/poller").expanduser()
PID_PATH = STATE_DIR / "poller.pid"
STATUS_PATH = STATE_DIR / "poller_status.json"


class PollerAlreadyRunning(RuntimeError):
    pass


def _safe_read_json(path: Path) -> Optional[dict]:
    try:
        return read_json(path)
    except Exception:
        return None


def _write_status(update: dict) -> None:
    ensure_dir(STATE_DIR)
    data = _safe_read_json(STATUS_PATH) or {}
    data.update(update)
    atomic_write_json(STATUS_PATH, data)


def _register_poller_process(interval: int, logger) -> str:
    ensure_dir(STATE_DIR)
    existing = _safe_read_json(PID_PATH)
    if existing:
        try:
            pid = int(existing.get("pid", -1))
        except Exception:
            pid = -1
        if pid > 0 and is_process_alive(pid):
            logger.error(
                "poller already running",
                extra={"extra": {"pid": pid, "started_at": existing.get("started_at")}},
            )
            raise PollerAlreadyRunning
        try:
            PID_PATH.unlink()
        except FileNotFoundError:
            pass
    started_at = now_utc_iso()
    payload = {"pid": os.getpid(), "started_at": started_at, "interval": int(interval)}
    atomic_write_json(PID_PATH, payload)
    _write_status({
        "running": True,
        "pid": os.getpid(),
        "started_at": started_at,
        "interval": int(interval),
        "last_poll_ts": None,
        "next_poll_ts": None,
    })
    return started_at


def _clear_pid_file() -> None:
    try:
        PID_PATH.unlink()
    except FileNotFoundError:
        pass


def _update_cycle_status(cycle_dt: datetime, interval: int) -> None:
    next_dt = cycle_dt + timedelta(seconds=int(interval))
    _write_status(
        {
            "running": True,
            "pid": os.getpid(),
            "last_poll_ts": cycle_dt.isoformat(),
            "interval": int(interval),
            "next_poll_ts": next_dt.isoformat(),
        }
    )


def poller_runtime_state() -> dict:
    state = {
        "running": False,
        "pid": None,
        "started_at": None,
        "last_poll_ts": None,
        "next_poll_ts": None,
        "interval": None,
    }
    info = _safe_read_json(PID_PATH)
    if info:
        pid = int(info.get("pid", -1)) if info.get("pid") is not None else -1
        if pid > 0 and is_process_alive(pid):
            state["running"] = True
            state["pid"] = pid
            state["started_at"] = info.get("started_at")
            state["interval"] = info.get("interval")
        else:
            _clear_pid_file()
    status = _safe_read_json(STATUS_PATH)
    if status:
        for key in ("last_poll_ts", "next_poll_ts", "interval", "started_at"):
            if status.get(key) is not None:
                state[key] = status.get(key)
        if status.get("running") is False:
            state["running"] = False if not state["pid"] else state["running"]
        if status.get("pid") and not state["pid"]:
            pid = int(status.get("pid", -1))
            if pid > 0 and is_process_alive(pid):
                state["running"] = True
                state["pid"] = pid
    if not state["next_poll_ts"] and state["last_poll_ts"] and state["interval"]:
        try:
            last_dt = datetime.fromisoformat(state["last_poll_ts"])
            next_dt = last_dt + timedelta(seconds=int(state["interval"]))
            state["next_poll_ts"] = next_dt.isoformat()
        except Exception:
            pass
    if state["pid"] and not is_process_alive(int(state["pid"])):
        state["running"] = False
    return state


def stop_poller_daemon(timeout: float = 10.0, force: bool = False) -> dict:
    state = poller_runtime_state()
    pid = state.get("pid")
    target_pid = int(pid) if pid else None
    if not pid or not is_process_alive(int(pid)):
        _clear_pid_file()
        _write_status({"running": False, "pid": None})
        state = poller_runtime_state()
        return {"result": "not_running", "state": state, "pid": target_pid}
    pid = int(pid)
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        _clear_pid_file()
        _write_status({"running": False, "pid": None})
        state = poller_runtime_state()
        return {"result": "not_running", "state": state, "pid": target_pid}

    deadline = time.time() + max(0.0, float(timeout))
    while time.time() < deadline:
        if not is_process_alive(pid):
            _clear_pid_file()
            _write_status({"running": False, "pid": None, "stopped_at": now_utc_iso(), "next_poll_ts": None})
            state = poller_runtime_state()
            return {
                "result": "stopped",
                "state": state,
                "signal": "SIGTERM",
                "pid": target_pid,
            }
        time.sleep(0.2)

    if not force:
        state = poller_runtime_state()
        return {
            "result": "timeout",
            "state": state,
            "signal": "SIGTERM",
            "pid": target_pid,
        }

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        _clear_pid_file()
        _write_status({"running": False, "pid": None, "stopped_at": now_utc_iso(), "next_poll_ts": None})
        state = poller_runtime_state()
        return {
            "result": "stopped",
            "state": state,
            "signal": "SIGTERM",
            "pid": target_pid,
        }

    for _ in range(50):
        if not is_process_alive(pid):
            _clear_pid_file()
            _write_status({"running": False, "pid": None, "stopped_at": now_utc_iso(), "next_poll_ts": None})
            state = poller_runtime_state()
            return {
                "result": "stopped",
                "state": state,
                "signal": "SIGKILL",
                "pid": target_pid,
            }
        time.sleep(0.2)

    state = poller_runtime_state()
    return {
        "result": "failed",
        "state": state,
        "signal": "SIGKILL",
        "pid": target_pid,
    }


async def _probe_user_live(user: str, quality: str, timeout: int) -> bool:
    """Return True if streamlink reports user is live (via --stream-url)."""
    if not which("streamlink"):
        return False
    url = f"https://twitch.tv/{user}"
    cmd = ["streamlink", "--stream-url", url, quality]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            # Ensure the process is reaped to avoid resource leaks
            try:
                await proc.wait()
            except Exception:
                pass
            return False
        return proc.returncode == 0
    except Exception:
        return False


def _load_users(path: Path) -> list[str]:
    users: list[str] = []
    if not path.exists():
        return users
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            users.append(s)
    return users


def _detached_popen(cmd: list[str], *, logfile: Path) -> None:
    ensure_dir(logfile.parent)
    # Append logs
    logf = logfile.open("a", buffering=1)
    # Start detached from poller process group
    subprocess = __import__("subprocess")
    proc = subprocess.Popen(
        cmd,
        stdout=logf,
        stderr=logf,
        start_new_session=True,
        close_fds=True,
    )
    # Close parent's handle to avoid FD leaks; child keeps its own copy.
    try:
        logf.close()
    except Exception:
        pass


def _is_safe_download_cmd(download_cmd: str) -> tuple[bool, str]:
    """Basic validation for download command to avoid shells and meta tokens.

    Defense-in-depth: we never use a shell, but we also avoid shell interpreters.
    """
    forbidden_execs = {"sh", "bash", "zsh", "ksh", "fish", "pwsh", "powershell", "cmd", "cmd.exe"}
    meta_tokens = {";", "|", "&&", "||", ">", "<", "`"}
    try:
        parts = shlex.split(download_cmd)
    except Exception as e:
        return False, f"invalid download_cmd: {e}"
    if not parts:
        return False, "empty download_cmd"
    exe = Path(parts[0]).name.lower()
    if exe in forbidden_execs:
        return False, f"forbidden executable '{exe}' in download_cmd"
    if any(tok in meta_tokens for tok in parts):
        return False, "shell meta tokens not allowed in download_cmd"
    return True, ""


def _resolve_download_cmd(download_cmd: str) -> tuple[list[str] | None, str]:
    """Resolve the base executable; return (argv_parts, error).

    Ensures we have an absolute path to the executable so we don't depend on the
    parent process PATH. Also allows the common case where this process was
    started via an absolute path (e.g. systemd ExecStart) and the user provided
    just "twitchtool" as the download command.
    """
    try:
        parts = shlex.split(download_cmd)
    except Exception as e:
        return None, f"invalid download_cmd: {e}"
    if not parts:
        return None, "empty download_cmd"
    exe = parts[0]
    resolved = shutil.which(exe)
    if resolved is None:
        p = Path(exe)
        if p.is_absolute() and p.exists():
            resolved = str(p)
    # Fallback: reuse our own path if we were started via an absolute executable
    if resolved is None and exe == "twitchtool":
        me = Path(sys.argv[0])
        if me.is_absolute() and me.exists():
            resolved = str(me)
    if resolved is None:
        return None, f"cannot find executable '{exe}' in PATH"
    return [resolved] + parts[1:], ""


async def poller(opts: PollerOptions) -> int:
    logger = setup_logging("twitchtool.poller", json_logs=opts.json_logs)
    users_path = abspath(opts.users_file)
    logs_dir = abspath(opts.logs_dir)
    gsm = GlobalSlotManager(opts.record_limit, logger=logger)

    try:
        _register_poller_process(opts.interval, logger)
    except PollerAlreadyRunning:
        return 4

    stopping = False

    def _sig_handler(signum, frame):
        nonlocal stopping
        if not stopping:
            stopping = True
            logger.info("received signal, will stop after this cycle", extra={"extra": {"signal": signum}})

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    logger.info("poller started", extra={"extra": {"users_file": str(users_path), "interval": opts.interval}})

    try:
        while not stopping:
            cycle_dt = datetime.now(timezone.utc)
            users = _load_users(users_path)
            # Filter invalid usernames
            valid_users: list[str] = []
            for u in users:
                if is_valid_twitch_username(u):
                    valid_users.append(u)
                else:
                    logger.warning("skipping invalid username", extra={"extra": {"user": u}})

            # Validate & resolve download command once per cycle
            ok_cmd, reason = _is_safe_download_cmd(opts.download_cmd)
            base_cmd, resolve_err = _resolve_download_cmd(opts.download_cmd) if ok_cmd else (None, "")
            if not ok_cmd or base_cmd is None:
                _update_cycle_status(cycle_dt, opts.interval)
                logger.error(
                    "download_cmd not usable; skipping launches",
                    extra={"extra": {"reason": reason or resolve_err, "download_cmd": opts.download_cmd}},
                )
                await asyncio.sleep(opts.interval)
                continue

            if not valid_users:
                logger.debug("no users to poll")
                _update_cycle_status(cycle_dt, opts.interval)
                await asyncio.sleep(opts.interval)
                continue

            sem = asyncio.Semaphore(opts.probe_concurrency)

            async def _probe(u: str) -> tuple[str, bool]:
                async with sem:
                    live = await _probe_user_live(u, opts.quality, opts.timeout)
                    return u, live

            # Prepare probes
            tasks = [asyncio.create_task(_probe(u)) for u in valid_users]
            results: list[tuple[str, bool]] = await asyncio.gather(*tasks, return_exceptions=False)

            # Determine capacity
            active = gsm.active_count()
            capacity = max(0, opts.record_limit - active)
            logger.info(
                "poll cycle result",
                extra={
                    "extra": {
                        "active_slots": active,
                        "record_limit": opts.record_limit,
                        "capacity": capacity,
                        "live": [u for (u, live) in results if live],
                    }
                },
            )

            _update_cycle_status(cycle_dt, opts.interval)

            # Launch recorders up to capacity
            launches = 0
            for user, live in results:
                if launches >= capacity:
                    break
                if not live:
                    continue
                # Skip if user is already being recorded
                if PerUserLock.is_user_locked(user):
                    logger.debug("user is already locked/recording", extra={"extra": {"user": user}})
                    continue
                # Build command
                cmd = base_cmd + [user, "--quality", opts.quality]
                # Log file per user
                logfile = logs_dir / f"{user}.log"
                try:
                    _detached_popen(cmd, logfile=logfile)
                except FileNotFoundError as e:
                    logger.error(
                        "failed to launch recorder",
                        extra={"extra": {"user": user, "error": str(e)}},
                    )
                    continue
                launches += 1
                logger.info("launched recorder", extra={"extra": {"user": user, "cmd": " ".join(cmd)}})

            # Sleep until next interval or until stopped
            for _ in range(opts.interval):
                if stopping:
                    break
                await asyncio.sleep(1)
    finally:
        _clear_pid_file()
        _write_status({
            "running": False,
            "pid": None,
            "stopped_at": now_utc_iso(),
            "next_poll_ts": None,
        })
        logger.info("poller stopped")
    return 0
