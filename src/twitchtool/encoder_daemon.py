from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .locks import GlobalSlotManager
from .queue import JobEntry, oldest_job, write_error_for_job
from .utils import (
    abspath,
    build_nice_ionice_prefix,
    ensure_dir,
    is_process_alive,
    setup_logging,
    which,
    sigcont_if_stopped,
    atomic_write_json,
    now_utc_iso,
    read_json,
)


ENCODER_LOCK_PATH = Path("/tmp/twitch-encoderd.lock")
STATE_DIR = Path("~/.local/state/twitchtool/encoder").expanduser()
PID_PATH = STATE_DIR / "encoder.pid"
STATUS_PATH = STATE_DIR / "encoder_status.json"


@dataclass
class EncodeOptions:
    queue_dir: Path
    preset: str = "medium"
    crf: int = 26
    threads: int = 1
    height: int = 480
    # fps: "auto" to preserve source; or a number/fraction like "30000/1001"
    fps: str = "auto"
    loglevel: str = "error"
    json_logs: bool = False
    record_limit: int = 6
    encoder_concurrency: int = 1  # reserved; current implementation: 1
    disk_free_min_bytes: int = 10 * 1024 * 1024 * 1024


class SingleInstanceLock:
    def __init__(self, path: Path):
        self.path = path
        self._fh = None

    def acquire(self) -> bool:
        import fcntl

        self._fh = self.path.open("a+")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._fh.close()
            self._fh = None
            return False
        return True

    def release(self) -> None:
        if not self._fh:
            return
        import fcntl

        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None


def _build_ffmpeg_cmd(job: JobEntry, opts: EncodeOptions) -> list[str]:
    ffmpeg = which("ffmpeg") or "ffmpeg"
    # Build video filtergraph with optional fps= and vsync cfr
    vf_filters: list[str] = [f"scale=-2:{int(opts.height)}"]
    vsync: list[str] = []
    fps_val = str(opts.fps).strip().lower() if opts.fps is not None else "auto"
    if fps_val and fps_val != "auto":
        vf_filters.append(f"fps={fps_val}")
        vsync = ["-vsync", "cfr"]

    # Add +genpts before -i when input is TS to stabilize timestamps
    in_suffix = Path(job.job.input).suffix.lower()
    ts_fix = ["-fflags", "+genpts"] if in_suffix == ".ts" else []

    cmd = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        opts.loglevel,
        "-y",
        *ts_fix,
        "-i",
        job.job.input,
        "-vf",
        ",".join(vf_filters),
        "-c:v",
        "libx265",
        "-crf",
        str(int(opts.crf)),
        "-preset",
        str(opts.preset),
        "-threads",
        str(int(opts.threads)),
        *vsync,
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-ar",
        "48000",
        "-af",
        "aresample=async=1:first_pts=0",
        "-movflags",
        "+faststart",
        job.job.output,
    ]
    prefix = build_nice_ionice_prefix()
    return prefix + cmd


def _active_downloads(gsm: GlobalSlotManager) -> int:
    return gsm.active_count()


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


class EncoderAlreadyRunning(RuntimeError):
    pass


def register_encoder_process() -> None:
    ensure_dir(STATE_DIR)
    existing = _safe_read_json(PID_PATH)
    if existing:
        pid = int(existing.get("pid", -1)) if existing.get("pid") is not None else -1
        if pid > 0 and is_process_alive(pid):
            raise EncoderAlreadyRunning
        try:
            PID_PATH.unlink()
        except FileNotFoundError:
            pass
    payload = {"pid": os.getpid(), "started_at": now_utc_iso()}
    atomic_write_json(PID_PATH, payload)
    _write_status({
        "running": True,
        "pid": os.getpid(),
        "started_at": payload["started_at"],
        "current_job": None,
        "last_job": None,
    })


def clear_encoder_pid() -> None:
    try:
        PID_PATH.unlink()
    except FileNotFoundError:
        pass


def encoder_runtime_state() -> dict:
    state = {
        "running": False,
        "pid": None,
        "started_at": None,
        "current_job": None,
        "last_job": None,
    }
    info = _safe_read_json(PID_PATH)
    if info:
        pid = int(info.get("pid", -1)) if info.get("pid") is not None else -1
        if pid > 0 and is_process_alive(pid):
            state["running"] = True
            state["pid"] = pid
            state["started_at"] = info.get("started_at")
        else:
            clear_encoder_pid()
    status = _safe_read_json(STATUS_PATH)
    if status:
        for key in ("current_job", "last_job", "started_at"):
            if status.get(key) is not None:
                state[key] = status.get(key)
        if status.get("pid") and not state.get("pid"):
            pid = int(status.get("pid", -1))
            if pid > 0 and is_process_alive(pid):
                state["running"] = True
                state["pid"] = pid
    if state.get("pid") and not is_process_alive(int(state["pid"])):
        state["running"] = False
    return state


def stop_encoder_daemon(timeout: float = 10.0, force: bool = False) -> dict:
    state = encoder_runtime_state()
    pid = state.get("pid")
    target_pid = int(pid) if pid else None
    if not pid or not is_process_alive(int(pid)):
        clear_encoder_pid()
        _write_status({"running": False, "pid": None})
        state = encoder_runtime_state()
        return {"result": "not_running", "state": state, "pid": target_pid}
    pid = int(pid)
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        clear_encoder_pid()
        _write_status({"running": False, "pid": None})
        state = encoder_runtime_state()
        return {"result": "not_running", "state": state, "pid": target_pid}

    deadline = time.time() + max(0.0, float(timeout))
    while time.time() < deadline:
        if not is_process_alive(pid):
            clear_encoder_pid()
            _write_status({"running": False, "pid": None, "stopped_at": now_utc_iso(), "current_job": None})
            state = encoder_runtime_state()
            return {
                "result": "stopped",
                "state": state,
                "signal": "SIGTERM",
                "pid": target_pid,
            }
        time.sleep(0.2)

    if not force:
        state = encoder_runtime_state()
        return {
            "result": "timeout",
            "state": state,
            "signal": "SIGTERM",
            "pid": target_pid,
        }

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        clear_encoder_pid()
        _write_status({"running": False, "pid": None, "stopped_at": now_utc_iso(), "current_job": None})
        state = encoder_runtime_state()
        return {
            "result": "stopped",
            "state": state,
            "signal": "SIGTERM",
            "pid": target_pid,
        }

    for _ in range(50):
        if not is_process_alive(pid):
            clear_encoder_pid()
            _write_status({"running": False, "pid": None, "stopped_at": now_utc_iso(), "current_job": None})
            state = encoder_runtime_state()
            return {
                "result": "stopped",
                "state": state,
                "signal": "SIGKILL",
                "pid": target_pid,
            }
        time.sleep(0.2)

    state = encoder_runtime_state()
    return {
        "result": "failed",
        "state": state,
        "signal": "SIGKILL",
        "pid": target_pid,
    }


def encode_daemon(opts: EncodeOptions) -> int:
    logger = setup_logging("twitchtool.encoderd", json_logs=opts.json_logs)
    if not which("ffmpeg"):
        logger.error("ffmpeg not found in PATH")
        return 2

    # Sanitize encode parameters
    def _clamp(n: int, lo: int, hi: int) -> int:
        return max(lo, min(hi, n))

    original = (opts.crf, opts.threads, opts.height, str(opts.fps))
    opts.crf = _clamp(int(opts.crf), 0, 51)
    opts.threads = _clamp(int(opts.threads), 1, 64)
    opts.height = _clamp(int(opts.height), 144, 4320)
    # fps may be 'auto' or a fraction; leave as-is
    if (opts.crf, opts.threads, opts.height, str(opts.fps)) != original:
        logger.warning(
            "sanitized encode options",
            extra={
                "extra": {
                    "crf": opts.crf,
                    "threads": opts.threads,
                    "height": opts.height,
                    "fps": str(opts.fps),
                }
            },
        )

    qdir = abspath(opts.queue_dir)
    ensure_dir(qdir)
    ensure_dir(qdir / "jobs")

    inst = SingleInstanceLock(ENCODER_LOCK_PATH)
    if not inst.acquire():
        logger.error("another encoder daemon is already running")
        return 3
    try:
        register_encoder_process()
    except EncoderAlreadyRunning:
        inst.release()
        logger.error("encoder daemon already running")
        return 3

    stopping = False

    def _sig_handler(signum, frame):
        nonlocal stopping
        if not stopping:
            stopping = True
            logger.info("received signal, stopping daemon", extra={"extra": {"signal": signum}})

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    gsm = GlobalSlotManager(opts.record_limit, logger=logger)

    logger.info("encode daemon started", extra={"extra": {"queue_dir": str(qdir)}})

    try:
        while not stopping:
            # Clean up old failed jobs (older than 7 days)
            try:
                jobs_dir = qdir / "jobs"
                for failed in jobs_dir.glob("*.failed.json"):
                    try:
                        if (time.time() - failed.stat().st_mtime) > (7 * 86400):
                            failed.unlink()
                            logger.info(f"Cleaned old failed job: {failed.name}")
                    except Exception:
                        pass
            except Exception:
                pass

            job = oldest_job(qdir)
            if not job:
                time.sleep(2)
                continue

            # Validate input/output
            in_path = Path(job.job.input)
            out_path = Path(job.job.output)
            if not in_path.exists():
                write_error_for_job(job.path, f"input not found: {in_path}")
                logger.error("job input missing", extra={"extra": {"job": job.path.name}})
                # Remove the job to avoid blocking the queue
                try:
                    job.path.unlink()
                except FileNotFoundError:
                    pass
                continue

            ensure_dir(out_path.parent)

            cmd = _build_ffmpeg_cmd(job, opts)
            logger.info(
                "starting encode",
                extra={"extra": {"job": job.path.name, "cmd": " ".join(cmd)}},
            )
            _write_status(
                {
                    "running": True,
                    "pid": os.getpid(),
                    "current_job": job.path.name,
                    "last_job": job.path.name,
                }
            )
            # Low-space check before starting
            try:
                import shutil

                free = shutil.disk_usage(str(out_path.parent)).free
                if free < int(opts.disk_free_min_bytes):
                    logger.warning(
                        "low free space on output volume; delaying encode",
                        extra={
                            "extra": {
                                "free_bytes": free,
                                "min_required": int(opts.disk_free_min_bytes),
                            }
                        },
                    )
                    time.sleep(5)
                    continue
            except Exception:
                pass
            try:
                proc = subprocess.Popen(cmd)
            except OSError as e:
                write_error_for_job(job.path, f"spawn failed: {e}")
                logger.error("failed to start ffmpeg", extra={"extra": {"error": str(e)}})
                time.sleep(1)
                continue

            paused = False
            start_ts = time.time()
            while True:
                rc = proc.poll()
                if rc is not None:
                    break
                # Pause/resume logic
                active = _active_downloads(gsm)
                if active > 0 and not paused:
                    try:
                        os.kill(proc.pid, signal.SIGSTOP)
                        paused = True
                        logger.info("paused encode due to active downloads", extra={"extra": {"active": active}})
                    except ProcessLookupError:
                        pass
                elif active == 0 and paused:
                    try:
                        os.kill(proc.pid, signal.SIGCONT)
                        paused = False
                        logger.info("resumed encode; no active downloads")
                    except ProcessLookupError:
                        pass
                if stopping:
                    # Unpause before exit
                    sigcont_if_stopped(proc.pid)
                    try:
                        proc.terminate()
                    except ProcessLookupError:
                        pass
                    break
                time.sleep(2)

            rc = proc.wait()
            dur = int(time.time() - start_ts)
            if rc == 0 and out_path.exists() and out_path.stat().st_size > 0:
                logger.info("encode complete", extra={"extra": {"job": job.path.name, "seconds": dur}})
                # Post-success cleanup
                if bool(job.job.delete_input_on_success):
                    try:
                        Path(job.job.input).unlink()
                    except FileNotFoundError:
                        pass
                try:
                    job.path.unlink()
                except FileNotFoundError:
                    pass
            else:
                write_error_for_job(job.path, f"ffmpeg failed rc={rc}")
                logger.error("encode failed", extra={"extra": {"job": job.path.name, "rc": rc}})
                # Move failed job aside to prevent immediate reprocessing
                try:
                    failed = job.path.with_suffix(".failed.json")
                    job.path.rename(failed)
                except Exception:
                    # As a fallback, keep a small delay to avoid hot loop
                    time.sleep(2)
            _write_status(
                {
                    "running": True,
                    "pid": os.getpid(),
                    "current_job": None,
                    "last_job": job.path.name,
                }
            )
    finally:
        clear_encoder_pid()
        _write_status({"running": False, "pid": None, "stopped_at": now_utc_iso(), "current_job": None})
        inst.release()
        logger.info("encode daemon stopped")
    return 0
