from __future__ import annotations

import os
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from .locks import GlobalSlotManager, PerUserLock, SlotUnavailable, UserAlreadyRecording
from .config import DEFAULTS
from .queue import Job, write_job
from .utils import (
    ISO,
    abspath,
    ensure_dir,
    now_utc_iso,
    run_capture,
    safe_unlink,
    setup_logging,
    which,
)


@dataclass
class RecordOptions:
    username: str
    quality: str = "best"
    retry_delay: int = 60
    retry_window: int = 900
    loglevel: str = "error"
    output_dir: Path = Path(DEFAULTS["paths"]["record_dir"])  # Align with config default
    queue_dir: Path = Path(DEFAULTS["paths"]["queue_dir"])    # Align with config default
    enable_remux: bool = True
    delete_ts_after_remux: bool = True
    delete_input_on_success: bool = False
    record_limit: int = 6
    fail_fast: bool = False
    json_logs: bool = False
    disk_free_min_bytes: int = 10 * 1024 * 1024 * 1024


class GracefulTerm:
    def __init__(self):
        self.stop = False

    def install(self, logger):
        def _handler(signum, frame):
            if not self.stop:
                logger.info("received signal, finishing current part then finalizing", extra={"extra": {"signal": signum}})
                self.stop = True

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)


def _fmt_basename(username: str, start: datetime) -> str:
    return f"{username}_{start.strftime('%Y-%m-%d_%H-%M')}"


def _streamlink_cmd(username: str, quality: str, outfile: Path, loglevel: str) -> list[str]:
    url = f"https://twitch.tv/{username}"
    # Streamlink must exist
    sl = which("streamlink") or "streamlink"
    return [sl, url, quality, "-o", str(outfile), "--loglevel", loglevel]


def _ffmpeg_concat(parts: list[Path], out_ts: Path, loglevel: str) -> int:
    """Concatenate TS parts using ffmpeg concat demuxer (stream copy)."""
    ffmpeg = which("ffmpeg") or "ffmpeg"
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(out_ts.parent)) as tf:
        for p in parts:
            # Escape single quotes for ffmpeg concat demuxer quoting rules
            sp = p.as_posix().replace("'", "'\\''")
            tf.write(f"file '{sp}'\n")
        tf.flush()
        list_path = tf.name
    cmd = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        list_path,
        "-c",
        "copy",
        "-loglevel",
        loglevel,
        "-y",
        str(out_ts),
    ]
    ret = 1
    try:
        proc = subprocess.Popen(cmd)
        ret = proc.wait()
    finally:
        try:
            os.unlink(list_path)
        except FileNotFoundError:
            pass
    return ret


def _ffmpeg_remux_to_mp4(in_ts: Path, out_mp4: Path, loglevel: str) -> int:
    ffmpeg = which("ffmpeg") or "ffmpeg"
    cmd = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-i",
        str(in_ts),
        "-c",
        "copy",
        "-bsf:a",
        "aac_adtstoasc",
        "-movflags",
        "+faststart",
        "-loglevel",
        loglevel,
        "-y",
        str(out_mp4),
    ]
    proc = subprocess.Popen(cmd)
    return proc.wait()


def record(opts: RecordOptions) -> int:
    logger = setup_logging(
        f"twitchtool.recorder.{opts.username}",
        json_logs=opts.json_logs,
    )
    logger.info("recorder start", extra={"extra": {"user": opts.username}})
    # Basic username validation
    from .utils import is_valid_twitch_username
    if not is_valid_twitch_username(opts.username):
        logger.error("invalid twitch username", extra={"extra": {"user": opts.username}})
        return 2
    # Validate tools
    if not which("streamlink"):
        logger.error("streamlink not found in PATH")
        return 2
    if not which("ffmpeg"):
        logger.error("ffmpeg not found in PATH")
        return 2

    out_dir = abspath(opts.output_dir)
    ensure_dir(out_dir)
    # Write all in-progress artifacts to a temp subdirectory, then move final outputs
    # Prefer placing temp under a TwitchTool subfolder if the output dir is a common
    # parent like Downloads or Videos, to keep things tidy by default.
    temp_base = out_dir
    try:
        base_name = out_dir.name.lower()
        if base_name in {"downloads", "videos"}:
            temp_base = ensure_dir(out_dir / "TwitchTool")
    except Exception:
        # If any issue determining/creating a subfolder, fall back to out_dir
        temp_base = out_dir
    temp_dir = ensure_dir(temp_base / "temp")
    # Low-space warning
    try:
        import shutil

        free = shutil.disk_usage(str(out_dir)).free
        if free < int(opts.disk_free_min_bytes):
            logger.error(
                "Insufficient free space on output volume",
                extra={"extra": {"free_bytes": free, "min_required": int(opts.disk_free_min_bytes)}},
            )
            return 7
    except Exception:
        pass
    base_dt = datetime.now()
    base = _fmt_basename(opts.username, base_dt)
    part_idx = 1
    parts: list[Path] = []

    # Per-user lock
    user_lock = PerUserLock(opts.username)
    try:
        user_lock.acquire()
    except UserAlreadyRecording as e:
        logger.error(str(e))
        return 3

    # Global slot
    gsm = GlobalSlotManager(opts.record_limit, logger=logger)
    try:
        gsm.acquire_slot(opts.username, fail_fast=opts.fail_fast)
    except SlotUnavailable as e:
        logger.error(str(e))
        user_lock.release()
        return 4

    # Capture loop
    gt = GracefulTerm()
    gt.install(logger)
    deadline = time.time() + float(opts.retry_window)

    logger.info(
        "begin capture loop",
        extra={"extra": {"quality": opts.quality, "retry_delay": opts.retry_delay, "retry_window": opts.retry_window}},
    )

    while True:
        if gt.stop and not parts:
            # If user requested stop before any data, just exit
            break

        part = temp_dir / f"{base}_part{part_idx:02d}.ts"
        cmd = _streamlink_cmd(opts.username, opts.quality, part, opts.loglevel)
        logger.info("start part", extra={"extra": {"part": part.name, "cmd": " ".join(shlex.quote(c) for c in cmd)}})
        try:
            proc = subprocess.Popen(cmd)
            stop_mark = None  # type: Optional[float]
            while True:
                try:
                    ret = proc.wait(timeout=1)
                    break
                except subprocess.TimeoutExpired:
                    if gt.stop:
                        # escalate signals over time to ensure child exit
                        if stop_mark is None:
                            stop_mark = time.time()
                            try:
                                proc.send_signal(signal.SIGINT)
                            except ProcessLookupError:
                                pass
                        else:
                            waited = time.time() - stop_mark
                            if waited > 10:
                                try:
                                    proc.kill()
                                except ProcessLookupError:
                                    pass
                            elif waited > 5:
                                try:
                                    proc.terminate()
                                except ProcessLookupError:
                                    pass
            logger.info("part finished", extra={"extra": {"part": part.name, "exit": ret}})
        except Exception as e:
            logger.error("failed to run streamlink", extra={"extra": {"error": str(e)}})
            ret = 1

        size = part.stat().st_size if part.exists() else 0
        if ret == 0 and size > 0:
            # success, reset deadline
            parts.append(part)
            part_idx += 1
            deadline = time.time() + float(opts.retry_window)
            if gt.stop:
                break
            else:
                # Immediately continue to try capturing next contiguous segment
                continue
        else:
            # failure / offline
            if gt.stop or time.time() > deadline:
                break
            time.sleep(float(opts.retry_delay))
            continue

    # Merge if any parts
    merged_ts = temp_dir / f"{base}.ts"
    if not parts:
        logger.warning("no parts captured; exiting without outputs")
        gsm.release_slot()
        user_lock.release()
        return 5

    merge_rc = _ffmpeg_concat(parts, merged_ts, opts.loglevel)
    if merge_rc != 0 or not merged_ts.exists() or merged_ts.stat().st_size == 0:
        logger.error("merge failed", extra={"extra": {"rc": merge_rc}})
        gsm.release_slot()
        # Keep parts for inspection
        user_lock.release()
        return 6

    # Remove parts to save space
    removed = 0
    for p in parts:
        try:
            p.unlink()
            removed += 1
        except FileNotFoundError:
            pass
    logger.info("merged parts", extra={"extra": {"out": merged_ts.name, "parts_removed": removed}})

    # Release global slot immediately after merging
    gsm.release_slot()

    if not bool(opts.enable_remux):
        # Move merged TS to final directory before exiting
        final_ts = out_dir / merged_ts.name
        try:
            merged_ts.replace(final_ts)
            logger.info("finalized output", extra={"extra": {"moved_to": final_ts.name}})
        except Exception as e:
            logger.error("failed to move final TS", extra={"extra": {"error": str(e), "src": str(merged_ts), "dst": str(final_ts)}})
            final_ts = merged_ts  # fallback: keep in temp
        logger.info(
            "remux disabled; leaving merged TS and skipping encode queue",
            extra={"extra": {"output": final_ts.name}},
        )
        user_lock.release()
        logger.info("recorder done")
        return 0

    # Try remux
    remux_mp4 = temp_dir / f"{base}.mp4"
    remux_rc = _ffmpeg_remux_to_mp4(merged_ts, remux_mp4, opts.loglevel)
    use_input = merged_ts
    if remux_rc == 0 and remux_mp4.exists() and remux_mp4.stat().st_size > 0:
        logger.info("remux success", extra={"extra": {"out": remux_mp4.name}})
        use_input = remux_mp4
        if opts.delete_ts_after_remux and merged_ts.exists():
            try:
                merged_ts.unlink()
            except Exception:
                pass
    else:
        logger.warning("remux failed, keeping TS for encode", extra={"extra": {"rc": remux_rc}})

    # Move final input(s) from temp to the configured output directory
    moved_input = out_dir / use_input.name
    try:
        use_input.replace(moved_input)
        logger.info("finalized input for encode", extra={"extra": {"moved_to": moved_input.name}})
    except Exception as e:
        logger.error("failed to move input to final directory", extra={"extra": {"error": str(e), "src": str(use_input), "dst": str(moved_input)}})
        moved_input = use_input  # fallback: encode from temp

    # If we kept the TS alongside a successful remux, also move it to final dir
    if use_input == remux_mp4 and merged_ts.exists() and not opts.delete_ts_after_remux:
        ts_final = out_dir / merged_ts.name
        try:
            merged_ts.replace(ts_final)
            logger.info("kept TS alongside MP4", extra={"extra": {"moved_to": ts_final.name}})
        except Exception as e:
            logger.error("failed to move TS to final directory", extra={"extra": {"error": str(e), "src": str(merged_ts), "dst": str(ts_final)}})

    # Enqueue encode job
    final_out = out_dir / f"{base}_compressed.mp4"
    job = Job(
        input=str(moved_input.resolve()),
        output=str(final_out.resolve()),
        loglevel=opts.loglevel,
        delete_input_on_success=bool(opts.delete_input_on_success),
    )
    job_path = write_job(opts.queue_dir.expanduser(), job)
    logger.info("enqueued encode job", extra={"extra": {"job": str(job_path)}})

    # Release per-user lock
    user_lock.release()
    logger.info("recorder done")
    return 0
