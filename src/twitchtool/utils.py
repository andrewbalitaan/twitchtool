from __future__ import annotations

import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, Optional
import re


ISO = "%Y-%m-%dT%H:%M:%S%z"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent)) as tf:
        json.dump(data, tf, ensure_ascii=False, separators=(",", ":"))
        tf.flush()
        os.fsync(tf.fileno())
        tmpname = tf.name
    os.replace(tmpname, path)


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but owned by another user
        return True
    else:
        return True


def which(cmd: str) -> Optional[str]:
    return shutil.which(cmd)


@dataclass
class Completed:
    returncode: int
    stdout: str
    stderr: str


def run_capture(cmd: list[str], timeout: Optional[int] = None) -> Completed:
    """Run command, capture stdout/err, return Completed.

    Raises subprocess.TimeoutExpired on timeout.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        out, err = proc.communicate(timeout=timeout)
    finally:
        # nothing special
        pass
    return Completed(proc.returncode, out or "", err or "")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload = {
            "ts": datetime.utcfromtimestamp(record.created).replace(tzinfo=timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if hasattr(record, "extra"):
            try:
                payload.update(getattr(record, "extra"))
            except Exception:
                pass
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(
    name: str,
    *,
    level: int = logging.INFO,
    json_logs: bool = False,
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """Configure logging.

    - If JOURNAL_STREAM is present, prefer stdout.
    - Else, if log_file is given, rotate there.
    - Else, stdout.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    under_systemd = "JOURNAL_STREAM" in os.environ
    fmt_text = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = JsonFormatter() if json_logs else logging.Formatter(fmt_text)

    if under_systemd or log_file is None:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        ensure_dir(log_file.parent)
        handler = RotatingFileHandler(str(log_file), maxBytes=5 * 1024 * 1024, backupCount=5)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.debug("logger initialized", extra={"extra": {"systemd": under_systemd, "json": json_logs}})
    return logger


def build_nice_ionice_prefix() -> list[str]:
    parts: list[str] = []
    if which("nice"):
        parts += ["nice", "-n", "10"]
    if which("ionice"):
        # Best effort idle-ish
        parts += ["ionice", "-c", "2", "-n", "7"]
    return parts


def abspath(p: Path) -> Path:
    return p.expanduser().resolve()


def safe_unlink(p: Path) -> None:
    try:
        p.unlink()
    except FileNotFoundError:
        pass


def sigcont_if_stopped(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGCONT)
    except ProcessLookupError:
        pass
    except PermissionError:
        pass


def human_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.0f}{unit}"
        n = int(n / 1024)
    return f"{n}PB"


_TWITCH_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,25}$")


def is_valid_twitch_username(name: str) -> bool:
    """Basic validation for Twitch usernames.

    Twitch allows letters, numbers, underscores; typical length 3-25.
    """
    return bool(_TWITCH_USERNAME_RE.match(name))
