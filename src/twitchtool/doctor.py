from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from .locks import GlobalSlotManager
from .queue import queue_dir
from .utils import setup_logging, which, abspath


def doctor(queue_dir_path: Path, logs_dir: Path, record_limit: int = 6) -> int:
    logger = setup_logging("twitchtool.doctor")

    ok = True

    # Check binaries
    for bin_name in ("streamlink", "ffmpeg"):
        path = which(bin_name)
        if not path:
            logger.error(f"{bin_name} not found in PATH")
            ok = False
        else:
            try:
                if bin_name == "streamlink":
                    out = subprocess.check_output([path, "--version"], text=True).strip()
                else:
                    out = subprocess.check_output([path, "-version"], text=True).splitlines()[0]
            except Exception:
                out = "unknown"
            logger.info(f"{bin_name}: {path} ({out})")

    # Check slots directory (respect configured record limit)
    gsm = GlobalSlotManager(record_limit, logger=logger)
    logger.info(f"slots directory: {gsm.dir}")

    # Check queue dir
    q = queue_dir(abspath(queue_dir_path))
    logger.info(f"queue jobs dir: {q} (exists={q.exists()})")

    # Check logs dir
    logs = abspath(logs_dir)
    logs.mkdir(parents=True, exist_ok=True)
    logger.info(f"logs dir: {logs} (exists={logs.exists()})")

    # Check disk space of queue dir
    usage = shutil.disk_usage(str(q))
    logger.info(
        "disk usage",
        extra={"extra": {"free": usage.free, "total": usage.total, "percent_free": int(usage.free * 100 / usage.total)}},
    )

    if ok:
        logger.info("doctor: environment looks OK")
        return 0
    else:
        logger.error("doctor: issues found (see above)")
        return 2
