from __future__ import annotations

import fcntl
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .encoder_daemon import ENCODER_LOCK_PATH
from .locks import GlobalSlotManager, OwnerInfo
from .queue import JobEntry, list_jobs
from .utils import abspath, ensure_dir


@dataclass
class QueueStatus:
    jobs: list[JobEntry]
    failed: list[Path]
    errors: list[Path]
    queue_dir: Path


@dataclass
class StatusReport:
    downloads: list[OwnerInfo]
    queue: QueueStatus
    encoder_running: Optional[bool]
    downloads_error: Optional[str]
    encoder_error: Optional[str]


def _detect_encoder_running() -> bool:
    path = ENCODER_LOCK_PATH
    ensure_dir(path.parent)
    with path.open("a+") as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Already locked by running daemon
            return True
        else:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            return False


def gather_status(queue_dir: Path, record_limit: int) -> StatusReport:
    qdir = abspath(queue_dir)
    ensure_dir(qdir)

    downloads: list[OwnerInfo] = []
    downloads_error: Optional[str] = None
    try:
        gsm = GlobalSlotManager(record_limit)
        downloads = gsm.list_active_owners()
    except Exception as exc:
        downloads_error = str(exc)

    jobs = list_jobs(qdir)
    jobs_dir = qdir / "jobs"
    ensure_dir(jobs_dir)
    failed = sorted(jobs_dir.glob("*.failed.json"))
    errors = sorted(jobs_dir.glob("*.error.json"))

    encoder_running: Optional[bool] = None
    encoder_error: Optional[str] = None
    try:
        encoder_running = _detect_encoder_running()
    except Exception as exc:
        encoder_error = str(exc)

    return StatusReport(
        downloads=downloads,
        queue=QueueStatus(jobs=jobs, failed=failed, errors=errors, queue_dir=qdir),
        encoder_running=encoder_running,
        downloads_error=downloads_error,
        encoder_error=encoder_error,
    )


def format_report(report: StatusReport) -> str:
    lines: list[str] = []

    lines.append("Active downloads:")
    if report.downloads:
        for owner in sorted(report.downloads, key=lambda o: o.slot_index):
            lines.append(
                f"  slot {owner.slot_index}: {owner.username} (pid={owner.pid}, since={owner.started_at})"
            )
    elif report.downloads_error:
        lines.append(f"  unavailable: {report.downloads_error}")
    else:
        lines.append("  none")

    lines.append("")

    q = report.queue
    total_jobs = len(q.jobs)
    queue_path = q.queue_dir / "jobs"
    if total_jobs:
        lines.append(f"Pending encode jobs ({total_jobs}):")
        for idx, entry in enumerate(q.jobs, start=1):
            job = entry.job
            input_path = Path(job.input)
            output_path = Path(job.output)
            lines.append(f"  {idx}. {input_path.stem}")
            lines.append(f"     input:  {input_path.name}")
            lines.append(f"     output: {output_path.name}")
            lines.append(f"     (created {job.created_at or 'n/a'})")
            lines.append("")
        if lines and lines[-1] == "":
            lines.pop()
    else:
        lines.append("Pending encode jobs: none")
    lines.append("")
    lines.append(f"queue: {queue_path}")

    if q.failed:
        lines.append(f"Failed jobs ({len(q.failed)}):")
        for path in q.failed:
            lines.append(f"  {path}")

    if q.errors:
        lines.append(f"Errored jobs ({len(q.errors)}):")
        for path in q.errors:
            lines.append(f"  {path}")

    lines.append("")
    if report.encoder_running is None:
        if report.encoder_error:
            lines.append(f"Encoder daemon: unknown ({report.encoder_error})")
        else:
            lines.append("Encoder daemon: unknown")
    else:
        lines.append(f"Encoder daemon: {'running' if report.encoder_running else 'not running'}")
    if report.encoder_error:
        lines.append(f"  note: {report.encoder_error}")

    return "\n".join(lines).rstrip() + "\n"


def print_report(report: StatusReport) -> None:
    sysout = format_report(report)
    print(sysout, end="")
