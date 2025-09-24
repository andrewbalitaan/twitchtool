from __future__ import annotations

from pathlib import Path

import pytest

from twitchtool.locks import OwnerInfo
from twitchtool.queue import Job, write_job
from twitchtool.status import QueueStatus, gather_status, format_report


class DummyGSM:
    def __init__(self, record_limit, *, slots_dir=None, logger=None):
        self._owners = [
            OwnerInfo(
                slot_index=1,
                pid=1234,
                username="djalpha",
                started_at="2025-01-01T00:00:00Z",
                owner_path=Path("/tmp/slot1.owner"),
            )
        ]

    def list_active_owners(self):
        return list(self._owners)


def test_gather_status(monkeypatch, tmp_path):
    from twitchtool import status

    monkeypatch.setattr(status, "GlobalSlotManager", DummyGSM)
    monkeypatch.setattr(status, "_detect_encoder_running", lambda: True)

    queue_dir = tmp_path / "queue"
    job = Job(
        input=str((tmp_path / "input.ts").resolve()),
        output=str((tmp_path / "output.mp4").resolve()),
        loglevel="error",
    )
    write_job(queue_dir, job)
    jobs_dir = (queue_dir / "jobs")
    failed = jobs_dir / "failed.failed.json"
    failed.write_text("{}")
    error = jobs_dir / "failed.error.json"
    error.write_text("{}")

    report = gather_status(queue_dir, record_limit=2)

    assert report.downloads and report.downloads[0].username == "djalpha"
    assert report.queue.jobs and report.queue.jobs[0].job.output.endswith("output.mp4")
    assert report.queue.failed == [failed]
    assert report.queue.errors == [error]
    assert report.encoder_running is True
    assert report.downloads_error is None


def test_format_report(monkeypatch, tmp_path, capsys):
    from twitchtool import status

    # Prepare report manually
    jobs_dir = tmp_path / "jobs" / "jobs"
    jobs_dir.mkdir(parents=True)
    job = Job(
        input=str((tmp_path / "input.ts").resolve()),
        output=str((tmp_path / "output.mp4").resolve()),
        loglevel="error",
    )
    write_job(tmp_path / "jobs", job)

    queue_status = QueueStatus(
        jobs=status.list_jobs(tmp_path / "jobs"),
        failed=[],
        errors=[],
        queue_dir=(tmp_path / "jobs").resolve(),
    )

    report = status.StatusReport(
        downloads=[
            OwnerInfo(
                slot_index=2,
                pid=5678,
                username="djbeta",
                started_at="2025-01-02T00:00:00Z",
                owner_path=Path("/tmp/slot2.owner"),
            )
        ],
        queue=queue_status,
        encoder_running=False,
        downloads_error=None,
        encoder_error=None,
    )

    output = status.format_report(report)
    assert "  1." in output or "  2." in output
    assert "djbeta" in output
    assert "input:" in output and "output:" in output
    assert "Encoder daemon: not running" in output
    expected_queue = (tmp_path / "jobs").resolve() / "jobs"
    assert f"queue: {expected_queue}" in output
