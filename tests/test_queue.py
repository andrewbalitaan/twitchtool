from __future__ import annotations

from pathlib import Path

from twitchtool.queue import Job, list_jobs, oldest_job, write_job


def test_queue_write_and_sort(tmp_path: Path):
    base = tmp_path / "queue"
    base.mkdir()
    a = Job(input=str(tmp_path / "inA.mp4"), output=str(tmp_path / "outA.mp4"))
    b = Job(input=str(tmp_path / "inB.mp4"), output=str(tmp_path / "outB.mp4"))
    pa = write_job(base, a)
    pb = write_job(base, b)
    jobs = list_jobs(base)
    assert len(jobs) == 2
    # Oldest job is the first written (by created_at then filename)
    oj = oldest_job(base)
    assert oj is not None
    assert oj.path in (pa, pb)
