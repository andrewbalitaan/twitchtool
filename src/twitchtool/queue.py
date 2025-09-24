from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .utils import atomic_write_json, ensure_dir, read_json, abspath, now_utc_iso


JOBS_SUBDIR = "jobs"


@dataclass
class Job:
    input: str
    output: str
    loglevel: str = "error"
    delete_input_on_success: bool = False
    created_at: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "in": self.input,
            "out": self.output,
            "loglevel": self.loglevel,
            "delete_input_on_success": self.delete_input_on_success,
            "created_at": self.created_at or now_utc_iso(),
        }


@dataclass
class JobEntry:
    path: Path
    job: Job


def queue_dir(base: Path) -> Path:
    return ensure_dir(base.expanduser() / JOBS_SUBDIR)


def write_job(base: Path, job: Job) -> Path:
    jd = job.as_dict()
    in_path = Path(jd["in"])
    out_path = Path(jd["out"])
    if not in_path.is_absolute() or not out_path.is_absolute():
        raise ValueError("job paths must be absolute")
    jobs = queue_dir(base)
    # Use created_at + basename to keep sort stable
    created = jd["created_at"].replace(":", "").replace("-", "")
    basename = out_path.stem
    name = f"{created}_{basename}.json"
    path = jobs / name
    atomic_write_json(path, jd)
    return path


def read_job(path: Path) -> Job:
    d = read_json(path)
    return Job(
        input=str(d["in"]),
        output=str(d["out"]),
        loglevel=str(d.get("loglevel", "error")),
        delete_input_on_success=bool(d.get("delete_input_on_success", False)),
        created_at=str(d.get("created_at", "")),
    )


def list_jobs(base: Path) -> list[JobEntry]:
    jobs = queue_dir(base)
    entries: list[JobEntry] = []
    for p in sorted(jobs.glob("*.json")):
        # Skip sidecar error/failed records
        if p.name.endswith(".error.json") or p.name.endswith(".failed.json"):
            continue
        try:
            j = read_job(p)
        except (json.JSONDecodeError, KeyError, ValueError, OSError):
            # Skip unreadable job
            continue
        entries.append(JobEntry(p, j))
    # Sort by created_at then filename (already lexicographically sorted)
    entries.sort(key=lambda e: (e.job.created_at, e.path.name))
    return entries


def oldest_job(base: Path) -> Optional[JobEntry]:
    lst = list_jobs(base)
    if not lst:
        return None
    return lst[0]


def write_error_for_job(job_path: Path, reason: str) -> Path:
    err = job_path.with_suffix(".error.json")
    atomic_write_json(err, {"job": job_path.name, "reason": reason, "ts": now_utc_iso()})
    return err
