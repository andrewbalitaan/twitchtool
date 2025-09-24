from __future__ import annotations

import fcntl
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Optional

from .utils import atomic_write_json, ensure_dir, is_process_alive, read_json, now_utc_iso


class SlotUnavailable(RuntimeError):
    pass


class UserAlreadyRecording(RuntimeError):
    pass


def _default_runtime_dir() -> Path:
    uid = os.getuid()
    run_base = Path(f"/run/user/{uid}")
    if run_base.exists():
        return run_base / "twitch-record-slots"
    return Path("/tmp") / "twitch-record-slots"


@dataclass
class OwnerInfo:
    slot_index: int
    pid: int
    username: str
    started_at: str
    owner_path: Path


class GlobalSlotManager:
    """Manages global recording slots (N concurrent recordings).

    Uses flock-exclusively held files `slot1..slotN` in the slots directory.
    For each held slot, an adjacent `slotN.owner` JSON file is written atomically with PID and username.

    The encoder daemon uses the presence of valid owner files to determine if downloads are active.
    """

    def __init__(self, record_limit: int, *, slots_dir: Optional[Path] = None, logger=None):
        if record_limit <= 0:
            raise ValueError("record_limit must be >= 1")
        self.record_limit = record_limit
        self.dir = slots_dir or _default_runtime_dir()
        self.logger = logger
        ensure_dir(self.dir)
        # Precreate slot files
        for i in range(1, self.record_limit + 1):
            (self.dir / f"slot{i}").touch(exist_ok=True)
        self._held: list[tuple[int, IO[str]]] = []
        # Cleanup any stale owner files
        try:
            self.cleanup_stale_owners()
        except OSError:
            # best effort: filesystem hiccup
            pass

    def slot_path(self, i: int) -> Path:
        return self.dir / f"slot{i}"

    def owner_path(self, i: int) -> Path:
        return self.dir / f"slot{i}.owner"

    def _slot_is_locked(self, i: int) -> bool:
        """Return True if slot i is currently flocked by some process (not us)."""
        p = self.slot_path(i)
        f = p.open("a+")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Locked by another process
            try:
                f.close()
            except Exception:
                pass
            return True
        else:
            # Not locked; release and return False
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            finally:
                f.close()
            return False

    def acquire_slot(self, username: str, *, fail_fast: bool = False, wait_log_every: float = 5.0) -> int:
        """Acquire a free slot. Returns slot index (1-based). Writes owner file.

        If fail_fast is True and no slot immediately available, raises SlotUnavailable.
        Otherwise waits, logging every few seconds.
        """
        start = time.time()
        _last_logged_s = -1
        while True:
            for i in range(1, self.record_limit + 1):
                # Skip slots that have a live owner file regardless of process identity
                op = self.owner_path(i)
                if op.exists():
                    try:
                        d = read_json(op)
                        pid = int(d.get("pid", -1))
                    except Exception:
                        pid = -1
                    if is_process_alive(pid):
                        continue
                p = self.slot_path(i)
                f = p.open("a+")
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    f.close()
                    continue
                # Got a slot
                self._held.append((i, f))
                owner = {"pid": os.getpid(), "username": username, "started_at": now_utc_iso()}
                atomic_write_json(self.owner_path(i), owner)
                if self.logger:
                    self.logger.info(f"acquired global slot {i}", extra={"extra": {"slot": i, "username": username}})
                return i

            if fail_fast:
                raise SlotUnavailable("no slot available (fail-fast)")

            # Wait and try again
            waited = time.time() - start
            cur_s = int(waited)
            if self.logger and cur_s % int(wait_log_every) == 0 and cur_s != _last_logged_s:
                try:
                    active = [o.username for o in self.list_active_owners()]
                    self.logger.info(
                        "waiting for a free recording slot",
                        extra={"extra": {"active_users": active, "waited_s": int(waited)}},
                    )
                except Exception:
                    pass
                _last_logged_s = cur_s
            time.sleep(1.0)
            # Periodically drop stale owners
            try:
                self.cleanup_stale_owners()
            except OSError:
                pass

    def release_slot(self) -> None:
        """Release the held slot and remove owner file."""
        if not self._held:
            return
        i, f = self._held.pop()
        try:
            owner = self.owner_path(i)
            try:
                owner.unlink()
            except FileNotFoundError:
                pass
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            f.close()
            if self.logger:
                self.logger.info(f"released global slot {i}", extra={"extra": {"slot": i}})
        finally:
            pass

    def list_active_owners(self) -> list[OwnerInfo]:
        """Return validated owner infos (pid alive AND slot currently locked).
        Also cleans stale owner files.
        """
        infos: list[OwnerInfo] = []
        for i in range(1, self.record_limit + 1):
            op = self.owner_path(i)
            if not op.exists():
                continue
            try:
                data = read_json(op)
                pid = int(data.get("pid", -1))
                username = str(data.get("username", ""))
                started_at = str(data.get("started_at", ""))
            except Exception:
                # Bad file, remove it
                try:
                    op.unlink()
                except FileNotFoundError:
                    pass
                continue
            # Treat as active only if PID appears alive AND slot is actually locked.
            if is_process_alive(pid) and self._slot_is_locked(i):
                infos.append(OwnerInfo(i, pid, username, started_at, op))
            else:
                # Stale, remove
                try:
                    op.unlink()
                except FileNotFoundError:
                    pass
        return infos

    def active_count(self) -> int:
        return len(self.list_active_owners())

    def cleanup_stale_owners(self) -> int:
        """Remove owner files whose PIDs are not alive OR whose slots are not locked.
        Returns number removed.
        """
        removed = 0
        for i in range(1, self.record_limit + 1):
            op = self.owner_path(i)
            if not op.exists():
                continue
            try:
                d = read_json(op)
                pid = int(d.get("pid", -1))
            except Exception:
                pid = -1
            if (not is_process_alive(pid)) or (not self._slot_is_locked(i)):
                try:
                    op.unlink()
                    removed += 1
                except FileNotFoundError:
                    pass
        if self.logger and removed:
            self.logger.info("cleaned stale owner files", extra={"extra": {"removed": removed}})
        return removed


class PerUserLock:
    """Exclusive per-user lock to prevent duplicate recordings.

    Uses flock on /tmp/twitch-active-users/<user>.lock
    """

    def __init__(self, username: str, *, base_dir: Optional[Path] = None):
        self.username = username
        self.dir = base_dir or Path("/tmp") / "twitch-active-users"
        ensure_dir(self.dir)
        self.path = self.dir / f"{username}.lock"
        self._f: Optional[IO[str]] = None

    def acquire(self, *, fail_fast: bool = True) -> None:
        f = self.path.open("a+")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            f.close()
            if fail_fast:
                raise UserAlreadyRecording(f"user '{self.username}' already being recorded")
            else:
                # Optional blocking behavior if ever needed
                f = self.path.open("a+")
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        self._f = f

    def release(self) -> None:
        if not self._f:
            return
        try:
            fcntl.flock(self._f.fileno(), fcntl.LOCK_UN)
        finally:
            self._f.close()
            self._f = None
        # We keep the .lock file around to minimize races; file presence alone is not authoritative.

    @classmethod
    def is_user_locked(cls, username: str, *, base_dir: Optional[Path] = None) -> bool:
        path = (base_dir or Path("/tmp") / "twitch-active-users") / f"{username}.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        f = path.open("a+")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            f.close()
            return True
        else:
            # Not locked; release immediately
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            f.close()
            return False
