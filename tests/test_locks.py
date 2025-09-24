from __future__ import annotations

import os
from pathlib import Path

import pytest

from twitchtool.locks import GlobalSlotManager, PerUserLock, UserAlreadyRecording
from twitchtool.utils import atomic_write_json


def test_per_user_lock(tmp_path: Path):
    base = tmp_path / "locks"
    base.mkdir()
    l1 = PerUserLock("alice", base_dir=base)
    l1.acquire()
    with pytest.raises(UserAlreadyRecording):
        l2 = PerUserLock("alice", base_dir=base)
        l2.acquire()
    l1.release()
    # Should be acquirable again
    l3 = PerUserLock("alice", base_dir=base)
    l3.acquire()
    l3.release()


def test_global_slots(tmp_path: Path):
    slots = tmp_path / "slots"
    gsm = GlobalSlotManager(2, slots_dir=slots)
    s1 = gsm.acquire_slot("alice")
    assert s1 in (1, 2)
    s2 = gsm.acquire_slot("bob")
    assert s2 in (1, 2) and s2 != s1
    # No slot left; next call should block. We simulate fail_fast mode.
    with pytest.raises(Exception):
        gsm.acquire_slot("carol", fail_fast=True)
    gsm.release_slot()
    # After release, should be free again
    s3 = gsm.acquire_slot("carol", fail_fast=True)
    assert s3 in (1, 2)
    gsm.release_slot()
    # Cleanup owners works and doesn't crash
    gsm.cleanup_stale_owners()


def test_cleanup_removes_unlocked_owner(tmp_path: Path):
    # Prepare slots dir and manager
    slots = tmp_path / "slots_unlocked"
    gsm = GlobalSlotManager(1, slots_dir=slots)

    # Manually create an owner file with a live PID but without taking the lock
    op = gsm.owner_path(1)
    atomic_write_json(
        op,
        {
            "pid": os.getpid(),
            "username": "alice",
            "started_at": "2025-01-01T00:00:00Z",
        },
    )

    assert op.exists()

    # Since the slot file is not flocked, cleanup should remove the owner file
    removed = gsm.cleanup_stale_owners()
    assert removed == 1
    assert not op.exists()
