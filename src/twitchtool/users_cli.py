from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Sequence

from .utils import ensure_dir, is_valid_twitch_username


def _read_users(path: Path) -> list[str]:
    users: list[str] = []
    if not path.exists():
        return users
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            name = line.strip()
            if not name or name.startswith("#"):
                continue
            users.append(name)
    return users


def _normalize(name: str) -> str:
    return name.lower()


def list_users(path: Path) -> int:
    path = path.expanduser()
    users = sorted(_read_users(path), key=_normalize)
    if not users:
        print(f"No users configured (source: {path}).")
        return 0
    print(f"Users file: {path}")
    for user in users:
        print(user)
    return 0


def add_users(path: Path, names: Sequence[str]) -> int:
    path = path.expanduser()
    ensure_dir(path.parent)
    existing = _read_users(path)
    existing_norm = {_normalize(u) for u in existing}

    added: list[str] = []
    added_norm: set[str] = set()
    skipped: list[str] = []
    invalid: list[str] = []

    for name in names:
        norm = _normalize(name)
        if not is_valid_twitch_username(name):
            invalid.append(name)
            continue
        if norm in existing_norm or norm in added_norm:
            skipped.append(name)
            continue
        added.append(name)
        added_norm.add(norm)

    if added:
        with path.open("a", encoding="utf-8") as f:
            for name in added:
                f.write(name + "\n")
        print(f"Added {len(added)} user(s): {', '.join(added)}")
    if skipped:
        print(f"Skipped existing user(s): {', '.join(skipped)}")
    if invalid:
        print(f"Invalid username(s): {', '.join(invalid)}", file=sys.stderr)
    return 0 if not invalid else 1


def remove_users(path: Path, names: Iterable[str]) -> int:
    path = path.expanduser()
    if not path.exists():
        print(f"Users file does not exist: {path}")
        return 1

    targets = {_normalize(n) for n in names}
    if not targets:
        print("No users specified for removal.")
        return 0

    removed: list[str] = []
    lines_out: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and _normalize(stripped) in targets:
                removed.append(stripped)
                continue
            lines_out.append(line.rstrip("\n"))

    if removed:
        if lines_out:
            path.write_text("\n".join(lines_out) + "\n", encoding="utf-8")
        else:
            path.write_text("", encoding="utf-8")
        print(f"Removed {len(removed)} user(s): {', '.join(removed)}")
        return 0
    else:
        print("No matching users found to remove.")
        return 1
