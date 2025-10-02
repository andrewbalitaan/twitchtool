#!/usr/bin/env python3
"""Helper wrapper around `twitchtool tscompress` for batch remux + encode."""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running directly from the repository without installing the package.
if __name__ == "__main__" and __package__ is None:
    repo_root = Path(__file__).resolve().parents[1]
    src_path = repo_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

from twitchtool.cli import main as twitchtool_main  # noqa: E402


def main(argv: list[str]) -> int:
    try:
        twitchtool_main(["tscompress", *argv])
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
