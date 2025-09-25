#!/usr/bin/env python3
"""
Remux and compress multiple .ts files serially.

For each input .ts:
  1) Attempt fast remux to .mp4 (stream copy, +faststart, aac_adtstoasc)
  2) Encode H.265 (libx265) to <basename>_compressed.mp4 with defaults matching the encoder daemon:
     - scale=-2:<height> (even width), fps, CRF, preset, threads, +faststart, AAC 128k

Usage:
  python3 scripts/remux_compress_serial.py file1.ts file2.ts ...

Options mirror encoder defaults; see --help.

Notes:
  - Processes inputs one-by-one (serial).
  - Skips encoding if the final output exists unless --overwrite is given.
  - By default, keeps the .ts after successful remux; use --delete-ts-after-remux to remove it.
  - Requires ffmpeg in PATH.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List


def which(cmd: str) -> str | None:
    return shutil.which(cmd)


def run_ffmpeg(cmd: list[str]) -> int:
    try:
        proc = subprocess.Popen(cmd)
        return proc.wait()
    except OSError as e:
        print(f"Failed to start ffmpeg: {e}", file=sys.stderr)
        return 1


def remux_ts_to_mp4(in_ts: Path, out_mp4: Path, loglevel: str) -> int:
    ffmpeg = which("ffmpeg") or "ffmpeg"
    cmd = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-i",
        str(in_ts),
        "-c",
        "copy",
        "-bsf:a",
        "aac_adtstoasc",
        "-movflags",
        "+faststart",
        "-loglevel",
        loglevel,
        "-y",
        str(out_mp4),
    ]
    print("[remux]", " ".join(cmd))
    return run_ffmpeg(cmd)


def encode_h265(
    src: Path,
    dst: Path,
    *,
    height: int,
    fps: int,
    crf: int,
    preset: str,
    threads: int,
    loglevel: str,
) -> int:
    ffmpeg = which("ffmpeg") or "ffmpeg"
    cmd = [
        ffmpeg,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        loglevel,
        "-y",
        "-i",
        str(src),
        "-vf",
        f"scale=-2:{int(height)}",
        "-r",
        str(int(fps)),
        "-c:v",
        "libx265",
        "-crf",
        str(int(crf)),
        "-preset",
        str(preset),
        "-threads",
        str(int(threads)),
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(dst),
    ]
    print("[encode]", " ".join(cmd))
    return run_ffmpeg(cmd)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Remux and compress .ts files serially")
    ap.add_argument("inputs", nargs="+", help="One or more .ts files (shell globs OK)")
    ap.add_argument("--height", type=int, default=480, help="Output height (scale=-2:HEIGHT) [default: 480]")
    ap.add_argument("--fps", type=int, default=30, help="Output FPS [default: 30]")
    ap.add_argument("--crf", type=int, default=26, help="x265 CRF [default: 26]")
    ap.add_argument("--preset", default="medium", help="x265 preset [default: medium]")
    ap.add_argument("--threads", type=int, default=1, help="Encoder threads [default: 1]")
    ap.add_argument("--loglevel", default="error", help="ffmpeg loglevel [default: error]")
    ap.add_argument("--keep-ts", action="store_true", default=None, help="Keep .ts after successful remux (default)")
    ap.add_argument(
        "--delete-ts-after-remux",
        action="store_true",
        help="Delete .ts after successful remux",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing outputs (remux/encoded). Default: skip if exists",
    )
    ap.add_argument(
        "--delete-input-on-success",
        action="store_true",
        help="Delete input used for encode (TS or remuxed MP4) after successful encode",
    )

    args = ap.parse_args(argv)

    if not which("ffmpeg"):
        print("ffmpeg not found in PATH", file=sys.stderr)
        return 2

    # Expand inputs; shell usually expands globs, but support literal patterns too
    files: list[Path] = []
    for pat in args.inputs:
        # If the path exists as-is, use directly; otherwise try glob
        p = Path(pat)
        if p.exists():
            files.append(p)
            continue
        import glob

        matched = [Path(x) for x in glob.glob(pat)]
        if matched:
            files.extend(matched)
        else:
            print(f"[warn] no match for input: {pat}", file=sys.stderr)

    # De-duplicate and keep stable order
    seen = set()
    uniq_files: list[Path] = []
    for f in files:
        rp = f.resolve()
        if rp not in seen:
            seen.add(rp)
            uniq_files.append(f)

    if not uniq_files:
        print("No inputs to process", file=sys.stderr)
        return 1

    rc_overall = 0
    for in_path in uniq_files:
        try:
            in_path = in_path.resolve()
            if not in_path.exists():
                print(f"[skip] missing: {in_path}")
                rc_overall = rc_overall or 1
                continue
            if in_path.suffix.lower() != ".ts":
                print(f"[skip] not a .ts: {in_path}")
                rc_overall = rc_overall or 1
                continue

            base = in_path.with_suffix("").name
            out_dir = in_path.parent
            remux_mp4 = out_dir / f"{base}.mp4"
            final_mp4 = out_dir / f"{base}_compressed.mp4"

            print(f"\n==> Processing {in_path.name}")

            # Remux step
            do_remux = args.overwrite or not (remux_mp4.exists() and remux_mp4.stat().st_size > 0)
            use_input = in_path
            if do_remux:
                rrc = remux_ts_to_mp4(in_path, remux_mp4, args.loglevel)
                if rrc == 0 and remux_mp4.exists() and remux_mp4.stat().st_size > 0:
                    print(f"[ok] remuxed -> {remux_mp4.name}")
                    use_input = remux_mp4
                    # Default: keep TS unless explicitly asked to delete
                    delete_ts = bool(args.delete_ts_after_remux) and not bool(args.keep_ts)
                    if delete_ts:
                        try:
                            in_path.unlink()
                            print(f"[info] deleted TS: {in_path.name}")
                        except Exception:
                            pass
                else:
                    print(f"[warn] remux failed (rc={rrc}); will encode from TS")
            else:
                print(f"[skip] remux exists: {remux_mp4.name}")
                use_input = remux_mp4 if remux_mp4.exists() else in_path

            # Encode step
            if final_mp4.exists() and final_mp4.stat().st_size > 0 and not args.overwrite:
                print(f"[skip] encoded exists: {final_mp4.name}")
                continue

            erc = encode_h265(
                use_input,
                final_mp4,
                height=args.height,
                fps=args.fps,
                crf=args.crf,
                preset=args.preset,
                threads=args.threads,
                loglevel=args.loglevel,
            )
            if erc == 0 and final_mp4.exists() and final_mp4.stat().st_size > 0:
                print(f"[ok] encoded -> {final_mp4.name}")
                if args.delete_input_on_success:
                    try:
                        use_input.unlink()
                        print(f"[info] deleted input: {use_input.name}")
                    except Exception:
                        pass
            else:
                print(f"[fail] encode rc={erc} for {in_path.name}")
                rc_overall = rc_overall or (erc or 1)
        except KeyboardInterrupt:
            print("Interrupted")
            return 130
    return rc_overall


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
