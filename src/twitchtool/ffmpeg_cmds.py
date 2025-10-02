from __future__ import annotations

import shutil
import glob
from pathlib import Path
from typing import List, Sequence

_MAX_INT = str(2_147_483_647)


class FfmpegNotFound(RuntimeError):
    """Raised when the requested ffmpeg binary cannot be located."""


def resolve_ffmpeg(binary: str) -> str:
    """Return the absolute path to *binary* or raise if not found."""
    path = shutil.which(binary)
    if not path:
        raise FfmpegNotFound(f"ffmpeg binary '{binary}' not found in PATH")
    return path


def build_scale_filter(max_height: int | None) -> str | None:
    """Return a scale filter that caps height while preserving aspect ratio."""
    if max_height is None:
        return None
    if max_height < 0:
        raise ValueError("max height must be positive")
    if max_height == 0:
        return None
    if max_height % 2:
        max_height -= 1
    if max_height <= 0:
        return None
    return (
        f"scale=-2:{max_height}:"
        "flags=lanczos:force_original_aspect_ratio=decrease:force_divisible_by=2"
    )


def _base_ts_args(
    src: Path,
    *,
    loglevel: str,
    stats: bool,
    overwrite: bool,
    ffmpeg_bin: str,
) -> List[str]:
    parts: List[str] = [
        ffmpeg_bin,
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        loglevel,
    ]
    if stats:
        parts.append("-stats")
    parts.extend(
        [
            "-copyts",
            "-start_at_zero",
            "-fflags",
            "+discardcorrupt",
            "-analyzeduration",
            _MAX_INT,
            "-probesize",
            _MAX_INT,
            "-i",
            str(src),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-dn",
        ]
    )
    if overwrite:
        parts.extend(["-y"])
    return parts


def build_remux_cmd(
    ffmpeg_bin: str,
    src: Path,
    dst: Path,
    *,
    loglevel: str,
    stats: bool = False,
    overwrite: bool = True,
) -> List[str]:
    cmd = _base_ts_args(src, loglevel=loglevel, stats=stats, overwrite=overwrite, ffmpeg_bin=ffmpeg_bin)
    cmd.extend(
        [
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            "-movflags",
            "+faststart",
            "-video_track_timescale",
            "90000",
            str(dst),
        ]
    )
    return cmd


def build_encode_cmd(
    ffmpeg_bin: str,
    src: Path,
    dst: Path,
    *,
    video_codec: str,
    preset: str,
    crf: int,
    audio_bitrate: str,
    audio_rate: int,
    max_height: int | None,
    threads: int | None,
    loglevel: str,
    x265_params: str | None = None,
    stats: bool = False,
    overwrite: bool = True,
) -> List[str]:
    cmd = _base_ts_args(src, loglevel=loglevel, stats=stats, overwrite=overwrite, ffmpeg_bin=ffmpeg_bin)
    cmd.extend(
        [
            "-c:v",
            video_codec,
            "-preset",
            preset,
            "-crf",
            str(crf),
            "-pix_fmt",
            "yuv420p",
        ]
    )
    scale_filter = build_scale_filter(max_height)
    if scale_filter:
        cmd.extend(["-vf", scale_filter])
    if video_codec == "libx265" and x265_params:
        cmd.extend(["-x265-params", x265_params])
    if threads and threads > 0:
        cmd.extend(["-threads", str(threads)])
    cmd.extend(
        [
            "-enc_time_base:v",
            "demux",
            "-fps_mode:v",
            "vfr",
            "-c:a",
            "aac",
            "-b:a",
            audio_bitrate,
            "-ar",
            str(audio_rate),
            "-af",
            "aresample=async=1000:min_hard_comp=0.100:first_pts=0",
            "-max_muxing_queue_size",
            "4000",
            "-movflags",
            "+faststart",
            "-video_track_timescale",
            "90000",
            str(dst),
        ]
    )
    return cmd


def normalize_inputs(raw_inputs: Sequence[str]) -> tuple[list[Path], list[str]]:
    """Resolve .ts inputs from paths, globs, or directories and dedupe results.

    Returns a tuple of (paths, unmatched_patterns).
    """
    results: list[Path] = []
    unmatched: list[str] = []
    for item in raw_inputs:
        path = Path(item).expanduser()
        if path.is_dir():
            results.extend(sorted(path.rglob("*.ts")))
            continue
        if path.exists():
            results.append(path)
            continue
        matches = [Path(x) for x in glob.glob(str(path))]
        if matches:
            results.extend(sorted(matches))
        else:
            unmatched.append(item)
            results.append(path)
    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in results:
        try:
            resolved = candidate.resolve()
        except FileNotFoundError:
            resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(candidate)
    return unique, unmatched
