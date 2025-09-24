from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

DEFAULT_CONFIG_PATH = Path("~/.config/twitchtool/config.toml").expanduser()


def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_bool(name: str, default: Optional[bool]) -> Optional[bool]:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    if v.lower() in {"1", "true", "yes", "y"}:
        return True
    if v.lower() in {"0", "false", "no", "n"}:
        return False
    return default


def _env_str(name: str, default: Optional[str]) -> Optional[str]:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v


def _default_record_dir() -> Path:
    vids = Path("~/Videos").expanduser()
    if vids.exists():
        return vids / "TwitchTool"
    return Path("~/Downloads").expanduser() / "TwitchTool"


DEFAULTS: Dict[str, Any] = {
    "paths": {
        # XDG-style state directories for queue and logs
        "queue_dir": str(Path("~/.local/state/twitchtool/encode-queue").expanduser()),
        "logs_dir": str(Path("~/.local/state/twitchtool/logs").expanduser()),
        # Default recordings directory prefers ~/Videos/TwitchTool, else ~/Downloads/TwitchTool
        "record_dir": str(_default_record_dir()),
    },
    "limits": {
        "record_limit": 6,
    },
    "storage": {
        # Minimum free space before starting/continuing heavy operations (bytes)
        # Default: 10 GiB
        "disk_free_min_bytes": 10 * 1024 * 1024 * 1024,
    },
    "record": {
        "quality": "best",
        "retry_delay": 60,
        "retry_window": 900,
        "loglevel": "error",
        "enable_remux": True,
        "delete_ts_after_remux": True,
        "delete_input_on_success": False,
    },
    "encode_daemon": {
        "preset": "medium",
        "crf": 26,
        "threads": 1,
        "height": 480,
        "fps": 30,
        "loglevel": "error",
    },
    "poller": {
        # Default users list under config dir to avoid cluttering $HOME
        "users_file": str(Path("~/.config/twitchtool/users.txt").expanduser()),
        "interval": 300,
        "quality": "best",
        "download_cmd": "twitchtool record",
        "timeout": 15,
        "probe_concurrency": 10,
    },
}


def load_config_file(path: Optional[Path]) -> Dict[str, Any]:
    p = (path or DEFAULT_CONFIG_PATH).expanduser()
    if not p.exists():
        return {}
    with p.open("rb") as f:
        return tomllib.load(f)


def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            out[k] = merge_dicts(base[k], v)  # type: ignore[index]
        else:
            out[k] = v
    return out


def apply_env(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(cfg)
    # Global
    rl = _env_int("RECORD_LIMIT", None)
    if rl is not None:
        out.setdefault("limits", {})["record_limit"] = rl  # type: ignore[index]

    qdir = _env_str("QUEUE_DIR", None)
    if qdir:
        out.setdefault("paths", {})["queue_dir"] = qdir  # type: ignore[index]

    # Storage thresholds
    s = out.setdefault("storage", {})
    gb = _env_int("DISK_FREE_MIN_GB", None)
    if gb is not None:
        s["disk_free_min_bytes"] = int(gb) * 1024 * 1024 * 1024
    else:
        b = _env_int("DISK_FREE_MIN_BYTES", None)
        if b is not None:
            s["disk_free_min_bytes"] = int(b)

    # Record
    r = out.setdefault("record", {})
    r_quality = _env_str("QUALITY", None)
    if r_quality:
        r["quality"] = r_quality
    r_delay = _env_int("RETRY_DELAY", None)
    if r_delay is not None:
        r["retry_delay"] = r_delay
    r_window = _env_int("RETRY_WINDOW", None)
    if r_window is not None:
        r["retry_window"] = r_window
    r_loglevel = _env_str("LOGLEVEL", None)
    if r_loglevel:
        r["loglevel"] = r_loglevel
    remux_enabled = _env_bool("REMUX_ENABLED", None)
    if remux_enabled is not None:
        r["enable_remux"] = remux_enabled
    del_ts = _env_bool("DELETE_TS_AFTER_REMUX", None)
    if del_ts is not None:
        r["delete_ts_after_remux"] = del_ts
    del_in = _env_bool("DELETE_INPUT_ON_SUCCESS", None)
    if del_in is not None:
        r["delete_input_on_success"] = del_in

    # Encoder
    e = out.setdefault("encode_daemon", {})
    for env, key in [
        ("ENCODER_PRESET", "preset"),
        ("ENCODER_CRF", "crf"),
        ("ENCODER_THREADS", "threads"),
        ("ENCODER_HEIGHT", "height"),
        ("ENCODER_FPS", "fps"),
        ("ENCODER_LOGLEVEL", "loglevel"),
    ]:
        v = _env_str(env, None)
        if v is not None:
            # ints where needed
            if key in {"crf", "threads", "height", "fps"}:
                try:
                    e[key] = int(v)
                except ValueError:
                    pass
            else:
                e[key] = v

    # Poller
    p = out.setdefault("poller", {})
    for env, key in [
        ("USERS_FILE", "users_file"),
        ("POLL_INTERVAL", "interval"),
        ("DOWNLOAD_CMD", "download_cmd"),
        ("PROBE_TIMEOUT", "timeout"),
        ("PROBE_CONCURRENCY", "probe_concurrency"),
    ]:
        v = _env_str(env, None)
        if v is not None:
            if key in {"interval", "timeout", "probe_concurrency"}:
                try:
                    p[key] = int(v)
                except ValueError:
                    pass
            else:
                p[key] = v

    return out


def effective_config(config_path: Optional[Path]) -> Dict[str, Any]:
    cfg = merge_dicts(DEFAULTS, load_config_file(config_path))
    cfg = apply_env(cfg)
    # Normalize storage config if provided in GiB in config file
    try:
        storage = cfg.setdefault("storage", {})
        if "disk_free_min_gb" in storage and "disk_free_min_bytes" not in storage:
            val = storage["disk_free_min_gb"]
            try:
                storage["disk_free_min_bytes"] = int(val) * 1024 * 1024 * 1024
            except Exception:
                pass
    except Exception:
        pass
    return cfg
