from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict

try:
    import tomllib  # Python 3.11+
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]
from pathlib import Path
import re

from . import __version__
from .config import DEFAULT_CONFIG_PATH, effective_config
from .doctor import doctor as doctor_cmd
from .encoder_daemon import (
    EncodeOptions,
    encode_daemon,
    encoder_runtime_state,
    stop_encoder_daemon,
)
from .locks import GlobalSlotManager
from .poller import PollerOptions, poller, poller_runtime_state, stop_poller_daemon
from .recorder import RecordOptions, record
from .utils import abspath, is_process_alive
from .status import gather_status, print_report
from .users_cli import add_users, list_users, remove_users


class CustomHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog: str) -> None:
        super().__init__(prog, max_help_position=28)

    def _format_action(self, action: argparse.Action) -> str:
        # Tweak subparser entries so descriptions stay on the same line.
        if isinstance(action, argparse._SubParsersAction._ChoicesPseudoAction):  # type: ignore[attr-defined]
            invocation = self._format_action_invocation(action)
            help_text = self._expand_help(action) if action.help else ""
            indent = " " * self._current_indent
            if help_text:
                padding = 18
                spacing = " " * max(1, padding - len(invocation))
                return f"{indent}{invocation}{spacing}{help_text}\n"
            return f"{indent}{invocation}\n"
        return super()._format_action(action)


def _serialize_toml(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = (
            value.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
            .replace("\b", "\\b")
            .replace("\f", "\\f")
        )
        return f'"{escaped}"'
    if isinstance(value, list):
        inner = ", ".join(_serialize_toml(item) for item in value)
        return f"[{inner}]"
    raise TypeError(f"Unsupported value for TOML serialization: {value!r}")


def _dump_toml(data: Dict[str, Any]) -> str:
    lines: list[str] = []

    def _dump_table(prefix: str | None, mapping: Dict[str, Any]) -> None:
        simple_items: list[tuple[str, Any]] = []
        nested_items: list[tuple[str, Dict[str, Any]]] = []
        for key, val in mapping.items():
            if isinstance(val, dict):
                nested_items.append((key, val))
            else:
                simple_items.append((key, val))
        if prefix is not None:
            lines.append(f"[{prefix}]")
        for key, val in simple_items:
            lines.append(f"{key} = {_serialize_toml(val)}")
        for idx, (key, val) in enumerate(nested_items):
            if lines and lines[-1] != "":
                lines.append("")
            new_prefix = f"{prefix}.{key}" if prefix else key
            _dump_table(new_prefix, val)

    top_simple: list[tuple[str, Any]] = []
    top_nested: list[tuple[str, Dict[str, Any]]] = []
    for key, val in data.items():
        if isinstance(val, dict):
            top_nested.append((key, val))
        else:
            top_simple.append((key, val))

    for key, val in top_simple:
        lines.append(f"{key} = {_serialize_toml(val)}")

    for idx, (key, val) in enumerate(top_nested):
        if lines and lines[-1] != "":
            lines.append("")
        _dump_table(key, val)

    return "\n".join(lines).rstrip() + "\n"


def _load_raw_config(path: Path) -> Dict[str, Any]:
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except FileNotFoundError:
        return {}
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"failed to read config {path}: {exc}") from exc


def _write_raw_config(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    toml_text = _dump_toml(data)
    path.write_text(toml_text, encoding="utf-8")


_TRUE_STRS = {"1", "true", "yes", "on", "y", "t"}
_FALSE_STRS = {"0", "false", "no", "off", "n", "f"}


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in _TRUE_STRS:
            return True
        if v in _FALSE_STRS:
            return False
        return default
    return bool(value)


def _set_enable_remux_in_config_text(text: str, desired: bool) -> tuple[str, bool]:
    """Return (new_text, changed) with only record.enable_remux toggled.

    - If a [record] table exists, update or insert enable_remux there.
    - Else, if a one-line inline table exists (record = {...}), update/insert it.
    - Else, append a [record] section with enable_remux.

    Attempts to preserve layout and comments where practical.
    """
    value = "true" if desired else "false"

    # 1) [record] explicit table
    table_pat = re.compile(r"^\s*\[record\]\s*$", re.MULTILINE)
    m = table_pat.search(text)
    if m:
        start = m.end()
        # Find end of table (next [section])
        next_table = re.search(r"^\s*\[[^\]\n]+\]\s*$", text[start:], re.MULTILINE)
        end = start + next_table.start() if next_table else len(text)
        block = text[start:end]
        # Replace existing enable_remux line if present
        line_pat = re.compile(r"^(?P<prefix>\s*enable_remux\s*=\s*)(?P<val>[^#\r\n]+)(?P<suffix>\s*(#.*)?)$", re.MULTILINE)
        if line_pat.search(block):
            new_block, n = line_pat.subn(lambda mo: f"{mo.group('prefix')}{value}{mo.group('suffix')}", block)
            if n:
                return text[:start] + new_block + text[end:], True
        # Otherwise insert after header or at end of block
        insertion = f"\nenable_remux = {value}\n"
        # If block starts with a newline already, just append at start
        new_block = insertion + block
        return text[:start] + new_block + text[end:], True

    # 2) One-line inline table: record = { ... }
    inline_pat = re.compile(r"^(?P<prefix>\s*record\s*=\s*\{)(?P<body>[^}]*)\}(?P<suffix>\s*(#.*)?)$", re.MULTILINE)
    m2 = inline_pat.search(text)
    if m2:
        body = m2.group('body')
        # Check if enable_remux present
        kv_pat = re.compile(r"(\benable_remux\s*=\s*)([^,}]+)")
        if kv_pat.search(body):
            new_body, n = kv_pat.subn(lambda mo: f"{mo.group(1)}{value}", body)
            if n:
                return text[:m2.start()] + f"{m2.group('prefix')}{new_body}}}{m2.group('suffix')}" + text[m2.end():], True
        # Insert at beginning of body
        new_body = (" " if body.strip() else " ") + f"enable_remux = {value}" + (", " + body.lstrip() if body.strip() else " ")
        # Normalize spacing: avoid trailing space before closing brace
        new_line = f"{m2.group('prefix')}{new_body.strip()}}}{m2.group('suffix')}"
        return text[:m2.start()] + new_line + text[m2.end():], True

    # 3) Append a new table at end
    sep = "" if text.endswith("\n") or text == "" else "\n"
    new_text = f"{text}{sep}\n[record]\nenable_remux = {value}\n"
    return new_text, True


def _set_enable_remux_in_config(path: Path, desired: bool) -> None:
    """Update only record.enable_remux in the TOML file, preserving comments."""
    path = path.expanduser()
    try:
        original = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        original = ""
    new_text, changed = _set_enable_remux_in_config_text(original, desired)
    if changed:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(new_text, encoding="utf-8")


def _add_common_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("--json-logs", action="store_true", help="emit JSON log lines instead of human-readable")
    p.add_argument("--config", type=Path, default=None, help="path to config.toml (default: ~/.config/twitchtool/config.toml)")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="twitchtool",
        description="Twitch recorder + encode queue + poller",
        formatter_class=CustomHelpFormatter,
    )
    ap.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    sub = ap.add_subparsers(dest="cmd", required=True, metavar="command")
    ap._subparsers_main = sub  # type: ignore[attr-defined]

    # record
    rp = sub.add_parser("record", help="record a Twitch user")
    _add_common_flags(rp)
    rp.add_argument("username", help="twitch username")
    rp.add_argument("--quality", default=None, help="streamlink quality (default: best)")
    rp.add_argument("--retry-delay", type=int, default=None, help="seconds between retries when offline (default: 60)")
    rp.add_argument("--retry-window", type=int, default=None, help="keep trying this many seconds after a cut (default: 900)")
    rp.add_argument("--loglevel", default=None, help="ffmpeg/streamlink loglevel (default: error)")
    rp.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="directory for outputs (default: ~/Videos/TwitchTool or ~/Downloads/TwitchTool)",
    )
    rp.add_argument(
        "--queue-dir",
        type=Path,
        default=None,
        help="encode queue base dir (default: ~/.local/state/twitchtool/encode-queue)",
    )
    rp.add_argument("--remux", dest="enable_remux", action="store_true", default=None, help="enable remuxing (default: on)")
    rp.add_argument("--no-remux", dest="enable_remux", action="store_false", help="skip remux and skip encode queue")
    rp.add_argument("--delete-ts-after-remux", action="store_true", default=None, help="delete .ts when remux succeeds")
    rp.add_argument("--no-delete-ts-after-remux", dest="delete_ts_after_remux", action="store_false")
    rp.add_argument("--delete-input-on-success", action="store_true", default=None, help="delete input after encode success")
    rp.add_argument("--no-delete-input-on-success", dest="delete_input_on_success", action="store_false")
    rp.add_argument("--record-limit", type=int, default=None, help="max concurrent recordings (default: 6)")
    rp.add_argument("--fail-fast", action="store_true", help="fail immediately if no global slot is available")

    # status
    status = sub.add_parser("status", help="show downloads and encode queue status")
    _add_common_flags(status)
    status.add_argument("--queue-dir", type=Path, default=None, help="override queue directory")
    status.add_argument("--record-limit", type=int, default=None, help="max concurrent recordings")

    stop = sub.add_parser("stop", help="gracefully stop a recording slot")
    _add_common_flags(stop)
    stop.add_argument("slot", type=int, help="slot number as shown in 'twitchtool status'")
    stop.add_argument("--record-limit", type=int, default=None, help="max concurrent recordings")
    stop.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="seconds to wait for the recorder to exit after signalling (default: 10)",
    )
    stop.add_argument(
        "--force",
        action="store_true",
        help="after the timeout, send SIGKILL if the recorder is still running",
    )

    # users management
    up = sub.add_parser("users", help="manage poller user list")
    _add_common_flags(up)
    up.add_argument(
        "--users-file",
        type=Path,
        default=None,
        help="override path to users.txt (default: config [poller.users_file])",
    )
    users_sub = up.add_subparsers(dest="users_cmd", required=True)

    users_sub.add_parser("list", help="list configured users")

    up_add = users_sub.add_parser("add", help="add one or more users")
    up_add.add_argument("usernames", nargs="+", help="twitch username(s) to add")

    up_remove = users_sub.add_parser("remove", help="remove one or more users")
    up_remove.add_argument("usernames", nargs="+", help="twitch username(s) to remove")

    # encode-mode
    em = sub.add_parser("encode-mode", help="control remux/encode pipeline")
    _add_common_flags(em)
    em_sub = em.add_subparsers(dest="encode_mode_cmd", required=True, metavar="command")
    em_sub.add_parser("status", help="show current encode mode")
    em_sub.add_parser("on", help="enable remuxing/encoding")
    em_sub.add_parser("off", help="disable remuxing/encoding")

    # encode-daemon
    ep = sub.add_parser("encode-daemon", help="manage encoder daemon")
    _add_common_flags(ep)
    enc_sub = ep.add_subparsers(dest="enc_cmd", required=False, metavar="command")

    enc_run = enc_sub.add_parser("run", help="run encoder daemon")
    _add_common_flags(enc_run)
    enc_run.add_argument("--queue-dir", type=Path, default=None)
    enc_run.add_argument("--preset", default=None)
    enc_run.add_argument("--crf", type=int, default=None)
    enc_run.add_argument("--threads", type=int, default=None)
    enc_run.add_argument("--height", type=int, default=None)
    enc_run.add_argument(
        "--fps",
        type=str,
        default=None,
        help="Output FPS: 'auto' to preserve; or a number/fraction like 30000/1001",
    )
    enc_run.add_argument("--loglevel", default=None)
    enc_run.add_argument("--record-limit", type=int, default=None, help="record limit used to detect downloads (default: 6)")

    enc_stop = enc_sub.add_parser("stop", help="stop encoder daemon")
    enc_stop.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="seconds to wait for graceful shutdown (default: 10)",
    )
    enc_stop.add_argument(
        "--force",
        action="store_true",
        help="after timeout, send SIGKILL if still running",
    )

    enc_sub.add_parser("status", help="show encoder status")

    # poller
    pp = sub.add_parser("poller", help="manage poller daemon")
    _add_common_flags(pp)
    poller_sub = pp.add_subparsers(dest="poller_cmd", required=False, metavar="command")

    pp_run = poller_sub.add_parser("run", help="run poller daemon")
    _add_common_flags(pp_run)
    pp_run.add_argument("--users-file", type=Path, default=None)
    pp_run.add_argument("--interval", type=int, default=None)
    pp_run.add_argument("--quality", default=None)
    pp_run.add_argument(
        "--download-cmd",
        default=None,
        help="command to run for downloads (default: 'twitchtool record')",
    )
    pp_run.add_argument("--timeout", type=int, default=None)
    pp_run.add_argument("--probe-concurrency", type=int, default=None)
    pp_run.add_argument("--record-limit", type=int, default=None)
    pp_run.add_argument("--logs-dir", type=Path, default=None)

    pp_stop = poller_sub.add_parser("stop", help="stop poller daemon")
    pp_stop.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="seconds to wait for graceful shutdown (default: 10)",
    )
    pp_stop.add_argument(
        "--force",
        action="store_true",
        help="after timeout, send SIGKILL if still running",
    )

    poller_sub.add_parser("status", help="show poller status")

    # tscompress (batch remux + encode for existing .ts files)
    tc = sub.add_parser("tscompress", help="remux and compress .ts files serially")
    _add_common_flags(tc)
    tc.add_argument("inputs", nargs="+", help="one or more .ts file paths or globs")
    tc.add_argument("--height", type=int, default=None)
    tc.add_argument(
        "--fps",
        type=str,
        default=None,
        help="Output FPS: 'auto' to preserve; or a number/fraction like 30000/1001",
    )
    tc.add_argument("--crf", type=int, default=None)
    tc.add_argument("--preset", default=None)
    tc.add_argument("--threads", type=int, default=None)
    tc.add_argument("--loglevel", default=None)
    tc.add_argument("--keep-ts", action="store_true", default=None, help="keep .ts after successful remux (default)")
    tc.add_argument("--delete-ts-after-remux", action="store_true", help="delete .ts after successful remux")
    tc.add_argument("--overwrite", action="store_true", help="overwrite existing outputs if present")
    tc.add_argument(
        "--delete-input-on-success",
        action="store_true",
        default=None,
        help="delete the input used for encode after success",
    )

    # doctor
    dp = sub.add_parser("doctor", help="check environment")
    _add_common_flags(dp)
    dp.add_argument("--queue-dir", type=Path, default=None)
    dp.add_argument("--logs-dir", type=Path, default=None)

    # clean (simple helper)
    cp = sub.add_parser("clean", help="clean stale owner files and print status")
    _add_common_flags(cp)
    cp.add_argument("--record-limit", type=int, default=None)

    help_parser = sub.add_parser("help", help="show help for a command")
    help_parser.add_argument("topic", nargs="?", help="command to describe")

    return ap


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    argv = list(argv)
    def _auto_insert_run(cmd: str, args: list[str]) -> None:
        if not args or args[0] != cmd:
            return
        if len(args) == 1:
            args.insert(1, "run")
            return
        if args[1] in {"-h", "--help", "run", "stop", "status"}:
            return
        if args[1].startswith("-"):
            args.insert(1, "run")

    _auto_insert_run("poller", argv)
    _auto_insert_run("encode-daemon", argv)
    if os.environ.get("TWITCHTOOL_DEBUG_ARGS"):
        print(f"argv after auto-insert: {argv}", file=sys.stderr)

    ap = build_parser()
    ns = ap.parse_args(argv)

    if ns.cmd == "help":
        topic = getattr(ns, "topic", None)
        subparsers = getattr(ap, "_subparsers_main", None)
        if not topic:
            ap.print_help()
            sys.exit(0)
        if subparsers and topic in subparsers.choices:
            subparsers.choices[topic].print_help()
            sys.exit(0)
        print(f"Unknown command '{topic}'", file=sys.stderr)
        print()
        ap.print_help()
        sys.exit(1)

    cfg = effective_config(getattr(ns, "config", None))

    if ns.cmd == "record":
        c = cfg
        opts = RecordOptions(
            username=ns.username,
            quality=ns.quality or c["record"]["quality"],
            retry_delay=ns.retry_delay or c["record"]["retry_delay"],
            retry_window=ns.retry_window or c["record"]["retry_window"],
            loglevel=ns.loglevel or c["record"]["loglevel"],
            output_dir=ns.output_dir or Path(c["paths"]["record_dir"]),
            queue_dir=ns.queue_dir or Path(c["paths"]["queue_dir"]),
            enable_remux=(
                _coerce_bool(c["record"].get("enable_remux", True), True)
                if ns.enable_remux is None
                else bool(ns.enable_remux)
            ),
            delete_ts_after_remux=(
                _coerce_bool(c["record"].get("delete_ts_after_remux", True), True)
                if ns.delete_ts_after_remux is None
                else bool(ns.delete_ts_after_remux)
            ),
            delete_input_on_success=(
                _coerce_bool(c["record"].get("delete_input_on_success", False), False)
                if ns.delete_input_on_success is None
                else bool(ns.delete_input_on_success)
            ),
            record_limit=ns.record_limit or c["limits"]["record_limit"],
            fail_fast=bool(ns.fail_fast),
            json_logs=bool(ns.json_logs),
            disk_free_min_bytes=int(c.get("storage", {}).get("disk_free_min_bytes", 10 * 1024 * 1024 * 1024)),
        )
        rc = record(opts)
        sys.exit(rc)

    elif ns.cmd == "encode-daemon":
        c = cfg

        def _emit_enc(event: str, **extra: object) -> None:
            if ns.json_logs:
                payload: dict[str, object] = {"event": event}
                if extra:
                    payload.update(extra)
                print(json.dumps(payload))
            else:
                if extra:
                    details = " ".join(f"{k}={extra[k]}" for k in sorted(extra))
                    print(f"{event}: {details}")
                else:
                    print(event)

        enc_cmd = ns.enc_cmd or "run"

        if enc_cmd == "run":
            opts = EncodeOptions(
                queue_dir=ns.queue_dir or Path(c["paths"]["queue_dir"]),
                preset=ns.preset or c["encode_daemon"]["preset"],
                crf=ns.crf or c["encode_daemon"]["crf"],
                threads=ns.threads or c["encode_daemon"]["threads"],
                height=ns.height or c["encode_daemon"]["height"],
                fps=str(ns.fps or c["encode_daemon"]["fps"]).strip() or "auto",
                loglevel=ns.loglevel or c["encode_daemon"]["loglevel"],
                json_logs=bool(ns.json_logs),
                record_limit=ns.record_limit or c["limits"]["record_limit"],
                disk_free_min_bytes=int(c.get("storage", {}).get("disk_free_min_bytes", 10 * 1024 * 1024 * 1024)),
            )
            rc = encode_daemon(opts)
            sys.exit(rc)

        elif enc_cmd == "stop":
            result = stop_encoder_daemon(timeout=float(ns.timeout), force=bool(ns.force))
            res = result.get("result", "unknown")
            state = result.get("state", {})
            if res == "stopped":
                _emit_enc("encoder-stopped", signal=result.get("signal"), pid=result.get("pid"))
                sys.exit(0)
            elif res == "not_running":
                _emit_enc("encoder-not-running", pid=result.get("pid"))
                sys.exit(0)
            elif res == "timeout":
                _emit_enc("encoder-stop-timeout", pid=result.get("pid"), signal=result.get("signal"))
                sys.exit(2)
            else:
                _emit_enc("encoder-stop-failed", pid=result.get("pid"))
                sys.exit(2)

        elif enc_cmd == "status":
            state = encoder_runtime_state()
            if ns.json_logs:
                print(json.dumps({"event": "encoder-status", **state}))
            else:
                if state.get("running"):
                    print(
                        f"Encoder daemon: running (pid={state.get('pid')}, started={state.get('started_at')})"
                    )
                else:
                    print("Encoder daemon: not running")
                current = state.get("current_job")
                last = state.get("last_job")
                if current:
                    print(f"Current job: {current}")
                if last:
                    print(f"Last job: {last}")
            sys.exit(0)

        else:
            ap.print_help()
            sys.exit(1)

    elif ns.cmd == "encode-mode":
        def _emit_mode(event: str, **extra: object) -> None:
            if ns.json_logs:
                payload: dict[str, object] = {"event": event}
                if extra:
                    payload.update(extra)
                print(json.dumps(payload))
            else:
                if extra:
                    details = " ".join(f"{k}={extra[k]}" for k in sorted(extra))
                    print(f"{event}: {details}")
                else:
                    print(event)

        cfg_path = Path(ns.config).expanduser() if ns.config else DEFAULT_CONFIG_PATH

        if ns.encode_mode_cmd == "status":
            current_cfg = effective_config(ns.config)
            enabled = _coerce_bool(current_cfg.get("record", {}).get("enable_remux", True), True)
            _emit_mode("encode-mode-status", enabled=enabled)
            sys.exit(0)

        desired = True if ns.encode_mode_cmd == "on" else False
        # Determine current value from file to avoid env-masked surprises
        data = _load_raw_config(cfg_path)
        record_cfg = data.get("record", {}) if isinstance(data, dict) else {}
        previous_val = record_cfg.get("enable_remux")
        previous_bool = _coerce_bool(previous_val, True)
        if previous_bool == desired and cfg_path.exists():
            _emit_mode("encode-mode-unchanged", enabled=desired, config=str(cfg_path))
            sys.exit(0)

        # Edit only the specific key in the TOML, preserving comments
        _set_enable_remux_in_config(cfg_path, desired)
        _emit_mode("encode-mode-set", enabled=desired, config=str(cfg_path))
        sys.exit(0)

    elif ns.cmd == "poller":
        c = cfg

        def _emit(event: str, **extra: object) -> None:
            if ns.json_logs:
                payload: dict[str, object] = {"event": event}
                if extra:
                    payload.update(extra)
                print(json.dumps(payload))
            else:
                if extra:
                    details = " ".join(f"{k}={extra[k]}" for k in sorted(extra))
                    print(f"{event}: {details}")
                else:
                    print(event)

        poller_cmd = ns.poller_cmd or "run"

        if poller_cmd == "run":
            opts = PollerOptions(
                users_file=ns.users_file or Path(c["poller"]["users_file"]),
                interval=ns.interval or c["poller"]["interval"],
                quality=ns.quality or c["poller"]["quality"],
                download_cmd=ns.download_cmd or c["poller"]["download_cmd"],
                timeout=ns.timeout or c["poller"]["timeout"],
                probe_concurrency=ns.probe_concurrency or c["poller"]["probe_concurrency"],
                record_limit=ns.record_limit or c["limits"]["record_limit"],
                logs_dir=ns.logs_dir or Path(c["paths"]["logs_dir"]),
                json_logs=bool(ns.json_logs),
                config_path=getattr(ns, "config", None),
            )
            rc = asyncio.run(poller(opts))
            sys.exit(rc)

        elif poller_cmd == "stop":
            result = stop_poller_daemon(timeout=float(ns.timeout), force=bool(ns.force))
            res = result.get("result", "unknown")
            state = result.get("state", {})
            if res == "stopped":
                sig = result.get("signal", "SIGTERM")
                _emit(
                    "poller-stopped",
                    signal=sig,
                    pid=result.get("pid"),
                    last_poll=state.get("last_poll_ts"),
                )
                sys.exit(0)
            elif res == "not_running":
                _emit("poller-not-running", pid=result.get("pid"))
                sys.exit(0)
            elif res == "timeout":
                _emit("poller-stop-timeout", pid=result.get("pid"), signal=result.get("signal"))
                sys.exit(2)
            else:
                _emit("poller-stop-failed", pid=result.get("pid"))
                sys.exit(2)

        elif poller_cmd == "status":
            state = poller_runtime_state()
            if ns.json_logs:
                print(json.dumps({"event": "poller-status", **state}))
            else:
                if state.get("running"):
                    print(
                        f"Poller: running (pid={state.get('pid')}, started={state.get('started_at')})"
                    )
                else:
                    print("Poller: not running")
                if state.get("last_poll_ts"):
                    print(f"Last poll: {state.get('last_poll_ts')}")
                if state.get("next_poll_ts") and state.get("running"):
                    print(f"Next poll: {state.get('next_poll_ts')}")
                    try:
                        from datetime import datetime, timezone
                        next_dt = datetime.fromisoformat(str(state.get('next_poll_ts')))
                        minutes = max((next_dt - datetime.now(timezone.utc)).total_seconds(), 0) / 60.0
                        print(f"Next poll in: {minutes:.1f} minute(s)")
                    except Exception:
                        pass
                elif state.get("next_poll_ts"):
                    print(f"Next poll (projected): {state.get('next_poll_ts')}")
                elif state.get("interval"):
                    last_ts = state.get("last_poll_ts")
                    try:
                        if last_ts:
                            from datetime import datetime, timezone
                            last_dt = datetime.fromisoformat(str(last_ts))
                            next_dt = last_dt + timedelta(seconds=int(state["interval"]))
                            print(f"Next poll (projected): {next_dt.isoformat()}")
                            minutes = max((next_dt - datetime.now(timezone.utc)).total_seconds(), 0) / 60.0
                            print(f"Next poll in: {minutes:.1f} minute(s)")
                    except Exception:
                        pass
                interval = state.get("interval")
                if interval:
                    print(f"Interval: {interval} seconds")
            sys.exit(0)

        else:
            ap.print_help()
            sys.exit(1)

    elif ns.cmd == "doctor":
        c = cfg
        qd = ns.queue_dir or Path(c["paths"]["queue_dir"])
        ld = ns.logs_dir or Path(c["paths"]["logs_dir"])
        rc = doctor_cmd(qd, ld, c["limits"]["record_limit"])
        sys.exit(rc)

    elif ns.cmd == "clean":
        c = cfg
        record_limit = ns.record_limit or c["limits"]["record_limit"]
        gsm = GlobalSlotManager(record_limit)
        removed = gsm.cleanup_stale_owners()
        active = gsm.list_active_owners()
        print(f"Removed {removed} stale owner files.")
        if active:
            print("Active downloads:")
            for o in active:
                print(f"  slot {o.slot_index}: {o.username} (pid={o.pid}, since={o.started_at})")
        else:
            print("No active downloads.")
        sys.exit(0)

    elif ns.cmd == "stop":
        c = cfg
        record_limit = ns.record_limit or c["limits"]["record_limit"]
        slot = int(ns.slot)

        def _emit(event: str, **extra: object) -> None:
            if ns.json_logs:
                payload: dict[str, object] = {"event": event}
                if extra:
                    payload.update(extra)
                print(json.dumps(payload))
            else:
                if extra:
                    details = " ".join(f"{k}={extra[k]}" for k in sorted(extra))
                    print(f"{event}: {details}")
                else:
                    print(event)

        if slot < 1 or slot > record_limit:
            _emit("invalid-slot", slot=slot, record_limit=record_limit)
            sys.exit(2)

        gsm = GlobalSlotManager(record_limit)
        owner = next((o for o in gsm.list_active_owners() if o.slot_index == slot), None)
        if owner is None:
            _emit("slot-idle", slot=slot)
            sys.exit(1)

        sig = signal.SIGINT
        sig_name = signal.Signals(sig).name
        try:
            os.kill(owner.pid, sig)
        except ProcessLookupError:
            _emit("process-missing", slot=slot, pid=owner.pid)
            try:
                gsm.cleanup_stale_owners()
            except Exception:
                pass
            sys.exit(1)

        _emit(
            "signal-sent",
            slot=slot,
            pid=owner.pid,
            username=owner.username,
            signal=sig_name,
        )

        timeout = max(0.0, float(ns.timeout)) if hasattr(ns, "timeout") else 0.0
        if timeout > 0:
            deadline = time.time() + timeout
            while time.time() < deadline:
                if not is_process_alive(owner.pid):
                    _emit("stopped", slot=slot, pid=owner.pid, method=sig_name)
                    try:
                        gsm.cleanup_stale_owners()
                    except Exception:
                        pass
                    sys.exit(0)
                time.sleep(0.3)

            if ns.force:
                kill_sig = signal.SIGKILL
                kill_name = signal.Signals(kill_sig).name
                try:
                    os.kill(owner.pid, kill_sig)
                except ProcessLookupError:
                    _emit("stopped", slot=slot, pid=owner.pid, method=kill_name)
                    try:
                        gsm.cleanup_stale_owners()
                    except Exception:
                        pass
                    sys.exit(0)

                _emit("signal-sent", slot=slot, pid=owner.pid, username=owner.username, signal=kill_name)
                for _ in range(20):
                    if not is_process_alive(owner.pid):
                        _emit("stopped", slot=slot, pid=owner.pid, method=kill_name)
                        try:
                            gsm.cleanup_stale_owners()
                        except Exception:
                            pass
                        sys.exit(0)
                    time.sleep(0.3)

                _emit("still-running", slot=slot, pid=owner.pid)
                sys.exit(2)
            else:
                _emit("still-running", slot=slot, pid=owner.pid)
                sys.exit(2)

        sys.exit(0)

    elif ns.cmd == "status":
        c = cfg
        queue_dir = ns.queue_dir or Path(c["paths"]["queue_dir"])
        record_limit = ns.record_limit or c["limits"]["record_limit"]
        report = gather_status(queue_dir, record_limit)
        print_report(report)
        sys.exit(0)

    elif ns.cmd == "tscompress":
        c = cfg

        def _emit(event: str, **extra: object) -> None:
            if ns.json_logs:
                payload: dict[str, object] = {"event": event}
                if extra:
                    payload.update(extra)
                print(json.dumps(payload))
            else:
                if extra:
                    details = " ".join(f"{k}={extra[k]}" for k in sorted(extra))
                    print(f"{event}: {details}")
                else:
                    print(event)

        # Resolve defaults from config
        height = ns.height or c["encode_daemon"]["height"]
        fps = (ns.fps if getattr(ns, "fps", None) is not None else c["encode_daemon"].get("fps", "auto"))
        crf = ns.crf or c["encode_daemon"]["crf"]
        preset = ns.preset or c["encode_daemon"]["preset"]
        threads = ns.threads or c["encode_daemon"]["threads"]
        loglevel = ns.loglevel or c["encode_daemon"]["loglevel"]
        delete_input_on_success = (
            _coerce_bool(c["record"].get("delete_input_on_success", False), False)
            if ns.delete_input_on_success is None
            else bool(ns.delete_input_on_success)
        )
        # keep-ts flag maps inversely to delete_ts_after_remux
        # Default: keep TS by default; allow explicit delete via flag.
        if getattr(ns, "keep_ts", None):
            delete_ts_after_remux = False
        elif getattr(ns, "delete_ts_after_remux", False):
            delete_ts_after_remux = True
        else:
            delete_ts_after_remux = False

        from .utils import which, build_nice_ionice_prefix
        import glob
        import subprocess
        from pathlib import Path as _Path

        if not which("ffmpeg"):
            _emit("ffmpeg-missing")
            sys.exit(2)

        # Expand inputs (support shell-expanded and literal globs)
        patterns: list[str] = list(getattr(ns, "inputs", []) or [])
        files: list[_Path] = []
        for pat in patterns:
            p = _Path(pat)
            if p.exists():
                files.append(p)
                continue
            matched = [
                _Path(x) for x in glob.glob(pat)
            ]
            if matched:
                files.extend(matched)
            else:
                _emit("no-match", pattern=pat)

        # De-duplicate by real path, keep order
        seen: set[_Path] = set()
        ordered: list[_Path] = []
        for f in files:
            try:
                rp = f.resolve()
            except FileNotFoundError:
                rp = f
            if rp not in seen:
                seen.add(rp)
                ordered.append(f)

        if not ordered:
            _emit("no-inputs")
            sys.exit(1)

        def _run(cmd: list[str]) -> int:
            try:
                proc = subprocess.Popen(cmd)
            except OSError as e:
                _emit("spawn-failed", error=str(e))
                return 1
            return proc.wait()

        rc_overall = 0
        for ip in ordered:
            ip = ip.resolve()
            if ip.suffix.lower() != ".ts":
                _emit("skip-non-ts", path=str(ip))
                rc_overall = rc_overall or 1
                continue

            base = ip.with_suffix("").name
            out_dir = ip.parent
            remux_mp4 = out_dir / f"{base}.mp4"
            final_mp4 = out_dir / f"{base}_compressed.mp4"

            _emit("begin", input=str(ip))

            # Remux
            do_remux = bool(ns.overwrite) or not (remux_mp4.exists() and remux_mp4.stat().st_size > 0)
            use_input = ip
            if do_remux:
                cmd = [
                    which("ffmpeg") or "ffmpeg",
                    "-hide_banner",
                    "-nostdin",
                    "-i",
                    str(ip),
                    "-c",
                    "copy",
                    "-bsf:a",
                    "aac_adtstoasc",
                    "-movflags",
                    "+faststart",
                    "-loglevel",
                    str(loglevel),
                    "-y",
                    str(remux_mp4),
                ]
                _emit("remux-start", cmd=" ".join(cmd))
                rrc = _run(cmd)
                if rrc == 0 and remux_mp4.exists() and remux_mp4.stat().st_size > 0:
                    _emit("remux-ok", output=str(remux_mp4))
                    use_input = remux_mp4
                    if delete_ts_after_remux:
                        try:
                            ip.unlink()
                            _emit("ts-deleted", path=str(ip))
                        except Exception as e:
                            _emit("ts-delete-failed", path=str(ip), error=str(e))
                else:
                    _emit("remux-failed", rc=rrc)
            else:
                _emit("remux-skip-exists", output=str(remux_mp4))
                use_input = remux_mp4 if remux_mp4.exists() else ip

            # Encode
            if final_mp4.exists() and final_mp4.stat().st_size > 0 and not bool(ns.overwrite):
                _emit("encode-skip-exists", output=str(final_mp4))
                continue

            # Build filters and options
            vf_filters = [f"scale=-2:{int(height)}"]
            vsync = []
            fps_val = str(fps).strip().lower() if fps is not None else "auto"
            if fps_val and fps_val != "auto":
                vf_filters.append(f"fps={fps_val}")
                vsync = ["-vsync", "cfr"]

            ts_fix = ["-fflags", "+genpts"] if str(use_input).lower().endswith(".ts") else []
            cmd = (
                build_nice_ionice_prefix()
                + [
                    which("ffmpeg") or "ffmpeg",
                    "-hide_banner",
                    "-nostdin",
                    "-loglevel",
                    str(loglevel),
                    "-y",
                    *ts_fix,
                    "-i",
                    str(use_input),
                    "-vf",
                    ",".join(vf_filters),
                    "-c:v",
                    "libx265",
                    "-crf",
                    str(int(crf)),
                    "-preset",
                    str(preset),
                    "-threads",
                    str(int(threads)),
                    *vsync,
                    "-c:a",
                    "aac",
                    "-b:a",
                    "128k",
                    "-ar",
                    "48000",
                    "-af",
                    "aresample=async=1:first_pts=0",
                    "-movflags",
                    "+faststart",
                    str(final_mp4),
                ]
            )
            _emit("encode-start", cmd=" ".join(cmd))
            erc = _run(cmd)
            if erc == 0 and final_mp4.exists() and final_mp4.stat().st_size > 0:
                _emit("encode-ok", output=str(final_mp4))
                if delete_input_on_success:
                    try:
                        use_input.unlink()
                        _emit("input-deleted", path=str(use_input))
                    except Exception as e:
                        _emit("input-delete-failed", path=str(use_input), error=str(e))
            else:
                _emit("encode-failed", rc=erc, input=str(ip))
                rc_overall = rc_overall or (erc or 1)

        sys.exit(rc_overall)

    elif ns.cmd == "users":
        c = cfg
        users_path = Path(ns.users_file or c["poller"]["users_file"])
        if ns.users_cmd == "list":
            rc = list_users(users_path)
        elif ns.users_cmd == "add":
            rc = add_users(users_path, ns.usernames)
        elif ns.users_cmd == "remove":
            rc = remove_users(users_path, ns.usernames)
        else:
            ap.print_help()
            sys.exit(1)
        sys.exit(rc)

    else:
        ap.print_help()
        sys.exit(1)
