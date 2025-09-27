# twitchtool

Production‑grade, Linux‑focused Python application that replaces a Bash setup for:

- **Recording** Twitch streams into TS parts with resilient reconnects,
- **Merging** to a single `.ts`, attempting **remux** to `.mp4`,
- **Enqueueing** an encode job,
- **Encoding daemon** that processes jobs in order (x265 by default) and **pauses** whenever any recordings are active,
- **Poller daemon** that probes a list of users and **auto‑starts** recordings, respecting a global cap of concurrent recordings,
- **Systemd** user units and clear logging.

Targets a single‑vCPU VPS. No heavy deps—pure stdlib + `tomli` for Python 3.10 TOML.

---

## What’s new in 0.1.1

- Default recordings directory now prefers `~/Videos/TwitchTool` if `~/Videos` exists, otherwise `~/Downloads/TwitchTool`.
- x265 scale fix: encoder uses `scale=-2:480` (instead of `-1:480`) to guarantee an even width (e.g., 854×480), preventing libx265 alignment errors.

---

## Overview

```
                +------------------+        owner files        +--------------------+
  streamlink ──>|  Recorder (CLI)  |─┐  /run/user/<uid>/...   | Encoder Daemon     |
 (parts .ts)    +------------------+ │ slotN.owner (JSON)     |  ffmpeg x265       |
        └───┐       ^ per-user lock│ │                        |  SIGSTOP/SIGCONT   |
  reconnect │       | /tmp/twitch-  │ │                        +--------------------+
  & retry   │       | active-users  │ │                         ^ consumes job JSON
            │       |               │ │                         | ~/twitch-encode-queue/jobs/*.json
            ▼       |   +-----------┘ │                         |
   merge parts  ->  .ts  -> remux -> .mp4 -> enqueue job  ──────┘
                                            (~/.local/state/twitchtool/encode-queue/jobs/*.json)

  Poller Daemon
  --------------
  - Every N seconds runs `streamlink --stream-url` for users.
  - If user is live and not locked, launches `twitchtool record <user>`.
  - Skips launches when active slots >= RECORD_LIMIT.
```

---

## Install

Using pipx (recommended for user-level installs):

```bash
sudo apt update
sudo apt install -y streamlink ffmpeg pipx
pipx ensurepath
pipx install twitchtool
```

Alternatively with pip (also user-level):

```bash
sudo apt update
sudo apt install -y streamlink ffmpeg python3-pip python3-venv
python3 -m pip install --user --upgrade twitchtool
```

> **Note:** Python 3.10 uses `tomli` automatically; on 3.11+ stdlib `tomllib` is used.

### Install from local repo with pipx

From the twitchtool repository directory:

```bash
# Editable (dev): reflects local code changes without reinstall
pipx install --editable .

# Or non-editable (snapshots current checkout)
pipx install .

# If you previously installed from PyPI, replace it with local
pipx uninstall twitchtool
pipx install --editable .

# After (re)install, restart services if using systemd user units
systemctl --user restart twitch-poller.service twitch-encoderd.service
```

---

## Using uv (recommended for dev)

If you use the ultra‑fast `uv` toolchain, you can run with a recent Python (recommended 3.12), even if your system Python is older:

```bash
# Install a modern Python and create a venv
uv python install 3.12
uv venv --python 3.12

# Install deps + test extras into that venv
uv sync --extra test

# Run tests
uv run pytest -q

# Install the package editable into the venv
uv pip install -e .

# Alternatively, install the CLI as a tool (user-level); uv uses a managed Python
uv tool install twitchtool
```

Make targets prefer `uv` and fall back to pip/pytest when `uv` is not available. You can also choose the Python version with `UV_PY`:

```bash
make dev                # sets up .venv with Python 3.12 and installs deps
make UV_PY=3.11 dev     # same but with Python 3.11
make test               # runs tests via uv (inside .venv)
```

---

## Install From Source

If you are working from a local clone (or if the package is not yet published), install editable:

```bash
# Using uv (fastest)
uv pip install -e .

# Or with pip
python3 -m pip install --user -e .
```

---

## Quick start

Manual recording (writes files to `~/Videos/TwitchTool` if `~/Videos` exists, otherwise `~/Downloads/TwitchTool` by default):

```bash
twitchtool record somechannel --quality best
```

Add `--no-remux` if you want to keep the merged `.ts` as-is and skip the encode queue.

This will:

1. Acquire a **per-user lock** (`/tmp/twitch-active-users/somechannel.lock`) and a **global slot** (1..N).
2. Capture `.ts` parts (resilient reconnects) under `<output_dir>/temp/`.
3. Merge parts to `<base>.ts` in `temp/`, then finalize outputs by moving them into `<output_dir>`.
4. Attempt remux to `<base>.mp4` (stream copy, +faststart). On success, the finalized `.mp4` appears in `<output_dir>`; on failure, the merged `.ts` is kept and moved there.
5. Enqueue an encode job to `~/.local/state/twitchtool/encode-queue/jobs` targeting the finalized file in `<output_dir>`.
6. Release the global slot at **merge time** (not after remux/queue) to maximize capacity.

Run the encoder daemon:

```bash
twitchtool encode-daemon run
```

Check its status / stop it:

```bash
twitchtool encode-daemon status
twitchtool encode-daemon stop
```

Run the poller daemon:

```bash
# Create a users file:
mkdir -p ~/.config/twitchtool
printf '%s\n' somechannel anotherchannel thirdone > ~/.config/twitchtool/users.txt

twitchtool poller run --users-file ~/.config/twitchtool/users.txt --interval 300
```

Recorders launched by the poller automatically inherit the same `--config` path
you pass to the poller. For example, `twitchtool poller run --config ~/.config/twitchtool/config.toml ...`
will launch `twitchtool record ... --config ~/.config/twitchtool/config.toml` for each recording.

Manage the poller user list:

```bash
# List current users
twitchtool users list

# Add or remove users (accepts multiple names)
twitchtool users add djalpha djbeta
twitchtool users remove djbeta
```

Check current activity:

```bash
twitchtool status
```

Stop a running recorder (use the slot number from `twitchtool status`):

```bash
twitchtool stop 2
```

This sends SIGINT to the recorder and waits up to 10 seconds; add `--force` to escalate to SIGKILL after the wait.

Toggle whether recordings remux & enter the encode queue:

```bash
twitchtool encode-mode off   # or 'on'
twitchtool encode-mode status
```

Check the poller service:

```bash
twitchtool poller status
```

Stop the poller daemon (waits 10 seconds for a graceful exit, add `--force` to escalate):

```bash
twitchtool poller stop
```

Check your environment:

```bash
twitchtool doctor
```

### In-progress temp directory

- While recording, all in-progress files live under a `temp/` subfolder inside your configured recordings folder (e.g., `~/Downloads/TwitchTool/temp`). If you point `--output-dir` to a common parent like `~/Downloads` or `~/Videos`, twitchtool will place temp under a `TwitchTool/temp` subfolder (e.g., `~/Downloads/TwitchTool/temp`) to keep things tidy.
- Final deliverables are moved atomically into the recordings folder once ready:
  - Remux disabled/failed: the merged `<base>.ts` is moved to `<output_dir>`.
  - Remux succeeded: the `<base>.mp4` (and, if configured to keep it, `<base>.ts`) is moved to `<output_dir>`.
- The encode queue always references the finalized path in `<output_dir>`, avoiding partial files.
- If a run is interrupted (power loss, kill -9), you may see leftovers in `temp/`. After confirming no recorder is running (`twitchtool status`), you can safely remove stale files from `temp/`.

## Batch remux + compress existing .ts files

If you have existing `.ts` recordings and want to remux and compress them serially without the queue/daemon, use the helper script in this repo:

```bash
python3 scripts/remux_compress_serial.py ~/Downloads/TwitchTool/*.ts
```

Or run it via the CLI (installed as part of twitchtool):

```bash
twitchtool tscompress ~/Downloads/TwitchTool/*.ts
```

Notes:
- Requires `ffmpeg` in PATH.
- Processes inputs one-by-one (serial).
- Produces `<basename>.mp4` (remux) and `<basename>_compressed.mp4` (x265).
- Keeps the merged `.ts` after a successful remux by default; add `--delete-ts-after-remux` to remove it.
- Skips existing outputs unless you pass `--overwrite`.
- Encoding parameters match the encoder daemon defaults: `scale=-2:HEIGHT`, `fps`, `CRF`, `preset`, `threads`, AAC 128k, `+faststart`.

Common options:

```bash
python3 scripts/remux_compress_serial.py \
  --height 480 --fps 30 --crf 26 --preset medium --threads 1 \
  --loglevel error --overwrite --delete-input-on-success --delete-ts-after-remux \
  /path/to/*.ts
```

---

## Configuration

Create `~/.config/twitchtool/config.toml` (all keys optional):

```toml
[paths]
# XDG-style defaults keep things tidy under ~/.local/state
queue_dir = "~/.local/state/twitchtool/encode-queue"
logs_dir  = "~/.local/state/twitchtool/logs"
# Default recordings directory prefers ~/Videos/TwitchTool if it exists,
# otherwise falls back to ~/Downloads/TwitchTool
record_dir = "~/Videos/TwitchTool"

[limits]
record_limit = 6

[storage]
# Minimum free space required to record/encode. Use either:
# disk_free_min_gb (int, convenient) or disk_free_min_bytes (advanced)
disk_free_min_gb = 10

[record]
quality = "best"
retry_delay = 60
retry_window = 900
loglevel = "error"
enable_remux = true
delete_ts_after_remux = true
delete_input_on_success = false

[encode_daemon]
preset = "medium"
crf = 26
threads = 1
height = 480
fps = 30
loglevel = "error"

[poller]
# Keep the users list in config dir to avoid clutter under $HOME
users_file = "~/.config/twitchtool/users.txt"
interval = 300
quality = "best"
# Leave default; poller resolves `twitchtool` via PATH set in the systemd unit.
# Do not set `download_cmd` unless you have a special need.
# download_cmd = "twitchtool record"
timeout = 15
probe_concurrency = 10
```

**Precedence:** CLI flags > environment variables > config file > internal defaults.

Poller config passthrough: When you start the poller with `--config <path>`, that
same path is appended to launched `twitchtool record` commands so those recorders
read the same configuration file.

Helpful env vars:

- `RECORD_LIMIT` (global concurrent recordings cap),
- `QUALITY`, `RETRY_DELAY`, `RETRY_WINDOW`, `LOGLEVEL`,
- `QUEUE_DIR`, `DELETE_TS_AFTER_REMUX`, `DELETE_INPUT_ON_SUCCESS`,
- `REMUX_ENABLED`,
- `ENCODER_PRESET`, `ENCODER_CRF`, `ENCODER_THREADS`, `ENCODER_HEIGHT`, `ENCODER_FPS`,
- `USERS_FILE`, `POLL_INTERVAL`, `DOWNLOAD_CMD`, `PROBE_TIMEOUT`, `PROBE_CONCURRENCY`.
- `DISK_FREE_MIN_GB`, `DISK_FREE_MIN_BYTES`.

---

## Applying config changes

- Manual CLI runs: New `twitchtool ...` invocations read config at startup. No restart needed.
- Poller service: Restart to pick up `[poller]` changes. Recorders launched by the poller are not killed on restart (`KillMode=process`).

```bash
systemctl --user restart twitch-poller.service
journalctl --user -u twitch-poller -n1 --no-pager  # confirm restart
```

- Encoder daemon: Restart to apply `[encode_daemon]` changes. This interrupts any in‑flight encode; consider waiting until the queue is idle.

```bash
systemctl --user restart twitch-encoderd.service
journalctl --user -u twitch-encoderd -n1 --no-pager
```

- If you modify unit files under `~/.config/systemd/user/`, reload the user daemon first:

```bash
systemctl --user daemon-reload
systemctl --user restart twitch-poller.service twitch-encoderd.service
```

---

## Systemd user services

Install units:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/twitch-encoderd.service ~/.config/systemd/user/
cp systemd/twitch-poller.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now twitch-encoderd.service
systemctl --user enable --now twitch-poller.service
```

To keep the poller and encoder running after you log out or the host reboots, enable linger for your user (run once):

```bash
loginctl enable-linger $USER
```

This setting persists across sessions; without it the user systemd instance, and therefore the poller/encoder services, stop whenever your last login ends.

Logs go to `journalctl --user -u twitch-poller -f` and `--user -u twitch-encoderd -f`.
- **Service logs (poller/encoder):** `journalctl --user -u twitch-poller.service` / `journalctl --user -u twitch-encoderd.service`.
- **Total journal size:**

  ```bash
  journalctl --user --disk-usage
  ```

- **Recorder logs:** detached recorders write to `~/.local/state/twitchtool/logs/<username>.log`.

  ```bash
  ls -lah ~/.local/state/twitchtool/logs
  du -sh ~/.local/state/twitchtool/logs
  du -sh ~/.local/state/twitchtool/logs/*
  ```

To check when the poller last ran (you can add the configured interval to infer the next poll), inspect the latest cycle entry:

```bash
journalctl --user -u twitch-poller.service -n1 --no-pager
```


The poller launches recorders **detached**; restarts of the poller will **not** kill active recorders (`KillMode=process`).

---

Note on PATH for user services
------------------------------
User-level installs often place the `twitchtool` entry point in `~/.local/bin`,
which may not be included in the PATH for systemd user services. The provided
`systemd/twitch-poller.service` sets PATH so spawned recorders resolve:

```
Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin
```

Recommendation: do not override `download_cmd`; let it default to
`twitchtool record` so it resolves to the pipx/pip entrypoint via PATH.
Using `python -m twitchtool` can fail when installed via pipx because the
system Python won’t see the pipx-managed environment.

---

## Global Flags

- `--json-logs` (all commands): emit JSON log lines
- `--config <path>` (all commands): use an alternate `config.toml`

---

## How the 6‑recording cap works (and changing it)

- Slots directory: `/run/user/<uid>/twitch-record-slots` (fallback `/tmp/twitch-record-slots`).
- Files `slot1 .. slotN` are flocked; an adjacent `slotN.owner` JSON is written with `{"pid", "username", "started_at"}`.
- The recorder holds a slot while capturing parts, releases it immediately after **merging**, then enqueues the job.
- The encoder daemon pauses whenever **any** valid owner files exist (i.e., downloads are active), resuming when none remain.

Change the cap by setting `RECORD_LIMIT` or `[limits].record_limit` (and restart services).

---

## Storage considerations

- Parts are deleted after a successful merge.
- If remux fails, the `.ts` remains and is queued for encode.
- The encoder can **optionally delete the input** after success (`delete_input_on_success = true`).
- Queue directory: `~/.local/state/twitchtool/encode-queue/jobs`. Keep an eye on free space:
  - `twitchtool doctor` reports disk usage.

- Temp directory hygiene:
  - While a recording is in progress, working files live under `<output_dir>/temp` (e.g., `~/Downloads/TwitchTool/temp`).
  - Final files are moved atomically into `<output_dir>` when ready, so partials shouldn’t appear outside `temp/`.
  - If a machine crash or kill -9 leaves files behind, verify no recorder is running (`twitchtool status`) and then remove stale files from `temp/` safely.

### Outputs and file retention

- After recording, parts are merged to `<base>.ts`.
- We attempt a fast remux to `<base>.mp4` (stream copy):
  - On success: by default `delete_ts_after_remux = true` deletes the merged `.ts`.
  - On failure: the `.ts` is kept and used for encode.
- Disable remuxing entirely by setting `[record] enable_remux = false` (or CLI `--no-remux`); the recorder leaves the merged `.ts` in place and does **not** enqueue an encode job.
- The encoder daemon always produces `<base>_compressed.mp4` (x265). It encodes from the remuxed `.mp4` when available, otherwise from the merged `.ts`.
- To keep only the compressed output, set `[record] delete_input_on_success = true` (removes the input file used for encode after success).
- To keep the merged `.ts` even if remux succeeds, set `[record] delete_ts_after_remux = false`.

---

## Troubleshooting

- **Check environment:** `twitchtool doctor`
- **Missing ffmpeg/streamlink:** `sudo apt install ffmpeg streamlink`
- **Stale owner files:** `twitchtool clean` (also auto-cleaned on startup and periodically)
- **7th recording waits forever:** expected when at cap; use `--fail-fast` on `record` to exit immediately.
- **Low disk space:** free space on recordings/queue volume. Encoder won't fix space issues.
- **Permissions:** Ensure the user has `/run/user/<uid>`; otherwise we use `/tmp`.

---

## x265 note: scale=-2:480

The encoder daemon calls:

```
ffmpeg -i input -vf "scale=-2:480" -r 30 -c:v libx265 -crf 26 -preset medium -threads 1 -c:a aac -b:a 128k -movflags +faststart output.mp4
```

`-2` forces FFmpeg to compute a width that preserves aspect ratio **and** is divisible by 2, which libx265 requires.

---

## Developer notes

- Python 3.10+, stdlib only (aside from the tiny `tomli` for 3.10).
- Type hints and docstrings throughout.
- Tests under `tests/` use pytest and mocks; no network.

---

## Commands

- `twitchtool record <username> [--quality best] [--retry-delay 60] [--retry-window 900] [--loglevel error] [--output-dir DIR] [--queue-dir DIR] [--record-limit 6] [--delete-ts-after-remux|--no-delete-ts-after-remux] [--delete-input-on-success|--no-delete-input-on-success] [--fail-fast]`
  - `--output-dir` default: `~/Videos/TwitchTool` if `~/Videos` exists, else `~/Downloads/TwitchTool`.
- `twitchtool encode-daemon run [--queue-dir DIR] [--preset medium] [--crf 26] [--threads 1] [--height 480] [--fps auto] [--loglevel error] [--record-limit 6]`
- `twitchtool encode-daemon stop [--timeout 10] [--force]`
- `twitchtool encode-daemon status`
- `twitchtool tscompress [--height 480] [--fps auto] [--crf 26] [--preset medium] [--threads 1] [--loglevel error] [--delete-ts-after-remux] [--overwrite] [--delete-input-on-success] <.ts ...>`
  - `--fps` default: taken from config (`[encode_daemon].fps`), where `"auto"` preserves source FPS; pass a number or fraction like `30000/1001` to override.
- `twitchtool encode-mode on|off|status`
- `twitchtool help [command]`
- `twitchtool poller run [--users-file ~/.config/twitchtool/users.txt] [--interval 300] [--quality best] [--download-cmd 'twitchtool record'] [--timeout 15] [--probe-concurrency 10] [--record-limit 6] [--logs-dir DIR]`
- `twitchtool poller stop [--timeout 10] [--force]`
- `twitchtool poller status`
- `twitchtool doctor [--queue-dir DIR] [--logs-dir DIR]`
- `twitchtool clean [--record-limit 6]`
- `twitchtool stop <slot> [--record-limit 6] [--timeout 10] [--force]`
- `twitchtool status`
- `twitchtool users list`
- `twitchtool users add <name> [<name> ...]`
- `twitchtool users remove <name> [<name> ...]`

Hint: run `twitchtool <command> --help` for full option descriptions.

---

## License

MIT
