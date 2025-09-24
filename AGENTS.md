# AGENTS

This repository is routinely managed by human operators and automated coding agents.
The notes below capture the expectations for any future agent session.

## Installation & Environment
- Prefer `pipx install --editable .` during development so the CLI reflects local changes.
- Runtime state lives under `~/.local/state/twitchtool/`; poller and encoder now maintain their own status subdirectories there.
- Default config is `~/.config/twitchtool/config.toml`; recorder outputs go to `~/Downloads/TwitchTool` unless overridden.

## CLI Usage Notes
- `twitchtool poller` and `twitchtool encode-daemon` accept optional subcommands; if omitted they default to `run`. Both also accept `--json-logs` either before or after the subcommand.
- `twitchtool poller status` prints the last/next poll timestamps and the time until the next poll.
- `twitchtool encode-mode on|off|status` toggles `[record].enable_remux` in the config (disables remux/encode queue when off).

## Systemd Integration
- User units live at `~/.config/systemd/user/twitch-poller.service` and `~/.config/systemd/user/twitch-encoderd.service`. They call the new subcommands (`poller run`, `encode-daemon run`).
- After editing units, run `systemctl --user daemon-reload` before restarting services.

## Troubleshooting Checklist
- If the poller/encoder refuses to start with "invalid choice" errors, confirm the installed CLI version includes the default-subcommand shim and that the service ExecStart lines use `poller run` / `encode-daemon run`.
- Poller status not updating? Check `journalctl --user -u twitch-poller.service` and look for JSON logs; ensure `~/.local/state/twitchtool/poller/` is writable.
- Recorder slots are in `/run/user/<uid>/twitch-record-slots`; `twitchtool clean` removes stale owner files.

## Coding Conventions
- Stay within ASCII unless a file already uses Unicode.
- When modifying CLI behaviour, update the README quick-start and command list; they are heavily relied on.
- Keep automated changes additive—do not rewrite user config files beyond specific keys.
