---
name: everloop
description: >
  Manage persistent Claude Code loops — recurring instructions that never expire
  and survive session restarts and reboots, with no external service. They are
  scheduled outside the session by whatever the host has: systemd user timers, a
  launchd agent, or everloop's own scheduler daemon on a box (or container) with
  no init system to borrow. Use when the user says "everloop", wants a
  durable/persistent loop, a loop that "doesn't expire" or "outlives /loop", a
  recurring task that survives restarts, asks how loops keep running without
  systemd or inside a container, or asks to create/list/update/delete such loops
  or push an ad-hoc message into a listening session.
---

# everloop — persistent loops for Claude Code

everloop replaces the built-in `/loop` (whose CronCreate schedule expires after
~7 days) by moving the schedule **out of the session** and into something that
outlives it, so a recurring instruction fires forever — across session restarts
and reboots — with no external service.

What does the scheduling is picked at runtime: systemd user timers on Linux,
launchd agents on macOS, and everloop's own `everloop scheduler` daemon where
there is no usable init system (our agent workspace containers: no systemd, a
read-only `/sys/fs/cgroup`, no Kubernetes API). The CLI and tools are identical
on all three; only calendar expressions differ, and only by being a subset.

- **Repo & source:** `~/projects/52labs/everloop`
- **Binary:** `~/.local/bin/everloop` (rebuild with `cd ~/projects/52labs/everloop && go build -o ~/.local/bin/everloop .`)
- **State:** loop defs + spool in `~/.local/share/everloop/`; timers in `~/.config/systemd/user/everloop-<name>.{timer,service}` (systemd) or `schedule/<name>.json` beside the spool (portable)

## How it works

One Go binary, four roles:

- `everloop serve` — the MCP **channel** server Claude Code spawns over stdio.
  Declares `claude/channel`, drains the spool every ~2s, and pushes each firing
  into the session as `<channel source="everloop" ...>`. Also exposes the loop
  management tools below. It never schedules anything — if it did, loops would
  die with the session, which is the whole problem everloop solves.
- `everloop scheduler` — the supervised daemon that fires loops where systemd
  and launchd are unavailable. One per data dir; logs to stdout; a missed
  window catches up exactly once. Not needed when systemd is doing the work.
- `everloop tick <name>` — one firing, spooled. Coalescing: at most one pending
  tick per loop, so an outage never floods the session (repeat fires bump
  `coalesced_count`).
- CLI — `create` / `list` / `update` / `delete` / `send`.

Delivery is at-least-once (claim → notify → ack); each event carries an
`event_id` meta attribute usable as an idempotency key.

## Watch, don't sweep (`--command`)

**Default to a command loop.** A plain loop is a heartbeat: it fires the same
message every interval and the session usually burns a turn concluding "nothing
changed". A `--command` loop is a watch — the command runs each firing and an
event is delivered **only if it wrote to stdout**:

```bash
everloop create covers --command "/opt/stub/luma-watch.py check" --every 10m \
  --message "A Luma event changed. Judge whether the change looks accidental."
```

If you catch yourself writing a message that starts "check whether X changed",
the checking belongs in a command. Change detection is cheap and belongs in a
script; judgement is expensive and should only run when there is something to
judge.

- exit 0 + no stdout → **nothing is delivered**. Silence is the feature.
- exit 0 + stdout → an event whose body is that output.
- non-zero exit or timeout → a failure report, damped to the 1st/2nd/4th/8th...
  consecutive failure plus one recovery notice. `--timeout` bounds a run
  (default 60s).
- `--message` is optional here and renders as a preamble above the output, so
  the loop can still carry standing instructions.
- `everloop update NAME --command ""` turns a watch back into a heartbeat.

**Reading the events.** A command loop's `coalesced_count` is the number of
firings that produced output, each under its own `[everloop] run N of M`
header, in order. Unlike a static tick these are *different* events, not
repeats — handle every one, don't collapse them. `status="error"` /
`status="timeout"` marks a body that is a diagnostic rather than an
instruction.

**Writing the command.** It runs in the scheduler's environment — the systemd
user manager, launchd, or the scheduler daemon — and **never a login shell**:
`~/.profile` is not sourced, so no fnox/mise/direnv activation and no
interactive-shell secrets. Use absolute paths, have the script fetch its own
secrets (`fnox get KEY`), and test it the way the timer will run it:

```bash
systemd-run --user --wait --pipe --quiet /full/path/to/your-command
```

A command that works pasted into a terminal can still fail under the timer.

## Managing loops

The channel server, when connected, exposes these MCP tools (prefer them inside
a session): `create_loop`, `list_loops`, `update_loop`, `delete_loop`,
`send_message`. If the server is not connected, or you're acting from a shell,
use the CLI — it is the same operations:

```bash
# interval loop (90s, 5m, 1h30m, 2d — minimum 10s)
everloop create reconcile --message "Reconcile the ledger and report anomalies." --every 1h

# command loop — silent unless the command prints something (see below)
everloop create covers --command "/opt/stub/luma-watch.py check" --every 10m

# calendar loop (OnCalendar syntax; a fire missed while off runs once, on return)
everloop create standup --message "Draft the daily standup summary." --calendar "Mon..Fri 09:00"

everloop list
everloop update reconcile --every 30m          # reschedules from now
everloop update reconcile --disable            # stop without deleting
everloop update reconcile --enable
everloop delete reconcile

# push an ad-hoc message into the currently-listening session from any process
everloop send "deploy finished: v1.2.3"
```

Exactly one of `--every` / `--calendar` per loop, and at least one of
`--message` / `--command`; setting one schedule clears the other. Names are
lowercase letters, digits, hyphens (≤41 chars). Invalid intervals, timeouts and
`OnCalendar` expressions are rejected up front.

## Which backend is scheduling (and the container case)

`everloop list` names it per loop, so start there when a loop "never fired":

```
- reconcile: every 1h | enabled=true | timer=systemd: active (next: Mon 2026-07-27 08:00:00 UTC)
- sweep: every 10m | enabled=true | timer=portable: next Mon 2026-07-27 07:20:00 UTC — NO SCHEDULER RUNNING: start `everloop scheduler`
```

`portable` means there is no systemd/launchd to hold the loop, so **something
must be running `everloop scheduler`** — a container's PID 1 supervisor, a
tmux/`herdr` pane, whatever. It logs to stdout, refuses to start twice, stops
cleanly on SIGTERM, and catches a missed window up exactly once rather than
once per missed interval. If the status says NO SCHEDULER RUNNING, that is the
bug: the loops are fine, nothing is firing them.

`EVERLOOP_BACKEND=auto|systemd|launchd|portable` forces the choice; `auto`
(default) probes for a usable systemd user manager and falls back. On the
portable backend, calendar expressions are parsed in-process and anything
outside the supported subset (see the repo README) is refused at create time —
cron syntax like `17 * * * *` is not OnCalendar and will be rejected.

## Connecting a session to receive firings

Loops only *deliver* into a session running the channel server. Register it in
the project's `.mcp.json` (an example ships in the repo) or `~/.claude.json`:

```json
{ "mcpServers": { "everloop": { "command": "/home/stephan/.local/bin/everloop", "args": ["serve"] } } }
```

Channels are a research preview, so launch with the development flag:

```bash
claude --dangerously-load-development-channels server:everloop
```

On connect, the server drains any backlog first (the reconnect/replay path),
then polls. Run one listening session per queue — two `serve` processes would
race for the same spool.

## Notes

- Linger is enabled on titan (`loginctl enable-linger` already done), so systemd
  timers fire even while logged out. In a container the equivalent is simply
  keeping `everloop scheduler` supervised.
- Under systemd, intervals use `OnUnitActiveSec` (monotonic) and calendars use
  `OnCalendar` with `Persistent=true` (catches a missed wall-clock fire up at
  boot). The portable scheduler reproduces both, including the catch-up.
- Local-only, no network listener: anything that can run `everloop send` as the
  user can put text in front of Claude — same trust boundary as the shell.
- `EVERLOOP_DATA_DIR` overrides state location; `EVERLOOP_POLL_SECONDS` the spool
  poll; `EVERLOOP_SCAN_SECONDS` the portable scheduler's scan.
- **Instances**: `EVERLOOP_INSTANCE=<name>` isolates a session's loops into
  their own data dir (`~/.local/share/everloop/<name>/`) and unit namespace
  (`everloop-<name>-<loop>`). Several orchestrators run concurrently this way —
  the 52labs (`bot`), `jessica` (linear), and `clem` sessions each set their own
  instance. Manage one from a shell with `EVERLOOP_INSTANCE=<name> everloop …`.
  Unset = the default instance this skill drives. On the portable backend, run
  one `everloop scheduler` per instance.

See `README.md` in the repo for architecture and delivery-semantics detail.
