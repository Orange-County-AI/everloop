---
name: everloop
description: >
  Manage persistent Claude Code loops backed by systemd user timers — recurring
  instructions that never expire and survive reboots, with no external service.
  Use when the user says "everloop", wants a durable/persistent loop, a loop that
  "doesn't expire" or "outlives /loop", a systemd-backed recurring task, or asks
  to create/list/update/delete such loops or push an ad-hoc message into a
  listening session.
---

# everloop — persistent loops for Claude Code (systemd-backed)

everloop replaces the built-in `/loop` (whose CronCreate schedule expires after
~7 days) with **systemd user timers**, so a recurring instruction fires forever
— across session restarts and reboots — with no external service. (On macOS the
same binary uses launchd agents instead; the CLI and tools are identical, but
calendar expressions are limited to a subset — `hourly`, `daily`, `weekly`,
`*-*-* HH:MM`, `Mon *-*-* HH:MM`.)

Everything below runs the `everloop` binary on PATH.

## How it works

One Go binary, three roles:

- `everloop serve` — the MCP **channel** server Claude Code spawns over stdio.
  Declares `claude/channel`, drains the spool every ~2s, and pushes each firing
  into the session as `<channel source="everloop" ...>`. Also exposes the loop
  management tools below.
- `everloop tick <name>` — what each OS timer runs; spools one firing.
  Coalescing: at most one pending tick per loop, so an outage never floods the
  session (repeat fires bump `coalesced_count`).
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

**Writing the command.** It runs in the systemd user environment (launchd on
macOS), **not a login shell** — `~/.profile` is not sourced, so no mise/direnv
activation and no interactive-shell secrets. Use absolute paths, have the
script fetch its own secrets (on this fleet, `secret KEY`), and test it the way
the timer will run it:

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

# command loop — silent unless the command prints something (see above)
everloop create covers --command "/opt/stub/luma-watch.py check" --every 10m

# calendar loop (systemd OnCalendar syntax; a fire missed while off runs at next boot)
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

## Instances

An **instance** is a self-contained everloop: its own loop definitions, its own
spool, and its own timer namespace (`everloop-<instance>-<loop>` on Linux). Two
long-lived sessions that shared one spool would steal each other's firings, so
each gets its own instance.

Select one with the global `--instance NAME` flag — valid on every subcommand
and anywhere in the argument list — or with `EVERLOOP_INSTANCE=NAME`. The flag
wins when both are set; neither means the default instance.

```bash
everloop --instance <name> list
EVERLOOP_INSTANCE=<name> everloop list     # same thing
```

**Use the same instance for the CLI and for `serve`.** A loop created in one
instance is invisible to a session serving another. The instance is baked into
each generated timer unit, so a timer-fired `tick` always lands in the instance
that created the loop, whichever way you selected it.

## Connecting a session to receive firings

Loops only *deliver* into a session running the channel server. Register it in
the project's `.mcp.json` (an example ships in the repo) or `~/.claude.json`:

```json
{
  "mcpServers": {
    "everloop": {
      "command": "everloop",
      "args": ["serve", "--instance", "<name>"]
    }
  }
}
```

Drop the `--instance` pair for the default instance. Equivalently, pass it as
an env block: `"args": ["serve"], "env": {"EVERLOOP_INSTANCE": "<name>"}`.

Channels are a research preview, so launch with the development flag:

```bash
claude --dangerously-load-development-channels server:everloop
```

On connect, the server drains any backlog first (the reconnect/replay path),
then polls. **Run one listening session per instance** — two `serve` processes
on the same instance would race for the same spool.

## Notes

- Intervals use `OnUnitActiveSec` (monotonic); calendars use `OnCalendar` with
  `Persistent=true`, which catches up a wall-clock fire missed while the machine
  was off. Note that enabling a calendar timer that has never run counts as a
  missed fire, so it goes off once immediately.
- **Calendar expressions carry no zone by default.** A systemd user manager
  usually runs in UTC, so write the zone explicitly:
  `--calendar "*-*-* 07:47 America/Los_Angeles"`.
- On Linux, timers only fire while logged out if the user has linger enabled
  (`loginctl enable-linger $USER`, once per host).
- Local-only, no network listener: anything that can run `everloop send` as the
  user can put text in front of Claude — same trust boundary as the shell.
- `EVERLOOP_DATA_DIR` overrides the state location outright (instance and all);
  `EVERLOOP_POLL_SECONDS` sets the poll interval.

See `README.md` in the repo for architecture and delivery-semantics detail.
