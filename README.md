# everloop

Persistent loops for a coding agent, scheduled **outside the session** — by
systemd user timers, launchd agents, or everloop's own scheduler daemon where
there is no init system to borrow (containers). No external service, no
expiration.

A harness's own scheduler is tied to its session (Claude Code's `/loop` expires
after 7 days; codex and omp have no equivalent at all). everloop moves the
schedule out of the session and into something that outlives it. On Linux each
loop is normally a `.timer`/`.service` pair under `~/.config/systemd/user/`; on
macOS a LaunchAgent plist under `~/Library/LaunchAgents/`; in a container with no
systemd, a row of state that `everloop scheduler` acts on. Either way it
survives session restarts and machine reboots, and (on Linux, with linger
enabled) fires even while you're logged out.

## How it works

```
timer / scheduler ──▶ everloop tick NAME ──▶ ~/.local/share/everloop/queue/
                                                       │
   the agent's session ◀── transit op:"send" ◀── everloop serve (drain loop)
                        ◀── herdr agent.prompt ◀──┘
```

**Delivery is a unix socket either way.** By default it is the local
[Transit](https://github.com/Orange-County-AI/transit) daemon's: the delivery
lands in the Transit ledger and carries an idempotency id the agent settles
explicitly, so a firing is auditable from the same place as everything else on
the mesh. Set `TRANSIT_TARGET` to the agent's Transit address.

`CHANNEL_SINK=herdr` selects [herdr](https://herdr.dev)'s socket instead, which
is still fully supported — a box with no Transit daemon, or an agent herdr still
owns, sets that plus `HERDR_TARGET` and behaves exactly as before.
`CHANNEL_SINK=none` turns delivery off for a checkout that should only expose
tools.

Either way `serve` stays harness-agnostic: not a menu of per-model planes, but
two transports that do not care which model is on the other end — claude, codex,
omp, opencode, pi.

One static Go binary, four roles:

- **`everloop serve`** — the MCP server the agent spawns over stdio. It exposes
  the loop tools (`create_loop`, `list_loops`, `update_loop`, `delete_loop`,
  `send_message`) so the agent can manage its own loops, and runs the drain
  loop that submits each spooled message to the configured sink. It never
  schedules anything — see [Backends](#backends-systemd-launchd-portable).
- **`everloop scheduler`** — the supervised daemon that fires loops when the
  portable backend is live. Not needed when systemd or launchd is doing the
  scheduling.
- **`everloop tick NAME`** — one firing, spooled to the queue: what a systemd
  timer or launchd agent executes (the scheduler daemon does the same work
  in-process). At most one pending tick per loop: repeat firings bump
  `coalesced_count` instead of piling up, so an outage never floods the
  session. If the loop has a `--command`, the tick runs it first and spools
  **only if it produced output** (see [Command loops](#command-loops-a-watch-not-a-heartbeat)).
- **CLI** — `create` / `list` / `update` / `delete` / `send`, mirroring the
  MCP tools, so loops can be managed from any shell and any process can push
  a message into the session with `everloop send`.

Delivery is **at-least-once**: messages are claimed (renamed), submitted, then
acked (deleted), so a crash mid-delivery redelivers. Each event carries an
`event_id` meta attribute to use as an idempotency key. Ticks that fire while
no session is listening wait in the spool and are drained — coalesced — the
moment a session reconnects.

**A tick therefore only lands while the agent is alive.** On herdr,
`agent.prompt` fails with `agent_not_found` when nothing is in the pane; on
transit, the daemon refuses a target that resolves to no local session with the
same code. Either way the message stays claimed for the next poll. That is what
lets a liveness heartbeat be a loop: a `--message` loop cannot ping on behalf of
a dead session, while a `--command` loop runs in the timer and would.

## Install

```bash
go build -o ~/.local/bin/everloop .
```

Register it as an MCP server in the harness's project config — `.omp/mcp.json`
for omp, `.mcp.json` for claude/codex — with the target it should deliver to:

```json
{
  "mcpServers": {
    "everloop": {
      "command": "/home/stephan/.local/bin/everloop",
      "args": ["serve"],
      "env": {
        "EVERLOOP_INSTANCE": "clem",
        "TRANSIT_TARGET": "clem@titan"
      }
    }
  }
}
```

To stay on herdr, say so — the default is transit, so `HERDR_TARGET` alone is no
longer a complete config and `serve` refuses at startup rather than picking a
transport for you:

```json
{
  "mcpServers": {
    "everloop": {
      "command": "/home/stephan/.local/bin/everloop",
      "args": ["serve"],
      "env": {
        "EVERLOOP_INSTANCE": "clem",
        "CHANNEL_SINK": "herdr",
        "HERDR_TARGET": "clem"
      }
    }
  }
}
```

No launch flag is needed on any harness on either transport. Delivery does not
use a harness channel plane: Transit's daemon injects the event through
whichever adapter already owns the target, and herdr submits it as session
input. There is nothing to enable and nothing to opt into beyond `CHANNEL_SINK`.

For loops to fire while you're logged out (Linux, systemd backend), enable
linger once:

```bash
loginctl enable-linger $USER
```

## Backends: systemd, launchd, portable

everloop picks its scheduler at **runtime**, not by build tag — the same Linux
binary runs on a box with a systemd user manager and inside a container that
has none.

| backend | where the schedule lives | picked when |
| --- | --- | --- |
| `systemd` | `~/.config/systemd/user/everloop-<name>.{timer,service}` | Linux, and `systemctl --user` actually works |
| `launchd` | `~/Library/LaunchAgents/com.52labs.everloop.<name>.plist` | macOS |
| `portable` | `schedule/<name>.json` + the `everloop scheduler` daemon | nothing usable to borrow |

`EVERLOOP_BACKEND=auto|systemd|launchd|portable` overrides the choice (`auto`
is the default). Forcing a backend that cannot work here is an error, never a
silent downgrade. Every status line names the backend holding the loop, because
"my loop never fired" starts with "which scheduler was supposed to fire it":

```
$ everloop list
- reconcile: every 1h | enabled=true | timer=systemd: active (next: Mon 2026-07-27 08:00:00 UTC)
- sweep: every 10m | enabled=true | timer=portable: next Mon 2026-07-27 07:20:00 UTC
```

### Running in a container (no systemd)

Our agent workspace pods are stateful Linux containers with sshd and
passwordless sudo, `hostUsers: false`, `/sys/fs/cgroup` mounted read-only, and
no systemd in the image at all — making the cgroup tree writable would hand back
the privileges the sandbox exists to remove. Kubernetes CronJobs are not the
alternative either: loops are created at runtime by the agent, and the pods
deliberately run with `automountServiceAccountToken: false`, so there is no API
to create them with.

So run everloop's own scheduler:

```bash
everloop scheduler
```

It is built to be **supervised, not self-supervising**: no daemonising, no
forking, logs to stdout, exits non-zero on anything fatal, and a flock makes a
second copy refuse to start rather than double-fire every loop. `SIGTERM` stops
it cleanly (in-flight command loops get 10s of grace), `SIGHUP` just brings the
next scan forward. In the workspace image PID 1 is a small supervisor running
sshd and this side by side; anything that restarts a process and captures its
stdout works.

```
2026-07-27T07:14:29Z everloop[scheduler]: started: pid 1, instance "clem", data /root/.local/share/everloop/clem, scan 1s, 4 loop(s)
2026-07-27T07:14:33Z everloop[scheduler]: fire: loop "veto-merge-sweep" (every 10m)
2026-07-27T07:16:04Z everloop[scheduler]: catch-up: loop "inbox-sweep" missed 7 fires since 2026-07-27T07:15:02Z — firing once
```

Loops are created by a *different* process than the one that fires them (an MCP
`serve`, or a shell), so there is no reload RPC and nothing to signal: the
daemon re-reads the store every scan (1s, `EVERLOOP_SCAN_SECONDS`) and treats it
as the source of truth. A loop created in a session is picked up within a
second, and `everloop scheduler` can be restarted at any time without losing
one.

**Scheduling deliberately does not live in `everloop serve`.** That process is a
child of the agent's session, so loops scheduled there would die with the
session — exactly the in-session-scheduler failure mode everloop was built to
fix. `serve` warns on stderr if it starts with the portable backend
live and nothing scheduling.

### What the portable backend has to keep that systemd kept for us

- **Fire history.** `schedule/<loop>.json` holds the next and last fire, written
  with the contents fsynced before the rename and the directory fsynced after —
  it is the only record of when a loop last ran, so a power cut must not lose it.
- **Catch-up.** systemd's `Persistent=true` runs a fire missed while the machine
  was off exactly once at next boot. The daemon reproduces that without a
  special case: a loop whose next fire is in the past is simply *due*, so it
  fires **once** — not once per missed interval — and resumes its normal
  cadence. That is also what the session is told to expect, since
  `coalesced_count` means catch up once rather than repeat the work N times.
- **At-least-once, not at-most-once.** The schedule advances *after* the tick is
  spooled, never before. A crash in between costs a repeat, which the spool
  coalesces away; advancing first would turn the same crash into a silently
  skipped sweep, and a loop that quietly stops is the failure everloop exists to
  prevent.
- **Overrun handling.** A loop still running from its last fire is skipped, not
  started twice — the same thing systemd does with a oneshot service that has
  not finished.
- **OnCalendar parsing.** `systemd-analyze calendar` is not in the image either,
  so expressions are parsed in-process (below).

### Calendar expressions

The systemd backend hands `--calendar` to systemd verbatim. The launchd and
portable backends parse it themselves, and accept a subset:

| form | example |
| --- | --- |
| shorthands | `minutely`, `hourly`, `daily`, `weekly`, `monthly`, `yearly` |
| time of day | `09:00`, `17:30:15` |
| date + time | `*-*-* 09:00:00`, `*-*-01 00:00:00`, `2026-12-25 06:30:00` |
| weekday + time | `Mon..Fri 09:00`, `Sat,Sun 12:00`, `Fri *-*-* 18:00:00` |
| lists, ranges, steps in any component | `*:0/15`, `*:0,30`, `9..17:00` |

Everything else — timezone suffixes, `~` (last day of month), `@`-timestamps,
and cron syntax, which is a different language entirely — is **rejected at
create time**, with an error naming what is supported. That is deliberate: a
mis-scheduled loop still looks alive, so nobody investigates it. An expression
that parses but can never occur (`*-02-30`) is refused for the same reason.

## Usage

From inside a session, just ask the agent — the tools are self-describing:

> set a loop that reconciles the ledger every hour

Or from any shell:

```bash
# interval loop (90s, 5m, 1h30m, 2d — min 10s)
everloop create reconcile --message "Reconcile the ledger and report anomalies." --every 1h

# calendar loop (OnCalendar syntax; a fire missed while the machine was down
# runs once when it comes back — see "Calendar expressions" for the subset the
# launchd and portable backends accept)
everloop create standup --message "Draft the daily standup summary." --calendar "Mon..Fri 09:00"

# command loop (a watch: silent unless the command prints something)
everloop create covers --command "/opt/stub/luma-watch.py check" --every 10m \
  --message "A Luma event changed. Judge whether the change looks accidental."

everloop list
everloop update reconcile --every 30m
everloop update reconcile --disable
everloop delete reconcile

# push an ad-hoc message into the listening session from any script
everloop send "deploy finished: v1.2.3"
```

## Command loops: a watch, not a heartbeat

A plain loop is a **heartbeat** — it delivers its message every interval
whether or not anything happened, and the session burns a turn concluding
"nothing changed". Adding `--command` makes it a **watch**: the command runs on
each firing and an event is spooled *only when there is something to say*.

```bash
everloop create covers --command "/opt/stub/luma-watch.py check" --every 10m
```

| command result | what everloop does |
| --- | --- |
| exit 0, empty stdout | **nothing** — no event, no wake-up. This is the point. |
| exit 0, stdout | spools an event whose body is that output |
| non-zero exit | spools a failure report, **damped** (below) |
| still running at `--timeout` | killed, reported as a damped `timeout` failure |

stdout is what decides. A command that writes only to stderr and exits 0 is
still silent, so ordinary progress logging doesn't wake anybody.

**`--message` is a preamble, not a replacement.** With a command set, the
message is optional; when present it is rendered once above the command's
output. The two carry different things — the command supplies the facts, the
message supplies the standing instruction about them ("spawn a sonnet subagent
to judge this"). Ignoring the message would force that instruction down into
whatever script you were wrapping. It is omitted from pure-failure events,
where "judge this" applied to a stack trace would be nonsense.

### Coalescing inverts (and this is the part that matters)

For a static loop, coalescing overwrites: the payload is identical every time,
so five firings collapsing into one lose nothing. **For a command loop every
firing carries different output**, so overwriting would mean an outage during
four cover changes delivers only the fourth — a watch that lies, which is worse
than no watch.

So command firings **accumulate** into the loop's single pending slot instead:

```
<channel source="everloop" kind="tick" loop="covers" coalesced_count="3" ...>
A Luma event changed. Judge whether the change looks accidental.

[everloop] run 1 of 3 — 2026-07-27T05:37:40Z
LUMA COVER-CHANGED evt-aaa "Kickoff" by=Li new=https://cdn/x.png

[everloop] run 2 of 3 — 2026-07-27T05:37:51Z
LUMA NEW-EVENT evt-bbb "Office Hours" by=Don

[everloop] run 3 of 3 — 2026-07-27T05:38:02Z
LUMA EDITED evt-aaa start_at: '2026-08-01' -> '2026-08-02'
</channel>
```

One event per loop (so a reconnect is never flooded with N separate messages),
every firing preserved in order. `coalesced_count` is the number of firings
that *produced output*, not the number of times the timer fired — silent
firings leave no trace at all. Per-firing spool files would also preserve the
data, but they reintroduce exactly the flood coalescing exists to prevent.

Accumulation is **bounded** at 20 runs / 64 KiB per event, and any single run's
output at 16 KiB. Past the bound the oldest runs are dropped and the body says
so (`[everloop] 12 earlier run(s) dropped to bound the spool.`) — an outage plus
a chatty command cannot grow the spool without limit, and the loss is stated
rather than silently pretended away.

### Failure damping

A permanently broken command must not page the session every interval forever;
everloop's whole value is silence when nothing is happening, and a watch that
cries every 10 minutes trains you to ignore it. Failures are reported on the
**1st, 2nd, 4th, 8th, 16th... consecutive** failure, and recovery is reported
exactly once:

```
[everloop] loop "covers" command failed (exit 127) — consecutive failure 2; damped, next report at 4.
$ /opt/stub/luma-watch.py check
env: 'uv': No such file or directory
```

Failure events carry `status="error"` (or `status="timeout"`) in the channel
meta so an agent can tell a diagnostic from a watch hit without parsing the
body. stderr is included because that is where a broken environment announces
itself — which brings us to:

Command loops **poll**. Sources that push instead — `tail -f`, `inotifywait -m`,
a WebSocket — need a supervised long-lived process rather than a timer. That is
designed but deliberately not built: see [docs/streaming.md](docs/streaming.md).

### The command runs under the scheduler, not your shell

This has bitten us twice. The command is executed by whatever is scheduling —
the **systemd user manager**, launchd, or the `everloop scheduler` daemon — and
never by a login shell:

- **`~/.profile`, `~/.bashrc` and `~/.bash_profile` are NOT sourced.** Anything
  they export — `fnox activate`, mise activation, `direnv`, a project `venv` —
  is absent.
- **PATH is the unit's PATH, not yours.** On this machine
  `~/.config/systemd/user/service.d/10-path.conf` puts `~/.local/bin` and
  `~/.local/share/mise/shims` on PATH for every user service, so mise-managed
  tools (`uv`, `bun`, `fnox`, `node`) do resolve. On a machine without that
  drop-in they will not.
- Secrets that live in your interactive environment are not there either. Have
  the script fetch them itself (`fnox get KEY`) rather than assuming `$KEY`.
- Under the portable backend the command inherits the **daemon's** environment,
  which is whatever the supervisor gave PID 1 — usually smaller still, and
  nothing like an SSH session's.

A command that works pasted into a terminal can still fail under the timer, and
before this feature it failed *invisibly* — a script died with
`env: 'uv': No such file or directory` and nothing was ever spooled. That is
why failure events exist at all, and why they carry stderr. When in doubt use
absolute paths and test the real thing:

```bash
# runs in a transient user unit, so it inherits the same drop-ins the timer does
systemd-run --user --wait --pipe --quiet /full/path/to/your-command
```

## Event format

Events arrive in the session as:

```
<channel source="everloop" kind="tick" loop="reconcile" coalesced_count="1"
         event_id="a1b2c3d4e5f6" queued_at="2026-07-09T08:00:00Z">
Reconcile the ledger and report anomalies.
</channel>
```

- `kind="tick"` — a loop firing; the body is the loop's message. If
  `coalesced_count` > 1 the loop fired that many times while nobody was
  listening: catch up once.
- `kind="message"` — an ad-hoc message from `everloop send` or the
  `send_message` tool.
- For a **command loop** the body is the command's output instead, and
  `coalesced_count` is how many firings produced output — each under its own
  `[everloop] run N of M` header, in the order it happened. Those are distinct
  events, not repeats: handle every one.
- `status="error"` / `status="timeout"` — present only when the loop's command
  is failing rather than reporting. The body is a diagnostic, not an
  instruction.

## Delivery (`CHANNEL_SINK`)

Everything above the last hop — the OS timers, the spool, claim → ack,
coalescing — never cared which harness was listening, and the last hop does not
either. There are two transports, both unix sockets, and events arrive wrapped
in a `<channel source="everloop" ...>` envelope on both, so one set of agent
instructions is correct everywhere.

| `CHANNEL_SINK` | what happens |
|---|---|
| unset / `transit` | **default.** `op:"send"` over the local Transit daemon's IPC socket. |
| `herdr` | `agent.prompt` over herdr's unix socket. Still fully supported. |
| `none` / `tools` | exposes the tools and never drains, for a checkout or a deployment where something else owns delivery. |

Any other value is refused at startup rather than falling back to a transport.

**The default flipped from `herdr` to `transit`.** A config written before that
— `HERDR_TARGET` set, `CHANNEL_SINK` unset — used to be complete and now is not:
`serve` refuses at startup and names both remedies (add `TRANSIT_TARGET`, or set
`CHANNEL_SINK=herdr`). It does **not** quietly fall back to herdr. An implicit
transport chosen by whichever env var happens to be set is the silent guess this
whole mechanism refuses to make, and a box that stayed on herdr without saying
so would be exactly the ledger blind spot the new default closes.

Mount `everloop serve` as an MCP server in the harness with the envs set — one
process then does both directions: the harness gets the `create_loop` /
`send_message` / etc. tools over stdio, and the drain loop submits inbound
events over the socket.

### `CHANNEL_SINK=transit` (default)

Delivers through the local
[Transit](https://github.com/Orange-County-AI/transit) daemon. This is the
default; `herdr` below is still fully supported and is one env var away.

Two things it buys, and they are why it is the default. The delivery lands in
the **Transit ledger**, so anything audited from the ledger — a census, an
outage sweep — can see everloop's traffic; a delivery outside the ledger is a
blind spot in exactly the place one recently cost the fleet 28 unnoticed
minutes, and everloop was the last herdr-only delivery path left. And the
message carries a Transit **idempotency id with explicit settlement**, so a
redelivery is recognisable to the agent rather than a second identical paste.

| env | default | |
|---|---|---|
| `TRANSIT_TARGET` | *(required)* | the Transit address to deliver to: `name`, `name@host`, or `#room`. Absent, the sink refuses at startup rather than guessing an address and delivering into someone else's session. |
| `TRANSIT_SEND_TIMEOUT_MS` | `45000` | the send bound in milliseconds; a non-numeric or non-positive value is refused at startup. The default is deliberately past the daemon's own bounds — it holds an IPC connection for 35s, and a cross-host send waits up to 10s for the Worker's commit — so everloop never abandons a send the daemon is still completing. |
| `TRANSIT_SOCKET` / `TRANSIT_DATA_DIR` | `~/.local/share/transit/transit.sock` | same precedence transit itself uses, so the two cannot disagree about where the socket is |

**Why the IPC socket and not the CLI.** There is no `transit send`. Transit's
CLI dispatches `daemon`, `adapter`, `enroll`, `status`, `inbox`, `pause`, `mcp`,
`version` and nothing else; the only sending surface it ships is the MCP tool
`send_message`, which is itself a thin wrapper over `{"op":"send",…}` on this
same socket. Going through the CLI would mean spawning `transit mcp`, completing
an MCP handshake over its stdio and calling a tool, to reach a socket everloop
can dial directly — and it would put a subprocess back on a delivery path the
herdr sink deliberately cleared, so that a body carrying `$(...)`, quotes,
backticks or newlines travels as one literal JSON string.

**everloop does not render `transit/1`.** That envelope — attribute order,
escaping, the 4,000-rune clip with `truncated="1"`, the channel preview rules —
is produced daemon-side and golden-vector tested in the Transit repo. everloop
hands over a *body* and lets Transit wrap it, so on this transport the
`<channel source="everloop" ...>` envelope simply arrives **inside** a
`transit/1` one and the meta contract is unchanged. A body over 4,000 runes is
clipped for terminal injection with `truncated="1"`; `read_message` returns the
rest.

**Sender identity.** Transit derives the sender from the caller, never from the
request: a herdr pane id, or the pid whose process ancestry reaches a registered
native adapter. `everloop serve` is a child of the agent's harness process,
which is that ancestry, so it resolves with `herdr.service` stopped. everloop
sends `HERDR_PANE_ID` too when it is set — an optional hint, not a dependency.
The consequence worth knowing: when `serve` runs under the same agent it
delivers to, the tick's `from` **is that agent** — a self-addressed message.
everloop cannot mint an `everloop@host` identity of its own; the daemon only
registers `claude`, `omp`, `pi` and `opencode` adapters.

**Ack semantics, precisely.** `injected`, `committed` and `spooled` all mean
Transit took custody and owns the retry, so all three ack. Holding the message
so everloop retries as well is how an agent gets the same tick twice. A
`spooled`-with-warning custody (a held composer, say) is logged to stderr rather
than swallowed. A structured refusal keeps its code — `agent_not_found` means
`TRANSIT_TARGET` names nobody, or everloop's own caller identity did not
resolve, both configuration faults — while a bare message means the daemon could
not be reached and the outcome is unknown. Either way the message stays claimed
and the spool redelivers it next poll, in order.

**Body size.** Transit stores at most 64 KiB per message and refuses more with
`body_too_large`, which is permanent — the drain loop would retry it every poll
forever and block every message behind it. everloop's own event cap is already
64 KiB of content before headers and the channel wrapper, so the body is clipped
to fit with everloop's usual `[everloop] output truncated, N bytes dropped]`
marker. A dropped tail is visible in the body, never silent.

```jsonc
// .omp/mcp.json — everloop delivering over Transit (the default: no CHANNEL_SINK)
{
  "mcpServers": {
    "everloop": {
      "type": "stdio",
      "command": "everloop",
      "args": ["serve"],
      "env": {
        "EVERLOOP_INSTANCE": "clem",
        "TRANSIT_TARGET": "clem@titan"
      }
    }
  }
}
```

### `CHANNEL_SINK=herdr`

Still fully supported, and unchanged: a box with no Transit daemon, or an agent
herdr still owns, sets `CHANNEL_SINK=herdr` and behaves exactly as it did
before the default moved.

| env | default | |
|---|---|---|
| `HERDR_TARGET` | *(required)* | the agent's herdr target. Prefer the **agent name** — a pane id dies with the pane, a name survives a restart. |
| `HERDR_PROMPT_TIMEOUT_MS` | `120000` | the wait bound in milliseconds; a non-numeric or non-positive value is refused at startup rather than silently defaulted |
| `HERDR_SOCKET_PATH` / `HERDR_SESSION` | `~/.config/herdr/herdr.sock` | same precedence the CLI uses: explicit path, then session, then the default socket |
| `EVERLOOP_HERDR_PROTOCOL_ALLOW` | *(empty)* | extra accepted protocol versions, comma-separated. 19 and 20 are accepted without it; an unparseable token is ignored rather than fatal, because a typo here must not stop delivery on eight boxes at once |

The call is `agent.prompt` with `wait.until = [idle, done, blocked]`, over
newline-delimited JSON. A `ping` on its own connection checks the protocol
first, so an unaccepted server is never sent an operation.

**Why a socket and not the CLI.** This used to exec
`herdr agent prompt … --wait --timeout N`, which worked and could not do three
things. A non-zero exit says only "non-zero", so a missing agent
(`agent_not_found` — fix `HERDR_TARGET`) was indistinguishable from an
unreachable herdr (delivery outcome unknown); the two now stay apart, and only
the second is a reason to suspect the transport. `agent_prompt_stalled` is
recoverable and recovery needs several calls with a proof step between them
(below), which one exec cannot express. And it spawned a process per tick for a
socket already open to us.

**Ack semantics, precisely.** A submitted prompt acks; any failure returns an
error, so the message stays claimed and the spool redelivers it next poll, in
order. The caveat worth stating outright: herdr reports a **settled lifecycle
state, which includes `blocked`** (the agent stopped on a permission prompt).
That is evidence the tick was *delivered*, not proof it was *processed*.
everloop's design already tolerates exactly this — ticks coalesce, so a firing
the agent parked on is carried into the next event's `coalesced_count` rather
than lost, and at-least-once was never a promise that the agent acted.

**The stalled-paste recovery.** Measured on omp: a large bracketed paste can
collapse into an attachment chip and absorb herdr's submit key, so herdr answers
`agent_prompt_stalled` for a prompt that is sitting in the composer. everloop
resolves the pane, records its `state_change_seq`, sends one `Enter`, and polls
for ~15s. Only if that sequence **moves** does it wait for the agent to settle
and ack. Accepting the keypress proves nothing, so when the sequence does not
move the original stall is reported rather than a success nobody observed.

```jsonc
// .omp/mcp.json — everloop delivering into a herdr-driven pane
{
  "mcpServers": {
    "everloop": {
      "type": "stdio",
      "command": "everloop",
      "args": ["serve"],
      "env": {
        "EVERLOOP_INSTANCE": "clem",
        "CHANNEL_SINK": "herdr",
        "HERDR_TARGET": "clem"
      }
    }
  }
}
```

## Notes & semantics

- **Latency**: timer accuracy is 1s (the portable scheduler scans every 1s,
  `EVERLOOP_SCAN_SECONDS`) and the spool poll is 2s (`EVERLOOP_POLL_SECONDS`),
  so end-to-end latency is a few seconds — but events only enter the
  conversation between turns, like any channel.
- **Interval vs calendar**: on Linux `--every` uses `OnUnitActiveSec`
  (monotonic, reschedules from activation) and `--calendar` uses `OnCalendar`
  with `Persistent=true` (wall-clock, a fire missed while the machine was off
  runs at next boot). On macOS `--every` uses `StartInterval` and `--calendar`
  maps a subset of OnCalendar syntax to `StartCalendarInterval`; launchd has no
  missed-fire catch-up and exposes no next-fire time in `list`. The portable
  backend anchors an interval to the *scheduled* fire (so a slow command does
  not make the loop drift) and catches up a missed window exactly once, for
  both schedule kinds.
- **State**: loop definitions, the spool, per-loop command-failure damping
  memory (`state/<loop>.json`) and — under the portable backend — fire history
  (`schedule/<loop>.json`) live in `~/.local/share/everloop/`
  (`EVERLOOP_DATA_DIR` to override). Timers are
  `~/.config/systemd/user/everloop-<name>.{timer,service}` on Linux,
  `~/Library/LaunchAgents/com.52labs.everloop.<name>.plist` on macOS (tick
  output logs to `~/Library/Logs/everloop/<name>.log`).
- **Concurrency**: spool mutations are serialized with a `flock` on
  `queue.lock`; delivery claims rename the file first, so a tick landing
  mid-claim starts a fresh entry and no coalesce increment (or accumulated
  command output) is lost. A loop's command runs *outside* that lock — it may
  take up to its full timeout, and holding the flock that long would stall the
  drain loop and every other loop's tick.
- **Command loops off systemd**: the semantics above are identical (silence,
  accumulation, damping, timeout), but one mechanic differs. systemd units get
  `TimeoutStartSec = timeout + 30s` so the tick outlives its own deadline and
  can spool the failure report; launchd and the portable scheduler have no
  per-invocation runtime cap, so everloop's in-process timeout is the only
  bound there — a hung command is still killed, it just isn't double-covered.
  launchd also has no equivalent of the `10-path.conf` drop-in: its PATH
  defaults to `/usr/bin:/bin:/usr/sbin:/sbin`, so absolute paths matter more,
  not less. Failure damping state lives in
  `~/.local/share/everloop/state/<loop>.json` everywhere.
- **Security**: delivery is local-only — a unix socket, no network listener.
  Anything that can run `everloop send` as your user can put text in front of
  the agent, which is the same trust boundary as your shell.
- **One session per instance**: run one draining session
  per queue. Two concurrent `serve` processes sharing a spool would race for it
  (each message still goes to exactly one of them). To run **several**
  independent orchestrators at once, give each its own instance. Under the
  portable backend the same goes for the daemon — one `everloop scheduler` per
  data dir, which the lock enforces rather than trusts.

## Multiple instances (`EVERLOOP_INSTANCE`)

Several long-lived sessions (e.g. one orchestrator per org) can each
own their own loops by setting `EVERLOOP_INSTANCE=<name>` on the `serve`
process. An instance gets:

- its own data dir: `~/.local/share/everloop/<name>/`
- its own timer namespace: `everloop-<name>-<loop>.{timer,service}` on Linux,
  `com.52labs.everloop.<name>.<loop>.plist` on macOS. Under the portable
  backend the data dir *is* the namespace — one `everloop scheduler` per
  instance, each pointed at its own `EVERLOOP_INSTANCE` (or
  `EVERLOOP_DATA_DIR`).

The instance is baked into each generated timer (`Environment=` /
`EnvironmentVariables`), so the timer-fired `tick` resolves the same data dir
the `create` used. Loop names never collide across instances.

Register it per session in `.mcp.json` (or `--mcp-config`):

```json
{ "mcpServers": { "everloop": {
  "command": "/home/stephan/.local/bin/everloop", "args": ["serve"],
  "env": { "EVERLOOP_INSTANCE": "clem" } } } }
```

To manage an instance's loops from a shell, set the same env:

```bash
EVERLOOP_INSTANCE=clem everloop list
```

An unset `EVERLOOP_INSTANCE` is the default instance (`~/.local/share/everloop/`,
units `everloop-<loop>`), which is what the `/everloop` skill uses.
