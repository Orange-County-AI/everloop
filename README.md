# everloop

Persistent loops for Claude Code, backed by **OS user timers** — systemd on
Linux, launchd on macOS. No external service, no expiration.

Claude Code's built-in `/loop` (CronCreate) expires after 7 days. everloop
moves the schedule out of the session and into the OS scheduler. On Linux each
loop is a `.timer`/`.service` pair under `~/.config/systemd/user/`; on macOS
it is a LaunchAgent plist under `~/Library/LaunchAgents/`. Either way it
survives session restarts and machine reboots, and (on Linux, with linger
enabled) fires even while you're logged out.

## How it works

```
OS timer ──▶ everloop tick NAME ──▶ ~/.local/share/everloop/queue/
                                                       │
Claude Code ◀── notifications/claude/channel ◀── everloop serve (MCP channel)
```

One static Go binary, three roles:

- **`everloop serve`** — the MCP channel server Claude Code spawns over stdio.
  It declares the `claude/channel` capability, polls the spool every 2s, and
  pushes each message into the session as a `<channel source="everloop" ...>`
  event. It also exposes MCP tools (`create_loop`, `list_loops`,
  `update_loop`, `delete_loop`, `send_message`) so Claude can manage loops
  from inside the session.
- **`everloop tick NAME`** — what each OS timer executes. It spools one
  firing to the queue. At most one pending tick per loop: repeat firings bump
  `coalesced_count` instead of piling up, so an outage never floods the
  session.
- **CLI** — `create` / `list` / `update` / `delete` / `send`, mirroring the
  MCP tools, so loops can be managed from any shell and any process can push
  a message into the session with `everloop send`.

Delivery is **at-least-once**: messages are claimed (renamed), notified, then
acked (deleted), so a crash mid-delivery redelivers. Each event carries an
`event_id` meta attribute to use as an idempotency key. Ticks that fire while
no session is listening wait in the spool and are drained — coalesced — the
moment a session reconnects.

## Install

```bash
go build -o ~/.local/bin/everloop .
```

Register the channel server in `.mcp.json` (project) or `~/.claude.json`
(user, use the absolute path):

```json
{
  "mcpServers": {
    "everloop": {
      "command": "/home/stephan/.local/bin/everloop",
      "args": ["serve"]
    }
  }
}
```

Channels are a research preview, so launch with the development flag:

```bash
claude --dangerously-load-development-channels server:everloop
```

For loops to fire while you're logged out (Linux), enable linger once:

```bash
loginctl enable-linger $USER
```

## Usage

From inside a session, just ask Claude — the tools are self-describing:

> set a loop that reconciles the ledger every hour

Or from any shell:

```bash
# interval loop (90s, 5m, 1h30m, 2d — min 10s)
everloop create reconcile --message "Reconcile the ledger and report anomalies." --every 1h

# calendar loop (systemd OnCalendar syntax; missed fires run at next boot)
# macOS supports a subset: hourly, daily, weekly, "*-*-* HH:MM", "Mon *-*-* HH:MM"
everloop create standup --message "Draft the daily standup summary." --calendar "Mon..Fri 09:00"

everloop list
everloop update reconcile --every 30m
everloop update reconcile --disable
everloop delete reconcile

# push an ad-hoc message into the listening session from any script
everloop send "deploy finished: v1.2.3"
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

## Other harnesses (`CHANNEL_SINK`)

Everything above the last hop — the OS timers, the spool, claim → ack,
coalescing — is harness-agnostic; only the default MCP-channel push is
Claude-Code-specific. The last hop is a pluggable **sink**
(`CHANNEL_SINK=claude|opencode|hermes`, default claude), shared with
[tincan](../tincan). Events always arrive wrapped in the same
`<channel source="everloop" ...>` envelope, so agent instructions are
portable across harnesses, and delivery through any sink keeps the
at-least-once contract: a failed delivery leaves the message claimed and it
is retried next poll, in order.

Mount `everloop serve` as an MCP server in the harness with the sink envs
set — one process then does both directions (the harness gets the
`create_loop` / `send_message` / etc. tools over stdio, and the drain loop
injects inbound events over HTTP). `CHANNEL_SINK=none` gives a tools-only
serve (no draining) for deployments where a separate process owns delivery.

**OpenCode** — targets a live [`opencode serve`](https://opencode.ai/docs/server/)
(`OPENCODE_URL`, default `http://127.0.0.1:4096`); each event becomes a user
turn via `POST /session/{id}/prompt_async`. The session is resolved by title
(`OPENCODE_SESSION_TITLE`, scoped by `OPENCODE_DIRECTORY`) — found or created
on first delivery, re-resolved if it vanishes — or pinned with
`OPENCODE_SESSION_ID`. Basic auth follows opencode's own
`OPENCODE_SERVER_USERNAME` / `OPENCODE_SERVER_PASSWORD`.

```jsonc
// opencode.json — one process: tools + injection
{
  "mcp": {
    "everloop": {
      "type": "local",
      "command": ["everloop", "serve"],
      "environment": {
        "EVERLOOP_INSTANCE": "clem",
        "CHANNEL_SINK": "opencode",
        "OPENCODE_SESSION_TITLE": "clem"
      }
    }
  }
}
```

**Hermes** — targets a [hermes gateway webhook route](https://hermes-agent.nousresearch.com/docs/user-guide/messaging/webhooks)
(`HERMES_WEBHOOK_URL`, e.g. `http://127.0.0.1:8644/webhooks/everloop`), one
POST per event, signed with the route's Generic V2 secret
(`HERMES_WEBHOOK_SECRET`; HMAC-SHA256 of `<timestamp>.<body>`) and
deduplicated by an `X-Request-ID` of `everloop-<event_id>` — hermes drops
repeats for 1h, which pairs with the spool's at-least-once redelivery. Each
event spawns a run; hermes has no persistent session to inject into. The
payload is `{"body": "<channel ...>...</channel>", "meta": {...}}`, so the
route's prompt template is just `{body}`.

## Notes & semantics

- **Latency**: timer accuracy is 1s and the spool poll is 2s
  (`EVERLOOP_POLL_SECONDS` to change), so end-to-end latency is a few
  seconds — but events only enter the conversation between turns, like any
  channel.
- **Interval vs calendar**: on Linux `--every` uses `OnUnitActiveSec`
  (monotonic, reschedules from activation) and `--calendar` uses `OnCalendar`
  with `Persistent=true` (wall-clock, a fire missed while the machine was off
  runs at next boot). On macOS `--every` uses `StartInterval` and `--calendar`
  maps a subset of OnCalendar syntax (`hourly`, `daily`, `weekly`,
  `*-*-* HH:MM`, `Mon *-*-* HH:MM`) to `StartCalendarInterval`; launchd has no
  missed-fire catch-up and exposes no next-fire time in `list`.
- **State**: loop definitions and the spool live in
  `~/.local/share/everloop/` (`EVERLOOP_DATA_DIR` to override). Timers are
  `~/.config/systemd/user/everloop-<name>.{timer,service}` on Linux,
  `~/Library/LaunchAgents/com.52labs.everloop.<name>.plist` on macOS (tick
  output logs to `~/Library/Logs/everloop/<name>.log`).
- **Concurrency**: spool mutations are serialized with a `flock` on
  `queue.lock`; delivery claims rename the file first, so a tick landing
  mid-claim starts a fresh entry and no coalesce increment is lost.
- **Security**: the channel is local-only — no network listener. Anything
  that can run `everloop send` as your user can put text in front of Claude,
  which is the same trust boundary as your shell.
- **One session per instance**: like all channels, run one listening session
  per queue. Two concurrent `serve` processes sharing a spool would race for it
  (each message still goes to exactly one of them). To run **several**
  independent orchestrators at once, give each its own instance.

## Multiple instances (`EVERLOOP_INSTANCE`)

Several long-lived sessions (e.g. distinct Claude Code orchestrators) can each
own their own loops by setting `EVERLOOP_INSTANCE=<name>` on the `serve`
process. An instance gets:

- its own data dir: `~/.local/share/everloop/<name>/`
- its own timer namespace: `everloop-<name>-<loop>.{timer,service}` on Linux,
  `com.52labs.everloop.<name>.<loop>.plist` on macOS

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
