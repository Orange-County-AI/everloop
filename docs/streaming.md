# Phase 2 — streaming loops (design only, not built)

**Status: design. Nothing here is implemented, and nothing currently needs it.**
This exists so the Phase 1 schema doesn't paint us into a corner and so the
next person doesn't re-derive the argument from scratch.

## What it is

Phase 1 (`--command`) is a **poll**: the OS timer fires, everloop runs a command
to completion, spools its stdout if any, exits. That covers everything whose
change can be detected by asking — an API, a git remote, a directory listing.

Some sources push instead of answering: `tail -f app.log`, `inotifywait -m`,
a WebSocket, `kubectl get --watch`, `journalctl -f`. Polling those either
misses events between ticks or means re-reading the world every time. They need
a **long-lived supervised process** whose stdout is spooled line by line as it
arrives.

The agreed shape: this is a **second unit shape inside everloop**, sharing the
same spool, the same claim→ack contract, and the same sinks. It is *not* a
separate channel implementation and *not* a separate binary. `everloop list`
shows both kinds; a session receives both as `<channel source="everloop">`
events and cannot tell which mechanism produced them.

## Why not now

Nothing we run needs it. The motivating consumer (a Luma calendar watch) polls
an HTTP API on a 10-minute interval — a poll loop is strictly better for it:
no supervised process, no restart semantics, no ingest-loss window. Building
streaming before something needs it would mean guessing at the backpressure
policy with no real traffic shape to calibrate against.

---

## Unit shape

A stream loop has no `.timer`. It gets a service unit that *is* the process:

```ini
[Unit]
Description=everloop stream: [inst] applog

[Service]
Type=simple
Environment=EVERLOOP_INSTANCE=inst
ExecStart=/home/stephan/.local/bin/everloop stream applog
Restart=always
RestartSec=1s
RestartSteps=4
RestartMaxDelaySec=120s
# Never let systemd give up on the unit. everloop reports a flapping stream
# into the session (damped); systemd silently parking it in `failed` is the
# one outcome a watch must not have.
StartLimitIntervalSec=0

[Install]
WantedBy=default.target
```

`everloop stream NAME` is a new subcommand, symmetrical with `tick`: it spawns
the loop's `Command` under `sh -c`, reads stdout line by line, and spools.
Note `WantedBy=default.target` rather than `timers.target` — and that streams
need `loginctl enable-linger` to survive logout just as timers do.

`RestartSteps`/`RestartMaxDelaySec` need systemd ≥ 254 (titan runs 259, verified).
On older systemd, drop them and accept a flat `RestartSec`.

## Restart is not transparent — say so in-band

This is the part that is easy to get wrong. `Restart=always` makes the *process*
durable; it does not make the *stream* durable. When `tail -f` is restarted it
resumes at end-of-file; when `inotifywait -m` is restarted, filesystem events
during the gap were never observed by anyone. There is no replay.

So a restart is a **reportable gap**, not an implementation detail. On startup,
if the previous exit was not a clean operator-initiated stop, `everloop stream`
should spool a damped notice:

```
[everloop] stream "applog" restarted after exit 1 — events between
2026-07-27T05:37:40Z and 2026-07-27T05:37:41Z were not observed.
```

Damped with the *existing* `shouldReport` schedule (1st, 2nd, 4th, 8th...
consecutive restart) and the existing `runState` file, with one recovery notice
once the process has stayed up past a stability threshold (say 5 minutes). A
flapping stream must not become the firehose Phase 1 exists to prevent. This
reuses `command.go` unchanged; only the trigger differs.

**Uncertain:** whether "consecutive restarts" should reset on the stability
threshold or on the first successfully spooled line. Stability threshold is
probably right — a process that comes up, emits one line, and dies is still
flapping — but this wants real traffic to settle.

## Per-line spooling vs coalescing

**The Phase 1 accumulation primitive is exactly what this needs**, which is the
main reason to be confident the schema isn't cornering us.

One spool file per line is not viable: a chatty stream would create thousands of
files, and a reconnecting session would eat thousands of separate events. Instead
the stream process appends into the loop's single pending slot with the same
`appendTickRuns` accumulation, batching on a short debounce:

- flush when **N lines** buffered (say 100), or
- flush after **T ms** of quiet (say 250ms), whichever comes first.

The drain loop then delivers one event containing many lines, `coalesced_count`
= number of accumulated batches. A session that is up sees near-real-time
delivery (250ms + the 2s poll); a session that is down sees one bounded catch-up
event.

Two changes Phase 1 would need:

1. A run's `Text` becomes a batch of lines rather than one command's stdout.
   The `runOutput` struct already supports this — no schema change.
2. The `[everloop] run N of M` header wording is wrong for a stream ("batch" or
   a timestamp range reads better). Cosmetic; the renderer would branch on mode.

## Backpressure

The Phase 1 bounds (20 runs / 64 KiB / 16 KiB per run) are calibrated for a
poll loop and are certainly wrong for a stream. A stream needs, in addition:

- **A hard line-rate ceiling.** Past some sustained rate (say 50 lines/sec over
  10s), stop spooling content and switch to a counter: `[everloop] stream
  "applog" emitting 400 lines/sec; 12,000 lines suppressed since 05:37:40Z`.
  A watch that floods the context window is a broken watch even when it is
  working exactly as configured.
- **A drop policy that matches the source.** Dropping the *oldest* is right for
  a diff stream (Phase 1's choice) but wrong for a log tail, where the newest
  lines are the interesting ones — arguably it should keep a head *and* a tail
  with a gap marker in between.
- **Spool-size awareness.** The stream process is the only writer, so it can
  cheaply check the pending file's size before appending and degrade to
  counting.

**Uncertain and worth prototyping:** whether the stream process should also
apply a token/byte budget per unit time rather than per event. A session that
is up drains every 2s, so the per-event bound never engages and a chatty stream
still floods — just in many small events instead of one big one. The per-event
bound alone does *not* solve the live-session flood.

## At-least-once: a genuine weakening, stated plainly

Phase 1's contract is end-to-end: the timer re-runs the command, the spool
claims and re-claims, so a crash anywhere redelivers. **Streaming cannot offer
that at the ingest edge.** If the stream process reads a line from its child
and dies before writing it to the spool, that line is gone — there is no
re-run, and the source has already moved on.

So the honest contract for stream loops is:

- **ingest (child → spool): at-most-once.** Lines can be lost to a crash or a
  restart gap.
- **delivery (spool → session): at-least-once, unchanged.** Once a line is in
  the spool it enjoys the same claim→ack redelivery and `event_id` idempotency
  key as everything else.

The debounce window is the size of the loss: a 250ms flush means at most 250ms
of lines are at risk. Do **not** market this as at-least-once. The README's
delivery-semantics section would need a sentence distinguishing the two, or
agents will assume a guarantee that is not there.

Partial mitigation for the one source where replay *is* possible: `tail -f` on a
regular file could checkpoint a byte offset and resume with `tail -c +N`,
recovering both crash loss and the restart gap. That is source-specific and
should not be generalised — it does not exist for inotify or a socket. Probably
worth doing as an opt-in `--resume-offset` for file tails only, if file tails
turn out to be the dominant use.

## `list`, `delete`, and instance namespacing

Namespacing is unchanged: `unitBase()` already produces
`everloop-<instance>-<loop>`, and a stream loop just occupies the `.service`
name without a sibling `.timer`. Loop names still cannot collide within an
instance, and `EVERLOOP_INSTANCE` still isolates spools. No new namespace.

What does need to change:

- `installUnits` / `removeUnits` branch on mode — a stream writes one unit and
  `enable --now`s the **service**, not a timer. `removeUnits` must stop the
  service (a `.timer`-only teardown would leave an orphaned process running,
  which is the sharpest failure mode in this whole design).
- `timerStatus` becomes mode-aware: for a stream, `ActiveState` plus `NRestarts`
  and uptime is the useful line — `active (up 4h, 3 restarts)`. The name should
  probably become `unitStatus`.
- `list` should visibly distinguish the two so an operator isn't guessing why a
  loop has no next-fire time.
- `delete` also removes the run-state file (Phase 1 already does this).

## launchd

**The premise that launchd has no `Restart=always` equivalent is wrong — it
does.** Verified on minime (macOS 26.5.1):

- `KeepAlive=<true/>` "unconditionally keep the job alive" — restart on exit,
  the direct analogue. (A dictionary form allows conditional restart, e.g.
  `SuccessfulExit=false`, closer to `Restart=on-failure`.)
- `ThrottleInterval` — "by default, jobs will not be spawned more than once
  every 10 seconds", overridable.

So the supervision itself ports cleanly. What does **not** port:

- **No exponential backoff.** `ThrottleInterval` is a flat floor, not
  `RestartSteps`. A flapping stream on macOS respawns every 10s forever. Since
  everloop is doing its own damped reporting anyway, the user-visible behaviour
  is close enough; the cost is wasted respawns, not noise.
- **No `StartLimitIntervalSec` equivalent**, and launchd never gives up — which
  is the behaviour we wanted on Linux anyway, so this is accidentally correct.
- **`RunAtLoad` must become true** for stream jobs (Phase 1 sets it false),
  otherwise nothing starts the process.
- launchd will still respawn a job whose binary is missing, hammering the
  10s throttle. Same on systemd; neither is worse.

Uncertainty: `KeepAlive` interacting with `bootout`/`bootstrap` during
`update` — the Phase 1 bootout-then-bootstrap dance should still apply, but it
is untested for a long-lived job and macOS is not where this would be developed
first.

## What Phase 1 locked in, and why it isn't a corner

Deliberate choices made in Phase 1 with this document in view:

- **`Command` is named for what it is, not for the timer.** A stream loop runs
  the same `Command` string under a different unit shape. No rename, no
  migration, no second field.
- **The mode discriminator is reserved, not spent.** Phase 2 adds
  `mode string \`json:"mode,omitempty"\`` where `""` == today's timer loop and
  `"stream"` is the new shape. Every existing loop file on disk is already a
  valid Phase 2 loop with a zero-value mode. No migration.
- **`runOutput` is a list of timestamped chunks**, not a single blob, so
  batched lines need no schema change.
- **`runState` (the damping memory) is keyed by loop, not by tick**, so restart
  damping reuses it as-is.

The **one** place Phase 1 constrains Phase 2: `loopSpec.apply` enforces
"exactly one of every/calendar", which a stream loop satisfies neither of.
Phase 2 relaxes that to "exactly one of every/calendar **unless** mode=stream,
which must have neither". That is a conditional in one function and touches no
stored data.

`Timeout` is meaningless for a stream (the process is *supposed* to run
forever) and should be rejected rather than silently ignored when mode=stream.
Phase 2's own knobs — `RestartSec`, flush debounce, rate ceiling — are new
fields, not overloads of existing ones.

## Open questions

Honest list of what this design does not settle:

1. **Live-session flood.** The per-event bound does not stop a chatty stream
   from flooding a session that is up and draining every 2s. Needs a rate-based
   budget, and the right numbers need real traffic.
2. **Drop policy per source.** Oldest-first is right for diffs, wrong for logs.
   Possibly a per-loop setting; possibly everloop should not be in this business
   and should just say "your command should be less chatty".
3. **Restart-streak reset condition** (stability threshold vs first line).
4. **Whether `tail -f` offset resume is worth the special case**, given it
   generalises to nothing else.
5. **Does a stream loop need `--message` at all?** A standing preamble on every
   batch is probably noise; on the first batch after a restart it is probably
   useful. Undecided.
6. **Ordering between a stream loop and poll loops in the same spool.** The
   drain sorts by `FirstAt`, so a long-accumulating stream event can appear
   "older" than poll ticks that happened after some of its lines. Probably
   fine, possibly confusing.
