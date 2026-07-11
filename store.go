package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Loop is a persistent recurring instruction backed by a systemd user timer.
type Loop struct {
	Name      string    `json:"name"`
	Message   string    `json:"message"`
	Every     string    `json:"every,omitempty"`    // interval, e.g. "1h30m", "2d"
	Calendar  string    `json:"calendar,omitempty"` // systemd OnCalendar expression
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// QueueMsg is one spooled message awaiting delivery to a session.
type QueueMsg struct {
	ID      string            `json:"id"`
	Kind    string            `json:"kind"` // "tick" or "message"
	Loop    string            `json:"loop,omitempty"`
	Content string            `json:"content"`
	Count   int               `json:"count"`
	Meta    map[string]string `json:"meta,omitempty"`
	FirstAt time.Time         `json:"first_at"`
	LastAt  time.Time         `json:"last_at"`
}

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,40}$`)

// instanceName isolates one everloop from another: several long-lived sessions
// (each an MCP `serve`) must not share a spool or they would steal each other's
// ticks. Set EVERLOOP_INSTANCE per session to give it its own data dir and
// systemd unit namespace. Empty = the default (single-user) instance.
func instanceName() string { return os.Getenv("EVERLOOP_INSTANCE") }

// checkInstance validates the instance name, which becomes both a filesystem
// path component and part of a systemd unit name.
func checkInstance() error {
	inst := instanceName()
	if inst == "" || nameRe.MatchString(inst) {
		return nil
	}
	return fmt.Errorf("invalid EVERLOOP_INSTANCE %q: must match %s", inst, nameRe)
}

func dataDir() string {
	if d := os.Getenv("EVERLOOP_DATA_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, ".local", "share", "everloop")
	if inst := instanceName(); inst != "" {
		return filepath.Join(base, inst)
	}
	return base
}

func loopsDir() string { return filepath.Join(dataDir(), "loops") }
func queueDir() string { return filepath.Join(dataDir(), "queue") }

func ensureDirs() error {
	for _, d := range []string{loopsDir(), queueDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func validName(name string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("invalid loop name %q: must match %s", name, nameRe)
	}
	return nil
}

// withQueueLock serializes all queue mutations across processes (tick vs serve).
func withQueueLock(fn func() error) error {
	if err := ensureDirs(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dataDir(), "queue.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

func randomID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// --- loop store -------------------------------------------------------------

func loopPath(name string) string { return filepath.Join(loopsDir(), name+".json") }

func loadLoop(name string) (*Loop, error) {
	data, err := os.ReadFile(loopPath(name))
	if err != nil {
		return nil, err
	}
	var l Loop
	if err := json.Unmarshal(data, &l); err != nil {
		return nil, err
	}
	return &l, nil
}

func saveLoop(l *Loop) error {
	if err := ensureDirs(); err != nil {
		return err
	}
	return writeJSON(loopPath(l.Name), l)
}

func listLoops() ([]*Loop, error) {
	if err := ensureDirs(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(loopsDir())
	if err != nil {
		return nil, err
	}
	var loops []*Loop
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		l, err := loadLoop(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			continue
		}
		loops = append(loops, l)
	}
	sort.Slice(loops, func(i, j int) bool { return loops[i].Name < loops[j].Name })
	return loops, nil
}

// --- queue ------------------------------------------------------------------

// enqueueTick records a loop firing. At most one pending tick per loop:
// repeat firings bump Count and refresh the message, so an outage can never
// flood the session.
func enqueueTick(name string) error {
	l, err := loadLoop(name)
	if err != nil {
		return fmt.Errorf("loop %q: %w", name, err)
	}
	if !l.Enabled {
		return nil
	}
	return withQueueLock(func() error {
		path := filepath.Join(queueDir(), "tick-"+name+".json")
		now := time.Now().UTC()
		msg := QueueMsg{ID: randomID(), Kind: "tick", Loop: name, Content: l.Message, Count: 1, FirstAt: now, LastAt: now}
		if data, err := os.ReadFile(path); err == nil {
			var prev QueueMsg
			if json.Unmarshal(data, &prev) == nil {
				msg.ID = prev.ID
				msg.Count = prev.Count + 1
				msg.FirstAt = prev.FirstAt
			}
		}
		return writeJSON(path, &msg)
	})
}

// enqueueMessage spools an ad-hoc message (from `everloop send` or the
// send_message tool). Never coalesced.
func enqueueMessage(content string, meta map[string]string) error {
	return withQueueLock(func() error {
		now := time.Now().UTC()
		msg := QueueMsg{ID: randomID(), Kind: "message", Content: content, Count: 1, Meta: meta, FirstAt: now, LastAt: now}
		return writeJSON(filepath.Join(queueDir(), "msg-"+now.Format("20060102T150405")+"-"+msg.ID+".json"), &msg)
	})
}

// claimPending atomically claims every spooled message for delivery by
// renaming it to claimed-*. A tick that fires after the claim starts a fresh
// file, so no coalesce increment is ever lost. Claimed files left behind by a
// crash are re-claimed on the next call (at-least-once delivery).
func claimPending() ([]claimed, error) {
	var out []claimed
	err := withQueueLock(func() error {
		entries, err := os.ReadDir(queueDir())
		if err != nil {
			return err
		}
		for _, e := range entries {
			name := e.Name()
			isNew := strings.HasPrefix(name, "tick-") || strings.HasPrefix(name, "msg-")
			isOrphan := strings.HasPrefix(name, "claimed-")
			if !isNew && !isOrphan {
				continue
			}
			path := filepath.Join(queueDir(), name)
			if isNew {
				dst := filepath.Join(queueDir(), "claimed-"+randomID()+".json")
				if err := os.Rename(path, dst); err != nil {
					continue
				}
				path = dst
			}
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var msg QueueMsg
			if err := json.Unmarshal(data, &msg); err != nil {
				os.Remove(path) // corrupt; drop it
				continue
			}
			out = append(out, claimed{msg: msg, path: path})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].msg.FirstAt.Before(out[j].msg.FirstAt) })
	return out, err
}

type claimed struct {
	msg  QueueMsg
	path string
}

// ack deletes a claimed message after it has been written to the session.
func (c claimed) ack() { os.Remove(c.path) }
