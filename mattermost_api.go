package main

// Mattermost's REST API — the transport a drained message travels on when the
// agent on the other end is woken by a mattermost-agents listener.
//
// WHY THIS SINK POSTS AS SOMEBODY ELSE. A listener polls its own account's
// channels and deliberately never delivers a post that account wrote
// (mattermost-agents src/agent/ingest.ts: `if (post.user_id ===
// policy.selfUserId) return 'self'`), because otherwise every message an agent
// sent would wake it again. everloop therefore cannot post as the agent it is
// waking: it posts as a separate, clearly labelled automation identity
// (`everloop`, provisioned by mattermost-agents' operator CLI) and the
// recipient's own listener picks the post up and wakes the session. That is
// also why a config pointing this sink at the sending profile's own identity is
// refused at startup rather than accepted — it would drop every firing in
// silence, which is the one failure mode this transport has that the socket
// transports do not.
//
// WHY INLINE HTTP AND NOT THE mattermost-agents CLI. The CLI can do this
// (`bun src/agent/cli.ts --config P dm --user-id U --message M --request-id R`)
// and it was the first choice, since a sink should not reimplement what the
// project it borrows an identity from already does. Three things decided
// against it:
//
//   - The whole operation is two POSTs — `/channels/direct` (idempotent for a
//     pair) and `/posts` — against a documented REST API. Go's stdlib has the
//     client; the CLI path is a bun process, a TypeScript checkout and
//     node_modules that must be installed and current on all five boxes, plus
//     a `secret` subprocess of its own, per firing.
//   - Failure classification. This sink's callers depend on the same
//     distinction the herdr sink stopped exec'ing a CLI to get: Mattermost
//     answers a refusal with a JSON `id` (`api.context.permissions.app_error`,
//     `store.sql_user.missing_account.const`) which travels into the error as a
//     code, while a transport failure carries none. The CLI collapses both into
//     exit 1.
//   - No subprocess on the delivery path, which is what herdr_socket.go bought
//     deliberately: an envelope carrying `$(...)`, quotes, backticks or
//     newlines travels as one literal JSON string, never on a child's argv.
//
// What is NOT reimplemented is the credential contract: the operator points
// this sink at a mattermost-agents PROFILE, and the token is resolved from it
// exactly the way that project resolves it (src/agent/config.ts resolveToken) —
// the environment variable the profile names, then the `secret` CLI with the
// name it names. everloop never holds a server URL or a token of its own.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	// mattermostHTTPTimeout is the fallback bound when the sink passes none.
	mattermostHTTPTimeout = 15 * time.Second

	// mattermostMaxPostBytes is the server's MaxPostSize (16383 on
	// mattermost.orangecountyai.com, and the product default). The server
	// counts characters and this bounds bytes, so it is the stricter of the
	// two. Over it the post is refused permanently: the drain loop would leave
	// the message claimed and retry it every poll forever, and because
	// drainLoop preserves order it would block every message behind it.
	// everloop's own event bound is 64 KiB of content (maxEventBytes), so a
	// chatty watch loop crosses this line as a matter of course.
	mattermostMaxPostBytes = 16383

	// mattermostClipMargin covers clip()'s truncation marker, whose length
	// varies with the digit count of the dropped byte count: 45 bytes of fixed
	// text plus at most 20 digits.
	mattermostClipMargin = 80

	// mattermostIDLen is the width of every Mattermost id (26 base32 chars).
	mattermostIDLen = 26
)

// mattermostPostResult separates "the post landed" from "why not". Code is set
// only when Mattermost answered with a structured error id; a bare transport
// error leaves it empty, and that difference is what tells a misconfigured
// recipient apart from a server that could not be reached.
type mattermostPostResult struct {
	OK        bool
	PostID    string
	ChannelID string
	Code      string
	Error     string
}

// mattermostDest is the agent a firing is delivered to, by name or by id.
// Exactly one form is populated — parseMattermostTarget refuses anything else
// before serving. It is a comparable struct because the driver caches the
// resolved direct channel under it.
type mattermostDest struct {
	Username string
	UserID   string
}

func (d mattermostDest) String() string {
	if d.Username != "" {
		return "@" + d.Username
	}
	return "user " + d.UserID
}

// isMattermostID reports whether s is a Mattermost id: 26 characters of the
// lowercase base32 alphabet the server generates. Checking the shape offline is
// what lets a typo'd recipient be refused at startup instead of on the first
// firing, hours later.
func isMattermostID(s string) bool {
	if len(s) != mattermostIDLen {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

// mattermostDriver is the surface the sink needs. It exists so a test can
// substitute a driver, and so the sink never learns the wire format.
type mattermostDriver interface {
	post(ctx context.Context, dest mattermostDest, message string, timeout time.Duration) mattermostPostResult
}

type mattermostOptions struct {
	// ProfilePath is the sending identity's mattermost-agents profile.
	// Connection names which of its connections to use, and may be empty when
	// the profile holds exactly one.
	ProfilePath string
	Connection  string
	// BaseURL and Token bypass the profile entirely. Only a test sets them:
	// production always goes through a profile so the credential contract stays
	// mattermost-agents'.
	BaseURL string
	Token   string
	Log     func(string)
}

type mattermostHTTPDriver struct {
	baseURL     string
	tokenEnv    string
	tokenSecret string
	// selfUserID is the profile's expectedUserId when it pins one; otherwise it
	// is resolved from /users/me on first use. A DM needs it, and so does the
	// startup refusal to deliver to our own identity.
	selfUserID string
	client     *http.Client
	log        func(string)

	mu       sync.Mutex
	token    string
	channels map[mattermostDest]string
}

var _ mattermostDriver = (*mattermostHTTPDriver)(nil)

// mattermostConnection is the part of a mattermost-agents profile everloop
// reads. Unknown fields (channelIds, watchMemberships, pollIntervalMs, ...)
// belong to the listener and are ignored on purpose: this is one reader of a
// file that project owns, not a second definition of it.
type mattermostConnection struct {
	ID             string `json:"id"`
	URL            string `json:"url"`
	TokenEnv       string `json:"tokenEnv"`
	TokenSecret    string `json:"tokenSecret"`
	ExpectedUserID string `json:"expectedUserId"`
}

type mattermostProfile struct {
	Connections []mattermostConnection `json:"connections"`
}

// loadMattermostConnection resolves which server+identity pair to send as. The
// connection is required whenever the profile leaves room for doubt, mirroring
// mattermost-agents' own soleConnectionId: picking one of two identities by
// position is how a firing gets posted from the wrong account.
func loadMattermostConnection(path, id string) (mattermostConnection, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return mattermostConnection{}, fmt.Errorf("read mattermost profile %s: %w", path, err)
	}
	var profile mattermostProfile
	if err := json.Unmarshal(raw, &profile); err != nil {
		return mattermostConnection{}, fmt.Errorf("mattermost profile %s is not valid JSON: %w", path, err)
	}
	if len(profile.Connections) == 0 {
		return mattermostConnection{}, fmt.Errorf("mattermost profile %s has no connections", path)
	}
	var conn mattermostConnection
	if id == "" {
		if len(profile.Connections) != 1 {
			return mattermostConnection{}, fmt.Errorf("mattermost profile %s has %d connections (%s); set MATTERMOST_CONNECTION to the one you mean",
				path, len(profile.Connections), strings.Join(mattermostConnectionIDs(profile), ", "))
		}
		conn = profile.Connections[0]
	} else {
		found := false
		for _, candidate := range profile.Connections {
			if candidate.ID == id {
				conn, found = candidate, true
				break
			}
		}
		if !found {
			return mattermostConnection{}, fmt.Errorf("mattermost profile %s has no connection %q; configured: %s",
				path, id, strings.Join(mattermostConnectionIDs(profile), ", "))
		}
	}
	if conn.URL == "" {
		return mattermostConnection{}, fmt.Errorf("mattermost profile %s: connection %q has no url", path, conn.ID)
	}
	if conn.TokenEnv == "" && conn.TokenSecret == "" {
		return mattermostConnection{}, fmt.Errorf("mattermost profile %s: connection %q names neither tokenEnv nor tokenSecret, so no credential can be resolved", path, conn.ID)
	}
	return conn, nil
}

func mattermostConnectionIDs(profile mattermostProfile) []string {
	ids := make([]string, 0, len(profile.Connections))
	for _, conn := range profile.Connections {
		ids = append(ids, conn.ID)
	}
	return ids
}

// newMattermostHTTPDriver resolves configuration without contacting the server
// — the same rule the socket drivers follow. A sink is constructed at serve
// startup, when the network (or the secret store behind the token) may not be
// answering yet, and that must not stop the MCP tools from coming up. The first
// delivery resolves the credential and the destination; both are then cached.
func newMattermostHTTPDriver(opts mattermostOptions) (*mattermostHTTPDriver, error) {
	logf := opts.Log
	if logf == nil {
		// stderr, via the standard logger: stdout is the MCP JSON-RPC channel
		// and a stray line there corrupts the session.
		logf = func(message string) { log.Print(message) }
	}
	driver := &mattermostHTTPDriver{
		client:   &http.Client{},
		log:      logf,
		channels: map[mattermostDest]string{},
	}
	if opts.BaseURL != "" {
		driver.baseURL = strings.TrimRight(opts.BaseURL, "/")
		driver.token = opts.Token
		return driver, nil
	}
	if opts.ProfilePath == "" {
		return nil, fmt.Errorf("no mattermost profile and no explicit base url")
	}
	conn, err := loadMattermostConnection(opts.ProfilePath, opts.Connection)
	if err != nil {
		return nil, err
	}
	driver.baseURL = strings.TrimRight(conn.URL, "/")
	driver.tokenEnv = conn.TokenEnv
	driver.tokenSecret = conn.TokenSecret
	driver.selfUserID = conn.ExpectedUserID
	return driver, nil
}

// refuseSelfDelivery is the startup guard for the one misconfiguration this
// transport can have that fails silently rather than loudly: a listener never
// delivers a post its own account wrote, so a firing addressed to the sending
// identity is accepted by the server, posted, and never seen by anybody. Only
// the user-id form can be checked offline; a username is checked when it
// resolves, and a channel cannot be checked at all.
func (d *mattermostHTTPDriver) refuseSelfDelivery(dest mattermostDest) error {
	if d.selfUserID == "" || dest.UserID == "" || dest.UserID != d.selfUserID {
		return nil
	}
	return fmt.Errorf("CHANNEL_SINK=mattermost is addressed to %s, which is the sending profile's own identity: a mattermost-agents listener never delivers a post its own account wrote, so every firing would be dropped in silence. Point MATTERMOST_TARGET at the recipient agent and MATTERMOST_PROFILE at a separate automation identity", dest.UserID)
}

// resolveToken mirrors mattermost-agents' own resolver: the environment
// variable the profile names first, then the `secret` CLI with the name it
// names. The value is returned, never logged, and never reaches a command line
// — `secret NAME` takes only the name.
func (d *mattermostHTTPDriver) resolveToken(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.token != "" {
		return d.token, nil
	}
	if d.tokenEnv != "" {
		if token := os.Getenv(d.tokenEnv); token != "" {
			d.token = token
			return token, nil
		}
	}
	if d.tokenSecret == "" {
		return "", fmt.Errorf("$%s is unset and the profile names no tokenSecret fallback", d.tokenEnv)
	}
	cmd := exec.CommandContext(ctx, "secret", d.tokenSecret)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	token := strings.TrimSpace(stdout.String())
	if err != nil || token == "" {
		detail := strings.TrimSpace(stderr.String())
		if len(detail) > 300 {
			detail = detail[:300]
		}
		return "", fmt.Errorf("secret %s did not resolve ($%s is also unset): %v: %s", d.tokenSecret, d.tokenEnv, err, detail)
	}
	d.token = token
	return token, nil
}

// mattermostAPIError is a refusal Mattermost described (Code is its `id`) or a
// transport failure (Code empty). Callers turn that difference into the
// difference between a configuration fault and an unknown delivery outcome.
type mattermostAPIError struct {
	Status  int
	Code    string
	Message string
}

func (e *mattermostAPIError) Error() string { return e.Message }

func (d *mattermostHTTPDriver) call(ctx context.Context, method, path string, in, out any) *mattermostAPIError {
	var body *bytes.Reader
	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return &mattermostAPIError{Message: fmt.Sprintf("encode %s %s: %v", method, path, err)}
		}
		body = bytes.NewReader(encoded)
	} else {
		body = bytes.NewReader(nil)
	}
	token, err := d.resolveToken(ctx)
	if err != nil {
		return &mattermostAPIError{Message: fmt.Sprintf("mattermost credential: %v", err)}
	}
	url := d.baseURL + "/api/v4" + path
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return &mattermostAPIError{Message: fmt.Sprintf("%s %s: %v", method, url, err)}
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("User-Agent", "everloop/"+version)
	if in != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(request)
	if err != nil {
		return &mattermostAPIError{Message: fmt.Sprintf("mattermost is not reachable (%s %s): %v", method, url, err)}
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		var refusal struct {
			ID      string `json:"id"`
			Message string `json:"message"`
		}
		_ = json.NewDecoder(response.Body).Decode(&refusal)
		message := refusal.Message
		if message == "" {
			message = fmt.Sprintf("%s %s: HTTP %d", method, path, response.StatusCode)
		}
		return &mattermostAPIError{Status: response.StatusCode, Code: refusal.ID, Message: message}
	}
	if out != nil {
		if err := json.NewDecoder(response.Body).Decode(out); err != nil {
			return &mattermostAPIError{Message: fmt.Sprintf("decode %s %s: %v", method, path, err)}
		}
	}
	return nil
}

// selfID is the sending account's user id: the profile's pin when it has one,
// otherwise whoever the credential authenticates as.
func (d *mattermostHTTPDriver) selfID(ctx context.Context) (string, *mattermostAPIError) {
	d.mu.Lock()
	known := d.selfUserID
	d.mu.Unlock()
	if known != "" {
		return known, nil
	}
	var me struct {
		ID string `json:"id"`
	}
	if apiErr := d.call(ctx, http.MethodGet, "/users/me", nil, &me); apiErr != nil {
		return "", apiErr
	}
	if me.ID == "" {
		return "", &mattermostAPIError{Message: "/users/me returned no user id"}
	}
	d.mu.Lock()
	d.selfUserID = me.ID
	d.mu.Unlock()
	return me.ID, nil
}

// channelFor turns a destination into the channel to post in, and remembers it:
// a username costs a lookup and a DM costs a direct-channel create, and neither
// changes for the life of the process. The direct-channel create is idempotent
// for a pair, so a cache miss after a failure is safe to redo.
func (d *mattermostHTTPDriver) channelFor(ctx context.Context, dest mattermostDest) (string, *mattermostAPIError) {
	d.mu.Lock()
	cached := d.channels[dest]
	d.mu.Unlock()
	if cached != "" {
		return cached, nil
	}

	self, apiErr := d.selfID(ctx)
	if apiErr != nil {
		return "", apiErr
	}
	recipient := dest.UserID
	if recipient == "" {
		// By exact username, never a search hit: a fuzzy match delivers a
		// firing into a stranger's inbox.
		var user struct {
			ID string `json:"id"`
		}
		if apiErr := d.call(ctx, http.MethodGet, "/users/username/"+dest.Username, nil, &user); apiErr != nil {
			return "", apiErr
		}
		if user.ID == "" {
			return "", &mattermostAPIError{Message: fmt.Sprintf("no user id for @%s", dest.Username)}
		}
		recipient = user.ID
	}
	if recipient == self {
		// The same silent-drop refusal newSink makes offline, for the form that
		// can only be resolved against the server.
		return "", &mattermostAPIError{
			Code:    "everloop_self_delivery",
			Message: fmt.Sprintf("%s is the sending profile's own identity; a mattermost-agents listener never delivers a post its own account wrote, so this firing would be dropped in silence", dest),
		}
	}
	var channel struct {
		ID string `json:"id"`
	}
	if apiErr := d.call(ctx, http.MethodPost, "/channels/direct", []string{self, recipient}, &channel); apiErr != nil {
		return "", apiErr
	}
	if channel.ID == "" {
		return "", &mattermostAPIError{Message: fmt.Sprintf("no direct channel id for %s", dest)}
	}
	d.mu.Lock()
	d.channels[dest] = channel.ID
	d.mu.Unlock()
	return channel.ID, nil
}

func (d *mattermostHTTPDriver) post(ctx context.Context, dest mattermostDest, message string, timeout time.Duration) mattermostPostResult {
	if timeout <= 0 {
		timeout = mattermostHTTPTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	channelID, apiErr := d.channelFor(ctx, dest)
	if apiErr != nil {
		return mattermostPostResult{Code: apiErr.Code, Error: apiErr.Message}
	}
	// No pending_post_id: Mattermost's own duplicate handling for it is a
	// short-lived cache with its own error, and everloop already absorbs a
	// redelivery by design — ticks coalesce, so a repeated firing carries the
	// catch-up count rather than doubling the work.
	var created struct {
		ID        string `json:"id"`
		ChannelID string `json:"channel_id"`
	}
	if apiErr := d.call(ctx, http.MethodPost, "/posts", map[string]string{"channel_id": channelID, "message": message}, &created); apiErr != nil {
		return mattermostPostResult{Code: apiErr.Code, Error: apiErr.Message}
	}
	return mattermostPostResult{OK: true, PostID: created.ID, ChannelID: created.ChannelID}
}

// mattermostBody renders the channel envelope and keeps it inside the server's
// post bound. The clip lands on the *content*, never on the rendered envelope:
// cutting the wrapper would hand the agent an unterminated tag. clip() is
// everloop's existing truncation — the same one maxRunBytes and maxEventBytes
// use — so a dropped tail stays visible in the body rather than vanishing.
func mattermostBody(source, content string, meta map[string]string) string {
	body := envelope(source, content, meta)
	if len(body) <= mattermostMaxPostBytes {
		return body
	}
	budget := len(content) - (len(body) - mattermostMaxPostBytes) - mattermostClipMargin
	if budget < 0 {
		budget = 0
	}
	return envelope(source, clip(content, budget), meta)
}
