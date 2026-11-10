// Package gate implements the alpha KB and task service. Git's main ref is the
// sole publication point: content, task state and retry receipts advance together.
package gate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

const Version = "v1.0.0-beta.1"
const MaxBody = 1 << 20

var slug = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,95}$`)
var identityID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9:_-]{0,127}$`)

type Identity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type Task struct {
	ID          string    `json:"id"`
	Project     string    `json:"project,omitempty"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	Responsible string    `json:"responsible"`
	Status      string    `json:"status"`
	RunID       string    `json:"run_id,omitempty"`
	UpdatedBy   string    `json:"updated_by"`
	UpdatedAt   time.Time `json:"updated_at"`
}
type Result struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
	ETag   string `json:"etag,omitempty"`
}
type Receipt struct {
	Hash   string `json:"hash"`
	Result Result `json:"result"`
}
type state struct {
	Identities    []Identity         `json:"identities"`
	Tasks         []Task             `json:"tasks"`
	Receipts      map[string]Receipt `json:"receipts"`
	Events        []Event            `json:"events,omitempty"`
	EventSequence uint64             `json:"event_sequence,omitempty"`
}
type Store struct {
	mu       sync.Mutex
	root     string
	lock     *os.File
	watchers map[chan struct{}]bool
}

func tag(b []byte) string         { x := sha256.Sum256(b); return `"` + hex.EncodeToString(x[:]) + `"` }
func jsonText(v any) string       { b, _ := json.Marshal(v); return string(b) }
func fail(n int, s string) Result { return Result{Status: n, Body: s} }
func kbPath(p string) (string, error) {
	p = strings.TrimSuffix(p, ".md")
	if len(p) > 512 {
		return "", errors.New("path too long")
	}
	for _, part := range strings.Split(p, "/") {
		if !slug.MatchString(part) {
			return "", errors.New("use lowercase path segments with hyphens or underscores")
		}
	}
	return "kb/" + p + ".md", nil
}
func (s *Store) git(input []byte, env []string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"--git-dir=" + s.root}, args...)...)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Gate", "GIT_AUTHOR_EMAIL=gate@localhost", "GIT_COMMITTER_NAME=Gate", "GIT_COMMITTER_EMAIL=gate@localhost")
	cmd.Env = append(cmd.Env, env...)
	var errout bytes.Buffer
	cmd.Stderr = &errout
	b, e := cmd.Output()
	if e != nil {
		return nil, fmt.Errorf("git %s: %w: %s", args[0], e, errout.String())
	}
	return b, nil
}
func Open(root string) (*Store, error) { return OpenWithOwner(root, "human:owner") }

func OpenWithOwner(root, owner string) (*Store, error) {
	if !identityID.MatchString(owner) {
		return nil, errors.New("invalid owner identity")
	}
	root, e := filepath.Abs(root)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	resolved, e := filepath.EvalSymlinks(root)
	if e != nil || resolved != root {
		return nil, errors.New("repository path must not contain symlinks")
	}
	f, e := os.OpenFile(root+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, e
	}
	s := &Store{root: root, lock: f, watchers: map[chan struct{}]bool{}}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()
	if _, e = os.Stat(filepath.Join(root, "HEAD")); os.IsNotExist(e) {
		cmd := exec.Command("git", "init", "--bare", "--initial-branch=main", root)
		if b, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("git init: %s: %w", b, err)
		}
	}
	if _, e = s.git(nil, nil, "rev-parse", "--verify", "refs/heads/main"); e != nil {
		st := state{Identities: []Identity{{ID: owner, Name: owner}}, Tasks: []Task{}, Receipts: map[string]Receipt{}}
		if e = s.publish("", map[string][]byte{".gate/state.json": []byte(jsonText(st))}, "Initialize Gate"); e != nil {
			return nil, e
		}
	}
	if _, e = s.load(); e != nil {
		return nil, e
	}
	ok = true
	return s, nil
}
func (s *Store) Close() {
	if s.lock != nil {
		syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		s.lock.Close()
		s.lock = nil
	}
}
func (s *Store) head() string {
	b, e := s.git(nil, nil, "rev-parse", "refs/heads/main")
	if e != nil {
		panic(e)
	}
	return strings.TrimSpace(string(b))
}
func (s *Store) read(head, path string) ([]byte, error) {
	return s.git(nil, nil, "show", head+":"+path)
}
func (s *Store) load() (state, error) {
	var st state
	b, e := s.read(s.head(), ".gate/state.json")
	if e == nil {
		e = json.Unmarshal(b, &st)
	}
	return st, e
}
func (s *Store) publish(parent string, files map[string][]byte, message string) error {
	index, e := os.CreateTemp(filepath.Dir(s.root), "gate-index-")
	if e != nil {
		return e
	}
	name := index.Name()
	index.Close()
	os.Remove(name)
	defer os.Remove(name)
	defer os.Remove(name + ".lock")
	env := []string{"GIT_INDEX_FILE=" + name}
	if parent != "" {
		_, e = s.git(nil, env, "read-tree", parent)
	} else {
		_, e = s.git(nil, env, "read-tree", "--empty")
	}
	if e != nil {
		return e
	}
	for path, b := range files {
		oid, e := s.git(b, nil, "hash-object", "-w", "--stdin")
		if e != nil {
			return e
		}
		if _, e = s.git(nil, env, "update-index", "--add", "--cacheinfo", "100644", strings.TrimSpace(string(oid)), path); e != nil {
			return e
		}
	}
	tree, e := s.git(nil, env, "write-tree")
	if e != nil {
		return e
	}
	args := []string{"commit-tree", strings.TrimSpace(string(tree)), "-m", message}
	if parent != "" {
		args = append(args, "-p", parent)
	}
	commit, e := s.git(nil, nil, args...)
	if e != nil {
		return e
	}
	if parent == "" {
		parent = strings.Repeat("0", 40)
	}
	_, e = s.git(nil, nil, "update-ref", "refs/heads/main", strings.TrimSpace(string(commit)), parent)
	return e
}
func (s *Store) HasIdentity(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, e := s.load()
	if e != nil {
		return false
	}
	for _, x := range st.Identities {
		if x.ID == id {
			return true
		}
	}
	return false
}

// Read returns current, committed data. Caller-supplied paths never become refs or commands.
func (s *Store) Read(path string, query map[string]string) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	head := s.head()
	st, e := s.load()
	if e != nil {
		return fail(500, "cannot read committed state")
	}
	switch {
	case path == "/identities":
		return Result{Status: 200, Body: jsonText(st.Identities)}
	case path == "/tasks":
		tasks := []Task{}
		for _, t := range st.Tasks {
			if query["project"] != "" && t.Project != query["project"] || query["responsible"] != "" && t.Responsible != query["responsible"] || query["status"] != "" && t.Status != query["status"] {
				continue
			}
			tasks = append(tasks, t)
		}
		return Result{Status: 200, Body: jsonText(tasks)}
	case strings.HasPrefix(path, "/tasks/"):
		for _, t := range st.Tasks {
			if t.ID == strings.TrimPrefix(path, "/tasks/") {
				b := []byte(jsonText(t))
				return Result{Status: 200, Body: string(b), ETag: tag(b)}
			}
		}
		return fail(404, "task not found")
	case path == "/kb":
		raw, e := s.git(nil, nil, "ls-tree", "-r", "--name-only", head, "--", "kb/")
		if e != nil {
			return fail(500, "cannot list KB")
		}
		items := []map[string]string{}
		for _, p := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			if !strings.HasPrefix(p, "kb/") {
				continue
			}
			id := strings.TrimSuffix(strings.TrimPrefix(p, "kb/"), ".md")
			if prefix := query["prefix"]; prefix != "" && !strings.HasPrefix(id, prefix) {
				continue
			}
			if q := query["q"]; q != "" {
				b, e := s.read(head, p)
				if e != nil {
					return fail(500, "cannot search KB")
				}
				if !strings.Contains(strings.ToLower(id+"\n"+string(b)), strings.ToLower(q)) {
					continue
				}
			}
			items = append(items, map[string]string{"path": id, "href": "/kb/" + id})
			if len(items) >= 200 {
				break
			}
		}
		return Result{Status: 200, Body: jsonText(map[string]any{"items": items, "limit": 200})}
	case strings.HasPrefix(path, "/kb/"):
		p, e := kbPath(strings.TrimPrefix(path, "/kb/"))
		if e != nil {
			return fail(400, e.Error())
		}
		b, e := s.read(head, p)
		if e != nil {
			return fail(404, "KB note not found")
		}
		return Result{Status: 200, Body: string(b), ETag: tag(b)}
	default:
		return fail(404, "not found")
	}
}

// Write commits successful changes and their operation receipts in a single Git ref update.
func (s *Store) Write(actor, method, path, body, expected, key string) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(body) > MaxBody {
		return fail(413, "body too large")
	}
	if key == "" || len(key) > 128 {
		return fail(400, "Idempotency-Key required, at most 128 bytes")
	}
	st, e := s.load()
	if e != nil {
		return fail(500, "cannot read state")
	}
	known := false
	for _, i := range st.Identities {
		known = known || i.ID == actor
	}
	if !known {
		return fail(401, "unknown identity")
	}
	hash := tag([]byte(jsonText([]string{method, path, body, expected})))
	receiptKey := actor + "/" + key
	if old, ok := st.Receipts[receiptKey]; ok {
		if old.Hash != hash {
			return fail(409, "operation key reused with different request")
		}
		return old.Result
	}
	head := s.head()
	files := map[string][]byte{}
	var out Result
	var event *Event
	switch {
	case method == "PUT" && strings.HasPrefix(path, "/kb/"):
		p, e := kbPath(strings.TrimPrefix(path, "/kb/"))
		if e != nil {
			return fail(400, e.Error())
		}
		old, err := s.read(head, p)
		if expected == "" {
			return fail(428, "If-Match or If-None-Match: * required")
		}
		if expected == "*" {
			if err == nil {
				return fail(412, "note already exists")
			}
		} else {
			if err != nil || tag(old) != expected {
				return fail(412, "note changed; reread before replacing")
			}
		}
		files[p] = []byte(body)
		out = Result{Status: 204, ETag: tag([]byte(body))}
	case method == "POST" && path == "/identities":
		var i Identity
		if decode(body, &i) != nil || !identityID.MatchString(i.ID) || strings.TrimSpace(i.Name) == "" || len(i.Name) > 200 {
			return fail(400, "valid identity id and name required")
		}
		for _, old := range st.Identities {
			if old.ID == i.ID {
				return fail(409, "identity already exists")
			}
		}
		st.Identities = append(st.Identities, i)
		out = Result{Status: 201, Body: jsonText(i)}
	case method == "POST" && strings.HasPrefix(path, "/tasks/") && strings.HasSuffix(path, "/claim"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/tasks/"), "/claim")
		var input struct {
			RunID string `json:"run_id"`
		}
		if decode(body, &input) != nil || !slug.MatchString(input.RunID) {
			return fail(400, "valid run_id required")
		}
		pos := -1
		for n, t := range st.Tasks {
			if t.ID == id {
				pos = n
				break
			}
		}
		if pos < 0 {
			return fail(404, "task not found")
		}
		t := st.Tasks[pos]
		if expected == "" {
			return fail(428, "If-Match required")
		}
		if tag([]byte(jsonText(t))) != expected {
			return fail(412, "task changed; reread")
		}
		if t.Responsible != actor {
			return fail(409, "task is assigned to another identity")
		}
		if t.Status != "open" && t.Status != "ready" {
			return fail(409, "task is not ready for pickup")
		}
		t.Status = "in_progress"
		t.RunID = input.RunID
		t.UpdatedBy = actor
		t.UpdatedAt = time.Now().UTC()
		st.Tasks[pos] = t
		out = Result{Status: 200, Body: jsonText(t), ETag: tag([]byte(jsonText(t)))}
		event = &Event{Type: "task.claimed", TaskID: t.ID, Responsible: t.Responsible, Revision: out.ETag, Status: t.Status, At: t.UpdatedAt}
	case method == "POST" && path == "/tasks" || method == "PUT" && strings.HasPrefix(path, "/tasks/"):
		var input struct {
			Project     string `json:"project"`
			Title       string `json:"title"`
			Description string `json:"description"`
			Responsible string `json:"responsible"`
			Status      string `json:"status"`
		}
		if decode(body, &input) != nil {
			return fail(400, "invalid task JSON")
		}
		if strings.TrimSpace(input.Title) == "" || len(input.Title) > 1000 || len(input.Description) > 65536 || input.Project != "" && !slug.MatchString(input.Project) {
			return fail(400, "invalid title, description or project ID")
		}
		if input.Status == "" {
			input.Status = "open"
		}
		if len(input.Status) > 64 || strings.TrimSpace(input.Status) == "" {
			return fail(400, "invalid status")
		}
		found := false
		for _, i := range st.Identities {
			found = found || i.ID == input.Responsible
		}
		if !found {
			return fail(400, "responsible must name an existing identity")
		}
		id := hex.EncodeToString(shaBytes([]byte(receiptKey)))[:24]
		pos := -1
		if method == "PUT" {
			id = strings.TrimPrefix(path, "/tasks/")
			for n, t := range st.Tasks {
				if t.ID == id {
					pos = n
					break
				}
			}
			if pos < 0 {
				return fail(404, "task not found")
			}
			if expected == "" {
				return fail(428, "If-Match required")
			}
			if tag([]byte(jsonText(st.Tasks[pos]))) != expected {
				return fail(412, "task changed; reread")
			}
		}
		previous := ""
		runID := ""
		if pos >= 0 {
			previous = st.Tasks[pos].Responsible
			runID = st.Tasks[pos].RunID
		}
		if input.Status == "open" || input.Status == "ready" || previous != input.Responsible {
			runID = ""
		}
		t := Task{RunID: runID, ID: id, Project: input.Project, Title: input.Title, Description: input.Description, Responsible: input.Responsible, Status: input.Status, UpdatedBy: actor, UpdatedAt: time.Now().UTC()}
		if pos < 0 {
			st.Tasks = append(st.Tasks, t)
		} else {
			st.Tasks[pos] = t
		}
		code := 201
		if method == "PUT" {
			code = 200
		}
		out = Result{Status: code, Body: jsonText(t), ETag: tag([]byte(jsonText(t)))}
		kind := "task.updated"
		if method == "POST" {
			kind = "task.created"
		}
		event = &Event{Type: kind, TaskID: t.ID, Responsible: t.Responsible, PreviousResponsible: previous, Revision: out.ETag, Status: t.Status, At: t.UpdatedAt}
	default:
		return fail(405, "unsupported operation")
	}
	if event != nil {
		st.EventSequence++
		event.ID = st.EventSequence
		st.Events = append(st.Events, *event)
	}
	st.Receipts[receiptKey] = Receipt{Hash: hash, Result: out}
	files[".gate/state.json"] = []byte(jsonText(st))
	if e = s.publish(head, files, actor+" "+method+" "+path); e != nil {
		return fail(500, "publication uncertain; retry with the same operation key")
	}
	if event != nil {
		s.signal()
	}
	return out
}
func shaBytes(b []byte) []byte { x := sha256.Sum256(b); return x[:] }
func decode(body string, v any) error {
	d := json.NewDecoder(strings.NewReader(body))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("one JSON value required")
	}
	return nil
}
