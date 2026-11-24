// Package core implements the isolated Gate resource store. It never executes tasks.
package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"
)

const BlobGrace = 24 * time.Hour

type Error struct {
	Status   int
	Message  string
	Resource map[string]any
}

func (e *Error) Error() string              { return e.Message }
func fail(status int, message string) error { return &Error{Status: status, Message: message} }

type Event struct {
	Type             string `json:"type"`
	ID               string `json:"id"`
	Assignee         string `json:"assignee"`
	PreviousAssignee string `json:"previous_assignee,omitempty"`
}
type subscriber struct {
	identity string
	ch       chan Event
}
type Store struct {
	uploads               sync.Map // active private staging files; never execution state
	db                    *sql.DB
	dir                   string
	lock                  *os.File
	mu                    sync.Mutex   // one writer; reads use independent WAL connections
	files                 sync.RWMutex // protects in-flight reads from blob reclamation
	subsMu                sync.Mutex
	subs                  map[*subscriber]bool
	closed                bool
	fault                 func(string) error // package-local fault injection, never a wire option
	historyFailures       int64              // process-local diagnostics, protected by mu
	historyLastFailure    string
	historyProcessStarted string
}

var idRE = regexp.MustCompile(`^gate:(tx|bn|tk|tg):[a-z0-9]+(?:-[a-z0-9]+)*$`)
var identityRE = regexp.MustCompile(`^(agent|human|hermes):[a-z0-9]+(?:[._-][a-z0-9]+)*$`)

func validID(id string) bool { return len(id) <= 112 && idRE.MatchString(id) }
func kind(id string) string {
	if validID(id) {
		return strings.Split(id, ":")[1]
	}
	return ""
}
func hash(b []byte) string   { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func stateHash(v any) string { b, _ := json.Marshal(v); return hash(b) }
func now() string            { return time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z") }

const schema = `
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS identities(id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS resources(id TEXT PRIMARY KEY, type TEXT NOT NULL CHECK(type IN ('tx','bn','tk','tg')), created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS revisions(resource_id TEXT NOT NULL REFERENCES resources(id), number INTEGER NOT NULL CHECK(number>0), hash TEXT, mime TEXT NOT NULL, sealed INTEGER NOT NULL CHECK(sealed IN(0,1)), created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT, PRIMARY KEY(resource_id,number), CHECK((hash IS NULL)=(deleted_at IS NOT NULL)));
CREATE TABLE IF NOT EXISTS tags(id TEXT PRIMARY KEY REFERENCES resources(id), key TEXT NOT NULL, value TEXT NOT NULL, description_id TEXT REFERENCES resources(id));
CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY REFERENCES resources(id), creator TEXT NOT NULL REFERENCES identities(id), assignee TEXT NOT NULL REFERENCES identities(id), modified_by TEXT NOT NULL REFERENCES identities(id), status TEXT NOT NULL CHECK(status IN('triage','ready','in_progress','blocked','done')), priority INTEGER NOT NULL, body_hash TEXT NOT NULL, verification TEXT NOT NULL, execution_revision INTEGER NOT NULL, execution_updated_at TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS immutable_creator BEFORE UPDATE OF creator ON tasks WHEN old.creator!=new.creator BEGIN SELECT RAISE(ABORT,'immutable creator'); END;
CREATE TABLE IF NOT EXISTS sessions(seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES tasks(id), ref TEXT NOT NULL, registered_at TEXT NOT NULL, UNIQUE(task_id,ref));
CREATE TABLE IF NOT EXISTS associations(seq INTEGER PRIMARY KEY AUTOINCREMENT, tag_id TEXT NOT NULL REFERENCES tags(id), target_id TEXT NOT NULL REFERENCES resources(id), revision INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, deleted_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS active_edge ON associations(tag_id,target_id,revision) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS edges_by_target ON associations(target_id,revision,deleted_at,tag_id);
CREATE INDEX IF NOT EXISTS edges_by_tag ON associations(tag_id,deleted_at,target_id,revision);
CREATE TABLE IF NOT EXISTS activity(seq INTEGER PRIMARY KEY AUTOINCREMENT, resource_id TEXT NOT NULL REFERENCES resources(id), at TEXT NOT NULL, kind TEXT NOT NULL, actor TEXT NOT NULL REFERENCES identities(id));
CREATE INDEX IF NOT EXISTS activity_window ON activity(at,resource_id);
CREATE TABLE IF NOT EXISTS operations(actor TEXT NOT NULL REFERENCES identities(id), operation_id TEXT NOT NULL, fingerprint TEXT NOT NULL, task_id TEXT NOT NULL REFERENCES tasks(id), PRIMARY KEY(actor,operation_id));
CREATE TABLE IF NOT EXISTS notification_history(seq INTEGER PRIMARY KEY AUTOINCREMENT, emitted_at TEXT NOT NULL, type TEXT NOT NULL, task_id TEXT NOT NULL, assignee TEXT NOT NULL, previous_assignee TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS notification_history_time ON notification_history(emitted_at);
CREATE INDEX IF NOT EXISTS notification_history_assignee ON notification_history(assignee,seq);
CREATE INDEX IF NOT EXISTS notification_history_previous ON notification_history(previous_assignee,seq);
CREATE TABLE IF NOT EXISTS notification_history_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), started_at TEXT NOT NULL);
`

func New(dir string, identities []string) (*Store, error) {
	abs, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(abs, 0700); e != nil {
		return nil, e
	}
	resolved, e := filepath.EvalSymlinks(abs)
	if e != nil || resolved != abs {
		return nil, errors.New("store directory cannot contain symlinks")
	}
	fi, e := os.Stat(abs)
	if e != nil || fi.Mode().Perm()&0077 != 0 {
		return nil, errors.New("store directory must be private (0700)")
	}
	lf, e := os.OpenFile(filepath.Join(abs, "store.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lf.Close()
		return nil, errors.New("store already open by another process")
	}
	cleanup := func() { syscall.Flock(int(lf.Fd()), syscall.LOCK_UN); lf.Close() }
	for _, n := range []string{"metadata.db", "metadata.db-wal", "metadata.db-shm"} {
		p := filepath.Join(abs, n)
		st, e := os.Lstat(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			cleanup()
			return nil, errors.New("database files must be private regular files")
		}
	}
	f, e := os.OpenFile(filepath.Join(abs, "metadata.db"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		cleanup()
		return nil, e
	}
	f.Close()
	blobs := filepath.Join(abs, "objects")
	if e = os.MkdirAll(blobs, 0700); e != nil {
		cleanup()
		return nil, e
	}
	st, e := os.Lstat(blobs)
	if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		cleanup()
		return nil, errors.New("object directory must be private")
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(abs, "metadata.db")}
	db, e := sql.Open("sqlite", u.String()+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)")
	if e != nil {
		cleanup()
		return nil, e
	}
	db.SetMaxOpenConns(8)
	bad := func(err error) (*Store, error) { db.Close(); cleanup(); return nil, err }
	// Refuse pre-candidate schemas before any schema/identity/history write.
	// No migration/backfill: the historical alpha remains a separate baseline.
	var taskColumns, deliveryColumns int
	if e = db.QueryRow("SELECT count(*), coalesce(sum(name IN ('execution_revision','execution_updated_at')),0) FROM pragma_table_info('tasks')").Scan(&taskColumns, &deliveryColumns); e != nil {
		return bad(e)
	}
	if taskColumns != 0 && deliveryColumns != 2 {
		return bad(errors.New("incompatible pre-native task schema: fresh disposable store required; no data migration is performed"))
	}
	if _, e = db.Exec(schema); e != nil {
		return bad(e)
	}
	var integrity string
	if e = db.QueryRow("PRAGMA integrity_check").Scan(&integrity); e != nil || integrity != "ok" {
		return bad(errors.New("SQLite integrity check failed"))
	}
	tx, e := db.Begin()
	if e != nil {
		return bad(e)
	}
	for _, id := range identities {
		if !identityRE.MatchString(id) || len(id) > 128 {
			tx.Rollback()
			return bad(errors.New("malformed identity"))
		}
		if _, e = tx.Exec("INSERT OR IGNORE INTO identities VALUES(?)", id); e != nil {
			tx.Rollback()
			return bad(e)
		}
	}
	if e = tx.Commit(); e != nil {
		return bad(e)
	}
	s := &Store{db: db, dir: abs, lock: lf, subs: map[*subscriber]bool{}}
	s.historyProcessStarted = now()
	if e = s.initNotificationHistory(); e != nil {
		return bad(e)
	}
	rows, e := db.Query("SELECT hash FROM revisions WHERE hash IS NOT NULL UNION SELECT body_hash FROM tasks")
	if e != nil {
		return bad(e)
	}
	var hashes []string
	for rows.Next() {
		var h string
		if e = rows.Scan(&h); e != nil {
			rows.Close()
			return bad(e)
		}
		hashes = append(hashes, h)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return bad(e)
	}
	for _, h := range hashes {
		if e = s.verifyBlob(h); e != nil {
			return bad(fmt.Errorf("referenced object unavailable: %w", e))
		}
	}
	return s, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files.Lock()
	defer s.files.Unlock()
	s.subsMu.Lock()
	if s.closed {
		s.subsMu.Unlock()
		return nil
	}
	s.closed = true
	for sub := range s.subs {
		close(sub.ch)
		delete(s.subs, sub)
	}
	s.subsMu.Unlock()
	e := s.db.Close()
	syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	s.lock.Close()
	return e
}
func (s *Store) Subscribe(identity string) (<-chan Event, func()) {
	sub := &subscriber{identity: identity, ch: make(chan Event, 32)}
	s.subsMu.Lock()
	if s.closed {
		close(sub.ch)
	} else {
		s.subs[sub] = true
	}
	s.subsMu.Unlock()
	return sub.ch, func() {
		s.subsMu.Lock()
		defer s.subsMu.Unlock()
		if s.subs[sub] {
			delete(s.subs, sub)
			close(sub.ch)
		}
	}
}
func (s *Store) notify(e Event) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for sub := range s.subs {
		if sub.identity != e.Assignee && sub.identity != e.PreviousAssignee {
			continue
		}
		// Compact queued invalidations by ID; consumers fetch current state.
		pending := []Event{}
	drain:
		for {
			select {
			case old := <-sub.ch:
				if old.ID != e.ID {
					pending = append(pending, old)
				}
			default:
				break drain
			}
		}
		for _, old := range pending {
			sub.ch <- old
		}
		select {
		case sub.ch <- e:
		default:
			close(sub.ch)
			delete(s.subs, sub)
		}
	}
}
func (s *Store) inject(stage string) error {
	if s.fault != nil {
		return s.fault(stage)
	}
	return nil
}

// Collect removes only unreferenced object files while all store readers/writers
// are excluded. No backup retention or forensic erasure guarantee is implied.
func (s *Store) Collect() error { s.mu.Lock(); defer s.mu.Unlock(); return s.collect() }
func (s *Store) collect() error {
	s.files.Lock()
	defer s.files.Unlock()
	rows, e := s.db.Query("SELECT hash FROM revisions WHERE hash IS NOT NULL UNION SELECT body_hash FROM tasks")
	if e != nil {
		return e
	}
	refs := map[string]bool{}
	for rows.Next() {
		var h string
		if e = rows.Scan(&h); e != nil {
			rows.Close()
			return e
		}
		refs[h] = true
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	files, e := os.ReadDir(filepath.Join(s.dir, "objects"))
	if e != nil {
		return e
	}
	for _, f := range files {
		info, err := f.Info()
		if err != nil {
			return err
		}
		if !refs[f.Name()] && info.ModTime().Before(time.Now().Add(-BlobGrace)) {
			if e = os.Remove(filepath.Join(s.dir, "objects", f.Name())); e != nil {
				return e
			}
		}
	}
	incoming, e := os.ReadDir(s.dir)
	if e != nil {
		return e
	}
	for _, entry := range incoming {
		if !strings.HasPrefix(entry.Name(), ".incoming-") {
			continue
		}
		name := filepath.Join(s.dir, entry.Name())
		if _, active := s.uploads.Load(name); active {
			continue
		}
		info, e := entry.Info()
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if info.ModTime().Before(time.Now().Add(-BlobGrace)) {
			if e = os.Remove(name); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
	}
	return nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func registered(ctx context.Context, q queryer, actor string) error {
	var id string
	if e := q.QueryRowContext(ctx, "SELECT id FROM identities WHERE id=?", actor).Scan(&id); e != nil {
		return fail(403, "unknown identity")
	}
	return nil
}
func text(a map[string]any, k string) string { v, _ := a[k].(string); return v }
func integer(a map[string]any, k string) (int, error) {
	v, ok := a[k]
	if !ok {
		return 0, nil
	}
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		if n != float64(int(n)) {
			return 0, fail(400, "integer required: "+k)
		}
		return int(n), nil
	case json.Number:
		i, e := n.Int64()
		if e == nil {
			return int(i), nil
		}
	}
	return 0, fail(400, "integer required: "+k)
}
func strlist(a map[string]any, k string) ([]string, error) {
	v, ok := a[k]
	if !ok || v == nil {
		return []string{}, nil
	}
	if ss, ok := v.([]string); ok {
		return ss, nil
	}
	vv, ok := v.([]any)
	if !ok {
		return nil, fail(400, "string array required: "+k)
	}
	ss := []string{}
	for _, v := range vv {
		s, ok := v.(string)
		if !ok {
			return nil, fail(400, "string array required: "+k)
		}
		ss = append(ss, s)
	}
	return ss, nil
}
func known(a map[string]any, fields string) error {
	allowed := map[string]bool{}
	for _, s := range strings.Fields(fields) {
		allowed[s] = true
	}
	for k := range a {
		if !allowed[k] {
			return fail(400, "unknown field: "+k)
		}
	}
	return nil
}

// ValidateResourceID shares the literal ID/type grammar with transport-only
// preparation operations; it does not resolve or create a resource.
func ValidateResourceID(id string, t string) error { return requireID(id, t) }

func requireID(id string, t string) error {
	if !validID(id) {
		return fail(400, "malformed resource ID")
	}
	if t != "" && kind(id) != t {
		return fail(400, "wrong resource type")
	}
	return nil
}
func content(a map[string]any, binary bool) ([]byte, error) {
	var b []byte
	if binary {
		var ok bool
		b, ok = a["content"].([]byte)
		if !ok {
			return nil, fail(400, "binary bytes required through octet-stream")
		}
	} else {
		v, ok := a["body"].(string)
		if !ok {
			return nil, fail(400, "body must be a string")
		}
		if !utf8.ValidString(v) {
			return nil, fail(400, "invalid UTF-8")
		}
		b = []byte(v)
	}
	return b, nil
}
func getMeta(ctx context.Context, q queryer, id string, rev int) (map[string]any, error) {
	if e := requireID(id, ""); e != nil {
		return nil, e
	}
	m := map[string]any{"id": id}
	var typ, created, updated string
	e := q.QueryRowContext(ctx, "SELECT type,created_at,updated_at FROM resources WHERE id=?", id).Scan(&typ, &created, &updated)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, fail(404, "unknown resource")
	}
	if e != nil {
		return nil, e
	}
	m["type"] = typ
	m["created_at"] = created
	m["updated_at"] = updated
	if typ == "tx" || typ == "bn" {
		if rev == 0 {
			if e = q.QueryRowContext(ctx, "SELECT MAX(number) FROM revisions WHERE resource_id=?", id).Scan(&rev); e != nil {
				return nil, e
			}
		}
		var h, del sql.NullString
		var mime, rc, ru string
		var sealed int
		e = q.QueryRowContext(ctx, "SELECT hash,mime,sealed,created_at,updated_at,deleted_at FROM revisions WHERE resource_id=? AND number=?", id, rev).Scan(&h, &mime, &sealed, &rc, &ru, &del)
		if errors.Is(e, sql.ErrNoRows) {
			return nil, fail(404, "unknown revision")
		}
		if e != nil {
			return nil, e
		}
		m["revision"] = rev
		m["content_hash"] = nil
		if h.Valid {
			m["content_hash"] = h.String
		}
		m["mime"] = mime
		m["sealed"] = sealed == 1
		m["revision_created_at"] = rc
		m["revision_updated_at"] = ru
		m["deleted_at"] = nil
		if del.Valid {
			m["deleted_at"] = del.String
			m["token"] = stateHash([]any{id, rev, del.String, mime})
		} else {
			m["token"] = h.String
		}
	} else if typ == "tg" {
		if rev != 0 {
			return nil, fail(400, "unversioned resource")
		}
		var key, value string
		var desc sql.NullString
		if e = q.QueryRowContext(ctx, "SELECT key,value,description_id FROM tags WHERE id=?", id).Scan(&key, &value, &desc); e != nil {
			return nil, e
		}
		m["key"] = key
		m["value"] = value
		m["description_id"] = nil
		if desc.Valid {
			m["description_id"] = desc.String
		}
		m["token"] = stateHash(m)
	} else {
		if rev != 0 {
			return nil, fail(400, "unversioned resource")
		}
		var creator, assignee, editor, status, h, v, executionAt string
		var priority, executionRevision int
		if e = q.QueryRowContext(ctx, "SELECT creator,assignee,modified_by,status,priority,body_hash,verification,execution_revision,execution_updated_at FROM tasks WHERE id=?", id).Scan(&creator, &assignee, &editor, &status, &priority, &h, &v, &executionRevision, &executionAt); e != nil {
			return nil, e
		}
		m["creator"] = creator
		m["assignee"] = assignee
		m["modified_by"] = editor
		m["status"] = status
		m["priority"] = priority
		m["body_hash"] = h
		m["execution_revision"] = executionRevision
		m["execution_updated_at"] = executionAt
		var verification any
		if e = json.Unmarshal([]byte(v), &verification); e != nil {
			return nil, e
		}
		m["verification"] = verification
		var ref, at string
		e = q.QueryRowContext(ctx, "SELECT ref,registered_at FROM sessions WHERE task_id=? ORDER BY seq DESC LIMIT 1", id).Scan(&ref, &at)
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return nil, e
		}
		m["latest_session"] = nil
		if e == nil {
			m["latest_session"] = map[string]any{"ref": ref, "registered_at": at}
		}
		rows, e := q.QueryContext(ctx, "SELECT ref,registered_at FROM sessions WHERE task_id=? ORDER BY seq", id)
		if e != nil {
			return nil, e
		}
		sessions := [][2]string{}
		for rows.Next() {
			var ref, at string
			if e = rows.Scan(&ref, &at); e != nil {
				rows.Close()
				return nil, e
			}
			sessions = append(sessions, [2]string{ref, at})
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		m["sessions_hash"] = stateHash(sessions)
		m["token"] = stateHash(m)
	}
	return m, nil
}
func gone(m map[string]any) error {
	if m["deleted_at"] != nil {
		return &Error{Status: 410, Message: "revision deleted", Resource: m}
	}
	return nil
}
func (s *Store) read(ctx context.Context, q queryer, id string, rev int, body bool) (map[string]any, error) {
	m, e := getMeta(ctx, q, id, rev)
	if e != nil {
		return nil, e
	}
	if e = gone(m); e != nil {
		return nil, e
	}
	if !body {
		return m, nil
	}
	switch m["type"] {
	case "tx", "tk":
		h, _ := m["content_hash"].(string)
		if m["type"] == "tk" {
			h = m["body_hash"].(string)
		}
		b, e := s.getBlob(h)
		if e != nil {
			return nil, e
		}
		m["body"] = string(b)
	case "tg":
		m["description_state"] = "none"
		if id, ok := m["description_id"].(string); ok {
			d, e := getMeta(ctx, q, id, 0)
			if e != nil || gone(d) != nil {
				m["description_state"] = "unavailable"
			} else {
				h := d["content_hash"].(string)
				b, e := s.getBlob(h)
				if e != nil {
					m["description_state"] = "unavailable"
				} else {
					m["description_state"] = "available"
					m["description"] = map[string]any{"id": id, "revision": d["revision"], "token": d["token"], "body": string(b)}
				}
			}
		}
	}
	return m, nil
}

var readOps = map[string]bool{"read_resource": true, "read_revision": true, "list_revisions_for_resource": true, "list_tags_for_resource": true, "list_resources_for_tag": true, "list_tags": true, "list_tasks": true, "list_activity": true, "list_sessions_for_task": true, "list_verification_candidates": true}

func (s *Store) Call(ctx context.Context, actor, op string, a map[string]any) (map[string]any, error) {
	if a == nil {
		a = map[string]any{}
	}
	if e := registered(ctx, s.db, actor); e != nil {
		return nil, e
	}
	if op == "list_notification_history" {
		return s.notificationHistory(ctx, actor, a)
	}
	if readOps[op] {
		s.files.RLock()
		defer s.files.RUnlock()
		tx, e := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if e != nil {
			return nil, e
		}
		defer tx.Rollback()
		return s.readCall(ctx, tx, op, a)
	}
	var upload *pendingBlob
	if op == "create_binary" || op == "update_binary" {
		var reader io.Reader
		switch v := a["content"].(type) {
		case []byte:
			reader = bytes.NewReader(v)
		case io.Reader:
			reader = v
		default:
			return nil, fail(400, "binary bytes or stream required")
		}
		pending, e := s.stageBlob(ctx, reader)
		if e != nil {
			return nil, e
		}
		defer s.cleanupPending(pending)
		upload = &pending
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("store closed")
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}

	// Durable object publication precedes even the metadata transaction. Invalid
	// or conflicting requests can leave only grace-protected unreferenced bytes.
	stage := op == "create_text" || op == "update_text" || op == "create_binary" || op == "update_binary"
	if op == "create_task" || op == "update_task" {
		_, stage = a["body"]
	}
	if stage {
		if op == "create_binary" || op == "update_binary" {
			h, e := s.publishBlob(*upload)
			if e != nil {
				return nil, e
			}
			// Never mutate the caller's map or accept a caller-supplied digest.
			cloned := make(map[string]any, len(a))
			for k, v := range a {
				cloned[k] = v
			}
			cloned["content"] = stagedBlob{h}
			a = cloned
		} else {
			b, e := content(a, false)
			if e != nil {
				return nil, e
			}
			if e = s.putBlob(b); e != nil {
				return nil, e
			}
		}
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	m, event, e := s.write(ctx, tx, actor, op, a)
	if e != nil {
		return nil, e
	}
	if e = s.inject("before_commit"); e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	if e = s.inject("after_commit"); e != nil {
		// Test fault models an ambiguous committed response. Reset subscriptions
		// rather than permit silent loss between commit and notification.
		s.subsMu.Lock()
		for sub := range s.subs {
			close(sub.ch)
			delete(s.subs, sub)
		}
		s.subsMu.Unlock()
		return nil, e
	}
	if event != nil {
		// Separate post-commit diagnostic write: never a mutation receipt/outbox.
		// Failure cannot undo the committed task or suppress its invalidation.
		if err := s.recordNotification(*event, time.Now().UTC()); err != nil {
			s.historyFailures++
			s.historyLastFailure = now()
		}
		s.notify(*event)
	}
	// Reclamation is explicit, grace-period protected, and never on a write path.
	return m, nil
}
func touch(ctx context.Context, tx *sql.Tx, id, at, k, actor string) error {
	if _, e := tx.ExecContext(ctx, "UPDATE resources SET updated_at=? WHERE id=?", at, id); e != nil {
		return e
	}
	_, e := tx.ExecContext(ctx, "INSERT INTO activity(resource_id,at,kind,actor) VALUES(?,?,?,?)", id, at, k, actor)
	return e
}
func insertResource(ctx context.Context, tx *sql.Tx, id, t, at string) error {
	var n int
	if e := tx.QueryRowContext(ctx, "SELECT count(*) FROM resources WHERE id=?", id).Scan(&n); e != nil {
		return e
	}
	if n != 0 {
		return fail(409, "ID already reserved")
	}
	_, e := tx.ExecContext(ctx, "INSERT INTO resources VALUES(?,?,?,?)", id, t, at, at)
	return e
}
func precondition(a, m map[string]any) error {
	rev, e := integer(a, "expected_revision")
	if e != nil {
		return e
	}
	h := text(a, "expected_hash")
	if rev == 0 || h == "" {
		return fail(428, "expected_revision and expected_hash required")
	}
	if rev != m["revision"] || h != m["token"] {
		return fail(412, "stale revision or hash")
	}
	return nil
}
func tokenCheck(a, m map[string]any) error {
	t := text(a, "expected_token")
	if t == "" {
		return fail(428, "expected_token required")
	}
	if t != m["token"] {
		return fail(412, "stale token")
	}
	return nil
}
func validateStringFields(a map[string]any, fields string) error {
	for _, k := range strings.Fields(fields) {
		if v, ok := a[k]; ok {
			if _, ok = v.(string); !ok {
				return fail(400, "string required: "+k)
			}
		}
	}
	return nil
}
func statusOK(s string) bool {
	return s == "triage" || s == "ready" || s == "in_progress" || s == "blocked" || s == "done"
}
func attach(ctx context.Context, tx *sql.Tx, id, tag string, rev int, at string) error {
	if e := requireID(tag, "tg"); e != nil {
		return e
	}
	var n int
	if e := tx.QueryRowContext(ctx, "SELECT count(*) FROM tags WHERE id=?", tag).Scan(&n); e != nil {
		return e
	}
	if n == 0 {
		return fail(404, "unknown tag")
	}
	_, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO associations(tag_id,target_id,revision,created_at) VALUES(?,?,?,?)", tag, id, rev, at)
	return e
}
func initialTags(ctx context.Context, tx *sql.Tx, a map[string]any, id string, rev int, at string) error {
	tags, e := strlist(a, "tags")
	if e != nil {
		return e
	}
	if len(tags) > 64 {
		return fail(400, "too many tags")
	}
	for _, tag := range tags {
		if e = attach(ctx, tx, id, tag, rev, at); e != nil {
			return e
		}
	}
	return nil
}
func validateDescription(ctx context.Context, tx *sql.Tx, a map[string]any) (any, error) {
	v := a["description_id"]
	if v == nil {
		return nil, nil
	}
	id, ok := v.(string)
	if !ok {
		return nil, fail(400, "description must be text ID or null")
	}
	if e := requireID(id, "tx"); e != nil {
		return nil, e
	}
	if _, e := getMeta(ctx, tx, id, 0); e != nil {
		return nil, e
	}
	return id, nil
}
func validateVerification(ctx context.Context, tx *sql.Tx, v any) (string, error) {
	if v == nil {
		return "[]", nil
	}
	b, e := json.Marshal(v)
	if e != nil {
		return "", fail(400, "invalid verification")
	}
	var refs []map[string]any
	if e = json.Unmarshal(b, &refs); e != nil {
		return "", fail(400, "verification must be reference array")
	}
	if len(refs) > 64 {
		return "", fail(400, "too many verification references")
	}
	seen := map[string]bool{}
	for _, r := range refs {
		if e = known(r, "id revision"); e != nil {
			return "", e
		}
		id := text(r, "id")
		if e = requireID(id, "tx"); e != nil {
			return "", e
		}
		rev, e := integer(r, "revision")
		if e != nil || rev < 0 {
			return "", fail(400, "invalid verification revision")
		}
		if _, ok := r["revision"]; ok && rev == 0 {
			return "", fail(400, "revision must be positive")
		}
		m, e := getMeta(ctx, tx, id, rev)
		if e != nil {
			return "", fail(400, "verification instruction unavailable: "+id)
		}
		if e = gone(m); e != nil {
			return "", fail(400, "verification instruction deleted: "+id)
		}
		if rev != 0 && m["sealed"] != true {
			return "", fail(409, "verification pin must be sealed")
		}
		key := fmt.Sprint(id, ":", rev)
		if seen[key] {
			return "", fail(400, "duplicate verification reference")
		}
		seen[key] = true
	}
	if refs == nil {
		return "[]", nil
	}
	b, _ = json.Marshal(refs)
	return string(b), nil
}
func (s *Store) write(ctx context.Context, tx *sql.Tx, actor, op string, a map[string]any) (map[string]any, *Event, error) {
	at := now()
	id := text(a, "id")
	ret := func(m map[string]any, e error) (map[string]any, *Event, error) { return m, nil, e }
	if e := validateStringFields(a, "id resource_id tag_id body mime key value assignee status operation_id add_session expected_hash expected_token"); e != nil {
		return ret(nil, e)
	}
	switch op {
	case "create_text", "create_binary":
		binary := op == "create_binary"
		fields := "id body tags"
		typ := "tx"
		mime := "text/markdown; charset=utf-8"
		if binary {
			fields = "id content mime tags"
			typ = "bn"
			mime = text(a, "mime")
			if mime == "" {
				return ret(nil, fail(400, "mime required"))
			}
		}
		if e := known(a, fields); e != nil {
			return ret(nil, e)
		}
		if e := requireID(id, typ); e != nil {
			return ret(nil, e)
		}
		h, e := resourceContentHash(a, binary)
		if e != nil {
			return ret(nil, e)
		}
		if e = insertResource(ctx, tx, id, typ, at); e != nil {
			return ret(nil, e)
		}
		_, e = tx.ExecContext(ctx, "INSERT INTO revisions VALUES(?,1,?,?,0,?,?,NULL)", id, h, mime, at, at)
		if e != nil {
			return ret(nil, e)
		}
		if e = initialTags(ctx, tx, a, id, 1, at); e != nil {
			return ret(nil, e)
		}
		if e = s.inject("row"); e != nil {
			return ret(nil, e)
		}
		if e = touch(ctx, tx, id, at, "create", actor); e != nil {
			return ret(nil, e)
		}
		m, e := s.read(ctx, tx, id, 0, true)
		return ret(m, e)
	case "update_text", "update_binary":
		binary := op == "update_binary"
		fields := "id body expected_revision expected_hash"
		typ := "tx"
		if binary {
			fields = "id content mime expected_revision expected_hash"
			typ = "bn"
		}
		if e := known(a, fields); e != nil {
			return ret(nil, e)
		}
		if e := requireID(id, typ); e != nil {
			return ret(nil, e)
		}
		m, e := getMeta(ctx, tx, id, 0)
		if e != nil {
			return ret(nil, e)
		}
		if e = precondition(a, m); e != nil {
			return ret(nil, e)
		}
		h, e := resourceContentHash(a, binary)
		if e != nil {
			return ret(nil, e)
		}
		mime := m["mime"]
		if binary {
			mime = text(a, "mime")
			if mime == "" {
				return ret(nil, fail(400, "mime required"))
			}
		}
		_, e = tx.ExecContext(ctx, "UPDATE revisions SET hash=?,mime=?,updated_at=?,deleted_at=NULL WHERE resource_id=? AND number=?", h, mime, at, id, m["revision"])
		if e != nil {
			return ret(nil, e)
		}
		if e = s.inject("row"); e != nil {
			return ret(nil, e)
		}
		if e = touch(ctx, tx, id, at, "edit", actor); e != nil {
			return ret(nil, e)
		}
		m, e = s.read(ctx, tx, id, 0, true)
		return ret(m, e)
	case "seal_revision":
		if e := known(a, "id expected_revision expected_hash"); e != nil {
			return ret(nil, e)
		}
		if kind(id) != "tx" && kind(id) != "bn" {
			if e := requireID(id, ""); e != nil {
				return ret(nil, e)
			}
			return ret(nil, fail(400, "only content can be sealed"))
		}
		m, e := getMeta(ctx, tx, id, 0)
		if e != nil {
			return ret(nil, e)
		}
		if e = precondition(a, m); e != nil {
			return ret(nil, e)
		}
		if m["deleted_at"] != nil {
			return ret(nil, fail(409, "restore current before sealing"))
		}
		r := m["revision"].(int)
		if _, e = tx.ExecContext(ctx, "UPDATE revisions SET sealed=1 WHERE resource_id=? AND number=?", id, r); e != nil {
			return ret(nil, e)
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO revisions VALUES(?,?,?,?,0,?,?,NULL)", id, r+1, m["content_hash"], m["mime"], at, at); e != nil {
			return ret(nil, e)
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO associations(tag_id,target_id,revision,created_at) SELECT tag_id,target_id,?,? FROM associations WHERE target_id=? AND revision=? AND deleted_at IS NULL", r+1, at, id, r); e != nil {
			return ret(nil, e)
		}
		if e = touch(ctx, tx, id, at, "seal", actor); e != nil {
			return ret(nil, e)
		}
		m, e = s.read(ctx, tx, id, 0, true)
		return ret(m, e)
	case "delete_resource":
		if e := known(a, "id revision expected_revision expected_hash"); e != nil {
			return ret(nil, e)
		}
		if e := requireID(id, ""); e != nil {
			return ret(nil, e)
		}
		if kind(id) != "tx" && kind(id) != "bn" {
			return ret(nil, fail(405, "task/tag deletion unsupported; no cascade"))
		}
		rev, e := integer(a, "revision")
		if e != nil || rev < 0 {
			return ret(nil, fail(400, "invalid revision"))
		}
		m, e := getMeta(ctx, tx, id, rev)
		if e != nil {
			return ret(nil, e)
		}
		if e = precondition(a, m); e != nil {
			return ret(nil, e)
		}
		if m["deleted_at"] != nil {
			return ret(m, nil)
		}
		if _, e = tx.ExecContext(ctx, "UPDATE revisions SET hash=NULL,deleted_at=?,updated_at=? WHERE resource_id=? AND number=?", at, at, id, m["revision"]); e != nil {
			return ret(nil, e)
		}
		if e = touch(ctx, tx, id, at, "delete", actor); e != nil {
			return ret(nil, e)
		}
		m, e = getMeta(ctx, tx, id, m["revision"].(int))
		return ret(m, e)
	case "create_tag":
		if e := known(a, "id key value description_id"); e != nil {
			return ret(nil, e)
		}
		if e := requireID(id, "tg"); e != nil {
			return ret(nil, e)
		}
		key, value := text(a, "key"), text(a, "value")
		if key == "" || len(key) > 128 || len(value) > 512 {
			return ret(nil, fail(400, "invalid tag key/value"))
		}
		desc, e := validateDescription(ctx, tx, a)
		if e != nil {
			return ret(nil, e)
		}
		if e = insertResource(ctx, tx, id, "tg", at); e != nil {
			return ret(nil, e)
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO tags VALUES(?,?,?,?)", id, key, value, desc); e != nil {
			return ret(nil, e)
		}
		if e = touch(ctx, tx, id, at, "create", actor); e != nil {
			return ret(nil, e)
		}
		m, e := s.read(ctx, tx, id, 0, true)
		return ret(m, e)
	case "update_tag":
		if e := known(a, "id expected_token key value description_id"); e != nil {
			return ret(nil, e)
		}
		if e := requireID(id, "tg"); e != nil {
			return ret(nil, e)
		}
		m, e := getMeta(ctx, tx, id, 0)
		if e != nil {
			return ret(nil, e)
		}
		if e = tokenCheck(a, m); e != nil {
			return ret(nil, e)
		}
		for _, k := range []string{"key", "value"} {
			if v, ok := a[k]; ok {
				m[k] = v
			}
		}
		key, value := m["key"].(string), m["value"].(string)
		if key == "" || len(key) > 128 || len(value) > 512 {
			return ret(nil, fail(400, "invalid tag key/value"))
		}
		desc := m["description_id"]
		if _, ok := a["description_id"]; ok {
			desc, e = validateDescription(ctx, tx, a)
			if e != nil {
				return ret(nil, e)
			}
		}
		if _, e = tx.ExecContext(ctx, "UPDATE tags SET key=?,value=?,description_id=? WHERE id=?", key, value, desc, id); e != nil {
			return ret(nil, e)
		}
		if e = touch(ctx, tx, id, at, "edit", actor); e != nil {
			return ret(nil, e)
		}
		m, e = s.read(ctx, tx, id, 0, true)
		return ret(m, e)
	case "tag_resource", "untag_resource":
		if e := known(a, "resource_id tag_id revision expected_revision expected_hash"); e != nil {
			return ret(nil, e)
		}
		id = text(a, "resource_id")
		tag := text(a, "tag_id")
		if e := requireID(tag, "tg"); e != nil {
			return ret(nil, e)
		}
		if _, e := getMeta(ctx, tx, tag, 0); e != nil {
			return ret(nil, e)
		}
		rev, e := integer(a, "revision")
		if e != nil || rev < 0 {
			return ret(nil, fail(400, "invalid revision"))
		}
		m, e := getMeta(ctx, tx, id, rev)
		if e != nil {
			return ret(nil, e)
		}
		if r, ok := m["revision"].(int); ok {
			var head int
			if e = tx.QueryRowContext(ctx, "SELECT MAX(number) FROM revisions WHERE resource_id=?", id).Scan(&head); e != nil {
				return ret(nil, e)
			}
			if r == head {
				if e = precondition(a, m); e != nil {
					return ret(nil, e)
				}
			}
			rev = r
		}
		var n int
		if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM associations WHERE tag_id=? AND target_id=? AND revision=? AND deleted_at IS NULL", tag, id, rev).Scan(&n); e != nil {
			return ret(nil, e)
		}
		changed := (op == "tag_resource" && n == 0) || (op == "untag_resource" && n != 0)
		if changed {
			if op == "tag_resource" {
				e = attach(ctx, tx, id, tag, rev, at)
			} else {
				_, e = tx.ExecContext(ctx, "UPDATE associations SET deleted_at=? WHERE tag_id=? AND target_id=? AND revision=? AND deleted_at IS NULL", at, tag, id, rev)
			}
			if e != nil {
				return ret(nil, e)
			}
			if e = touch(ctx, tx, id, at, op, actor); e != nil {
				return ret(nil, e)
			}
			if m["type"] == "tk" {
				if _, e = tx.ExecContext(ctx, "UPDATE tasks SET modified_by=? WHERE id=?", actor, id); e != nil {
					return ret(nil, e)
				}
				assignee := m["assignee"].(string)
				return map[string]any{"id": id, "revision": rev, "tag_id": tag, "attached": op == "tag_resource", "changed": true}, &Event{Type: "task.updated", ID: id, Assignee: assignee, PreviousAssignee: assignee}, nil
			}
		}
		return ret(map[string]any{"id": id, "revision": rev, "tag_id": tag, "attached": op == "tag_resource", "changed": changed}, nil)
	case "create_task":
		if e := known(a, "id operation_id assignee body status priority tags verification"); e != nil {
			return ret(nil, e)
		}
		operation := text(a, "operation_id")
		if operation == "" || len(operation) > 128 {
			return ret(nil, fail(400, "operation_id required (max 128 bytes)"))
		}
		fingerprint := stateHash(a)
		var oldfp, oldid string
		e := tx.QueryRowContext(ctx, "SELECT fingerprint,task_id FROM operations WHERE actor=? AND operation_id=?", actor, operation).Scan(&oldfp, &oldid)
		if e == nil {
			if oldfp != fingerprint {
				return ret(nil, fail(409, "operation ID reused with different request"))
			}
			m, e := s.read(ctx, tx, oldid, 0, true)
			return ret(m, e)
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return ret(nil, e)
		}
		if id == "" {
			id = "gate:tk:task-" + hash([]byte(actor + "\x00" + operation))[:24]
		}
		if e = requireID(id, "tk"); e != nil {
			return ret(nil, e)
		}
		assignee := text(a, "assignee")
		if assignee == "" {
			return ret(nil, fail(400, "assignee required"))
		}
		if e = registered(ctx, tx, assignee); e != nil {
			return ret(nil, e)
		}
		status := text(a, "status")
		if status == "" {
			status = "triage"
		}
		if !statusOK(status) {
			return ret(nil, fail(400, "unknown status"))
		}
		priority, e := integer(a, "priority")
		if e != nil {
			return ret(nil, e)
		}
		b, e := content(a, false)
		if e != nil {
			return ret(nil, e)
		}
		v, e := validateVerification(ctx, tx, a["verification"])
		if e != nil {
			return ret(nil, e)
		}
		if e = insertResource(ctx, tx, id, "tk", at); e != nil {
			return ret(nil, e)
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?)", id, actor, assignee, actor, status, priority, hash(b), v, 1, at); e != nil {
			return ret(nil, e)
		}
		if e = initialTags(ctx, tx, a, id, 0, at); e != nil {
			return ret(nil, e)
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO operations VALUES(?,?,?,?)", actor, operation, fingerprint, id); e != nil {
			return ret(nil, e)
		}
		if e = s.inject("row"); e != nil {
			return ret(nil, e)
		}
		if e = s.putBlob(b); e != nil {
			return ret(nil, e)
		}
		if e = touch(ctx, tx, id, at, "create", actor); e != nil {
			return ret(nil, e)
		}
		m, e := s.read(ctx, tx, id, 0, true)
		return m, &Event{Type: "task.updated", ID: id, Assignee: assignee}, e
	case "update_task":
		if e := known(a, "id expected_token assignee body status priority add_session"); e != nil {
			return ret(nil, e)
		}
		if e := requireID(id, "tk"); e != nil {
			return ret(nil, e)
		}
		m, e := getMeta(ctx, tx, id, 0)
		if e != nil {
			return ret(nil, e)
		}
		if e = tokenCheck(a, m); e != nil {
			return ret(nil, e)
		}
		previous := m["assignee"].(string)
		previousStatus, previousPriority := m["status"], m["priority"]
		for _, k := range []string{"assignee", "status", "priority"} {
			if v, ok := a[k]; ok {
				m[k] = v
			}
		}
		assignee := m["assignee"].(string)
		if assignee == "" {
			return ret(nil, fail(400, "assignee required"))
		}
		if e = registered(ctx, tx, assignee); e != nil {
			return ret(nil, e)
		}
		status := m["status"].(string)
		if !statusOK(status) {
			return ret(nil, fail(400, "unknown status"))
		}
		priority, e := integer(m, "priority")
		if e != nil {
			return ret(nil, e)
		}
		v, _ := json.Marshal(m["verification"])
		verification := string(v)

		h := m["body_hash"].(string)
		var b []byte
		if _, ok := a["body"]; ok {
			b, e = content(a, false)
			if e != nil {
				return ret(nil, e)
			}
			h = hash(b)
		}
		if ref, ok := a["add_session"]; ok {
			r := ref.(string)
			if r == "" || len(r) > 1024 {
				return ret(nil, fail(400, "invalid session reference"))
			}
			var n int
			if e = tx.QueryRowContext(ctx, "SELECT count(*) FROM sessions WHERE task_id=? AND ref=?", id, r).Scan(&n); e != nil {
				return ret(nil, e)
			}
			if n != 0 {
				return ret(nil, fail(409, "session already registered"))
			}
			if _, e = tx.ExecContext(ctx, "INSERT INTO sessions(task_id,ref,registered_at) VALUES(?,?,?)", id, r, at); e != nil {
				return ret(nil, e)
			}
		}
		// Delivery revision is coordination metadata, NOT a run/claim engine. It
		// ignores status/session progress writes, so reconnect can reuse exactly
		// the same immutable native envelope without a connector-side inbox.
		executionRevision := m["execution_revision"].(int)
		executionAt := m["execution_updated_at"].(string)
		wasExecutable := previousStatus == "ready" || previousStatus == "in_progress"
		executable := status == "ready" || status == "in_progress"
		if assignee != previous || priority != previousPriority || h != m["body_hash"] || (executable && !wasExecutable) || (status == "ready" && previousStatus != "ready") {
			executionRevision++
			executionAt = at
		}
		if _, e = tx.ExecContext(ctx, "UPDATE tasks SET assignee=?,modified_by=?,status=?,priority=?,body_hash=?,verification=?,execution_revision=?,execution_updated_at=? WHERE id=?", assignee, actor, status, priority, h, verification, executionRevision, executionAt, id); e != nil {
			return ret(nil, e)
		}
		if e = s.inject("row"); e != nil {
			return ret(nil, e)
		}
		if b != nil {
			if e = s.putBlob(b); e != nil {
				return ret(nil, e)
			}
		}
		if e = touch(ctx, tx, id, at, "edit", actor); e != nil {
			return ret(nil, e)
		}
		m, e = s.read(ctx, tx, id, 0, true)
		return m, &Event{Type: "task.updated", ID: id, Assignee: assignee, PreviousAssignee: previous}, e
	}
	return ret(nil, fail(404, "unknown operation"))
}

func pageArgs(a map[string]any) (string, int, error) {
	n, e := integer(a, "limit")
	if e != nil {
		return "", 0, e
	}
	if n == 0 {
		n = 50
	}
	if n < 1 || n > 200 {
		return "", 0, fail(400, "limit must be 1..200")
	}
	after := text(a, "after")
	return after, n, nil
}
func page(items []any, limit int, cursor func(any) string) map[string]any {
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	var next any
	if more {
		next = cursor(items[len(items)-1])
	}
	return map[string]any{"items": items, "has_more": more, "next_cursor": next}
}
func scanIDs(ctx context.Context, q queryer, sqlq string, args ...any) ([]string, error) {
	rows, e := q.QueryContext(ctx, sqlq, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			return nil, e
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func compact(m map[string]any) map[string]any {
	n := map[string]any{}
	if _, ok := m["deleted_at"]; ok {
		n["deleted"] = m["deleted_at"] != nil
	}
	for _, k := range []string{"id", "type", "created_at", "updated_at", "revision", "content_hash", "token", "mime", "sealed", "deleted_at", "key", "value", "description_id", "creator", "assignee", "modified_by", "status", "priority", "latest_session", "execution_revision", "execution_updated_at"} {
		if v, ok := m[k]; ok {
			n[k] = v
		}
	}
	return n
}
func (s *Store) readCall(ctx context.Context, q queryer, op string, a map[string]any) (map[string]any, error) {
	id := text(a, "id")
	if e := validateStringFields(a, "id tag_id task_id after identity assignee creator modified_by status key tagged_with match from to as_of"); e != nil {
		return nil, e
	}
	switch op {
	case "read_resource", "read_revision":
		fields := "id"
		rev := 0
		if op == "read_revision" {
			fields = "id revision"
			var e error
			rev, e = integer(a, "revision")
			if e != nil || rev < 1 {
				return nil, fail(400, "positive revision required")
			}
			if kind(id) != "tx" && kind(id) != "bn" {
				return nil, fail(400, "content ID required")
			}
		}
		if e := known(a, fields); e != nil {
			return nil, e
		}
		return s.read(ctx, q, id, rev, true)
	case "list_revisions_for_resource":
		if e := known(a, "id after limit include_deleted"); e != nil {
			return nil, e
		}
		if kind(id) != "tx" && kind(id) != "bn" {
			if e := requireID(id, ""); e != nil {
				return nil, e
			}
			return nil, fail(400, "content ID required")
		}
		if _, e := getMeta(ctx, q, id, 0); e != nil {
			return nil, e
		}
		after, n, e := pageArgs(a)
		if e != nil {
			return nil, e
		}
		start := 0
		if after != "" {
			if _, e = fmt.Sscan(after, &start); e != nil || fmt.Sprint(start) != after || start < 1 {
				return nil, fail(400, "invalid revision cursor")
			}
		}
		deletedFilter := " AND deleted_at IS NULL"
		if include, _ := a["include_deleted"].(bool); include {
			deletedFilter = ""
		}
		if v, ok := a["include_deleted"]; ok {
			if _, ok := v.(bool); !ok {
				return nil, fail(400, "include_deleted must be boolean")
			}
		}
		rows, e := q.QueryContext(ctx, "SELECT number FROM revisions WHERE resource_id=? AND number>?"+deletedFilter+" ORDER BY number LIMIT ?", id, start, n+1)
		if e != nil {
			return nil, e
		}
		nums := []int{}
		for rows.Next() {
			var r int
			if e = rows.Scan(&r); e != nil {
				rows.Close()
				return nil, e
			}
			nums = append(nums, r)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		items := []any{}
		for _, r := range nums {
			m, e := getMeta(ctx, q, id, r)
			if e != nil {
				return nil, e
			}
			items = append(items, compact(m))
		}
		return page(items, n, func(v any) string { return fmt.Sprint(v.(map[string]any)["revision"]) }), nil
	case "list_sessions_for_task":
		if e := known(a, "task_id after limit"); e != nil {
			return nil, e
		}
		id = text(a, "task_id")
		if e := requireID(id, "tk"); e != nil {
			return nil, e
		}
		if _, e := getMeta(ctx, q, id, 0); e != nil {
			return nil, e
		}
		after, n, e := pageArgs(a)
		if e != nil {
			return nil, e
		}
		bound := int64(9223372036854775807)
		if after != "" {
			if _, e = fmt.Sscan(after, &bound); e != nil || fmt.Sprint(bound) != after || bound < 1 {
				return nil, fail(400, "invalid session cursor")
			}
		}
		rows, e := q.QueryContext(ctx, "SELECT seq,ref,registered_at FROM sessions WHERE task_id=? AND seq<? ORDER BY seq DESC LIMIT ?", id, bound, n+1)
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		items := []any{}
		for rows.Next() {
			var seq int64
			var ref, at string
			if e = rows.Scan(&seq, &ref, &at); e != nil {
				return nil, e
			}
			items = append(items, map[string]any{"ref": ref, "registered_at": at, "cursor": fmt.Sprint(seq)})
		}
		if e = rows.Err(); e != nil {
			return nil, e
		}
		return page(items, n, func(v any) string { return v.(map[string]any)["cursor"].(string) }), nil
	case "list_verification_candidates":
		if e := known(a, "seed_tags"); e != nil {
			return nil, e
		}
		seeds, e := strlist(a, "seed_tags")
		if e != nil {
			return nil, e
		}
		if len(seeds) != 1 {
			return nil, fail(400, "one project tag seed required")
		}
		seed, e := getMeta(ctx, q, seeds[0], 0)
		if e != nil {
			return nil, e
		}
		if seed["type"] != "tg" || seed["key"] != "project" {
			return nil, fail(400, "seed must be a project tag")
		}
		seen := map[string]bool{}
		queue := append([]string{}, seeds...)
		candidates := map[string]bool{}
		for len(queue) > 0 {
			level := queue
			queue = nil
			sort.Strings(level)
			for _, tag := range level {
				if seen[tag] {
					continue
				}
				if len(seen) > 10000 {
					return nil, fail(413, "verification graph too large")
				}
				if e = requireID(tag, "tg"); e != nil {
					return nil, e
				}
				if _, e = getMeta(ctx, q, tag, 0); e != nil {
					return nil, e
				}
				seen[tag] = true
				children, e := scanIDs(ctx, q, "SELECT a.target_id FROM associations a JOIN tags t ON t.id=a.target_id WHERE a.tag_id=? AND a.revision=0 AND a.deleted_at IS NULL ORDER BY a.target_id", tag)
				if e != nil {
					return nil, e
				}
				queue = append(queue, children...)
				ids, e := scanIDs(ctx, q, `SELECT DISTINCT r.id FROM resources r JOIN revisions v ON v.resource_id=r.id AND v.number=(SELECT MAX(number) FROM revisions WHERE resource_id=r.id) JOIN associations a ON a.target_id=r.id AND a.revision=v.number AND a.deleted_at IS NULL WHERE r.type='tx' AND v.deleted_at IS NULL AND a.tag_id=? AND EXISTS(SELECT 1 FROM associations b JOIN tags t ON t.id=b.tag_id WHERE b.target_id=r.id AND b.revision=v.number AND b.deleted_at IS NULL AND (t.key='verification' OR t.value='verification')) ORDER BY r.id`, tag)
				if e != nil {
					return nil, e
				}
				for _, id := range ids {
					candidates[id] = true
				}
			}
		}
		ids := []string{}
		for id := range candidates {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		items := []any{}
		if len(ids) > 1000 {
			return nil, fail(413, "too many verification candidates; narrow seeds")
		}
		for _, id := range ids {
			m, e := getMeta(ctx, q, id, 0)
			if e != nil {
				return nil, e
			}
			items = append(items, compact(m))
		}
		return map[string]any{"items": items, "has_more": false, "next_cursor": nil}, nil
	}
	after, n, e := pageArgs(a)
	if e != nil {
		return nil, e
	}
	where := []string{"r.id>?"}
	args := []any{after}
	from := "resources r"
	join := ""
	order := "r.id"
	selector := "r.id"
	switch op {
	case "list_tags_for_resource":
		if e = known(a, "id revision as_of after limit"); e != nil {
			return nil, e
		}
		rev, err := integer(a, "revision")
		if err != nil || rev < 0 {
			return nil, fail(400, "invalid revision")
		}
		m, err := getMeta(ctx, q, id, rev)
		if err != nil {
			return nil, err
		}
		if r, ok := m["revision"]; ok {
			rev = r.(int)
		}
		join = " JOIN tags t ON t.id=r.id JOIN associations a ON a.tag_id=t.id"
		where = append(where, "a.target_id=? AND a.revision=?")
		args = append(args, id, rev)
		if raw, ok := a["as_of"]; ok {
			at, err := time.Parse(time.RFC3339Nano, raw.(string))
			if err != nil {
				return nil, fail(400, "valid RFC3339 as_of required")
			}
			stamp := at.UTC().Format("2006-01-02T15:04:05.000000000Z")
			where = append(where, "a.created_at<=? AND (a.deleted_at IS NULL OR a.deleted_at>?)")
			args = append(args, stamp, stamp)
		} else {
			where = append(where, "a.deleted_at IS NULL")
		}
	case "list_resources_for_tag":
		if e = known(a, "tag_id all_tags types after limit include_deleted"); e != nil {
			return nil, e
		}
		include, _ := a["include_deleted"].(bool)
		if v, ok := a["include_deleted"]; ok {
			if _, ok := v.(bool); !ok {
				return nil, fail(400, "include_deleted must be boolean")
			}
		}
		if !include {
			where = append(where, "(r.type NOT IN('tx','bn') OR EXISTS(SELECT 1 FROM revisions v WHERE v.resource_id=r.id AND v.number=(SELECT MAX(number) FROM revisions WHERE resource_id=r.id) AND v.deleted_at IS NULL))")
		}
		tag := text(a, "tag_id")
		if e = requireID(tag, "tg"); e != nil {
			return nil, e
		}
		if _, e = getMeta(ctx, q, tag, 0); e != nil {
			return nil, e
		}
		tags, err := strlist(a, "all_tags")
		if err != nil {
			return nil, err
		}
		tags = append(tags, tag)
		for _, t := range tags {
			if e = requireID(t, "tg"); e != nil {
				return nil, e
			}
			where = append(where, `EXISTS(SELECT 1 FROM associations a WHERE a.target_id=r.id AND a.tag_id=? AND a.deleted_at IS NULL AND a.revision=CASE WHEN r.type IN('tx','bn') THEN (SELECT MAX(number) FROM revisions v WHERE v.resource_id=r.id) ELSE 0 END)`)
			args = append(args, t)
		}
		types, err := strlist(a, "types")
		if err != nil {
			return nil, err
		}
		if len(types) > 0 {
			parts := []string{}
			for _, t := range types {
				if t != "tx" && t != "bn" && t != "tk" && t != "tg" {
					return nil, fail(400, "unknown type")
				}
				parts = append(parts, "?")
				args = append(args, t)
			}
			where = append(where, "r.type IN("+strings.Join(parts, ",")+")")
		}
	case "list_tags":
		if e = known(a, "id key value tagged_with match after limit"); e != nil {
			return nil, e
		}
		join = " JOIN tags t ON t.id=r.id"
		if id != "" {
			if e = requireID(id, "tg"); e != nil {
				return nil, e
			}
			where = append(where, "r.id=?")
			args = append(args, id)
		}
		if value, ok := a["value"]; ok {
			if _, ok := value.(string); !ok {
				return nil, fail(400, "value must be string")
			}
			where = append(where, "t.value=?")
			args = append(args, value)
		}
		if key, ok := a["key"]; ok {
			where = append(where, "t.key=?")
			args = append(args, key)
		}
		if tag := text(a, "tagged_with"); tag != "" {
			if e = requireID(tag, "tg"); e != nil {
				return nil, e
			}
			where = append(where, "EXISTS(SELECT 1 FROM associations a WHERE a.target_id=r.id AND a.tag_id=? AND a.revision=0 AND a.deleted_at IS NULL)")
			args = append(args, tag)
		}
		if match := text(a, "match"); match != "" {
			where = append(where, "(gate_unicode_prefix(r.id,?) OR gate_unicode_prefix(t.key,?) OR gate_unicode_prefix(t.value,?))")
			args = append(args, match, match, match)
		}
	case "list_tasks":
		if e = known(a, "identity assignee creator modified_by status after limit"); e != nil {
			return nil, e
		}
		join = " JOIN tasks t ON t.id=r.id"
		for _, k := range []string{"assignee", "creator", "modified_by", "status"} {
			if v, ok := a[k]; ok {
				if k == "status" && !statusOK(v.(string)) {
					return nil, fail(400, "unknown status")
				}
				where = append(where, "t."+k+"=?")
				args = append(args, v)
			}
		}
		if who := text(a, "identity"); who != "" {
			where = append(where, "(t.creator=? OR t.assignee=? OR t.modified_by=?)")
			args = append(args, who, who, who)
		}
	case "list_activity":
		if e = known(a, "from to after limit"); e != nil {
			return nil, e
		}
		fromAt, e1 := time.Parse(time.RFC3339Nano, text(a, "from"))
		toAt, e2 := time.Parse(time.RFC3339Nano, text(a, "to"))
		if e1 != nil || e2 != nil || !fromAt.Before(toAt) {
			return nil, fail(400, "valid increasing RFC3339 interval required")
		}
		where = append(where, "EXISTS(SELECT 1 FROM activity a WHERE a.resource_id=r.id AND a.at>=? AND a.at<?)")
		args = append(args, fromAt.UTC().Format("2006-01-02T15:04:05.000000000Z"), toAt.UTC().Format("2006-01-02T15:04:05.000000000Z"))
	default:
		return nil, fail(404, "unknown operation")
	}
	args = append(args, n+1)
	ids, e := scanIDs(ctx, q, "SELECT DISTINCT "+selector+" FROM "+from+join+" WHERE "+strings.Join(where, " AND ")+" ORDER BY "+order+" LIMIT ?", args...)
	if e != nil {
		return nil, e
	}
	items := []any{}
	for _, id := range ids {
		m, e := getMeta(ctx, q, id, 0)
		if e != nil {
			return nil, e
		}
		entry := compact(m)
		if op == "list_activity" {
			fromAt, _ := time.Parse(time.RFC3339Nano, text(a, "from"))
			toAt, _ := time.Parse(time.RFC3339Nano, text(a, "to"))
			var matched string
			if e = q.QueryRowContext(ctx, "SELECT MAX(at) FROM activity WHERE resource_id=? AND at>=? AND at<?", id, fromAt.UTC().Format("2006-01-02T15:04:05.000000000Z"), toAt.UTC().Format("2006-01-02T15:04:05.000000000Z")).Scan(&matched); e != nil {
				return nil, e
			}
			entry["matched_at"] = matched
			entry["latest_at"] = m["updated_at"]
		}
		items = append(items, entry)
	}
	return page(items, n, func(v any) string { return v.(map[string]any)["id"].(string) }), nil
}
