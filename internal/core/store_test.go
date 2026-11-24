package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var ctx = context.Background()

const actor = "agent:hermes"

func setup(t *testing.T) *Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	s, e := New(dir, []string{actor, "agent:mai", "human:d0f"})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func call(t *testing.T, s *Store, op string, a map[string]any) map[string]any {
	t.Helper()
	m, e := s.Call(ctx, actor, op, a)
	if e != nil {
		t.Fatalf("%s: %v", op, e)
	}
	return m
}
func errStatus(t *testing.T, e error, want int) {
	t.Helper()
	var ce *Error
	if !errors.As(e, &ce) || ce.Status != want {
		t.Fatalf("error=%v want status %d", e, want)
	}
}
func bad(t *testing.T, s *Store, op string, a map[string]any, status int) {
	t.Helper()
	_, e := s.Call(ctx, actor, op, a)
	errStatus(t, e, status)
}
func create(t *testing.T, s *Store, id, body string) map[string]any {
	return call(t, s, "create_text", map[string]any{"id": id, "body": body})
}
func tag(t *testing.T, s *Store, id, key, value string) map[string]any {
	return call(t, s, "create_tag", map[string]any{"id": id, "key": key, "value": value})
}
func expected(m map[string]any) map[string]any {
	return map[string]any{"id": m["id"], "expected_revision": m["revision"], "expected_hash": m["token"]}
}
func taskExpected(m map[string]any) map[string]any {
	return map[string]any{"id": m["id"], "expected_token": m["token"]}
}
func items(m map[string]any) []any { return m["items"].([]any) }
func TestRevisionLifecycle(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:notes", "a")
	if m["revision"] != 1 {
		t.Fatal(m)
	}
	a := expected(m)
	a["body"] = "b"
	m = call(t, s, "update_text", a)
	if m["revision"] != 1 || m["token"] == hash([]byte("a")) {
		t.Fatal(m)
	}
	bad(t, s, "update_text", map[string]any{"id": m["id"], "body": "lost", "expected_revision": 1, "expected_hash": hash([]byte("a"))}, 412)
	original := expected(m)
	m = call(t, s, "seal_revision", original)
	if m["revision"] != 2 || m["body"] != "b" {
		t.Fatal(m)
	}
	bad(t, s, "seal_revision", original, 412)
	old := call(t, s, "read_revision", map[string]any{"id": m["id"], "revision": 1})
	if old["sealed"] != true || old["body"] != "b" {
		t.Fatal(old)
	}
	a = expected(m)
	a["body"] = "c"
	m = call(t, s, "update_text", a)
	old = call(t, s, "read_revision", map[string]any{"id": m["id"], "revision": 1})
	if old["body"] != "b" {
		t.Fatal(old)
	}
	d := call(t, s, "delete_resource", expected(m))
	_, e := s.Call(ctx, actor, "read_resource", map[string]any{"id": m["id"]})
	errStatus(t, e, 410)
	var ce *Error
	errors.As(e, &ce)
	if ce.Resource["revision"] != 2 || ce.Resource["token"] == nil {
		t.Fatal(ce)
	}
	bad(t, s, "seal_revision", expected(d), 409)
	a = expected(d)
	a["body"] = "restored"
	m = call(t, s, "update_text", a)
	if m["revision"] != 2 || m["deleted_at"] != nil {
		t.Fatal(m)
	}
	a = expected(old)
	a["revision"] = 1
	d = call(t, s, "delete_resource", a)
	a = expected(d)
	a["body"] = "no"
	bad(t, s, "update_text", a, 412)
	revs := call(t, s, "list_revisions_for_resource", map[string]any{"id": m["id"], "include_deleted": true})
	if len(items(revs)) != 2 {
		t.Fatal(revs)
	}
}
func TestConcurrentCASAndSeal(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:race", "same")
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := expected(m)
			a["body"] = fmt.Sprint(i)
			_, e := s.Call(ctx, actor, "update_text", a)
			if e == nil {
				successes.Add(1)
			} else {
				errStatus(t, e, 412)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatal(successes.Load())
	}
	m = call(t, s, "read_resource", map[string]any{"id": m["id"]})
	successes.Store(0)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.Call(ctx, actor, "seal_revision", expected(m))
			if e == nil {
				successes.Add(1)
			} else {
				errStatus(t, e, 412)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatal(successes.Load())
	}
}
func TestTemporalTagsLatestFirst(t *testing.T) {
	s := setup(t)
	tag(t, s, "gate:tg:project", "project", "Gate")
	tag(t, s, "gate:tg:topic", "topic", "architecture")
	m := call(t, s, "create_text", map[string]any{"id": "gate:tx:design", "body": "old", "tags": []string{"gate:tg:project"}})
	m = call(t, s, "seal_revision", expected(m))
	list := call(t, s, "list_resources_for_tag", map[string]any{"tag_id": "gate:tg:project"})
	if len(items(list)) != 1 {
		t.Fatal(list)
	}
	call(t, s, "untag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:project", "expected_revision": 2, "expected_hash": m["token"]})
	list = call(t, s, "list_resources_for_tag", map[string]any{"tag_id": "gate:tg:project"})
	if len(items(list)) != 0 {
		t.Fatal("historical match leaked", list)
	}
	call(t, s, "tag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:topic", "revision": 1})
	old := call(t, s, "read_revision", map[string]any{"id": m["id"], "revision": 1})
	if old["body"] != "old" {
		t.Fatal(old)
	}
	call(t, s, "tag_resource", map[string]any{"resource_id": "gate:tg:project", "tag_id": "gate:tg:topic"})
	list = call(t, s, "list_resources_for_tag", map[string]any{"tag_id": "gate:tg:topic"})
	if len(items(list)) != 1 || items(list)[0].(map[string]any)["id"] != "gate:tg:project" {
		t.Fatal(list)
	}
	for i := 0; i < 3; i++ {
		call(t, s, "tag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:project", "expected_revision": 2, "expected_hash": m["token"]})
		call(t, s, "untag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:project", "expected_revision": 2, "expected_hash": m["token"]})
	}
	var n int
	if e := s.db.QueryRow("SELECT count(*) FROM associations WHERE tag_id='gate:tg:project' AND target_id='gate:tx:design' AND revision=2").Scan(&n); e != nil || n != 4 {
		t.Fatal(n, e)
	}
	bad(t, s, "tag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:project", "expected_revision": 1, "expected_hash": m["token"]}, 412)
	current := call(t, s, "list_tags_for_resource", map[string]any{"id": m["id"]})
	historical := call(t, s, "list_tags_for_resource", map[string]any{"id": m["id"], "revision": 1})
	if len(items(current)) != 0 || len(items(historical)) != 2 {
		t.Fatal(current, historical)
	}
	renamed := call(t, s, "read_resource", map[string]any{"id": "gate:tg:topic"})
	a := taskExpected(renamed)
	a["value"] = "renamed"
	call(t, s, "update_tag", a)
	historical = call(t, s, "list_tags_for_resource", map[string]any{"id": m["id"], "revision": 1})
	found := false
	for _, v := range items(historical) {
		if v.(map[string]any)["value"] == "renamed" {
			found = true
		}
	}
	if !found {
		t.Fatal(historical)
	}
}
func TestActivityOccurrencesAndDetachedTags(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:activity", "yesterday")
	tag(t, s, "gate:tg:activity", "topic", "a")
	call(t, s, "tag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:activity", "expected_revision": 1, "expected_hash": m["token"]})
	call(t, s, "untag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:activity", "expected_revision": 1, "expected_hash": m["token"]})
	// Shift retained occurrences, not state; today's later edit must not erase yesterday.
	_, e := s.db.Exec("UPDATE activity SET at='2026-11-21T12:00:00.000000000Z' WHERE resource_id=?", m["id"])
	if e != nil {
		t.Fatal(e)
	}
	a := expected(m)
	a["body"] = "today"
	call(t, s, "update_text", a)
	out := call(t, s, "list_activity", map[string]any{"from": "2026-11-21T00:00:00Z", "to": "2026-11-22T00:00:00Z"})
	if len(items(out)) != 1 {
		t.Fatal(out)
	}
	// Exact upper bound is excluded; equivalent offset instants resolve identically.
	_, e = s.db.Exec("UPDATE activity SET at='2026-11-22T00:00:00.000000000Z' WHERE resource_id=?", m["id"])
	if e != nil {
		t.Fatal(e)
	}
	out = call(t, s, "list_activity", map[string]any{"from": "2026-11-21T01:00:00+01:00", "to": "2026-11-22T01:00:00+01:00"})
	if len(items(out)) != 0 {
		t.Fatal(out)
	}
}
func TestTaskIdempotencyAtomicSessionsAttribution(t *testing.T) {
	s := setup(t)
	a := map[string]any{"operation_id": "create-task-1", "assignee": actor, "body": "work", "status": "ready"}
	m := call(t, s, "create_task", a)
	retry := call(t, s, "create_task", a)
	if m["id"] != retry["id"] {
		t.Fatal(m, retry)
	}
	b := map[string]any{"operation_id": "create-task-1", "assignee": actor, "body": "different"}
	bad(t, s, "create_task", b, 409)
	b = taskExpected(m)
	b["body"] = "updated"
	b["add_session"] = "host:harness:one"
	b["status"] = "in_progress"
	b["assignee"] = "agent:mai"
	m, e := s.Call(ctx, "human:d0f", "update_task", b)
	if e != nil {
		t.Fatal(e)
	}
	if m["creator"] != actor || m["modified_by"] != "human:d0f" || m["assignee"] != "agent:mai" {
		t.Fatal(m)
	}
	before := m
	b = taskExpected(m)
	b["body"] = "partial must roll back"
	b["status"] = "done"
	b["add_session"] = "host:harness:one"
	bad(t, s, "update_task", b, 409)
	m = call(t, s, "read_resource", map[string]any{"id": m["id"]})
	if !reflect.DeepEqual(before, m) {
		t.Fatal("partial update", before, m)
	}
	b = taskExpected(m)
	b["add_session"] = "second"
	m = call(t, s, "update_task", b)
	if m["latest_session"].(map[string]any)["ref"] != "second" {
		t.Fatal(m)
	}
	if _, ok := m["sessions"]; ok {
		t.Fatal(m)
	}
	out := call(t, s, "list_sessions_for_task", map[string]any{"task_id": m["id"], "limit": 1})
	if len(items(out)) != 1 || out["has_more"] != true {
		t.Fatal(out)
	}
	next := call(t, s, "list_sessions_for_task", map[string]any{"task_id": m["id"], "after": out["next_cursor"]})
	if len(items(next)) != 1 || items(next)[0].(map[string]any)["ref"] != "host:harness:one" {
		t.Fatal(next)
	}
	out = call(t, s, "list_tasks", map[string]any{"identity": "human:d0f"})
	if len(items(out)) != 0 {
		t.Fatal("latest editor changed", out)
	}
	out = call(t, s, "list_tasks", map[string]any{"identity": actor})
	if len(items(out)) != 1 {
		t.Fatal(out)
	}
	b = taskExpected(m)
	b["creator"] = "human:d0f"
	bad(t, s, "update_task", b, 400)
	b = taskExpected(m)
	b["status"] = "open"
	bad(t, s, "update_task", b, 400)
	bad(t, s, "create_task", map[string]any{"operation_id": "no-assignee", "body": "bad"}, 400)
	other := call(t, s, "create_task", map[string]any{"operation_id": "other", "assignee": actor, "body": "other"})
	b = taskExpected(other)
	b["add_session"] = "second"
	call(t, s, "update_task", b)
}
func TestNotificationsPreviousNewBodyAndOverflow(t *testing.T) {
	s := setup(t)
	old, cancel := s.Subscribe(actor)
	defer cancel()
	newc, cancel2 := s.Subscribe("agent:mai")
	defer cancel2()
	m := call(t, s, "create_task", map[string]any{"operation_id": "notify", "assignee": actor, "body": "a", "status": "ready"})
	select {
	case e := <-old:
		if e.ID != m["id"] {
			t.Fatal(e)
		}
	default:
		t.Fatal("no ready notification")
	}
	a := taskExpected(m)
	a["assignee"] = "agent:mai"
	a["body"] = "changed"
	m = call(t, s, "update_task", a)
	for _, ch := range []<-chan Event{old, newc} {
		select {
		case e := <-ch:
			if e.PreviousAssignee != actor || e.Assignee != "agent:mai" {
				t.Fatal(e)
			}
		default:
			t.Fatal("lost reassignment invalidation")
		}
	}
	for i := 0; i < 34; i++ {
		call(t, s, "create_task", map[string]any{"operation_id": fmt.Sprintf("overflow-%d", i), "body": "overflow", "assignee": "agent:mai"})
	}
	closed := false
	for i := 0; i < 33; i++ {
		_, ok := <-newc
		if !ok {
			closed = true
			break
		}
	}
	if !closed {
		t.Fatal("overflow not reset")
	}
	cancel2()
	cancel2()
}
func TestVerificationCyclesPinsAndSelectionStability(t *testing.T) {
	s := setup(t)
	tag(t, s, "gate:tg:project", "project", "Gate")
	tag(t, s, "gate:tg:parent", "context", "root")
	tag(t, s, "gate:tg:verify", "verification", "instructions")
	for _, pair := range [][2]string{{"gate:tg:project", "gate:tg:parent"}, {"gate:tg:parent", "gate:tg:project"}} {
		call(t, s, "tag_resource", map[string]any{"resource_id": pair[0], "tag_id": pair[1]})
	}
	m := call(t, s, "create_text", map[string]any{"id": "gate:tx:checks", "body": "checks", "tags": []string{"gate:tg:parent", "gate:tg:verify"}})
	out := call(t, s, "list_verification_candidates", map[string]any{"seed_tags": []string{"gate:tg:project"}})
	if len(items(out)) != 1 {
		t.Fatal(out)
	}
	a := map[string]any{"operation_id": "verify", "assignee": actor, "body": "work", "verification": []any{map[string]any{"id": m["id"], "revision": 1}}}
	bad(t, s, "create_task", a, 409)
	call(t, s, "seal_revision", expected(m))
	task := call(t, s, "create_task", a)
	call(t, s, "untag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:verify", "expected_revision": 2, "expected_hash": m["token"]})
	task2 := call(t, s, "read_resource", map[string]any{"id": task["id"]})
	if !reflect.DeepEqual(task["verification"], task2["verification"]) {
		t.Fatal(task, task2)
	}
	out = call(t, s, "list_verification_candidates", map[string]any{"seed_tags": []string{"gate:tg:project"}})
	if len(items(out)) != 0 {
		t.Fatal(out)
	}
}
func TestDescriptionsAbsentDeletedLargeAndTyping(t *testing.T) {
	s := setup(t)
	m := tag(t, s, "gate:tg:optional", "project", "Optional")
	if m["description_state"] != "none" {
		t.Fatal(m)
	}
	text := create(t, s, "gate:tx:description", "description")
	a := taskExpected(m)
	a["description_id"] = text["id"]
	m = call(t, s, "update_tag", a)
	if m["description_state"] != "available" {
		t.Fatal(m)
	}
	call(t, s, "delete_resource", expected(text))
	m = call(t, s, "read_resource", map[string]any{"id": m["id"]})
	if m["description_state"] != "unavailable" {
		t.Fatal(m)
	}
	large := create(t, s, "gate:tx:large", strings.Repeat("x", (64<<10)+1))
	a = taskExpected(m)
	a["description_id"] = large["id"]
	m = call(t, s, "update_tag", a)
	if m["description_state"] != "available" || len(m["description"].(map[string]any)["body"].(string)) != (64<<10)+1 {
		t.Fatal(m)
	}
	a = taskExpected(m)
	a["description_id"] = m["id"]
	bad(t, s, "update_tag", a, 400)
}
func TestSharedBlobGCAndReopen(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:first", "shared")
	other := create(t, s, "gate:tx:other", "shared")
	path := filepath.Join(s.dir, "objects", m["content_hash"].(string))
	call(t, s, "delete_resource", expected(m))
	if _, e := os.Stat(path); e != nil {
		t.Fatal("shared blob deleted", e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	s2, e := New(s.dir, []string{actor})
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	got := call(t, s2, "read_resource", map[string]any{"id": other["id"]})
	if got["body"] != "shared" {
		t.Fatal(got)
	}
	call(t, s2, "delete_resource", expected(other))
	old := time.Now().Add(-2 * BlobGrace)
	if e = os.Chtimes(path, old, old); e != nil {
		t.Fatal(e)
	}
	if e = s2.Collect(); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("unreferenced bytes not reclaimed", e)
	}
}
func TestFaultRollbackAndRowFirstVisibility(t *testing.T) {
	for _, stage := range []string{"row", "staged", "published", "before_commit"} {
		t.Run(stage, func(t *testing.T) {
			s := setup(t)
			m := create(t, s, "gate:tx:fault", "before")
			s.fault = func(point string) error {
				if point != stage {
					return nil
				}
				read := call(t, s, "read_resource", map[string]any{"id": m["id"]})
				if read["body"] != "before" {
					t.Fatal("pending row exposed", read)
				}
				return errors.New("injected")
			}
			a := expected(m)
			a["body"] = "after-" + stage
			if _, e := s.Call(ctx, actor, "update_text", a); e == nil {
				t.Fatal("fault ignored")
			}
			s.fault = nil
			got := call(t, s, "read_resource", map[string]any{"id": m["id"]})
			if got["body"] != "before" {
				t.Fatal(got)
			}
			if e := s.Collect(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestActualProcessCrashRecovery(t *testing.T) {
	if dir := os.Getenv("GATE_ALPHA_CRASH_DIR"); dir != "" {
		s, e := New(dir, []string{actor})
		if e != nil {
			os.Exit(80)
		}
		m, e := s.Call(ctx, actor, "read_resource", map[string]any{"id": "gate:tx:crash"})
		if e != nil {
			os.Exit(81)
		}
		s.fault = func(point string) error {
			if point == os.Getenv("GATE_ALPHA_CRASH_STAGE") {
				os.Exit(77)
			}
			return nil
		}
		a := expected(m)
		a["body"] = "uncommitted"
		s.Call(ctx, actor, "update_text", a)
		os.Exit(82)
	}
	for _, stage := range []string{"row", "staged", "published", "before_commit", "after_commit"} {
		t.Run(stage, func(t *testing.T) {
			s := setup(t)
			create(t, s, "gate:tx:crash", "committed")
			s.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestActualProcessCrashRecovery$")
			cmd.Env = append(os.Environ(), "GATE_ALPHA_CRASH_DIR="+s.dir, "GATE_ALPHA_CRASH_STAGE="+stage)
			e := cmd.Run()
			var exit *exec.ExitError
			if !errors.As(e, &exit) || exit.ExitCode() != 77 {
				t.Fatalf("not interrupted as expected %v", e)
			}
			reopened, e := New(s.dir, []string{actor})
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			m := call(t, reopened, "read_resource", map[string]any{"id": "gate:tx:crash"})
			want := "committed"
			if stage == "after_commit" {
				want = "uncommitted"
			}
			if m["body"] != want {
				t.Fatal(m)
			}
			if e = reopened.Collect(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestIdentityMalformedInputsAndLimits(t *testing.T) {
	s := setup(t)
	for _, id := range []string{"gate:tx:../secret", "gate:tx:UPPER", "gate:tx:space word", "gate:tx:x:1", "gate:tx:slash/x", "gate:tx:-bad", "gate:xx:no"} {
		bad(t, s, "read_resource", map[string]any{"id": id}, 400)
	}
	bad(t, s, "read_resource", map[string]any{"id": "gate:tx:missing"}, 404)
	_, e := s.Call(ctx, "agent:unknown", "list_tags", nil)
	errStatus(t, e, 403)
	call(t, s, "create_text", map[string]any{"id": "gate:tx:oversized", "body": strings.Repeat("x", (1<<20)+1)})
	m := create(t, s, "gate:tx:valid", "body")
	bad(t, s, "update_text", map[string]any{"id": m["id"], "body": "x"}, 428)
	tag(t, s, "gate:tg:tag", "key", "value")
	bad(t, s, "delete_resource", map[string]any{"id": "gate:tg:tag"}, 405)
	bad(t, s, "list_tags", map[string]any{"limit": 201}, 400)
	bad(t, s, "read_resource", map[string]any{"id": m["id"], "include": "all"}, 400)
	for i := 0; i < 5; i++ {
		tag(t, s, fmt.Sprintf("gate:tg:page-%d", i), "project", fmt.Sprint(i))
	}
	seen := map[string]bool{}
	after := ""
	for {
		r := call(t, s, "list_tags", map[string]any{"key": "project", "limit": 2, "after": after})
		for _, v := range items(r) {
			id := v.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatal("duplicate", id)
			}
			seen[id] = true
		}
		if r["has_more"] != true {
			break
		}
		after = r["next_cursor"].(string)
	}
	if len(seen) != 5 {
		t.Fatal(seen)
	}
}
func TestNoMissingObjectsOrSymlinksOnReopen(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:corrupt", "body")
	s.Close()
	p := filepath.Join(s.dir, "objects", m["content_hash"].(string))
	if e := os.WriteFile(p, []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	if other, e := New(s.dir, []string{actor}); e == nil {
		other.Close()
		t.Fatal("corruption accepted")
	}
	dir := filepath.Join(t.TempDir(), "link")
	if e := os.Symlink(s.dir, dir); e != nil {
		t.Fatal(e)
	}
	if other, e := New(dir, []string{actor}); e == nil {
		other.Close()
		t.Fatal("symlink accepted")
	}
}
func TestParallelReadWriteSnapshots(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:parallel", "zero")
	var wg sync.WaitGroup
	deadline := time.Now().Add(100 * time.Millisecond)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				v, e := s.Call(ctx, actor, "read_resource", map[string]any{"id": "gate:tx:parallel"})
				if e != nil {
					t.Error(e)
					return
				}
				if hash([]byte(v["body"].(string))) != v["content_hash"] {
					t.Error("mixed snapshot")
				}
			}
		}()
	}
	for i := 0; i < 10; i++ {
		a := expected(m)
		a["body"] = fmt.Sprint(i)
		m = call(t, s, "update_text", a)
	}
	wg.Wait()
}
