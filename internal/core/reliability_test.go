package core

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSealUpdateRaceAndSealRollback(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:seal-race", "base")
	var ok atomic.Int32
	var wg sync.WaitGroup
	for _, op := range []string{"seal_revision", "update_text"} {
		wg.Add(1)
		go func(op string) {
			defer wg.Done()
			a := expected(m)
			if op == "update_text" {
				a["body"] = "edit"
			}
			_, e := s.Call(ctx, actor, op, a)
			if e == nil {
				ok.Add(1)
			} else {
				errStatus(t, e, 412)
			}
		}(op)
	}
	wg.Wait()
	if ok.Load() != 1 {
		t.Fatal(ok.Load())
	}
	m = call(t, s, "read_resource", map[string]any{"id": m["id"]})
	before := m["revision"]
	s.fault = func(stage string) error {
		if stage == "before_commit" {
			return errors.New("seal interruption")
		}
		return nil
	}
	if _, e := s.Call(ctx, actor, "seal_revision", expected(m)); e == nil {
		t.Fatal("fault ignored")
	}
	s.fault = nil
	m = call(t, s, "read_resource", map[string]any{"id": m["id"]})
	if m["revision"] != before || m["sealed"] != false {
		t.Fatal("partial seal", m)
	}
}
func TestTaskBodyMetadataSessionFaultAtomicAndNoEvent(t *testing.T) {
	s := setup(t)
	m := call(t, s, "create_task", map[string]any{"operation_id": "atomic-fault", "assignee": actor, "body": "before"})
	ch, cancel := s.Subscribe(actor)
	defer cancel()
	for _, point := range []string{"row", "staged", "published", "before_commit"} {
		s.fault = func(stage string) error {
			if stage == point {
				return errors.New("task interruption")
			}
			return nil
		}
		a := taskExpected(m)
		a["body"] = "after-" + point
		a["status"] = "done"
		a["assignee"] = "agent:mai"
		a["add_session"] = "uncommitted-session"
		if _, e := s.Call(ctx, actor, "update_task", a); e == nil {
			t.Fatal("fault ignored", point)
		}
		s.fault = nil
		got := call(t, s, "read_resource", map[string]any{"id": m["id"]})
		if got["token"] != m["token"] || got["body"] != "before" || got["latest_session"] != nil {
			t.Fatal("partial task edit", point, got)
		}
		select {
		case e := <-ch:
			t.Fatal("notified rolled back edit", e)
		default:
		}
	}
}
func TestThousandProjectMetadataPagingAndNoBlobScans(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:no-list-blob-scan", "do not read me to list metadata")
	tx, e := s.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	at := now()
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("gate:tg:project-%04d", i)
		if _, e = tx.Exec("INSERT INTO resources VALUES(?,'tg',?,?)", id, at, at); e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec("INSERT INTO tags VALUES(?,'project',?,NULL)", id, fmt.Sprint(i)); e != nil {
			t.Fatal(e)
		}
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	start := time.Now()
	count := 0
	after := ""
	for {
		out := call(t, s, "list_tags", map[string]any{"key": "project", "limit": 200, "after": after})
		count += len(items(out))
		if out["has_more"] != true {
			break
		}
		after = out["next_cursor"].(string)
	}
	if count != 1000 {
		t.Fatal(count)
	}
	t.Logf("1000 project tags, complete metadata paging: %s", time.Since(start))
	call(t, s, "tag_resource", map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:project-0000", "expected_revision": 1, "expected_hash": m["token"]})
	old := filepath.Join(s.dir, "objects")
	hidden := filepath.Join(s.dir, "objects-hidden")
	if e = os.Rename(old, hidden); e != nil {
		t.Fatal(e)
	}
	defer os.Rename(hidden, old)
	out := call(t, s, "list_resources_for_tag", map[string]any{"tag_id": "gate:tg:project-0000"})
	if len(items(out)) != 1 {
		t.Fatal(out)
	}
	rows, e := s.db.Query(`EXPLAIN QUERY PLAN SELECT r.id FROM resources r WHERE EXISTS(SELECT 1 FROM associations a WHERE a.target_id=r.id AND a.tag_id=? AND a.deleted_at IS NULL AND a.revision=CASE WHEN r.type IN('tx','bn') THEN (SELECT MAX(number) FROM revisions v WHERE v.resource_id=r.id) ELSE 0 END)`, "gate:tg:project-0000")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	plan := []string{}
	for rows.Next() {
		var i, p, u int
		var detail string
		if e = rows.Scan(&i, &p, &u, &detail); e != nil {
			t.Fatal(e)
		}
		plan = append(plan, detail)
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "INDEX") {
		t.Fatal("missing indexed lookup", joined)
	}
	t.Logf("latest-before-tag plan:\n%s", joined)
}
func TestStoppedStoreBackupRoundtrip(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:backup", "snapshot")
	call(t, s, "seal_revision", expected(m))
	task := call(t, s, "create_task", map[string]any{"operation_id": "backup", "assignee": actor, "body": "task snapshot"})
	s.Close()
	backup := filepath.Join(t.TempDir(), "restored")
	if e := os.Mkdir(backup, 0700); e != nil {
		t.Fatal(e)
	}
	e := filepath.WalkDir(s.dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(s.dir, path)
		if e != nil {
			return e
		}
		if rel == "." {
			return nil
		}
		dest := filepath.Join(backup, rel)
		if d.IsDir() {
			return os.Mkdir(dest, 0700)
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		return os.WriteFile(dest, b, 0600)
	})
	if e != nil {
		t.Fatal(e)
	}
	restored, e := New(backup, []string{actor})
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	old := call(t, restored, "read_revision", map[string]any{"id": m["id"], "revision": 1})
	if old["body"] != "snapshot" || old["sealed"] != true {
		t.Fatal(old)
	}
	got := call(t, restored, "read_resource", map[string]any{"id": task["id"]})
	if got["body"] != "task snapshot" {
		t.Fatal(got)
	}
}
