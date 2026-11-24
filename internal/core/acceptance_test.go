package core

import (
	"fmt"
	"sync"
	"testing"
)

func TestTaskClassificationEditorNotificationAndCurrentReplay(t *testing.T) {
	s := setup(t)
	tag(t, s, "gate:tg:task-project", "project", "task")
	args := map[string]any{"operation_id": "current-replay", "assignee": actor, "body": "initial"}
	task := call(t, s, "create_task", args)
	ch, cancel := s.Subscribe(actor)
	defer cancel()
	if _, e := s.Call(ctx, "human:d0f", "tag_resource", map[string]any{"resource_id": task["id"], "tag_id": "gate:tg:task-project"}); e != nil {
		t.Fatal(e)
	}
	select {
	case event := <-ch:
		if event.ID != task["id"] {
			t.Fatal(event)
		}
	default:
		t.Fatal("classification notification missing")
	}
	current := call(t, s, "read_resource", map[string]any{"id": task["id"]})
	if current["creator"] != actor || current["modified_by"] != "human:d0f" || current["token"] == task["token"] {
		t.Fatal(current)
	}
	a := taskExpected(task)
	a["status"] = "done"
	bad(t, s, "update_task", a, 412)
	a = taskExpected(current)
	a["body"] = "latest body"
	a["status"] = "ready"
	current = call(t, s, "update_task", a)
	s.Close()
	reopened, e := New(s.dir, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	replay := call(t, reopened, "create_task", args)
	if replay["id"] != current["id"] || replay["token"] != current["token"] || replay["body"] != "latest body" {
		t.Fatal(replay)
	}
	var n int
	if e = reopened.db.QueryRow("SELECT count(*) FROM operations").Scan(&n); e != nil || n != 1 {
		t.Fatal(n, e)
	}
}

func TestMaintenanceWithConcurrentReadersAndWriter(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:maintenance", "initial")
	id := m["id"]
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				r, e := s.Call(ctx, actor, "read_resource", map[string]any{"id": id})
				if e != nil {
					t.Error(e)
					return
				}
				if hash([]byte(r["body"].(string))) != r["content_hash"] {
					t.Error("mixed snapshot")
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if e := s.Collect(); e != nil {
				t.Error(e)
				return
			}
		}
	}()
	for i := 0; i < 10; i++ {
		a := expected(m)
		a["body"] = fmt.Sprint(i)
		m = call(t, s, "update_text", a)
	}
	wg.Wait()
}
