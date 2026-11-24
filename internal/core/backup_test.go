package core

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOnlineBackupReferencedBlobsAndConcurrentReads(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:online-backup", "snapshot")
	call(t, s, "seal_revision", expected(m))
	args := map[string]any{"operation_id": "online-backup", "assignee": actor, "body": "task snapshot"}
	task := call(t, s, "create_task", args)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			if _, e := s.Call(ctx, actor, "read_resource", map[string]any{"id": m["id"]}); e != nil {
				t.Error(e)
				return
			}
		}
	}()
	dest := filepath.Join(t.TempDir(), "backup")
	if e := s.Backup(ctx, dest); e != nil {
		t.Fatal(e)
	}
	wg.Wait()
	a := taskExpected(task)
	a["body"] = "changed after snapshot"
	call(t, s, "update_task", a)
	restored, e := New(dest, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	got := call(t, restored, "read_revision", map[string]any{"id": m["id"], "revision": 1})
	if got["body"] != "snapshot" || got["sealed"] != true {
		t.Fatal(got)
	}
	got = call(t, restored, "create_task", args)
	if got["body"] != "task snapshot" || got["id"] != task["id"] {
		t.Fatal(got)
	}
	files, e := os.ReadDir(filepath.Join(dest, "objects"))
	if e != nil || len(files) != 2 {
		t.Fatal("only snapshot refs expected", len(files), e)
	}
	if e = s.Backup(ctx, dest); e == nil {
		t.Fatal("overwrote backup")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	badDest := filepath.Join(t.TempDir(), "canceled")
	if e = s.Backup(canceled, badDest); e == nil {
		t.Fatal("cancellation ignored")
	}
	if _, e = os.Stat(badDest); !os.IsNotExist(e) {
		t.Fatal("partial backup published", e)
	}
}

func TestCoalescedInvalidationsAndRestartClosure(t *testing.T) {
	s := setup(t)
	ch, cancel := s.Subscribe(actor)
	defer cancel()
	m := call(t, s, "create_task", map[string]any{"operation_id": "coalesce", "assignee": actor, "body": "a"})
	for i := 0; i < 50; i++ {
		a := taskExpected(m)
		a["status"] = "ready"
		m = call(t, s, "update_task", a)
	}
	select {
	case e := <-ch:
		if e.ID != m["id"] {
			t.Fatal(e)
		}
	default:
		t.Fatal("no coalesced event")
	}
	select {
	case e := <-ch:
		t.Fatal("duplicate pending event", e)
	default:
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, ok := <-ch; ok {
		t.Fatal("restart/close did not close subscription")
	}
}
