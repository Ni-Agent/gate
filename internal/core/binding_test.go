package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBindingCurrentAssociationHash(t *testing.T) {
	s := setup(t)
	tag(t, s, "gate:tg:cas", "topic", "CAS")
	m := create(t, s, "gate:tx:cas", "old")
	a := expected(m)
	a["body"] = "new"
	next := call(t, s, "update_text", a)
	args := map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:cas", "expected_revision": 1, "expected_hash": m["token"]}
	bad(t, s, "tag_resource", args, 412)
	delete(args, "expected_hash")
	bad(t, s, "tag_resource", args, 428)
	args["expected_hash"] = next["token"]
	call(t, s, "tag_resource", args)
	a = expected(next)
	call(t, s, "seal_revision", a)
	bad(t, s, "untag_resource", args, 412)
	args["revision"] = 1
	delete(args, "expected_revision")
	delete(args, "expected_hash")
	call(t, s, "untag_resource", args) // explicit sealed history targets no mutable bytes
}

func TestBindingDeletedListsAndActivityTimesActor(t *testing.T) {
	s := setup(t)
	tag(t, s, "gate:tg:deleted", "project", "Deleted")
	m := call(t, s, "create_text", map[string]any{"id": "gate:tx:deleted", "body": "x", "tags": []string{"gate:tg:deleted"}})
	call(t, s, "seal_revision", expected(m))
	historical := expected(m)
	historical["revision"] = 1
	call(t, s, "delete_resource", historical) // historical tombstone
	current := call(t, s, "read_resource", map[string]any{"id": m["id"]})
	call(t, s, "delete_resource", expected(current))
	normal := call(t, s, "list_resources_for_tag", map[string]any{"tag_id": "gate:tg:deleted"})
	if len(items(normal)) != 0 {
		t.Error("default leaked tombstone", normal)
	}
	diagnostic, err := s.Call(ctx, actor, "list_resources_for_tag", map[string]any{"tag_id": "gate:tg:deleted", "include_deleted": true})
	if err != nil {
		t.Error(err)
	} else if len(items(diagnostic)) != 1 || items(diagnostic)[0].(map[string]any)["deleted"] != true {
		t.Error(diagnostic)
	}
	revs := call(t, s, "list_revisions_for_resource", map[string]any{"id": m["id"]})
	if len(items(revs)) != 0 {
		t.Error("revision list leaked tombstones", revs)
	}
	activity := call(t, s, "list_activity", map[string]any{"from": "2000-01-01T00:00:00Z", "to": "2100-01-01T00:00:00Z"})
	found := false
	for _, v := range items(activity) {
		r := v.(map[string]any)
		if r["id"] == m["id"] {
			found = true
			if r["deleted"] != true || r["matched_at"] == nil || r["latest_at"] == nil || r["created_at"] == nil {
				t.Error("missing marked times", r)
			}
		}
	}
	if !found {
		t.Error(activity)
	}
	var n int
	if err = s.db.QueryRow("SELECT count(*) FROM activity WHERE actor=?", actor).Scan(&n); err != nil || n != 5 {
		t.Error("activity actor/count", n, err)
	}
}

func TestBindingDiscoveryPrefixNotSubstring(t *testing.T) {
	s := setup(t)
	tag(t, s, "gate:tg:gate", "project", "Alpha")
	tag(t, s, "gate:tg:negate", "project", "Nalpha")
	out := call(t, s, "list_tags", map[string]any{"key": "project", "match": "ALp"})
	if len(items(out)) != 1 || items(out)[0].(map[string]any)["id"] != "gate:tg:gate" {
		t.Fatal(out)
	}
	out = call(t, s, "list_tags", map[string]any{"match": "GATE:TG:GA"})
	if len(items(out)) != 1 {
		t.Fatal(out)
	}
}

func TestBindingVerificationDirectedBFSRequiredFrozen(t *testing.T) {
	s := setup(t)
	for _, id := range []string{"project", "child", "outside", "verification"} {
		key := "context"
		if id == "project" {
			key = "project"
		}
		if id == "verification" {
			key = "verification"
		}
		tag(t, s, "gate:tg:"+id, key, id)
	}
	// project -> child; outside -> project must NOT traverse backwards.
	call(t, s, "tag_resource", map[string]any{"resource_id": "gate:tg:child", "tag_id": "gate:tg:project"})
	call(t, s, "tag_resource", map[string]any{"resource_id": "gate:tg:project", "tag_id": "gate:tg:outside"})
	for _, scope := range []string{"child", "outside"} {
		call(t, s, "create_text", map[string]any{"id": "gate:tx:" + scope, "body": "required", "tags": []string{"gate:tg:" + scope, "gate:tg:verification"}})
	}
	out := call(t, s, "list_verification_candidates", map[string]any{"seed_tags": []string{"gate:tg:project"}})
	if len(items(out)) != 1 || items(out)[0].(map[string]any)["id"] != "gate:tx:child" {
		t.Error("wrong direction/classifier", out)
	}
	_, e := s.Call(ctx, actor, "create_task", map[string]any{"operation_id": "missing-check", "assignee": actor, "body": "work", "verification": []any{map[string]any{"id": "gate:tx:missing-check"}}})
	errStatus(t, e, 400)
	if !strings.Contains(fmt.Sprint(e), "gate:tx:missing-check") {
		t.Error("must name missing instruction", e)
	}
	task := call(t, s, "create_task", map[string]any{"operation_id": "frozen-check", "assignee": actor, "body": "work", "verification": []any{map[string]any{"id": "gate:tx:child"}}})
	a := taskExpected(task)
	a["verification"] = []any{}
	bad(t, s, "update_task", a, 400)
}

func TestBindingGracePeriodGC(t *testing.T) {
	s := setup(t)
	m := create(t, s, "gate:tx:grace", "old")
	a := expected(m)
	a["body"] = "new"
	call(t, s, "update_text", a)
	p := filepath.Join(s.dir, "objects", m["content_hash"].(string))
	if _, e := os.Stat(p); e != nil {
		t.Fatal("newly unreferenced blob lost before grace", e)
	}
	if e := s.Collect(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(p); e != nil {
		t.Fatal(e)
	}
	old := time.Now().Add(-48 * time.Hour)
	if e := os.Chtimes(p, old, old); e != nil {
		t.Fatal(e)
	}
	if e := s.Collect(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(p); !os.IsNotExist(e) {
		t.Fatal("old orphan not collected", e)
	}
}
