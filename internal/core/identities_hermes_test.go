package core

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestHermesProfileIdentities(t *testing.T) {
	identities := []string{"hermes:default", "hermes:prometheus"}
	s, err := New(filepath.Join(t.TempDir(), "store"), identities)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	task, err := s.Call(ctx, identities[0], "create_task", map[string]any{
		"operation_id": "profile-identity", "assignee": identities[1], "body": "Collaborative task", "status": "ready",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task["creator"] != identities[0] || task["assignee"] != identities[1] {
		t.Fatal(task)
	}
	changed, err := s.Call(ctx, identities[1], "update_task", map[string]any{
		"id": task["id"], "expected_token": task["token"], "status": "done",
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed["modified_by"] != identities[1] || changed["status"] != "done" {
		t.Fatal(changed)
	}
}

func TestIdentityGrammar(t *testing.T) {
	for _, identity := range []string{"agent:old", "human:operator", "hermes:default", "hermes:foo.bar_baz-1", "hermes:" + strings.Repeat("x", 121)} {
		t.Run("valid/"+identity, func(t *testing.T) {
			s, err := New(filepath.Join(t.TempDir(), "store"), []string{identity})
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
		})
	}
	for _, identity := range []string{"default", "other:default", "hermes:", "hermes:Default", "hermes:-foo", "hermes:foo--bar", "hermes:foo-", "hermes:" + strings.Repeat("x", 122)} {
		t.Run("invalid/"+identity, func(t *testing.T) {
			s, err := New(filepath.Join(t.TempDir(), "store"), []string{identity})
			if err == nil {
				s.Close()
				t.Fatal("accepted invalid identity")
			}
		})
	}
}
