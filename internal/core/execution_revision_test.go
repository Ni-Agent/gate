package core

import "testing"

func TestExecutionRevisionIgnoresProgressAndPreservesReconnectEnvelope(t *testing.T) {
	s := setup(t)
	m := call(t, s, "create_task", map[string]any{"id": "gate:tk:delivery", "operation_id": "delivery", "assignee": actor, "body": "initial", "status": "ready"})
	revision, at := m["execution_revision"], m["execution_updated_at"]
	for _, change := range []map[string]any{{"status": "in_progress"}, {"add_session": "native:test"}, {"priority": 0}} {
		a := taskExpected(m)
		for k, v := range change {
			a[k] = v
		}
		m = call(t, s, "update_task", a)
		if m["execution_revision"] != revision || m["execution_updated_at"] != at {
			t.Fatal("advisory metadata changed delivery", m)
		}
	}
	a := taskExpected(m)
	a["body"] = "new body"
	m = call(t, s, "update_task", a)
	if m["execution_revision"] != 2 {
		t.Fatal(m)
	}
	a = taskExpected(m)
	a["status"] = "done"
	m = call(t, s, "update_task", a)
	if m["execution_revision"] != 2 {
		t.Fatal("terminal metadata changed revision", m)
	}
	a = taskExpected(m)
	a["status"] = "ready"
	m = call(t, s, "update_task", a)
	if m["execution_revision"] != 3 {
		t.Fatal("explicit requeue not delivered", m)
	}
	a = taskExpected(m)
	a["assignee"] = "agent:mai"
	m = call(t, s, "update_task", a)
	if m["execution_revision"] != 4 {
		t.Fatal("assignment not delivered", m)
	}
	s.Close()
	next, e := New(s.dir, []string{actor, "agent:mai"})
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	reopened := call(t, next, "read_resource", map[string]any{"id": m["id"]})
	if reopened["execution_revision"] != m["execution_revision"] || reopened["execution_updated_at"] != m["execution_updated_at"] {
		t.Fatal("reopen changed envelope")
	}
}
