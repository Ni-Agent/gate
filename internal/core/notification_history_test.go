package core

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func history(t *testing.T, s *Store, a map[string]any) map[string]any {
	t.Helper()
	return call(t, s, "list_notification_history", a)
}
func diagnostics(m map[string]any) map[string]any { return m["diagnostics"].(map[string]any) }
func TestNotificationHistoryPersistenceAndInterestedIdentities(t *testing.T) {
	s := setup(t)
	old, stop := s.Subscribe(actor)
	defer stop()
	next, stop2 := s.Subscribe("agent:mai")
	defer stop2()
	a := map[string]any{"operation_id": "history", "assignee": actor, "body": "secret original body"}
	m := call(t, s, "create_task", a)
	first := <-old
	if first.ID != m["id"] {
		t.Fatal(first)
	}
	// Idempotent create does not invent a second handoff.
	call(t, s, "create_task", a)
	update := taskExpected(m)
	update["assignee"] = "agent:mai"
	update["body"] = "secret replacement body"
	m = call(t, s, "update_task", update)
	for _, ch := range []<-chan Event{old, next} {
		e := <-ch
		if e.Assignee != "agent:mai" || e.PreviousAssignee != actor {
			t.Fatal(e)
		}
	}
	update = taskExpected(m)
	update["body"] = "body-only update"
	call(t, s, "update_task", update)
	if e := <-next; e.PreviousAssignee != "agent:mai" {
		t.Fatal(e)
	}
	out := history(t, s, nil)
	if len(items(out)) != 2 {
		t.Fatal(out)
	}
	event := items(out)[0].(map[string]any)
	if !reflect.DeepEqual(event["interested_identities"], []string{"agent:mai", actor}) {
		t.Fatal(event)
	}
	if _, ok := event["body"]; ok {
		t.Fatal("history leaked body", event)
	}
	mai := history(t, s, map[string]any{"identity": "agent:mai"})
	if len(items(mai)) != 2 || !reflect.DeepEqual(items(mai)[0].(map[string]any)["interested_identities"], []string{"agent:mai"}) {
		t.Fatal(mai)
	}
	empty := history(t, s, map[string]any{"identity": "human:d0f"})
	if len(items(empty)) != 0 || diagnostics(empty)["oldest_event_at"] != nil {
		t.Fatal(empty)
	}
	started := diagnostics(out)["history_started_at"]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(s.dir, []string{actor, "agent:mai"})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got := history(t, reopened, nil)
	if len(items(got)) != 2 || diagnostics(got)["history_started_at"] != started || !reflect.DeepEqual(items(got), items(out)) {
		t.Fatal(got, out)
	}
}

func TestNotificationHistoryTimePagingRetentionAndTaskFilter(t *testing.T) {
	s := setup(t)
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		if err := s.recordNotification(Event{Type: "task.updated", ID: "gate:tk:history-" + strconv.Itoa(i%2), Assignee: actor}, base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	from, to := historyStamp(base.Add(time.Minute)), historyStamp(base.Add(4*time.Minute))
	seen := []string{}
	after := ""
	for {
		out := history(t, s, map[string]any{"from": from, "to": to, "after": after, "limit": 1})
		for _, v := range items(out) {
			seen = append(seen, v.(map[string]any)["cursor"].(string))
		}
		if out["has_more"] == false {
			if out["next_cursor"] != nil {
				t.Fatal(out)
			}
			break
		}
		after = out["next_cursor"].(string)
	}
	if !reflect.DeepEqual(seen, []string{"4", "3", "2"}) {
		t.Fatal(seen)
	}
	out := history(t, s, map[string]any{"task_id": "gate:tk:history-1", "from": from, "to": to})
	if len(items(out)) != 2 || diagnostics(out)["retained_event_count"] != int64(5) {
		t.Fatal(out)
	}
	// UTC offsets compare as instants, not lexicographic caller text.
	offset := base.Add(time.Minute).In(time.FixedZone("offset", 3600)).Format(time.RFC3339Nano)
	if len(items(history(t, s, map[string]any{"from": offset, "to": to}))) != 3 {
		t.Fatal("offset filter")
	}
	// Stale rows are hidden and physically pruned on inspection even without new writes.
	if _, err := s.db.Exec("UPDATE notification_history SET emitted_at=? WHERE seq=1", historyStamp(base.Add(-25*time.Hour))); err != nil {
		t.Fatal(err)
	}
	out = history(t, s, map[string]any{"from": historyStamp(base.Add(-48 * time.Hour))})
	if len(items(out)) != 4 || diagnostics(out)["retention_seconds"] != int64(86400) {
		t.Fatal(out)
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM notification_history").Scan(&count); err != nil || count != 4 {
		t.Fatal(count, err)
	}
	if diagnostics(out)["effective_from"] != diagnostics(out)["retained_from"] || diagnostics(out)["oldest_event_at"] != from {
		t.Fatal(out)
	}
}

func TestNotificationHistoryPrunesOnRecordAndReopen(t *testing.T) {
	s := setup(t)
	if err := s.recordNotification(Event{Type: "task.updated", ID: "gate:tk:expired", Assignee: actor}, time.Now().Add(-25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.recordNotification(Event{Type: "task.updated", ID: "gate:tk:current", Assignee: actor}, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM notification_history").Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if _, err := s.db.Exec("UPDATE notification_history SET emitted_at=?", historyStamp(time.Now().Add(-25*time.Hour))); err != nil {
		t.Fatal(err)
	}
	s.Close()
	reopened, err := New(s.dir, []string{actor})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err = reopened.db.QueryRow("SELECT count(*) FROM notification_history").Scan(&n); err != nil || n != 0 {
		t.Fatal(n, err)
	}
}

func TestNotificationHistoryAbsenceAcrossFailureBoundaries(t *testing.T) {
	for _, stage := range []string{"before_commit", "after_commit", "notification_history"} {
		t.Run(stage, func(t *testing.T) {
			s := setup(t)
			ch, cancel := s.Subscribe(actor)
			defer cancel()
			s.fault = func(point string) error {
				if point == stage {
					return errors.New("injected")
				}
				return nil
			}
			m, err := s.Call(ctx, actor, "create_task", map[string]any{"id": "gate:tk:failure", "operation_id": "failure", "assignee": actor, "body": "committed?"})
			s.fault = nil
			if stage == "notification_history" {
				if err != nil || m["id"] != "gate:tk:failure" {
					t.Fatal(m, err)
				}
				if e := <-ch; e.ID != "gate:tk:failure" {
					t.Fatal(e)
				}
			} else if err == nil {
				t.Fatal("fault ignored")
			}
			_, readErr := s.Call(ctx, actor, "read_resource", map[string]any{"id": "gate:tk:failure"})
			if stage == "before_commit" {
				errStatus(t, readErr, 404)
				select {
				case e := <-ch:
					t.Fatal(e)
				default:
				}
			} else if readErr != nil {
				t.Fatal(readErr)
			}
			if stage == "after_commit" {
				if _, open := <-ch; open {
					t.Fatal("must reset subscription")
				}
			}
			out := history(t, s, nil)
			if len(items(out)) != 0 {
				t.Fatal("invented handoff", out)
			}
			want := int64(0)
			if stage == "notification_history" {
				want = 1
			}
			d := diagnostics(out)
			if d["recording_failures_since_process_start"] != want || d["exhaustive"] != false || d["delivery_receipt"] != false || d["replay_supported"] != false {
				t.Fatal(d)
			}
			if want == 1 && d["last_recording_failure_at"] == nil {
				t.Fatal(d)
			}
		})
	}
}

func TestNotificationHistoryClassificationAndNoop(t *testing.T) {
	s := setup(t)
	m := call(t, s, "create_task", map[string]any{"operation_id": "classify", "assignee": actor, "body": "task"})
	tag(t, s, "gate:tg:history", "topic", "history")
	for _, op := range []string{"tag_resource", "tag_resource", "untag_resource", "untag_resource"} {
		call(t, s, op, map[string]any{"resource_id": m["id"], "tag_id": "gate:tg:history"})
	}
	if n := len(items(history(t, s, nil))); n != 3 {
		t.Fatal(n)
	}
}

func TestNotificationHistoryValidationAndTrustedIdentity(t *testing.T) {
	s := setup(t)
	for _, a := range []map[string]any{{"after": "01"}, {"after": "-1"}, {"after": "9223372036854775808"}, {"after": 1}, {"limit": 201}, {"limit": -1}, {"identity": 12}, {"from": "yesterday"}, {"from": "2026-01-02T00:00:00Z", "to": "2026-01-01T00:00:00Z"}, {"task_id": "gate:tx:wrong"}, {"actor": actor}, {"task_id": ""}} {
		bad(t, s, "list_notification_history", a, 400)
	}
	bad(t, s, "list_notification_history", map[string]any{"identity": "agent:unknown"}, 403)
	_, err := s.Call(ctx, "agent:unknown", "list_notification_history", nil)
	errStatus(t, err, 403)
	// All registered identities are trusted; query filters do not grant acting identity.
	if _, err = s.Call(ctx, "agent:mai", "list_notification_history", map[string]any{"identity": actor}); err != nil {
		t.Fatal(err)
	}
	empty := history(t, s, map[string]any{"task_id": "gate:tk:missing"})
	if len(items(empty)) != 0 || empty["has_more"] != false || empty["next_cursor"] != nil {
		t.Fatal(empty)
	}
}
