package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNotificationHistoryRPCAndMCPDiagnostics(t *testing.T) {
	_, h := fixture(t)
	task := okRPC(t, h, "create_task", map[string]any{"operation_id": "diagnostics", "assignee": "agent:mai", "body": "not in diagnostics"})
	task = okRPC(t, h, "update_task", map[string]any{"id": task["id"], "expected_token": task["token"], "assignee": "agent:hermes", "body": "updated"})
	out := okRPC(t, h, "list_notification_history", map[string]any{"identity": "agent:mai", "task_id": task["id"], "limit": 1})
	if out["has_more"] != true || len(out["items"].([]any)) != 1 {
		t.Fatal(out)
	}
	event := out["items"].([]any)[0].(map[string]any)
	if event["previous_assignee"] != "agent:mai" || event["assignee"] != "agent:hermes" || event["body"] != nil {
		t.Fatal(event)
	}
	if _, err := time.Parse(time.RFC3339Nano, event["emitted_at"].(string)); err != nil {
		t.Fatal(err)
	}
	d := out["diagnostics"].(map[string]any)
	if d["retention_seconds"] != float64(86400) || d["retained_event_count"] != float64(2) || d["oldest_event_at"] == nil || d["exhaustive"] != false || d["delivery_receipt"] != false || d["replay_supported"] != false {
		t.Fatal(d)
	}
	session := connectMCP(t, h, nil, nil)
	listed, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, spec := range listed.Tools {
		if spec.Name == "list_notification_history" {
			found = true
			if !strings.Contains(spec.Description, "Not audit, replay or delivery receipt") {
				t.Fatal(spec.Description)
			}
		}
	}
	if !found {
		t.Fatal("missing tool")
	}
	got := tool(t, session, "list_notification_history", map[string]any{"identity": "agent:mai", "after": out["next_cursor"], "limit": 1}, false)
	if got["has_more"] != false || len(got["items"].([]any)) != 1 || got["next_cursor"] != nil {
		t.Fatal(got)
	}
	empty := tool(t, session, "list_notification_history", map[string]any{"task_id": "gate:tk:missing"}, false)
	if len(empty["items"].([]any)) != 0 {
		t.Fatal(empty)
	}
}

func TestNotificationHistoryAuthenticationValidationAndIdentityDefaults(t *testing.T) {
	_, h := fixture(t)
	okRPC(t, h, "create_task", map[string]any{"operation_id": "mai-only", "assignee": "agent:mai", "body": "assigned"})
	for _, headers := range []map[string]string{{"Authorization": ""}, {"Authorization": "Bearer invalid"}, {"X-Gate-Identity": "agent:mai"}} {
		w := request(t, h, http.MethodPost, "/rpc/list_notification_history", []byte("{}"), headers)
		if w.Code != 400 && w.Code != 401 {
			t.Fatal(w.Code)
		}
	}
	b := []byte("{}")
	w := request(t, h, http.MethodPost, "/rpc/list_notification_history", b, map[string]string{"Content-Type": "application/json", "Authorization": "Bearer fixture-token-mai"})
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil || w.Code != 200 || m["diagnostics"].(map[string]any)["identity"] != "agent:mai" || len(m["items"].([]any)) != 1 {
		t.Fatal(w.Code, m, err)
	}
	own := okRPC(t, h, "list_notification_history", map[string]any{})
	if len(own["items"].([]any)) != 0 {
		t.Fatal(own)
	}
	for _, a := range []map[string]any{{"actor": "agent:mai"}, {"identity": 7}, {"limit": 201}, {"from": "invalid"}, {"after": "01"}} {
		if code, _ := rpc(t, h, "list_notification_history", a); code != 400 {
			t.Fatal(code, a)
		}
	}
	session := connectMCP(t, h, nil, nil)
	invalid := tool(t, session, "list_notification_history", map[string]any{"after": "01"}, true)
	if invalid["status"] != float64(400) {
		t.Fatal(invalid)
	}
}

func TestNotificationHistoryDoesNotReplaySSE(t *testing.T) {
	_, h := fixture(t)
	okRPC(t, h, "create_task", map[string]any{"operation_id": "before-connect", "assignee": "agent:hermes", "body": "already emitted"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("GET", "https://test.invalid/events", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer fixture-token-hermes")
	r.Header.Set("Last-Event-ID", "1")
	w := &framesWriter{header: http.Header{}, frames: make(chan string, 10)}
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(w, r) }()
	ready := frame(t, w.frames)
	if !strings.Contains(ready, "reconcile_assigned") || strings.Contains(ready, "task.updated") || strings.Contains(ready, "id:") {
		t.Fatal(ready)
	}
	// The handler is canceled after ready; no real SSE network client/listener.
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not stop")
	}
	select {
	case f := <-w.frames:
		t.Fatal("history unexpectedly replayed", f)
	default:
	}
	out := okRPC(t, h, "list_notification_history", map[string]any{})
	if len(out["items"].([]any)) != 1 {
		t.Fatal(out)
	}
}
