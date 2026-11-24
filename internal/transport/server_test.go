package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gate-alpha/internal/core"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func fixture(t *testing.T) (*core.Store, http.Handler) {
	t.Helper()
	s, e := core.New(filepath.Join(t.TempDir(), "store"), []string{"agent:hermes", "agent:mai", "human:d0f"})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, New(s, map[string]string{"fixture-token-hermes": "agent:hermes", "fixture-token-mai": "agent:mai", "fixture-token-human": "human:d0f"})
}
func request(t *testing.T, h http.Handler, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "https://test.invalid"+path, bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer fixture-token-hermes")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func rpc(t *testing.T, h http.Handler, op string, a map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(a)
	w := request(t, h, "POST", "/rpc/"+op, b, map[string]string{"Content-Type": "application/json"})
	var out map[string]any
	if e := json.Unmarshal(w.Body.Bytes(), &out); e != nil {
		t.Fatalf("%s %d: %s", op, w.Code, w.Body.String())
	}
	return w.Code, out
}
func okRPC(t *testing.T, h http.Handler, op string, a map[string]any) map[string]any {
	t.Helper()
	code, out := rpc(t, h, op, a)
	if code != 200 {
		t.Fatalf("%s: %d %v", op, code, out)
	}
	return out
}
func pre(m map[string]any) map[string]any {
	return map[string]any{"id": m["id"], "expected_revision": m["revision"], "expected_hash": m["token"]}
}
func TestHTTPAuthenticationSchemasAndIdentity(t *testing.T) {
	_, h := fixture(t)
	for _, headers := range []map[string]string{{"Authorization": ""}, {"Authorization": "Bearer wrong"}, {"X-Gate-Actor": "human:d0f"}, {"Origin": "https://evil.invalid"}} {
		w := request(t, h, "POST", "/rpc/list_tags", []byte("{}"), headers)
		if w.Code != 401 && w.Code != 400 && w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	code, out := rpc(t, h, "create_task", map[string]any{"operation_id": "http-task", "body": "created", "assignee": "agent:mai", "status": "ready"})
	if code != 200 || out["creator"] != "agent:hermes" {
		t.Fatal(code, out)
	}
	code, _ = rpc(t, h, "update_task", map[string]any{"id": out["id"], "expected_token": out["token"], "creator": "human:d0f"})
	if code != 400 {
		t.Fatal(code)
	}
	code, _ = rpc(t, h, "read_resource", map[string]any{"id": "gate:tx:Bad"})
	if code != 400 {
		t.Fatal(code)
	}
	code, _ = rpc(t, h, "read_resource", map[string]any{"id": "gate:tx:missing", "arbitrary": true})
	if code != 400 {
		t.Fatal(code)
	}
	for _, path := range []string{"/binary?id=gate:bn:x&extra=1", "/binary?id=gate:bn:x&id=gate:bn:y"} {
		w := request(t, h, "GET", path, nil, nil)
		if w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
}
func TestHTTPBinaryFullLifecycleLimitsAndNoFallback(t *testing.T) {
	_, h := fixture(t)
	headers := map[string]string{"Content-Type": "application/octet-stream", "X-Gate-ID": "gate:bn:payload", "X-Gate-Mime": "application/octet-stream"}
	original := []byte{0, 255, 1, 2, 3, 0, 128}
	w := request(t, h, "POST", "/binary", original, headers)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var m map[string]any
	json.Unmarshal(w.Body.Bytes(), &m)
	w = request(t, h, "GET", "/binary?id=gate:bn:payload", nil, nil)
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), original) || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Content-Disposition") == "" {
		t.Fatal(w.Code, w.Body.String())
	}
	sealed := okRPC(t, h, "seal_revision", pre(m))
	if sealed["revision"] != float64(2) {
		t.Fatal(sealed)
	}
	headers["If-Match"] = sealed["token"].(string)
	headers["X-Gate-Revision"] = "2"
	updated := []byte("updated\x00bytes")
	w = request(t, h, "PUT", "/binary", updated, headers)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	json.Unmarshal(w.Body.Bytes(), &m)
	w = request(t, h, "PUT", "/binary", []byte("stale"), headers)
	if w.Code != 412 {
		t.Fatal(w.Code)
	}
	w = request(t, h, "GET", "/binary?id=gate:bn:payload&revision=1", nil, nil)
	if !bytes.Equal(w.Body.Bytes(), original) {
		t.Fatal(w.Body.String())
	}
	tomb := okRPC(t, h, "delete_resource", pre(m))
	w = request(t, h, "GET", "/binary?id=gate:bn:payload", nil, nil)
	if w.Code != 410 {
		t.Fatal(w.Code, w.Body.String())
	}
	var gone map[string]any
	json.Unmarshal(w.Body.Bytes(), &gone)
	if gone["resource"].(map[string]any)["token"] != tomb["token"] {
		t.Fatal(gone, tomb)
	}
	headers["If-Match"] = tomb["token"].(string)
	w = request(t, h, "PUT", "/binary", updated, headers)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	headers["X-Gate-Tags"] = "[]"
	w = request(t, h, "PUT", "/binary", updated, headers)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	delete(headers, "X-Gate-Tags")
	headers["X-Gate-ID"] = "gate:bn:oversized"
	delete(headers, "X-Gate-Revision")
	delete(headers, "If-Match")
	w = request(t, h, "POST", "/binary", make([]byte, (8<<20)+1), headers)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	code, _ := rpc(t, h, "read_resource", map[string]any{"id": "gate:bn:oversized"})
	if code != 200 {
		t.Fatal("large content missing", code)
	}
	headers["X-Gate-Mime"] = "image/png"
	w = request(t, h, "POST", "/binary", []byte("not a png"), headers)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	code, _ = rpc(t, h, "create_binary", map[string]any{"id": "gate:bn:no-json"})
	if code != 415 {
		t.Fatal(code)
	}
}

type framesWriter struct {
	header http.Header
	mu     sync.Mutex
	buffer bytes.Buffer
	frames chan string
}

func (w *framesWriter) Header() http.Header { return w.header }
func (w *framesWriter) WriteHeader(int)     {}
func (w *framesWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}
func (w *framesWriter) Flush() {
	w.mu.Lock()
	v := w.buffer.String()
	w.buffer.Reset()
	w.mu.Unlock()
	w.frames <- v
}
func stream(t *testing.T, h http.Handler, token string) (<-chan string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "https://test.invalid/events", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+token)
	w := &framesWriter{header: http.Header{}, frames: make(chan string, 100)}
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(w, r) }()
	select {
	case f := <-w.frames:
		if !strings.Contains(f, "event: ready") {
			t.Fatal(f)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream not ready")
	}
	return w.frames, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("stream did not stop")
		}
	}
}
func frame(t *testing.T, c <-chan string) string {
	t.Helper()
	select {
	case s := <-c:
		return s
	case <-time.After(3 * time.Second):
		t.Fatal("notification missing")
		return ""
	}
}
func TestSSEAssignReadyBodyReassignmentAndReconnect(t *testing.T) {
	_, h := fixture(t)
	old, stop := stream(t, h, "fixture-token-hermes")
	defer stop()
	newc, stop2 := stream(t, h, "fixture-token-mai")
	defer stop2()
	task := okRPC(t, h, "create_task", map[string]any{"operation_id": "sse", "assignee": "agent:hermes", "body": "start", "status": "triage"})
	frame(t, old)
	task = okRPC(t, h, "update_task", map[string]any{"id": task["id"], "expected_token": task["token"], "assignee": "agent:mai", "status": "ready", "body": "assigned and ready"})
	for _, c := range []<-chan string{old, newc} {
		f := frame(t, c)
		if !strings.Contains(f, "task.updated") || !strings.Contains(f, task["id"].(string)) {
			t.Fatal(f)
		}
	}
	got := okRPC(t, h, "read_resource", map[string]any{"id": task["id"]})
	if got["body"] != "assigned and ready" || got["status"] != "ready" {
		t.Fatal(got)
	}
	stop2()
	task = okRPC(t, h, "update_task", map[string]any{"id": task["id"], "expected_token": task["token"], "body": "while disconnected"})
	_, stop3 := stream(t, h, "fixture-token-mai")
	defer stop3()
	snapshot := okRPC(t, h, "list_tasks", map[string]any{"assignee": "agent:mai", "status": "ready"})
	if len(snapshot["items"].([]any)) != 1 {
		t.Fatal(snapshot)
	}
	got = okRPC(t, h, "read_resource", map[string]any{"id": task["id"]})
	if got["body"] != "while disconnected" {
		t.Fatal(got)
	}
}

// This RoundTripper exercises the actual HTTP/MCP handlers and SDK clients but
// never opens a TCP/Unix listener or performs a network request.
type localTransport struct {
	handler http.Handler
	before  func(*http.Request)
	after   func(*http.Request)
}

func (l localTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header = r.Header.Clone()
	r.Header.Set("Authorization", "Bearer fixture-token-hermes")
	if l.before != nil {
		l.before(r)
	}
	if l.after != nil {
		defer l.after(r)
	}
	w := httptest.NewRecorder()
	l.handler.ServeHTTP(w, r)
	return w.Result(), nil
}
func connectMCP(t *testing.T, h http.Handler, before, after func(*http.Request)) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "actual-sdk-client", Version: "test"}, nil)
	session, e := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: "https://test.invalid/mcp", HTTPClient: &http.Client{Transport: localTransport{h, before, after}}, MaxRetries: -1}, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { session.Close() })
	return session
}
func tool(t *testing.T, session *mcp.ClientSession, name string, args map[string]any, wantError bool) map[string]any {
	t.Helper()
	r, e := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if e != nil {
		t.Fatal(name, e)
	}
	if r.IsError != wantError {
		t.Fatal(name, r)
	}
	b, _ := json.Marshal(r.StructuredContent)
	var m map[string]any
	if e = json.Unmarshal(b, &m); e != nil {
		t.Fatal(string(b), e)
	}
	return m
}
func TestRealSDKStreamableMCPFlowsSchemasAndFiveReadOverlap(t *testing.T) {
	_, h := fixture(t)
	var enabled atomic.Bool
	var active, peak atomic.Int32
	arrived := make(chan struct{}, 5)
	release := make(chan struct{})
	before := func(r *http.Request) {
		if r.Method != "POST" || !enabled.Load() {
			return
		}
		n := active.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		arrived <- struct{}{}
		<-release
	}
	after := func(r *http.Request) {
		if r.Method == "POST" && enabled.Load() {
			active.Add(-1)
		}
	}
	session := connectMCP(t, h, before, after)
	listed, e := session.ListTools(context.Background(), nil)
	if e != nil || len(listed.Tools) != len(operations) {
		t.Fatal(e, len(listed.Tools))
	}
	m := tool(t, session, "create_text", map[string]any{"id": "gate:tx:mcp", "body": "hello"}, false)
	m = tool(t, session, "seal_revision", pre(m), false)
	if m["revision"] != float64(2) {
		t.Fatal(m)
	}
	old := tool(t, session, "read_revision", map[string]any{"id": m["id"], "revision": 1}, false)
	if old["body"] != "hello" || old["sealed"] != true {
		t.Fatal(old)
	}
	invalid := tool(t, session, "read_resource", map[string]any{"id": m["id"], "extra": 1}, true)
	if invalid["status"] != float64(400) {
		t.Fatal(invalid)
	}
	transfer := tool(t, session, "create_binary", map[string]any{"id": "gate:bn:mcp", "mime": "application/octet-stream"}, false)
	if transfer["transfer_required"] != true || transfer["resource_created"] != false {
		t.Fatal(transfer)
	}
	enabled.Store(true)
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "read_resource", Arguments: map[string]any{"id": m["id"]}})
			if e == nil && r.IsError {
				e = fmt.Errorf("tool error %v", r)
			}
			errs <- e
		}()
	}
	for i := 0; i < 5; i++ {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("actual SDK client serialized requests", peak.Load())
		}
	}
	close(release)
	wg.Wait()
	enabled.Store(false)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Error(e)
		}
	}
	if peak.Load() != 5 {
		t.Fatal(peak.Load())
	}
	t.Logf("actual Go SDK Streamable HTTP client: peak overlapping request intervals=%d (no socket listener)", peak.Load())
	tomb := tool(t, session, "delete_resource", pre(m), false)
	gone := tool(t, session, "read_resource", map[string]any{"id": m["id"]}, true)
	if gone["status"] != float64(410) || gone["resource"].(map[string]any)["token"] != tomb["token"] {
		t.Fatal(gone)
	}
}
func TestBuiltStdioServerWithRealSDKClient(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess integration disabled only by explicit -short")
	}
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	binary := filepath.Join(t.TempDir(), "gate-alpha")
	goexe := "/home/hermes/.local/go/bin/go"
	cmd := exec.Command(goexe, "build", "-o", binary, "./cmd/gate-alpha")
	cmd.Dir = root
	if b, e := cmd.CombinedOutput(); e != nil {
		t.Fatal(string(b), e)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "built-executable-sdk-client", Version: "test"}, nil)
	cmd = exec.Command(binary, "--store", filepath.Join(t.TempDir(), "stdio-store"))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session, e := client.Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if e != nil {
		t.Fatal(e, stderr.String())
	}
	m := tool(t, session, "create_task", map[string]any{"operation_id": "stdio", "assignee": "agent:hermes", "body": "real built process", "status": "ready"}, false)
	if m["creator"] != "agent:hermes" {
		t.Fatal(m)
	}
	tool(t, session, "update_task", map[string]any{"id": m["id"], "expected_token": m["token"], "add_session": "test:stdio:session", "status": "done", "body": "verified via real SDK stdio client"}, false)
	fds, e := filepath.Glob(fmt.Sprintf("/proc/%d/fd/*", cmd.Process.Pid))
	if e != nil || len(fds) == 0 {
		t.Fatal("cannot inspect stdio child descriptors", e)
	}
	for _, fd := range fds {
		target, e := os.Readlink(fd)
		if e == nil && strings.HasPrefix(target, "socket:") {
			t.Fatal("stdio child unexpectedly opened a socket")
		}
	}
	if e = session.Close(); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(stderr.String(), "panic") {
		t.Fatal(stderr.String())
	}
}
func TestJSONRequestSizeAndTrailingData(t *testing.T) {
	s, _ := fixture(t)
	h := NewWithOptions(s, map[string]string{"fixture-token-hermes": "agent:hermes"}, Options{MaxJSONBytes: 7 << 20})
	w := request(t, h, "POST", "/rpc/list_tags", []byte("{} {}"), map[string]string{"Content-Type": "application/json"})
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	w = request(t, h, "POST", "/rpc/create_text", []byte(`{"id":"gate:tx:huge","body":"`+strings.Repeat("x", (7<<20))+`"}`), map[string]string{"Content-Type": "application/json"})
	if w.Code != 413 {
		t.Fatalf("oversize JSON status %d body %.100s", w.Code, w.Body.String())
	}
}
