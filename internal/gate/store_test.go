package gate

import (
	"context"
	"encoding/json"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, e := Open(filepath.Join(t.TempDir(), "kb.git"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	return s
}
func want(t *testing.T, r Result, status int) Result {
	t.Helper()
	if r.Status != status {
		t.Fatalf("want %d got %+v", status, r)
	}
	return r
}
func TestPublication(t *testing.T) {
	s := openTest(t)
	actor := "human:owner"
	p := "/kb/projects/gate"
	r := want(t, s.Write(actor, "PUT", p, "# Gate\nRepo: git:example-project", "*", "create"), 204)
	before := s.head()
	want(t, s.Write(actor, "PUT", p, "other", "*", "other"), 412)
	want(t, s.Write(actor, "PUT", p, "lost response", r.ETag, "save"), 204)
	head := s.head()
	want(t, s.Write(actor, "PUT", p, "lost response", r.ETag, "save"), 204)
	if s.head() != head {
		t.Fatal("retry created another revision")
	}
	want(t, s.Write(actor, "PUT", p, "different", r.ETag, "save"), 409)
	want(t, s.Write(actor, "PUT", p, "stale", r.ETag, "stale"), 412)
	b, e := s.read(before, "kb/projects/gate.md")
	if e != nil || !strings.Contains(string(b), "git:example-project") {
		t.Fatal("history missing", e)
	}
	root := s.root
	s.Close()
	s, e = Open(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	want(t, s.Write(actor, "PUT", p, "lost response", r.ETag, "save"), 204)
	if s.Read(p, nil).Body != "lost response" {
		t.Fatal("restart lost write")
	}
}
func TestConcurrentAndBoundary(t *testing.T) {
	s := openTest(t)
	r := want(t, s.Write("human:owner", "PUT", "/kb/projects/p", "base", "*", "base"), 204)
	var wg sync.WaitGroup
	statuses := make(chan int, 2)
	for _, key := range []string{"a", "b"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			statuses <- s.Write("human:owner", "PUT", "/kb/projects/p", k, r.ETag, k).Status
		}(key)
	}
	wg.Wait()
	close(statuses)
	counts := map[int]int{}
	for n := range statuses {
		counts[n]++
	}
	if counts[204] != 1 || counts[412] != 1 {
		t.Fatal(counts)
	}
	for _, p := range []string{"/kb/../private", "/kb/.gate/state.json", "/kb/projects//p", "/kb/projects/P"} {
		want(t, s.Read(p, nil), 400)
	}
	want(t, s.Read("/secret", nil), 404)
	if _, e := Open(s.root); e == nil {
		t.Fatal("second writer permitted")
	}
}
func TestTasks(t *testing.T) {
	s := openTest(t)
	actor := "human:owner"
	want(t, s.Write(actor, "POST", "/identities", `{"id":"agent:hermes","name":"Hermes"}`, "", "identity"), 201)
	body := `{"project":"gate","title":"Send me a screenshot of your current work","responsible":"agent:hermes"}`
	r := want(t, s.Write(actor, "POST", "/tasks", body, "", "create-task"), 201)
	var task Task
	json.Unmarshal([]byte(r.Body), &task)
	if task.Responsible != "agent:hermes" || task.Status != "open" {
		t.Fatal(task)
	}
	want(t, s.Write(actor, "POST", "/tasks", `{"title":"No owner"}`, "", "missing"), 400)
	want(t, s.Write(actor, "PUT", "/tasks/"+task.ID, body, "", "missing-tag"), 428)
	want(t, s.Write(actor, "PUT", "/tasks/"+task.ID, body, r.ETag, "update"), 200)
	want(t, s.Write(actor, "PUT", "/tasks/"+task.ID, body, r.ETag, "stale"), 412)
	if !strings.Contains(s.Read("/tasks", map[string]string{"project": "gate", "responsible": "agent:hermes"}).Body, task.ID) {
		t.Fatal("filter")
	}
}
func TestMCPAndHTTP(t *testing.T) {
	s := openTest(t)
	verify := func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		if token != "test-token" {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{UserID: "human:owner", Expiration: time.Now().Add(time.Hour)}, nil
	}
	h, e := Handler(s, "https://gate.example", verify)
	if e != nil {
		t.Fatal(e)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	res, e := http.Get(srv.URL + "/kb")
	if e != nil {
		t.Fatal(e)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatal(res.StatusCode)
	}
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, e := c.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: testTransport{}}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	tools, e := session.ListTools(context.Background(), nil)
	if e != nil || len(tools.Tools) != 2 {
		t.Fatal(tools, e)
	}
	out, e := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "write_workspace", Arguments: WriteArgs{Method: "PUT", Path: "/kb/projects/gate", Body: "# Gate", ETag: "*", OperationID: "mcp-create"}})
	if e != nil || out.IsError {
		t.Fatal(out, e)
	}
	if s.Read("/kb/projects/gate", nil).Body != "# Gate" {
		t.Fatal("MCP write missing")
	}
	out, e = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "read_workspace", Arguments: ReadArgs{Path: "/kb?q=Gate"}})
	if e != nil || out.IsError {
		t.Fatal(out, e)
	}
	out, e = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "read_workspace", Arguments: ReadArgs{Path: "https://evil.example"}})
	if e != nil || !out.IsError {
		t.Fatal("URL accepted", out, e)
	}
}

type testTransport struct{}

func (testTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer test-token")
	return http.DefaultTransport.RoundTrip(r)
}
