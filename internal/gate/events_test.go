package gate

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func taskFixture(t *testing.T, s *Store) Task {
	r := want(t, s.Write("human:owner", "POST", "/tasks", `{"title":"Work","responsible":"human:owner"}`, "", "first"), 201)
	var task Task
	json.Unmarshal([]byte(r.Body), &task)
	return task
}
func TestEventPublicationAndClaim(t *testing.T) {
	s := openTest(t)
	task := taskFixture(t, s)
	events, cursor, e := s.replay(0, "human:owner")
	if e != nil || len(events) != 1 || cursor != 1 {
		t.Fatal(events, cursor, e)
	}
	r := s.Read("/tasks/"+task.ID, nil)
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, id := range []string{"run-a", "run-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			codes <- s.Write("human:owner", "POST", "/tasks/"+task.ID+"/claim", `{"run_id":"`+id+`"}`, r.ETag, id).Status
		}(id)
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for c := range codes {
		counts[c]++
	}
	if counts[200] != 1 || counts[412] != 1 {
		t.Fatal(counts)
	}
	_, cursor, _ = s.replay(0, "human:owner")
	if cursor != 2 {
		t.Fatal("claim not committed once")
	}
	root := s.root
	s.Close()
	s, e = Open(root)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	events, cursor, e = s.replay(1, "human:owner")
	if e != nil || len(events) != 1 || cursor != 2 || events[0].Type != "task.claimed" {
		t.Fatal(events, cursor, e)
	}
	current := s.Read("/tasks/"+task.ID, nil)
	want(t, s.Write("human:owner", "POST", "/tasks/"+task.ID+"/claim", `{"run_id":"duplicate"}`, current.ETag, "duplicate"), 409)
}
func TestSSEReplayAndWake(t *testing.T) {
	s := openTest(t)
	taskFixture(t, s)
	verify := func(context.Context, string, *http.Request) (*auth.TokenInfo, error) {
		return &auth.TokenInfo{UserID: "human:owner", Expiration: time.Now().Add(time.Minute)}, nil
	}
	h, e := Handler(s, "https://gate.example", verify)
	if e != nil {
		t.Fatal(e)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	req.Header.Set("Authorization", "Bearer test")
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	if res.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal(res.StatusCode, res.Header)
	}
	scan := bufio.NewScanner(res.Body)
	data := make(chan string, 20)
	go func() {
		for scan.Scan() {
			data <- scan.Text()
		}
		close(data)
	}()
	next := func(contains string) {
		t.Helper()
		timeout := time.After(2 * time.Second)
		for {
			select {
			case line, ok := <-data:
				if !ok {
					t.Fatal("stream closed")
				}
				if strings.Contains(line, contains) {
					return
				}
			case <-timeout:
				t.Fatal("event timeout", contains)
			}
		}
	}
	next("task.created")
	next("event: ready")
	want(t, s.Write("human:owner", "POST", "/tasks", `{"title":"Second","responsible":"human:owner"}`, "", "second"), 201)
	next("id: 2")
	cancel()
	res.Body.Close()
	req, _ = http.NewRequest("GET", srv.URL+"/events", nil)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Last-Event-ID", "1")
	res, e = http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	reader := bufio.NewReader(res.Body)
	line, _ := reader.ReadString('\n')
	if !strings.Contains(line, "id: 2") {
		t.Fatal("did not resume", line)
	}
}
func TestReplayDoesNotDuplicateEvent(t *testing.T) {
	s := openTest(t)
	body := `{"title":"Only once","responsible":"human:owner"}`
	first := want(t, s.Write("human:owner", "POST", "/tasks", body, "", "same"), 201)
	want(t, s.Write("human:owner", "POST", "/tasks", body, "", "same"), 201)
	events, _, _ := s.replay(0, "human:owner")
	if len(events) != 1 {
		t.Fatal(events)
	}
	var task Task
	json.Unmarshal([]byte(first.Body), &task)
	raw, e := s.read(s.head(), ".gate/state.json")
	if e != nil || !strings.Contains(string(raw), task.ID) {
		t.Fatal(e)
	}
	if !strings.Contains(string(raw), "task.created") {
		t.Fatal("event missing from accepted commit")
	}
}

func TestReassignmentReachesBothIdentitiesAndClearsRun(t *testing.T) {
	s := openTest(t)
	task := taskFixture(t, s)
	want(t, s.Write("human:owner", "POST", "/identities", `{"id":"agent:other","name":"Other"}`, "", "other"), 201)
	r := s.Read("/tasks/"+task.ID, nil)
	want(t, s.Write("human:owner", "POST", "/tasks/"+task.ID+"/claim", `{"run_id":"first-run"}`, r.ETag, "claim"), 200)
	r = s.Read("/tasks/"+task.ID, nil)
	updated := want(t, s.Write("human:owner", "PUT", "/tasks/"+task.ID, `{"title":"Reassigned","responsible":"agent:other","status":"open"}`, r.ETag, "reassign"), 200)
	var result Task
	json.Unmarshal([]byte(updated.Body), &result)
	if result.RunID != "" {
		t.Fatal("old worker run survived reassignment")
	}
	for _, owner := range []string{"human:owner", "agent:other"} {
		events, _, e := s.replay(2, owner)
		if e != nil || len(events) != 1 || events[0].Type != "task.updated" {
			t.Fatal(owner, events, e)
		}
	}
}
func TestEventsAuthenticationAndInput(t *testing.T) {
	s := openTest(t)
	verify := func(context.Context, string, *http.Request) (*auth.TokenInfo, error) {
		return &auth.TokenInfo{UserID: "human:owner", Expiration: time.Now().Add(time.Minute)}, nil
	}
	h, e := Handler(s, "https://gate.example", verify)
	if e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		path, cursor, bearer string
		code                 int
	}{
		{"/events", "", "", 401}, {"/whoami", "", "", 401}, {"/events", "bad", "test", 400}, {"/events?responsible=invalid%2Fid", "", "test", 400}, {"/events?extra=x", "", "test", 400},
	} {
		r := httptest.NewRequest("GET", tc.path, nil)
		if tc.bearer != "" {
			r.Header.Set("Authorization", "Bearer "+tc.bearer)
		}
		r.Header.Set("Last-Event-ID", tc.cursor)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.code {
			t.Fatal(tc, w.Code, w.Body.String())
		}
	}
}
