package content

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func fixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "note.md"), []byte("# Original\n"), 0600); err != nil {
		t.Fatal(err)
	}
	catalog := filepath.Join(t.TempDir(), "catalog.json")
	b, _ := json.Marshal(Catalog{Documents: []Document{{ID: "note", Title: "Note", Path: "note.md"}}, Resources: []Resource{{ID: "repo", Title: "Repo", Kind: "repository", Locations: []Location{{Protocol: "ssh", URL: "ssh://git@example.invalid/project.git"}}}}})
	if err := os.WriteFile(catalog, b, 0600); err != nil {
		t.Fatal(err)
	}
	return root, catalog
}

func request(h http.Handler, method, path, body, version string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if method == "PUT" {
		r.Header.Set("Content-Type", "text/markdown; charset=utf-8")
		r.Header.Set("If-Match", version)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestWriteConflictAndRestart(t *testing.T) {
	root, catalog := fixture(t)
	s, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	get := request(h, "GET", "/knowledge/note", "", "")
	if get.Code != 200 || get.Body.String() != "# Original\n" || !strings.HasPrefix(get.Header().Get("Content-Type"), "text/markdown") {
		t.Fatal(get)
	}
	version := get.Header().Get("ETag")
	if version == "" {
		t.Fatal("missing ETag")
	}
	if w := request(h, "HEAD", "/knowledge/note", "", ""); w.Code != 200 || w.Body.Len() != 0 || w.Header().Get("ETag") != version {
		t.Fatal(w)
	}
	if w := request(h, "PUT", "/knowledge/note", "replacement", ""); w.Code != 428 {
		t.Fatal(w)
	}
	if w := request(h, "PUT", "/knowledge/note", "replacement", "*"); w.Code != 400 {
		t.Fatal(w)
	}
	if w := request(h, "PUT", "/knowledge/note", "replacement", "W/"+version); w.Code != 412 {
		t.Fatal(w)
	}
	if w := request(h, "PUT", "/knowledge/note", "replacement", version); w.Code != 204 {
		t.Fatal(w)
	}
	if w := request(h, "PUT", "/knowledge/note", "overwrite", version); w.Code != 412 {
		t.Fatal(w)
	}
	// A lost-response retry cannot overwrite a newer version. Recovery is GET and
	// compare; this alpha does not claim durable per-operation replay receipts.
	if w := request(h, "PUT", "/knowledge/note", "replacement", version); w.Code != 412 {
		t.Fatal(w)
	}
	if w := request(h, "GET", "/knowledge/note", "", ""); w.Body.String() != "replacement" {
		t.Fatal(w)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if w := request(s.Handler(), "GET", "/knowledge/note", "", ""); w.Body.String() != "replacement" {
		t.Fatal(w)
	}
}

func TestConcurrentWriters(t *testing.T) {
	root, catalog := fixture(t)
	s, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	version := request(h, "GET", "/knowledge/note", "", "").Header().Get("ETag")
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, body := range []string{"worker A", "worker B"} {
		wg.Add(1)
		go func(body string) { defer wg.Done(); codes <- request(h, "PUT", "/knowledge/note", body, version).Code }(body)
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for c := range codes {
		counts[c]++
	}
	if counts[204] != 1 || counts[412] != 1 {
		t.Fatal(counts)
	}
	if second, err := Open(root, catalog); err == nil {
		second.Close()
		t.Fatal("second process/store must not acquire root")
	}
}

func TestBoundaryAndDiscovery(t *testing.T) {
	root, catalog := fixture(t)
	s, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	if w := request(h, "GET", "/knowledge", "", ""); w.Code != 200 || strings.Contains(w.Body.String(), "note.md") || !strings.Contains(w.Body.String(), "/knowledge/note") {
		t.Fatal(w)
	}
	if w := request(h, "GET", "/content/repo", "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "ssh://") {
		t.Fatal(w)
	}
	if w := request(h, "GET", "/knowledge/unregistered", "", ""); w.Code != 404 {
		t.Fatal(w)
	}
	version := request(h, "GET", "/knowledge/note", "", "").Header().Get("ETag")
	if w := request(h, "PUT", "/knowledge/note", strings.Repeat("a", MaxDocumentBytes+1), version); w.Code != 413 {
		t.Fatal(w.Code)
	}
	if w := request(h, "PUT", "/knowledge/note", string([]byte{0xff}), version); w.Code != 400 {
		t.Fatal(w.Code)
	}
	r := httptest.NewRequest("PUT", "/knowledge/note", strings.NewReader("{}"))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("If-Match", version)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatal(w)
	}
	if w := request(h, "GET", "/knowledge/note", "", ""); w.Body.String() != "# Original\n" {
		t.Fatal("rejected requests changed file")
	}
	// Reject a file replaced with a symlink after startup, including on PUT.
	outside := filepath.Join(t.TempDir(), "private.md")
	os.WriteFile(outside, []byte("private"), 0600)
	if err := os.Remove(filepath.Join(root, "note.md")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "note.md")); err != nil {
		t.Fatal(err)
	}
	if w := request(h, "GET", "/knowledge/note", "", ""); w.Code != 500 || strings.Contains(w.Body.String(), "private") {
		t.Fatal(w)
	}
}

func TestCatalogValidation(t *testing.T) {
	for _, path := range []string{"../private.md", "/tmp/private.md", "note.json"} {
		t.Run(path, func(t *testing.T) {
			root, catalog := fixture(t)
			b, _ := json.Marshal(Catalog{Documents: []Document{{ID: "note", Title: "Note", Path: path}}})
			os.WriteFile(catalog, b, 0600)
			if s, err := Open(root, catalog); err == nil {
				s.Close()
				t.Fatal("accepted invalid path")
			}
		})
	}
	for _, addr := range []string{"0.0.0.0:18081", "10.66.66.1:18081", "localhost:18081", ":18081"} {
		if ValidateLocalBind(addr) == nil {
			t.Fatalf("accepted %s", addr)
		}
	}
	for _, addr := range []string{"127.0.0.1:18081", "[::1]:18081"} {
		if err := ValidateLocalBind(addr); err != nil {
			t.Fatal(err)
		}
	}
}
