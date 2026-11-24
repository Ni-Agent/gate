package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type patternReader struct{ remaining int64 }

func (r *patternReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	for i := 0; i < n; i++ {
		p[i] = 0x5a
	}
	r.remaining -= int64(n)
	return n, nil
}

type failedReader struct{}

func (failedReader) Read(p []byte) (int, error) { return 0, errors.New("interrupted source") }
func allocStart() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}
func boundedAlloc(t *testing.T, start uint64, limit uint64, label string) {
	t.Helper()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	n := m.TotalAlloc - start
	t.Logf("%s allocated %d bytes", label, n)
	if n > limit {
		t.Fatalf("%s allocated %d bytes, whole-file buffering likely", label, n)
	}
}
func TestStreamingLargeBinaryBackupReopenAndGC(t *testing.T) {
	s := setup(t)
	const size = 32 << 20
	digest := sha256.New()
	io.Copy(digest, &patternReader{size})
	want := hex.EncodeToString(digest.Sum(nil))
	start := allocStart()
	m := call(t, s, "create_binary", map[string]any{"id": "gate:bn:large-stream", "mime": "application/octet-stream", "content": &patternReader{size}})
	boundedAlloc(t, start, 8<<20, "upload 32MiB")
	if m["content_hash"] != want {
		t.Fatal(m)
	}
	start = allocStart()
	f, meta, err := s.OpenBinary(ctx, actor, m["id"].(string), 0)
	if err != nil {
		t.Fatal(err)
	}
	got := sha256.New()
	n, err := io.Copy(got, f)
	boundedAlloc(t, start, 8<<20, "download 32MiB")
	if err != nil || n != size || hex.EncodeToString(got.Sum(nil)) != want || meta["token"] != want {
		t.Fatal(n, err)
	}
	f.Close()
	destination := filepath.Join(t.TempDir(), "backup")
	start = allocStart()
	if err = s.Backup(context.Background(), destination); err != nil {
		t.Fatal(err)
	}
	boundedAlloc(t, start, 8<<20, "backup 32MiB")
	restored, err := New(destination, []string{actor})
	if err != nil {
		t.Fatal(err)
	}
	rf, _, err := restored.OpenBinary(ctx, actor, m["id"].(string), 0)
	if err != nil {
		t.Fatal(err)
	}
	rf.Close()
	restored.Close()
	// Open descriptor remains a valid read after metadata deletion and explicit GC.
	f, _, err = s.OpenBinary(ctx, actor, m["id"].(string), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	call(t, s, "delete_resource", expected(m))
	path := filepath.Join(s.dir, "objects", want)
	old := time.Now().Add(-2 * BlobGrace)
	os.Chtimes(path, old, old)
	if err = s.Collect(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("not reclaimed", err)
	}
	got.Reset()
	if n, err = io.Copy(got, f); err != nil || n != size || hex.EncodeToString(got.Sum(nil)) != want {
		t.Fatal("open download lost", n, err)
	}
}
func TestStreamingInterruptedAndStaleHaveNoMetadataEffects(t *testing.T) {
	s := setup(t)
	_, err := s.Call(ctx, actor, "create_binary", map[string]any{"id": "gate:bn:failed", "mime": "application/octet-stream", "content": io.MultiReader(strings.NewReader("prefix"), failedReader{})})
	if err == nil {
		t.Fatal("interruption ignored")
	}
	_, err = s.Call(ctx, actor, "read_resource", map[string]any{"id": "gate:bn:failed"})
	errStatus(t, err, 404)
	pending, _ := filepath.Glob(filepath.Join(s.dir, ".incoming-*"))
	if len(pending) != 0 {
		t.Fatal(pending)
	}
	m := call(t, s, "create_binary", map[string]any{"id": "gate:bn:atomic", "mime": "application/octet-stream", "content": []byte("before")})
	a := expected(m)
	a["content"] = io.MultiReader(strings.NewReader("after"), failedReader{})
	a["mime"] = "application/octet-stream"
	if _, err = s.Call(ctx, actor, "update_binary", a); err == nil {
		t.Fatal("failed update succeeded")
	}
	a["content"] = &patternReader{10 << 20}
	a["expected_hash"] = strings.Repeat("0", 64)
	_, err = s.Call(ctx, actor, "update_binary", a)
	errStatus(t, err, 412)
	b, got, err := s.ReadBytes(ctx, actor, m["id"].(string), 0)
	if err != nil || string(b) != "before" || got["token"] != m["token"] {
		t.Fatal(got, err)
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.Call(c, actor, "create_binary", map[string]any{"id": "gate:bn:cancel", "mime": "application/octet-stream", "content": &patternReader{100}})
	if err == nil {
		t.Fatal("canceled upload committed")
	}
}
func TestLargeTaskAndAdvisorySelectionDoesNotGateCompletion(t *testing.T) {
	s := setup(t)
	doc := create(t, s, "gate:tx:guidance", "use judgment")
	body := strings.Repeat("task body\n", 200000)
	m := call(t, s, "create_task", map[string]any{"operation_id": "large-advisory", "assignee": actor, "body": body, "verification": []any{map[string]any{"id": doc["id"]}}})
	if m["body"] != body {
		t.Fatal("large task changed")
	}
	call(t, s, "delete_resource", expected(doc))
	a := taskExpected(m)
	a["status"] = "done"
	a["body"] = "Executor reports unavailable guidance; completed using judgment."
	m = call(t, s, "update_task", a)
	if m["status"] != "done" {
		t.Fatal(m)
	}
}
