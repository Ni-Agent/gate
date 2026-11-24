package transport

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

type streamPattern struct{ left int64 }

func (r *streamPattern) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.left {
		n = int(r.left)
	}
	for i := 0; i < n; i++ {
		p[i] = 0x5a
	}
	r.left -= int64(n)
	return n, nil
}

type digestResponse struct {
	header http.Header
	status int
	bytes  int64
	digest hash.Hash
}

func (w *digestResponse) Header() http.Header { return w.header }
func (w *digestResponse) WriteHeader(n int)   { w.status = n }
func (w *digestResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, e := w.digest.Write(p)
	w.bytes += int64(n)
	return n, e
}
func TestHTTPStreamingLargeUnknownLengthAndOperationalLimits(t *testing.T) {
	s, h := fixture(t)
	const size = 32 << 20
	makeRequest := func(id string, length int64) *http.Request {
		r := httptest.NewRequest("POST", "https://test.invalid/binary", &streamPattern{size})
		r.ContentLength = length
		r.Header.Set("Authorization", "Bearer fixture-token-hermes")
		r.Header.Set("Content-Type", "application/octet-stream")
		r.Header.Set("X-Gate-ID", id)
		r.Header.Set("X-Gate-Mime", "application/octet-stream")
		return r
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, makeRequest("gate:bn:http-large", -1))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	sink := &digestResponse{header: http.Header{}, digest: sha256.New()}
	r := httptest.NewRequest("GET", "https://test.invalid/binary?id=gate:bn:http-large", nil)
	r.Header.Set("Authorization", "Bearer fixture-token-hermes")
	h.ServeHTTP(sink, r)
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("32MiB HTTP upload+download allocated %d bytes", allocated)
	if allocated > 8<<20 {
		t.Fatal("HTTP buffered content", allocated)
	}
	want := sha256.New()
	io.Copy(want, &streamPattern{size})
	digest := hex.EncodeToString(want.Sum(nil))
	if sink.status != 200 || sink.bytes != size || hex.EncodeToString(sink.digest.Sum(nil)) != digest || sink.header.Get("ETag") != fmt.Sprintf("%q", digest) {
		t.Fatal(sink.status, sink.bytes, sink.header)
	}
	limited := NewWithOptions(s, map[string]string{"fixture-token-hermes": "agent:hermes"}, Options{MaxBinaryBytes: 1024})
	for _, length := range []int64{-1, size} {
		w = httptest.NewRecorder()
		limited.ServeHTTP(w, makeRequest("gate:bn:limited", length))
		if w.Code != 413 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if code, _ := rpc(t, h, "read_resource", map[string]any{"id": "gate:bn:limited"}); code != 404 {
		t.Fatal("limited upload committed")
	}
	for _, length := range []int64{size - 1, size + 1} {
		w = httptest.NewRecorder()
		h.ServeHTTP(w, makeRequest("gate:bn:length-mismatch", length))
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if code, _ := rpc(t, h, "read_resource", map[string]any{"id": "gate:bn:length-mismatch"}); code != 404 {
		t.Fatal("length mismatch committed")
	}
}
func TestConfiguredStdioFrameLimit(t *testing.T) {
	input := strings.NewReader(`{"x":"` + strings.Repeat("x", 2048) + `"}` + "\n")
	tr := NewStrictIOTransportWithLimit(io.NopCloser(input), io.Discard, 1024)
	if _, e := io.ReadAll(tr.Reader); e == nil {
		t.Fatal("configured stdio cap ignored")
	}
	raw := []byte("{\"x\":\"" + strings.Repeat("x", 4096) + "\"}\n")
	unlimited := NewStrictIOTransport(io.NopCloser(bytes.NewReader(raw)), io.Discard)
	got, e := io.ReadAll(unlimited.Reader)
	if e != nil || !bytes.Equal(got, raw) {
		t.Fatal(e)
	}
}
