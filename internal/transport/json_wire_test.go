package transport

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestReviewWireGuardBoundsAndExactPassThrough(t *testing.T) {
	input := []byte("{\"literal\":\"�\",\"pair\":\"\\uD83D\\uDE00\"}\r\n{\"escaped\":\"\\\\ud800\"}\n")
	tr := NewStrictIOTransport(io.NopCloser(bytes.NewReader(input)), io.Discard)
	got, e := io.ReadAll(tr.Reader)
	if e != nil || !bytes.Equal(input, got) {
		t.Fatal("valid stdio framing rewritten", e)
	}
	source := strings.NewReader(strings.Repeat(" ", (7<<20)+2))
	if _, e = readJSONWireLimit(source, 7<<20); e == nil || source.Len() != 1 {
		t.Fatal("wire read not capped at limit+1", e, source.Len())
	}
	for _, raw := range [][]byte{[]byte(`{"x":"\ud800"}`), []byte(`{"\udc00":"value"}`), []byte(`{"x":"\uDFFF"}`), []byte(`{"x":"\ud800\\udc00"}`), []byte(`{"x":"\udbff\u0000"}`), []byte{'"', 0xc0, 0xaf, '"'}, []byte{'"', 0xed, 0xa0, 0x80, '"'}} {
		if validateJSONUnicode(raw) == nil {
			t.Error("Unicode guard accepted invalid scalar", string(raw))
		}
	}
	for _, raw := range []string{`{"x":"\uD800\uDC00"}`, `{"x":"\uDBFF\uDFFF"}`, `{"x":"\uFFFD"}`, `{"x":"�"}`, `{"x":"\\ud800"}`} {
		if e = validateJSONUnicode([]byte(raw)); e != nil {
			t.Error("Unicode guard rejected valid scalar", raw, e)
		}
	}
}
