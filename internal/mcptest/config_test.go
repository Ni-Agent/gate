package mcptest

import "testing"

func TestPublicURLBoundary(t *testing.T) {
	for _, tc := range []struct {
		url string
		ok  bool
	}{
		{"http://127.0.0.1:18084", true}, {"https://example.com", true},
		{"http://localhost:18084", false}, {"http://127.0.0.2:18084", false},
		{"http://example.com", false}, {"https://example.com/path", false},
		{"http://127.0.0.1:18084/", false}, {"https://user@example.com", false},
	} {
		if got := (Config{PublicURL: tc.url}).Validate() == nil; got != tc.ok {
			t.Errorf("%s: valid=%v", tc.url, got)
		}
	}
}
