package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalHostBoundary(t *testing.T) {
	h := requireHost("127.0.0.1:18084", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for _, tc := range []struct {
		host   string
		status int
	}{{"127.0.0.1:18084", 204}, {"evil.example:18084", 403}, {"localhost:18084", 403}, {"127.0.0.1", 403}} {
		req := httptest.NewRequest("GET", "http://127.0.0.1:18084/healthz", nil)
		req.Host = tc.host
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != tc.status {
			t.Errorf("host %q: got %d", tc.host, res.Code)
		}
	}
}
