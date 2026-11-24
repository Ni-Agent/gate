package transport

import (
	"strings"
	"testing"
	"time"
)

func TestSSETransportHeartbeatAndStoreShutdown(t *testing.T) {
	s, h := fixture(t)
	frames, stop := stream(t, h, "fixture-token-hermes")
	defer stop()
	start := time.Now()
	select {
	case f := <-frames:
		if !strings.Contains(f, ": transport-health") {
			t.Fatal(f)
		}
	case <-time.After(24 * time.Second):
		t.Fatal("transport heartbeat missing")
	}
	t.Logf("actual configured SSE heartbeat received after %s", time.Since(start))
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	stop() // handler must finish on store closure; context cancellation remains safe.
}
