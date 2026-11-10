package gate

import (
	"encoding/json"
	"fmt"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"net/http"
	"strconv"
	"time"
)

// Events are committed with task state and receipts, never only held in RAM.
type Event struct {
	ID                  uint64    `json:"id"`
	Type                string    `json:"type"`
	TaskID              string    `json:"task_id,omitempty"`
	Responsible         string    `json:"responsible,omitempty"`
	PreviousResponsible string    `json:"previous_responsible,omitempty"`
	Revision            string    `json:"revision,omitempty"`
	Status              string    `json:"status,omitempty"`
	At                  time.Time `json:"at"`
}

func (s *Store) signal() {
	for ch := range s.watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
func (s *Store) subscribe() (chan struct{}, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.watchers) >= 32 {
		return nil, nil, fmt.Errorf("stream limit reached")
	}
	ch := make(chan struct{}, 1)
	s.watchers[ch] = true
	return ch, func() { s.mu.Lock(); delete(s.watchers, ch); s.mu.Unlock() }, nil
}
func (s *Store) replay(cursor uint64, responsible string) ([]Event, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, e := s.load()
	if e != nil {
		return nil, 0, e
	}
	events := []Event{}
	for _, event := range st.Events {
		if event.ID > cursor && (event.Responsible == responsible || event.PreviousResponsible == responsible) {
			events = append(events, event)
		}
	}
	return events, st.EventSequence, nil
}
func (s *Store) EventsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for k, v := range q {
			if k != "responsible" || len(v) != 1 {
				http.Error(w, "only responsible filter is supported", 400)
				return
			}
		}
		info := auth.TokenInfoFromContext(r.Context())
		responsible := q.Get("responsible")
		if responsible == "" {
			responsible = info.UserID
		}
		if !identityID.MatchString(responsible) {
			http.Error(w, "invalid identity", 400)
			return
		}
		cursor := uint64(0)
		if v := r.Header.Get("Last-Event-ID"); v != "" {
			var e error
			cursor, e = strconv.ParseUint(v, 10, 64)
			if e != nil {
				http.Error(w, "invalid cursor", 400)
				return
			}
		}
		ch, unsubscribe, e := s.subscribe()
		if e != nil {
			http.Error(w, "stream limit reached", 429)
			return
		}
		defer unsubscribe()
		controller := http.NewResponseController(w)
		if e = controller.SetWriteDeadline(time.Time{}); e != nil && e != http.ErrNotSupported {
			http.Error(w, "stream unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		send := func(kind string, id uint64, v any) error {
			b, e := json.Marshal(v)
			if e != nil {
				return e
			}
			_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, e = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, kind, b); e != nil {
				return e
			}
			return controller.Flush()
		}
		replay := func(initial bool) bool {
			events, head, e := s.replay(cursor, responsible)
			if e != nil {
				return false
			}
			if cursor > head {
				if send("reset", head, map[string]any{"cursor": head, "reason": "cursor_ahead_of_history"}) != nil {
					return false
				}
				cursor = head
				return true
			}
			for _, event := range events {
				if send("task", event.ID, event) != nil {
					return false
				}
			}
			cursor = head
			kind := "checkpoint"
			if initial {
				kind = "ready"
			}
			return send(kind, head, map[string]any{"cursor": head}) == nil
		}
		if !replay(true) {
			return
		}
		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()
		expiry := time.NewTimer(time.Until(info.Expiration))
		defer expiry.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-expiry.C:
				return // Re-authenticate and resume rather than outliving a grant.
			case <-ch:
				if !replay(false) {
					return
				}
			case <-heartbeat.C:
				_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, e = fmt.Fprint(w, ": keepalive\n\n"); e != nil {
					return
				}
				if controller.Flush() != nil {
					return
				}
			}
		}
	})
}
