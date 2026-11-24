package core

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type pausedUpload struct {
	entered chan struct{}
	release chan struct{}
	once    bool
}

func (p *pausedUpload) Read(b []byte) (int, error) {
	if !p.once {
		p.once = true
		close(p.entered)
		<-p.release
		b[0] = 'x'
		return 1, nil
	}
	return 0, io.EOF
}
func TestSlowUploadDoesNotBlockUnrelatedWritesOrGC(t *testing.T) {
	s := setup(t)
	reader := &pausedUpload{entered: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(reader.release)
		}
	}()
	uploaded := make(chan error, 1)
	go func() {
		_, e := s.Call(ctx, actor, "create_binary", map[string]any{"id": "gate:bn:slow", "mime": "application/octet-stream", "content": reader})
		uploaded <- e
	}()
	<-reader.entered
	pending, _ := filepath.Glob(filepath.Join(s.dir, ".incoming-*"))
	if len(pending) != 1 {
		t.Fatal(pending)
	}
	old := time.Now().Add(-2 * BlobGrace)
	if e := os.Chtimes(pending[0], old, old); e != nil {
		t.Fatal(e)
	}
	c, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, e := s.Call(c, actor, "create_tag", map[string]any{"id": "gate:tg:while-upload", "key": "topic", "value": "independent"})
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow stream blocked unrelated mutation")
	}
	if e := s.Collect(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(pending[0]); e != nil {
		t.Fatal("GC removed active staged bytes", e)
	}
	close(reader.release)
	released = true
	if e := <-uploaded; e != nil {
		t.Fatal(e)
	}
	b, _, e := s.ReadBytes(ctx, actor, "gate:bn:slow", 0)
	if e != nil || string(b) != "x" {
		t.Fatal(e)
	}
}
func TestCloseDuringStagingCannotPublishAfterReopen(t *testing.T) {
	s := setup(t)
	reader := &pausedUpload{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, e := s.Call(ctx, actor, "create_binary", map[string]any{"id": "gate:bn:closing", "mime": "application/octet-stream", "content": reader})
		done <- e
	}()
	<-reader.entered
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	next, e := New(s.dir, []string{actor})
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	close(reader.release)
	if e = <-done; e == nil {
		t.Fatal("closed store published")
	}
	_, e = next.Call(ctx, actor, "read_resource", map[string]any{"id": "gate:bn:closing"})
	errStatus(t, e, 404)
	pending, _ := filepath.Glob(filepath.Join(s.dir, ".incoming-*"))
	if len(pending) != 0 {
		t.Fatal("staging leaked", pending)
	}
}
