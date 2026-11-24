package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gate-alpha/internal/core"
)

// Exercise the actual built stdio entry point, not only NewMCP's callback (the
// SDK may normalize strings before that callback sees arguments).
func TestReviewBuiltStdioUnicodeAndLimits(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "gate-alpha")
	build := exec.Command("/home/hermes/.local/go/bin/go", "build", "-o", binary, "./cmd/gate-alpha")
	build.Dir = "../.."
	if overlay := os.Getenv("GATE_REVIEW_STDIO_OVERLAY"); overlay != "" {
		build.Args = append(build.Args[:2], append([]string{"-overlay", overlay}, build.Args[2:]...)...)
	}
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatal(e, string(out))
	}
	for _, test := range []struct {
		name   string
		value  []byte
		create bool
		want   string
	}{
		{name: "raw-invalid", value: []byte{0xff}},
		{name: "unpaired-surrogate", value: []byte(`\ud800`)},
		{name: "raw-invalid-create", value: []byte{0xff}, create: true},
		{name: "unpaired-create", value: []byte(`\ud800`), create: true},
		{name: "valid-pair", value: []byte(`\ud83d\ude00`), want: "😀"},
		{name: "literal-replacement", value: []byte("�"), want: "�"},
		{name: "escaped-boundary", value: []byte(strings.Repeat(`\u0000`, (1 << 20))), want: strings.Repeat("\x00", (1 << 20))},
		{name: "above-former-wire-limit", value: []byte(strings.Repeat("x", (7<<20)+1)), want: strings.Repeat("x", (7<<20)+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "--store", dir)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "TMPDIR=" + os.Getenv("TMPDIR")}
			input, e := cmd.StdinPipe()
			if e != nil {
				t.Fatal(e)
			}
			output, e := cmd.StdoutPipe()
			if e != nil {
				t.Fatal(e)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() {
				input.Close()
				if cmd.ProcessState == nil {
					cmd.Process.Kill()
					cmd.Wait()
				}
			})
			reader := bufio.NewReader(output)
			exchange := func(raw []byte) ([]byte, error) {
				if _, e := input.Write(append(raw, '\n')); e != nil {
					return nil, e
				}
				return reader.ReadBytes('\n')
			}
			init := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"raw-stdio-review","version":"test"}}}`)
			if _, e = exchange(init); e != nil {
				t.Fatal(e, stderr.String())
			}
			if _, e = io.WriteString(input, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n"); e != nil {
				t.Fatal(e)
			}
			args := []byte(`{"id":"gate:tx:stdio-review","body":"before"}`)
			envelope := func(op string, args []byte) []byte {
				return append(append([]byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":`+fmt.Sprintf("%q", op)+`,"arguments":`), args...), []byte(`}}`)...)
			}
			reply, e := exchange(envelope("create_text", args))
			if e != nil {
				t.Fatal(e)
			}
			var response struct {
				Result struct {
					StructuredContent map[string]any `json:"structuredContent"`
				} `json:"result"`
			}
			if e = json.Unmarshal(reply, &response); e != nil {
				t.Fatal(e)
			}
			token := response.Result.StructuredContent["token"].(string)
			args = []byte(`{"id":"gate:tx:stdio-review","expected_revision":1,"expected_hash":` + fmt.Sprintf("%q", token) + `,"body":"`)
			args = append(args, test.value...)
			args = append(args, []byte(`"}`)...)
			op := "update_text"
			if test.create {
				op = "create_text"
				args = append(append([]byte(`{"id":"gate:tx:stdio-invalid-create","body":"`), test.value...), []byte(`"}`)...)
			}
			reply, e = exchange(envelope(op, args))
			invalid := test.want == ""
			if invalid && e == nil {
				t.Error("invalid stdio frame reached SDK mutation dispatch", string(reply[:min(len(reply), 180)]))
			}
			if !invalid && e != nil {
				t.Fatal("valid stdio rejected", e, stderr.String())
			}
			input.Close()
			cmd.Wait()
			if ctx.Err() != nil {
				t.Fatal("stdio failed to terminate within bound")
			}
			s, e := core.New(dir, nil)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			got, e := s.Call(context.Background(), "agent:hermes", "read_resource", map[string]any{"id": "gate:tx:stdio-review"})
			if e != nil {
				t.Fatal(e)
			}
			want := test.want
			if invalid {
				want = "before"
			}
			if got["body"] != want {
				t.Errorf("stdio wire changed committed bytes: got length %d want %d", len(got["body"].(string)), len(want))
			}
			if test.create {
				_, e = s.Call(context.Background(), "agent:hermes", "read_resource", map[string]any{"id": "gate:tx:stdio-invalid-create"})
				ce, ok := e.(*core.Error)
				if !ok || ce.Status != 404 {
					t.Fatal("malformed create published state", e)
				}
			}
		})
	}
}
