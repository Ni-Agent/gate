// gate-testbridge is a test-only HTTP handler bridge over stdio. It opens no
// sockets. Base64 here is JSON encoding of the test envelope, not the Gate API.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"gate-alpha/internal/core"
	"gate-alpha/internal/transport"
	"net/http/httptest"
	"os"
)

type request struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    []byte            `json:"body"`
}
type response struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    []byte            `json:"body"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "required isolated store directory")
		os.Exit(2)
	}
	s, e := core.New(os.Args[1], []string{"agent:hermes", "agent:mai", "human:d0f"})
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	defer s.Close()
	h := transport.New(s, map[string]string{"fixture-token-hermes": "agent:hermes"})
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 65536), 24<<20)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var in request
		if e = json.Unmarshal(scanner.Bytes(), &in); e != nil {
			fmt.Fprintln(os.Stderr, "invalid test envelope")
			os.Exit(2)
		}
		if in.Path == "/events" {
			fmt.Fprintln(os.Stderr, "streaming events not supported by finite test bridge")
			os.Exit(2)
		}
		r := httptest.NewRequest(in.Method, "https://test.invalid"+in.Path, bytes.NewReader(in.Body))
		for k, v := range in.Headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		headers := map[string]string{}
		for k := range w.Header() {
			headers[k] = w.Header().Get(k)
		}
		if e = encoder.Encode(response{w.Code, headers, w.Body.Bytes()}); e != nil {
			os.Exit(1)
		}
	}
	if e = scanner.Err(); e != nil {
		fmt.Fprintln(os.Stderr, "test envelope read failed")
		os.Exit(1)
	}
}
