package transport

import (
	"bufio"
	"bytes"
	"io"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Validate raw Unicode before encoding/json or an SDK can normalize it.
// Optional operational limits are explicit; zero does not impose a product cap.
func readJSONWire(body io.Reader) ([]byte, error) { return readJSONWireLimit(body, 0) }
func readJSONWireLimit(body io.Reader, limit int64) ([]byte, error) {
	if limit > 0 && limit < int64(^uint64(0)>>1) {
		body = io.LimitReader(body, limit+1)
	}
	raw, e := io.ReadAll(body)
	if e != nil {
		return nil, e
	}
	if limit > 0 && int64(len(raw)) > limit {
		return nil, bad(413, "JSON wire body exceeds limit")
	}
	if e = validateJSONUnicode(raw); e != nil {
		return nil, e
	}
	return raw, nil
}

// Only Unicode validity is checked here. Syntax and protocol envelopes remain
// the responsibility of the existing decoder/SDK. Valid bytes are never changed.
func validateJSONUnicode(raw []byte) error {
	if !utf8.Valid(raw) {
		return bad(400, "JSON wire must be valid UTF-8")
	}
	inString := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return bad(400, "incomplete JSON escape")
		}
		if raw[i] != 'u' {
			continue
		}
		if i+5 > len(raw) {
			return bad(400, "incomplete Unicode escape")
		}
		code, ok := hex4(raw[i+1 : i+5])
		if !ok {
			return bad(400, "invalid Unicode escape")
		}
		i += 4
		switch {
		case code >= 0xd800 && code <= 0xdbff:
			if i+7 > len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return bad(400, "unpaired Unicode surrogate")
			}
			low, ok := hex4(raw[i+3 : i+7])
			if !ok || low < 0xdc00 || low > 0xdfff {
				return bad(400, "unpaired Unicode surrogate")
			}
			i += 6
		case code >= 0xdc00 && code <= 0xdfff:
			return bad(400, "unpaired Unicode surrogate")
		}
	}
	return nil
}
func hex4(b []byte) (uint16, bool) {
	var n uint16
	for _, c := range b {
		n <<= 4
		switch {
		case c >= '0' && c <= '9':
			n += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			n += uint16(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			n += uint16(c - 'A' + 10)
		default:
			return 0, false
		}
	}
	return n, true
}

type strictJSONLines struct {
	source  io.ReadCloser
	scanner *bufio.Scanner
	pending *bytes.Reader
	limit   int
}

func (r *strictJSONLines) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.pending == nil || r.pending.Len() == 0 {
		if !r.scanner.Scan() {
			if e := r.scanner.Err(); e != nil {
				return 0, e
			}
			return 0, io.EOF
		}
		frame := r.scanner.Bytes()
		raw := bytes.TrimSuffix(frame, []byte{'\n'})
		if r.limit > 0 && len(raw) > r.limit {
			return 0, bad(413, "stdio JSON frame exceeds limit")
		}
		if e := validateJSONUnicode(raw); e != nil {
			return 0, e
		}
		r.pending = bytes.NewReader(frame)
	}
	return r.pending.Read(p)
}
func (r *strictJSONLines) Close() error { return r.source.Close() }

type noCloseWriter struct{ io.Writer }

func (noCloseWriter) Close() error { return nil }

// NewStrictIOTransport guards whole incoming newline-delimited frames BEFORE
// SDK normalization. Invalid Unicode/oversized frames terminate the connection
// as transport read failures; they are never rewritten into valid tool calls.
// Valid JSON-RPC frames, line delimiters and outgoing SDK framing stay unchanged.
func NewStrictIOTransport(reader io.ReadCloser, writer io.Writer) *mcp.IOTransport {
	return NewStrictIOTransportWithLimit(reader, writer, 0)
}

// A zero limit disables the operational JSON frame cap. No SDK default cap.
func NewStrictIOTransportWithLimit(reader io.ReadCloser, writer io.Writer, limit int) *mcp.IOTransport {
	max := int(^uint(0) >> 1)
	scanMax, lineMax := max, max
	if limit > 0 && limit <= max-2 {
		scanMax, lineMax = limit+2, limit+1
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, min(64<<10, scanMax)), scanMax)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			return i + 1, data[:i+1], nil
		}
		if atEOF && len(data) > 0 {
			return len(data), data, nil
		}
		return 0, nil, nil
	})
	return &mcp.IOTransport{Reader: &strictJSONLines{source: reader, scanner: scanner, limit: limit}, Writer: noCloseWriter{writer}, MaxLineLength: lineMax}
}
