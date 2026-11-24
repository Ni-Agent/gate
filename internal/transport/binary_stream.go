package transport

import (
	"errors"
	"io"
	"net/http"
)

// uploadReader validates even chunked/unknown-length bodies without buffering.
// A length/read failure reaches storage before any metadata can commit.
type uploadReader struct {
	reader   io.Reader
	expected int64
	read     int64
}

func uploadError(e error) error {
	var max *http.MaxBytesError
	if errors.As(e, &max) {
		return bad(413, "binary exceeds configured limit")
	}
	return bad(400, "interrupted binary upload")
}
func (r *uploadReader) Read(p []byte) (int, error) {
	n, e := r.reader.Read(p)
	r.read += int64(n)
	if r.expected >= 0 && (r.read > r.expected || (e == io.EOF && r.read != r.expected)) {
		return n, bad(400, "binary length mismatch")
	}
	if e != nil && e != io.EOF {
		return n, uploadError(e)
	}
	return n, e
}
