package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// stagedBlob is private: callers cannot substitute a raw digest for uploaded bytes.
type stagedBlob struct{ hash string }

func resourceContentHash(a map[string]any, binary bool) (string, error) {
	if binary {
		if b, ok := a["content"].(stagedBlob); ok {
			return b.hash, nil
		}
		return "", fail(400, "binary must be staged before metadata publication")
	}
	b, e := content(a, false)
	if e != nil {
		return "", e
	}
	return hash(b), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.reader.Read(p)
}

// pendingBlob bytes are private until publishBlob runs under the writer mutex.
// Incoming files live outside objects so a stalled network read cannot lock out
// unrelated writes/maintenance. Active staging names are excluded from grace GC.
type pendingBlob struct{ name, hash string }

func (s *Store) cleanupPending(p pendingBlob) { os.Remove(p.name); s.uploads.Delete(p.name) }
func (s *Store) stageBlob(ctx context.Context, reader io.Reader) (pendingBlob, error) {
	f, e := os.CreateTemp(s.dir, ".incoming-")
	if e != nil {
		return pendingBlob{}, e
	}
	p := pendingBlob{name: f.Name()}
	s.uploads.Store(p.name, true)
	success := false
	defer func() {
		f.Close()
		if !success {
			s.cleanupPending(p)
		}
	}()
	digest := sha256.New()
	if _, e = io.Copy(io.MultiWriter(f, digest), contextReader{ctx, reader}); e != nil {
		return pendingBlob{}, e
	}
	if e = ctx.Err(); e != nil {
		return pendingBlob{}, e
	}
	if e = f.Sync(); e != nil {
		return pendingBlob{}, e
	}
	if e = f.Close(); e != nil {
		return pendingBlob{}, e
	}
	p.hash = hex.EncodeToString(digest.Sum(nil))
	success = true
	return p, nil
}

// Called under writer mutex: GC cannot unlink an existing object or the newly
// published object before the following metadata transaction publishes its ref.
func (s *Store) publishBlob(p pendingBlob) (string, error) {
	if e := s.verifyBlob(p.hash); e == nil {
		return p.hash, nil
	} else if !os.IsNotExist(e) {
		return "", e
	}
	if e := s.inject("staged"); e != nil {
		return "", e
	}
	dir := filepath.Join(s.dir, "objects")
	if e := os.Rename(p.name, filepath.Join(dir, p.hash)); e != nil {
		return "", e
	}
	if e := syncDir(dir); e != nil {
		return "", e
	}
	if e := s.inject("published"); e != nil {
		return "", e
	}
	return p.hash, nil
}
func (s *Store) putBlobReader(ctx context.Context, reader io.Reader) (string, error) {
	p, e := s.stageBlob(ctx, reader)
	if e != nil {
		return "", e
	}
	defer s.cleanupPending(p)
	return s.publishBlob(p)
}
func (s *Store) putBlob(b []byte) error {
	h := hash(b)
	if e := s.verifyBlob(h); e == nil {
		return nil
	} else if !os.IsNotExist(e) {
		return e
	}
	_, e := s.putBlobReader(context.Background(), bytes.NewReader(b))
	return e
}

// openBlob verifies with bounded memory and rewinds the SAME immutable inode.
// No second path open races reclamation. Caller closes the descriptor.
func (s *Store) openBlob(ctx context.Context, h string) (*os.File, error) {
	if len(h) != 64 {
		return nil, errors.New("invalid object hash")
	}
	if _, e := hex.DecodeString(h); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(filepath.Join(s.dir, "objects", h), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return nil, e
	}
	bad := func(e error) (*os.File, error) { f.Close(); return nil, e }
	st, e := f.Stat()
	if e != nil {
		return bad(e)
	}
	if !st.Mode().IsRegular() {
		return bad(errors.New("invalid object"))
	}
	digest := sha256.New()
	if _, e = io.Copy(digest, contextReader{ctx, f}); e != nil {
		return bad(e)
	}
	if hex.EncodeToString(digest.Sum(nil)) != h {
		return bad(errors.New("object digest mismatch"))
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return bad(e)
	}
	return f, nil
}
func (s *Store) verifyBlob(h string) error {
	f, e := s.openBlob(context.Background(), h)
	if e == nil {
		e = f.Close()
	}
	return e
}

// getBlob is only for JSON text/task bodies and the legacy ReadBytes API.
func (s *Store) getBlob(h string) ([]byte, error) {
	f, e := s.openBlob(context.Background(), h)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return io.ReadAll(f)
}

// OpenBinary authenticates, resolves an exact snapshot and opens verified disk
// bytes. Closing the returned file is mandatory. On Linux the descriptor remains
// valid after unlink, so GC/updates need not block on a slow download.
func (s *Store) OpenBinary(ctx context.Context, actor, id string, revision int) (*os.File, map[string]any, error) {
	s.files.RLock()
	defer s.files.RUnlock()
	if e := registered(ctx, s.db, actor); e != nil {
		return nil, nil, e
	}
	if e := requireID(id, "bn"); e != nil {
		return nil, nil, e
	}
	if revision < 0 {
		return nil, nil, fail(400, "invalid revision")
	}
	tx, e := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return nil, nil, e
	}
	defer tx.Rollback()
	m, e := getMeta(ctx, tx, id, revision)
	if e != nil {
		return nil, nil, e
	}
	if e = gone(m); e != nil {
		return nil, m, e
	}
	f, e := s.openBlob(ctx, m["content_hash"].(string))
	return f, m, e
}

// ReadBytes is a compatibility helper for callers explicitly requesting a byte
// slice. HTTP downloads use OpenBinary instead and never buffer the whole file.
func (s *Store) ReadBytes(ctx context.Context, actor, id string, revision int) ([]byte, map[string]any, error) {
	f, m, e := s.OpenBinary(ctx, actor, id, revision)
	if e != nil {
		return nil, m, e
	}
	defer f.Close()
	b, e := io.ReadAll(contextReader{ctx, f})
	return b, m, e
}
