package mcptest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ory/fosite"
	"github.com/ory/fosite/storage"
)

// Durable token sessions are separate from KB Git. All access to this store is
// serialized by LocalAuth.opMu; an OAuth response is released only after fsync.
// Short-lived authorization codes and in-progress browser forms remain transient.
type refreshSession struct {
	Request         json.RawMessage `json:"request"`
	Active          bool            `json:"active"`
	AccessSignature string          `json:"access_signature"`
}
type authState struct {
	Version int                        `json:"version"`
	Secret  string                     `json:"secret"`
	Access  map[string]json.RawMessage `json:"access"`
	Refresh map[string]refreshSession  `json:"refresh"`
}
type tokenStore struct {
	*storage.MemoryStore
	state authState
	path  string
	lock  *os.File
}

func newTokenStore(path string) (*tokenStore, error) {
	s := &tokenStore{MemoryStore: storage.NewMemoryStore(), path: path, state: authState{Version: 1, Secret: randomSecret(), Access: map[string]json.RawMessage{}, Refresh: map[string]refreshSession{}}}
	if path == "" {
		return s, nil
	}
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	resolved, e := filepath.EvalSymlinks(dir)
	absolute, _ := filepath.Abs(dir)
	if e != nil || resolved != absolute {
		return nil, errors.New("OAuth state directory must not contain symlinks")
	}
	info, e := os.Stat(dir)
	if e != nil || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("OAuth state directory must be private")
	}
	f, e := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, e
	}
	s.lock = f
	if info, e := os.Lstat(path); e == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			s.Close()
			return nil, errors.New("OAuth state must be a private regular file")
		}
		f, e := os.Open(path)
		if e != nil {
			s.Close()
			return nil, e
		}
		d := json.NewDecoder(io.LimitReader(f, 32<<20))
		e = d.Decode(&s.state)
		var extra any
		if e == nil && d.Decode(&extra) != io.EOF {
			e = errors.New("invalid OAuth state JSON")
		}
		f.Close()
		if e != nil || s.state.Version != 1 || len(s.state.Secret) < 32 || s.state.Access == nil || s.state.Refresh == nil {
			s.Close()
			return nil, errors.New("invalid OAuth state; refusing to silently reset sessions")
		}
	} else if !os.IsNotExist(e) {
		s.Close()
		return nil, e
	} else if e = s.save(); e != nil {
		s.Close()
		return nil, e
	}
	return s, nil
}
func (s *tokenStore) Close() {
	if s.lock != nil {
		syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		s.lock.Close()
		s.lock = nil
	}
}
func (s *tokenStore) save() error {
	if s.path == "" {
		return nil
	}
	// Prune expired sessions; inactive refresh records stay until their expiry so
	// replay remains detectable throughout the original credential's lifetime.
	for sig, b := range s.state.Access {
		r, e := s.unpack(b)
		if e == nil && time.Now().After(r.GetSession().GetExpiresAt(fosite.AccessToken)) {
			delete(s.state.Access, sig)
		}
	}
	for sig, v := range s.state.Refresh {
		r, e := s.unpack(v.Request)
		if e == nil && time.Now().After(r.GetSession().GetExpiresAt(fosite.RefreshToken)) {
			delete(s.state.Refresh, sig)
		}
	}
	b, e := json.Marshal(s.state)
	if e != nil {
		return e
	}
	if len(b) > 32<<20 {
		return errors.New("OAuth state exceeds limit")
	}
	dir := filepath.Dir(s.path)
	f, e := os.CreateTemp(dir, ".oauth-state-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), s.path); e != nil {
		return e
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (s *tokenStore) unpack(b []byte) (fosite.Requester, error) {
	r := &fosite.Request{Client: &fosite.DefaultClient{}, Session: &fosite.DefaultSession{}}
	if e := json.Unmarshal(b, r); e != nil {
		return nil, e
	}
	client, e := s.MemoryStore.GetClient(context.Background(), r.Client.GetID())
	if e != nil {
		return nil, e
	}
	r.Client = client
	return r, nil
}
func (s *tokenStore) CreateAccessTokenSession(ctx context.Context, sig string, r fosite.Requester) error {
	b, e := json.Marshal(r)
	if e == nil {
		s.state.Access[sig] = b
	}
	return e
}
func (s *tokenStore) GetAccessTokenSession(ctx context.Context, sig string, _ fosite.Session) (fosite.Requester, error) {
	b, ok := s.state.Access[sig]
	if !ok {
		return nil, fosite.ErrNotFound
	}
	return s.unpack(b)
}
func (s *tokenStore) DeleteAccessTokenSession(ctx context.Context, sig string) error {
	delete(s.state.Access, sig)
	return nil
}
func (s *tokenStore) CreateRefreshTokenSession(ctx context.Context, sig, access string, r fosite.Requester) error {
	b, e := json.Marshal(r)
	if e == nil {
		s.state.Refresh[sig] = refreshSession{Request: b, Active: true, AccessSignature: access}
	}
	return e
}
func (s *tokenStore) GetRefreshTokenSession(ctx context.Context, sig string, _ fosite.Session) (fosite.Requester, error) {
	v, ok := s.state.Refresh[sig]
	if !ok {
		return nil, fosite.ErrNotFound
	}
	r, e := s.unpack(v.Request)
	if e != nil {
		return nil, e
	}
	if !v.Active {
		return r, fosite.ErrInactiveToken
	}
	return r, nil
}
func (s *tokenStore) DeleteRefreshTokenSession(ctx context.Context, sig string) error {
	delete(s.state.Refresh, sig)
	return nil
}
func (s *tokenStore) RevokeAccessToken(ctx context.Context, id string) error {
	for sig, b := range s.state.Access {
		r, e := s.unpack(b)
		if e != nil {
			return e
		}
		if r.GetID() == id {
			delete(s.state.Access, sig)
		}
	}
	return nil
}
func (s *tokenStore) RevokeRefreshToken(ctx context.Context, id string) error {
	for sig, v := range s.state.Refresh {
		r, e := s.unpack(v.Request)
		if e != nil {
			return e
		}
		if r.GetID() == id {
			v.Active = false
			s.state.Refresh[sig] = v
		}
	}
	return nil
}
func (s *tokenStore) RotateRefreshToken(ctx context.Context, id, sig string) error {
	v, ok := s.state.Refresh[sig]
	if !ok {
		return fosite.ErrNotFound
	}
	if !v.Active {
		return fosite.ErrInactiveToken
	}
	r, e := s.unpack(v.Request)
	if e != nil || r.GetID() != id {
		return fmt.Errorf("invalid refresh grant")
	}
	v.Active = false
	s.state.Refresh[sig] = v
	delete(s.state.Access, v.AccessSignature)
	return nil
}
func (a *LocalAuth) Close() { a.opMu.Lock(); defer a.opMu.Unlock(); a.tokens.Close() }
func (a *LocalAuth) RevokeAll() error {
	a.opMu.Lock()
	defer a.opMu.Unlock()
	a.tokens.state.Access = map[string]json.RawMessage{}
	a.tokens.state.Refresh = map[string]refreshSession{}
	if e := a.tokens.save(); e != nil {
		a.failed = true
		return e
	}
	return nil
}
