// Package content implements the opt-in, local resource API. Only registered
// Markdown files are served; external resources are descriptors, never proxies.
package content

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"unicode/utf8"
)

const MaxDocumentBytes = 1 << 20

type Document struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	ProjectID string `json:"project_id,omitempty"`
	Path      string `json:"path"`
}

type Location struct {
	Protocol string `json:"protocol"`
	URL      string `json:"url"`
}

type Resource struct {
	ID        string     `json:"id"`
	Title     string     `json:"title"`
	ProjectID string     `json:"project_id,omitempty"`
	Kind      string     `json:"kind"`
	Locations []Location `json:"locations"`
}

type Catalog struct {
	Documents []Document `json:"documents"`
	Resources []Resource `json:"resources"`
}

type Store struct {
	mu      sync.Mutex
	root    string
	catalog Catalog
	lock    *os.File
	failed  bool
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

// ValidateLocalBind deliberately limits this unauthenticated alpha to a literal
// loopback address. A remote authorization boundary is a separate next step.
func ValidateLocalBind(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("content API requires a literal loopback bind address until authentication is implemented")
	}
	return nil
}

func Open(root, catalogPath string) (*Store, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return nil, errors.New("knowledge root must exist and contain no symlinks")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return nil, errors.New("knowledge root must be a directory")
	}
	f, err := os.Open(catalogPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, MaxDocumentBytes+1))
	d.DisallowUnknownFields()
	s := &Store{root: root}
	if err := d.Decode(&s.catalog); err != nil {
		return nil, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("catalog must contain one JSON value")
	}
	ids, paths := map[string]bool{}, map[string]bool{}
	for _, doc := range s.catalog.Documents {
		if !identifier.MatchString(doc.ID) || ids[doc.ID] || strings.TrimSpace(doc.Title) == "" {
			return nil, errors.New("invalid or duplicate document ID/title")
		}
		ids[doc.ID] = true
		p, err := s.documentPath(doc)
		if err != nil {
			return nil, err
		}
		if paths[p] {
			return nil, errors.New("documents must have distinct paths")
		}
		paths[p] = true
		if _, err := s.read(doc); err != nil {
			return nil, err
		}
	}
	for _, ref := range s.catalog.Resources {
		if !identifier.MatchString(ref.ID) || ids[ref.ID] || strings.TrimSpace(ref.Title) == "" || len(ref.Locations) == 0 {
			return nil, errors.New("invalid resource ID/title/locations")
		}
		ids[ref.ID] = true
		if ref.Kind != "repository" && ref.Kind != "folder" && ref.Kind != "asset" {
			return nil, errors.New("resource kind must be repository, folder or asset")
		}
		for _, loc := range ref.Locations {
			u, err := url.Parse(loc.URL)
			if err != nil || u.Scheme == "" || loc.Protocol != u.Scheme || u.RawQuery != "" || u.Fragment != "" {
				return nil, errors.New("resource locations need explicit matching protocols and no query/fragment")
			}
			if u.User != nil {
				if _, set := u.User.Password(); set {
					return nil, errors.New("resource URLs must not contain passwords")
				}
			}
			switch u.Scheme {
			case "https", "ssh", "s3", "smb":
				if u.Host == "" {
					return nil, errors.New("resource URL needs a host")
				}
			case "file":
				if !filepath.IsAbs(u.Path) {
					return nil, errors.New("file URL needs an absolute path")
				}
			default:
				return nil, errors.New("unsupported resource protocol")
			}
		}
	}
	lockPath := filepath.Join(root, ".workflow-content.lock")
	if i, e := os.Lstat(lockPath); e == nil && !i.Mode().IsRegular() {
		return nil, errors.New("lock must be a regular file")
	}
	s.lock, err = os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		s.lock.Close()
		return nil, errors.New("knowledge root already has a writer")
	}
	return s, nil
}

func (s *Store) Close() error { return s.lock.Close() }

// The root is owned exclusively by this service, not a concurrently edited or
// Syncthing-managed directory. Reject symlinks on every access as well as startup.
func (s *Store) documentPath(doc Document) (string, error) {
	if !filepath.IsLocal(doc.Path) || filepath.Ext(doc.Path) != ".md" {
		return "", errors.New("document path must be relative Markdown")
	}
	p := filepath.Join(s.root, doc.Path)
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil || resolved != p {
		return "", errors.New("document must exist and contain no symlinks")
	}
	info, err := os.Lstat(p)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("document must be a regular file")
	}
	return p, nil
}

func (s *Store) read(doc Document) ([]byte, error) {
	p, err := s.documentPath(doc)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	i, err := f.Stat()
	if err != nil || !i.Mode().IsRegular() {
		return nil, errors.New("document is not a regular file")
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxDocumentBytes+1))
	if err != nil || len(b) > MaxDocumentBytes || !utf8.Valid(b) {
		return nil, errors.New("document must be UTF-8 and at most 1 MiB")
	}
	return b, nil
}

func etag(b []byte) string { return fmt.Sprintf(`"%x"`, sha256.Sum256(b)) }

func jsonReply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func problem(w http.ResponseWriter, code int, msg string) {
	jsonReply(w, code, map[string]any{"error": map[string]string{"message": msg}})
}

func (s *Store) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /knowledge", func(w http.ResponseWriter, r *http.Request) {
		items := make([]map[string]string, 0, len(s.catalog.Documents))
		for _, d := range s.catalog.Documents {
			items = append(items, map[string]string{"id": d.ID, "title": d.Title, "project_id": d.ProjectID, "href": "/knowledge/" + d.ID, "media_type": "text/markdown", "ownership": "owned"})
		}
		jsonReply(w, 200, map[string]any{"items": items})
	})
	m.HandleFunc("GET /content", func(w http.ResponseWriter, r *http.Request) {
		items := make([]Resource, 0, len(s.catalog.Resources))
		items = append(items, s.catalog.Resources...)
		jsonReply(w, 200, map[string]any{"items": items})
	})
	m.HandleFunc("GET /content/{id}", func(w http.ResponseWriter, r *http.Request) {
		for _, ref := range s.catalog.Resources {
			if ref.ID == r.PathValue("id") {
				jsonReply(w, 200, ref)
				return
			}
		}
		problem(w, 404, "resource not found")
	})
	m.HandleFunc("GET /knowledge/{id}", s.document)
	m.HandleFunc("PUT /knowledge/{id}", s.document)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		m.ServeHTTP(w, r)
	})
}

func (s *Store) document(w http.ResponseWriter, r *http.Request) {
	var doc Document
	for _, d := range s.catalog.Documents {
		if d.ID == r.PathValue("id") {
			doc = d
			break
		}
	}
	if doc.ID == "" {
		problem(w, 404, "document not found")
		return
	}
	var replacement []byte
	if r.Method == http.MethodPut {
		media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "text/markdown" || (params["charset"] != "" && !strings.EqualFold(params["charset"], "utf-8")) {
			problem(w, 415, "use text/markdown with UTF-8")
			return
		}
		if r.Header.Get("If-Match") == "" {
			problem(w, 428, "If-Match with the document ETag is required")
			return
		}
		if r.Header.Get("If-Match") == "*" {
			problem(w, 400, "a specific ETag is required; wildcard writes are disabled")
			return
		}
		replacement, err = io.ReadAll(http.MaxBytesReader(w, r.Body, MaxDocumentBytes))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				problem(w, 413, "document exceeds 1 MiB")
			} else {
				problem(w, 400, "cannot read request body")
			}
			return
		}
		if !utf8.Valid(replacement) {
			problem(w, 400, "document must be UTF-8")
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		problem(w, 503, "storage outcome uncertain; inspect disk and restart")
		return
	}
	current, err := s.read(doc)
	if err != nil {
		problem(w, 500, "cannot read document")
		return
	}
	version := etag(current)
	if r.Method == http.MethodPut {
		match := false
		for _, candidate := range strings.Split(r.Header.Get("If-Match"), ",") {
			if strings.TrimSpace(candidate) == version {
				match = true
			}
		}
		if !match {
			problem(w, 412, "document changed; GET its current contents and reconcile before retrying")
			return
		}
		if !bytes.Equal(current, replacement) {
			if err := s.replace(doc, replacement); err != nil {
				problem(w, 500, "cannot persist document; GET to inspect the outcome")
				return
			}
		}
		w.Header().Set("ETag", etag(replacement))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Header().Set("ETag", version)
	w.Header().Set("Content-Length", fmt.Sprint(len(current)))
	if r.Method != http.MethodHead {
		_, _ = w.Write(current)
	}
}

func (s *Store) replace(doc Document, b []byte) error {
	p, err := s.documentPath(doc)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".workflow-write-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), p); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(p))
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	if err != nil {
		s.failed = true
	}
	return err
}
