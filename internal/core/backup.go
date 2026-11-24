package core

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"modernc.org/sqlite"
)

// Backup uses SQLite's online backup API while the store stays open for reads.
// Writes and explicit GC wait until the snapshot and all referenced bytes have
// been copied. A failed backup never publishes the destination directory.
// This is a local maintenance API, deliberately not an authenticated wire tool.
func (s *Store) Backup(ctx context.Context, destination string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files.RLock()
	defer s.files.RUnlock()
	dest, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(dest))
	if err != nil {
		return err
	}
	if parent != filepath.Dir(dest) {
		return errors.New("backup parent cannot contain symlinks")
	}
	if _, err = os.Lstat(dest); !os.IsNotExist(err) {
		return errors.New("backup destination must not exist")
	}
	stage, err := os.MkdirTemp(parent, ".gate-backup-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	dbPath := filepath.Join(stage, "metadata.db")
	f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	err = conn.Raw(func(raw any) error {
		source, ok := raw.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		})
		if !ok {
			return errors.New("SQLite online backup unavailable")
		}
		b, e := source.NewBackup(dbPath)
		if e != nil {
			return e
		}
		_, e = b.Step(-1)
		finish := b.Finish()
		if e != nil {
			return e
		}
		return finish
	})
	conn.Close()
	if err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT hash FROM revisions WHERE hash IS NOT NULL UNION SELECT body_hash FROM tasks")
	if err != nil {
		return err
	}
	refs := []string{}
	for rows.Next() {
		var h string
		if err = rows.Scan(&h); err != nil {
			rows.Close()
			return err
		}
		refs = append(refs, h)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	objects := filepath.Join(stage, "objects")
	if err = os.Mkdir(objects, 0700); err != nil {
		return err
	}
	for _, h := range refs {
		if err = ctx.Err(); err != nil {
			return err
		}
		source, e := s.openBlob(ctx, h)
		if e != nil {
			return e
		}
		target, e := os.OpenFile(filepath.Join(objects, h), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			source.Close()
			return e
		}
		_, e = io.Copy(target, contextReader{ctx, source})
		source.Close()
		if e == nil {
			e = target.Sync()
		}
		ce := target.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
	}
	f, err = os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	f.Close()
	if err != nil {
		return err
	}
	if err = syncDir(objects); err != nil {
		return err
	}
	if err = syncDir(stage); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = os.Rename(stage, dest); err != nil {
		return err
	}
	return syncDir(parent)
}
func syncWrite(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return e
	}
	return f.Sync()
}
func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
