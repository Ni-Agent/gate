package core

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreNativeStoreRejectedWithoutMigration(t *testing.T) {
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "metadata.db")
	db, e := sql.Open("sqlite", path)
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec("CREATE TABLE tasks(id TEXT PRIMARY KEY,creator TEXT,assignee TEXT,modified_by TEXT,status TEXT,priority INTEGER,body_hash TEXT,verification TEXT)")
	if e != nil {
		t.Fatal(e)
	}
	db.Close()
	if e = os.Chmod(path, 0600); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	opened, e := New(dir, []string{actor})
	if opened != nil {
		opened.Close()
		t.Fatal("old store accepted")
	}
	if e == nil || !strings.Contains(e.Error(), "fresh disposable store required") {
		t.Fatal(e)
	}
	after, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("old database altered")
	}
}
