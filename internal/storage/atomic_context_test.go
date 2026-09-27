package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteAtomicallyContextCanceledBeforeCommitPublishesNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WriteAtomicallyContext(ctx, path, []byte("{}\n")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot published despite canceled context: %v", err)
	}
	assertNoTempFiles(t, dir)
}

func TestWriteAtomicallyContextExpiredBeforeCommitPublishesNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := WriteAtomicallyContext(ctx, path, []byte("{}\n")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot published despite expired deadline: %v", err)
	}
	assertNoTempFiles(t, dir)
}

func TestWriteAtomicallyHappyPathReplacesSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := WriteAtomically(path, []byte("first\n")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := WriteAtomicallyContext(context.Background(), path, []byte("second\n")); err != nil {
		t.Fatalf("second write: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "second\n" {
		t.Fatalf("content = %q, want second", data)
	}
	assertNoTempFiles(t, dir)
}

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "state.json" {
			continue
		}
		t.Errorf("leftover temporary file: %s", name)
	}
}
