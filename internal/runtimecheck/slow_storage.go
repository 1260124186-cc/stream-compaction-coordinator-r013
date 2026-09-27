package runtimecheck

import (
	"context"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// slowMode selects how the controllable repository behaves during Save.
type slowMode int

const (
	// modeNormal adds no delay.
	modeNormal slowMode = iota
	// modeInterruptible waits for the delay but aborts as soon as the
	// context is done, so no snapshot is written.
	modeInterruptible
	// modeUninterruptible always elapses the delay and then commits even
	// when the deadline has passed; the call still reports success.
	modeUninterruptible
	// modeCommitThenReportCancel elapses the delay, commits the snapshot
	// durably despite the deadline, then returns the context error. It
	// reproduces a commit that lands during the deadline window.
	modeCommitThenReportCancel
	// modeHold blocks Save until release is closed, used to occupy the
	// serialization gate while another request waits behind it.
	modeHold
)

// slowRepository wraps a real repository and adds controllable timing to
// every Save so verification can place a request deadline inside the
// persistence phase.
type slowRepository struct {
	inner storage.Repository

	mu             sync.Mutex
	mode           slowMode
	delay          time.Duration
	release        chan struct{}
	savesStarted   int
	savesCompleted int
}

func newSlowRepository(inner storage.Repository) *slowRepository {
	return &slowRepository{inner: inner}
}

func (r *slowRepository) Load() (*domain.State, error) {
	return r.inner.Load()
}

func (r *slowRepository) Save(ctx context.Context, state *domain.State) error {
	r.mu.Lock()
	r.savesStarted++
	mode := r.mode
	delay := r.delay
	var release chan struct{}
	if mode == modeHold {
		release = r.release
	}
	r.mu.Unlock()

	if mode == modeHold && release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if delay > 0 && mode != modeHold {
		switch mode {
		case modeInterruptible:
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		case modeUninterruptible, modeCommitThenReportCancel:
			// A device that does not observe the caller's deadline: the
			// physical write takes the full delay regardless.
			time.Sleep(delay)
		}
	}

	// Uninterruptible devices finish the durable write independently of
	// the caller's canceled context.
	saveCtx := ctx
	if mode == modeUninterruptible || mode == modeCommitThenReportCancel {
		saveCtx = context.Background()
	}
	if err := r.inner.Save(saveCtx, state); err != nil {
		return err
	}
	r.mu.Lock()
	r.savesCompleted++
	r.mu.Unlock()

	if mode == modeCommitThenReportCancel && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (r *slowRepository) Path() string {
	return r.inner.Path()
}

func (r *slowRepository) configure(mode slowMode, delay time.Duration, release chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mode = mode
	r.delay = delay
	r.release = release
}

func (r *slowRepository) stats() (started, completed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.savesStarted, r.savesCompleted
}
