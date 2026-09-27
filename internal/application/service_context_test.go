package application

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

func newTestService(t *testing.T) (*Service, storage.Repository) {
	t.Helper()
	repo := storage.NewInMemoryRepository(t.TempDir(), nil)
	service, err := NewService(
		repo,
		&idgen.Counter{},
		clock.System{},
		observability.Discard{},
		Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service, repo
}

func TestContextErrorClassification(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := contextError(canceled)
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) || domainErr.Code != domain.CodeRequestCanceled {
		t.Fatalf("canceled = %v, want request_canceled domain error", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error must unwrap to context.Canceled")
	}

	deadline, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	err = contextError(deadline)
	if !errors.As(err, &domainErr) || domainErr.Code != domain.CodeRequestTimeout {
		t.Fatalf("deadline = %v, want request_timeout domain error", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error must unwrap to context.DeadlineExceeded")
	}

	if err := contextError(context.Background()); err != nil {
		t.Fatalf("active context = %v, want nil", err)
	}
}

func TestCreateStreamRejectsElapsedContextWithoutPublishing(t *testing.T) {
	service, _ := newTestService(t)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := service.CreateStream(ctx, CreateStreamCommand{
		Namespace: "ns", Name: "late", ThresholdBytes: 64,
	})
	if !domain.IsCode(err, domain.CodeRequestTimeout) {
		t.Fatalf("CreateStream err = %v, want request_timeout", err)
	}
	if _, err := service.GetStream(context.Background(), "str_000001"); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("stream published despite elapsed deadline: err=%v", err)
	}
}

// controllableRepository wraps an in-memory repository. Once armed, the next
// SaveContext reports started, then blocks until released or the context
// ends. Durable commits are counted.
type controllableRepository struct {
	storage.Repository
	mu      sync.Mutex
	commits int
	armed   bool
	started chan struct{}
	release chan struct{}
}

func (r *controllableRepository) arm() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = make(chan struct{})
	r.release = make(chan struct{})
	r.armed = true
}

func (r *controllableRepository) commitsCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commits
}

func (r *controllableRepository) SaveContext(ctx context.Context, state *domain.State) error {
	r.mu.Lock()
	armed := r.armed
	started := r.started
	release := r.release
	r.mu.Unlock()

	if armed {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := r.Repository.SaveContext(context.Background(), state); err != nil {
		return err
	}
	r.mu.Lock()
	r.commits++
	r.mu.Unlock()
	return nil
}

func newControllableService(t *testing.T) (*Service, *controllableRepository) {
	t.Helper()
	repo := &controllableRepository{
		Repository: storage.NewInMemoryRepository(t.TempDir(), nil),
	}
	service, err := NewService(repo, &idgen.Counter{}, clock.System{}, observability.Discard{},
		Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service, repo
}

func TestTimeoutWaitingForSerializedCommitDoesNotMutate(t *testing.T) {
	service, repo := newControllableService(t)
	seed, err := service.CreateStream(context.Background(), CreateStreamCommand{
		Namespace: "ns", Name: "seed", ThresholdBytes: 1000,
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := repo.commitsCount()

	repo.arm()
	holderErr := make(chan error, 1)
	go func() {
		_, err := service.AppendRecords(context.Background(), AppendRecordsCommand{
			StreamID: seed.Stream.ID, ExpectedRevision: 1,
			RecordCount: 1, PayloadBytes: 10,
		})
		holderErr <- err
	}()
	<-repo.started

	queued, cancelQueued := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelQueued()
	_, err = service.AppendRecords(queued, AppendRecordsCommand{
		StreamID: seed.Stream.ID, ExpectedRevision: 2,
		RecordCount: 1, PayloadBytes: 10,
	})
	if !domain.IsCode(err, domain.CodeRequestTimeout) {
		t.Fatalf("queued append err = %v, want request_timeout", err)
	}

	close(repo.release)
	select {
	case err := <-holderErr:
		if err != nil {
			t.Fatalf("holder append: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("holder never finished")
	}

	if got := repo.commitsCount(); got != before+1 {
		t.Fatalf("commit count = %d, want %d (queued timeout must not commit)", got, before+1)
	}
	stream, err := service.GetStream(context.Background(), seed.Stream.ID)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if stream.Revision != 2 {
		t.Fatalf("revision = %d, want 2", stream.Revision)
	}
}

// commitAfterContextRepository models storage that finishes a durable commit
// while the request context is ending: once armed, SaveContext returns nil
// regardless of the expired context, because the snapshot already landed.
type commitAfterContextRepository struct {
	storage.Repository
	mu      sync.Mutex
	armed   bool
	reached chan struct{}
	release chan struct{}
}

func (r *commitAfterContextRepository) arm() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reached = make(chan struct{})
	r.release = make(chan struct{})
	r.armed = true
}

func (r *commitAfterContextRepository) SaveContext(ctx context.Context, state *domain.State) error {
	r.mu.Lock()
	armed := r.armed
	reached := r.reached
	release := r.release
	r.mu.Unlock()

	if err := r.Repository.SaveContext(context.Background(), state); err != nil {
		return err
	}
	if !armed {
		return nil
	}
	close(reached)
	<-release
	return nil
}

func TestCommitWinsOverDeadline(t *testing.T) {
	repo := &commitAfterContextRepository{
		Repository: storage.NewInMemoryRepository(t.TempDir(), nil),
	}
	service, err := NewService(repo, &idgen.Counter{}, clock.System{}, observability.Discard{},
		Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	seed, err := service.CreateStream(context.Background(), CreateStreamCommand{
		Namespace: "ns", Name: "win", ThresholdBytes: 1000,
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	repo.arm()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := service.AppendRecords(ctx, AppendRecordsCommand{
			StreamID: seed.Stream.ID, ExpectedRevision: 1,
			RecordCount: 1, PayloadBytes: 10,
		})
		done <- err
	}()
	<-repo.reached
	cancel() // context ends while storage is finishing; snapshot already durable
	close(repo.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("committed append reported %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("committed append never returned")
	}

	stream, err := service.GetStream(context.Background(), seed.Stream.ID)
	if err != nil {
		t.Fatalf("read after raced commit: %v", err)
	}
	if stream.Revision != 2 {
		t.Fatalf("raced commit revision = %d, want 2", stream.Revision)
	}
}
