package application

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// controlledRepository wraps the in-memory repository with a blocking Save
// that occupies the serialization gate until released.
type controlledRepository struct {
	*storage.InMemoryRepository
	mu      sync.Mutex
	release chan struct{}
	started int
}

func newControlledRepository() *controlledRepository {
	return &controlledRepository{
		InMemoryRepository: storage.NewInMemoryRepository("mem", nil),
	}
}

func (r *controlledRepository) Save(ctx context.Context, state *domain.State) error {
	r.mu.Lock()
	r.started++
	release := r.release
	r.mu.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.InMemoryRepository.Save(ctx, state)
}

func newTestService(t *testing.T, repo storage.Repository) *Service {
	t.Helper()
	service, err := NewService(
		repo,
		&idgen.Counter{},
		clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Second),
		observability.Discard{},
		Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		t.Fatalf("construct service: %v", err)
	}
	return service
}

func createTestStream(t *testing.T, service *Service) string {
	t.Helper()
	result, err := service.CreateStream(context.Background(), CreateStreamCommand{
		Namespace: "ns", Name: "stream", ThresholdBytes: 64,
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return result.Stream.ID
}

func TestDeadlineBeforeStartIsTimeoutNotInvalidInput(t *testing.T) {
	service := newTestService(t, newControlledRepository())

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.CreateStream(canceled, CreateStreamCommand{
		Namespace: "ns", Name: "canceled", ThresholdBytes: 64,
	}); !domain.IsCode(err, domain.CodeRequestCanceled) {
		t.Fatalf("canceled create err = %v, want request_canceled", err)
	}

	expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expire()
	if _, err := service.CreateStream(expired, CreateStreamCommand{
		Namespace: "ns", Name: "expired", ThresholdBytes: 64,
	}); !domain.IsCode(err, domain.CodeRequestTimeout) {
		t.Fatalf("expired create err = %v, want request_timeout", err)
	}

	// No state may be published by pre-processing rejections.
	state := service.readState()
	if len(state.Streams) != 0 {
		t.Fatalf("deadline rejections published %d streams", len(state.Streams))
	}
}

func TestDeadlineDuringGateWaitDoesNotPublish(t *testing.T) {
	repo := newControlledRepository()
	service := newTestService(t, repo)
	streamID := createTestStream(t, service)

	release := make(chan struct{})
	repo.release = release
	leaderDone := make(chan error, 1)
	go func() {
		_, err := service.AppendRecords(context.Background(), AppendRecordsCommand{
			StreamID: streamID, ExpectedRevision: 1, RecordCount: 1, PayloadBytes: 10,
		})
		leaderDone <- err
	}()

	waitUntil(t, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.started >= 2 // create + leader append
	})

	queuedCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	_, err := service.AppendRecords(queuedCtx, AppendRecordsCommand{
		StreamID: streamID, ExpectedRevision: 1, RecordCount: 1, PayloadBytes: 20,
	})
	cancel()
	if !domain.IsCode(err, domain.CodeRequestTimeout) {
		t.Fatalf("gate-wait err = %v, want request_timeout", err)
	}

	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader append: %v", err)
	}

	state := service.readState()
	if state.Streams[streamID].Revision != 2 {
		t.Fatalf("revision = %d, want 2 (queued append leaked)", state.Streams[streamID].Revision)
	}
}

func TestLateCommittedMutationIsPublishedAndReplays(t *testing.T) {
	repo := &lateCommitRepository{
		InMemoryRepository: storage.NewInMemoryRepository("mem", nil),
	}
	service := newTestService(t, repo)
	streamID := createTestStream(t, service)

	repo.commitLate = true
	expired, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	_, err := service.AppendRecords(expired, AppendRecordsCommand{
		StreamID: streamID, ExpectedRevision: 1, RecordCount: 1, PayloadBytes: 10,
	})
	cancel()
	if err != nil {
		t.Fatalf("late committed append reported error: %v", err)
	}

	state := service.readState()
	if state.Streams[streamID].Revision != 2 {
		t.Fatalf("revision = %d, want 2 after late commit", state.Streams[streamID].Revision)
	}

	// Retrying against the advanced revision is a conflict, never a timeout.
	repo.commitLate = false
	if _, err := service.AppendRecords(context.Background(), AppendRecordsCommand{
		StreamID: streamID, ExpectedRevision: 1, RecordCount: 1, PayloadBytes: 10,
	}); !domain.IsCode(err, domain.CodeStaleRevision) {
		t.Fatalf("retry after late commit err = %v, want stale_revision", err)
	}
}

func TestValidationAndConflictClassificationUnchanged(t *testing.T) {
	service := newTestService(t, newControlledRepository())

	if _, err := service.CreateStream(context.Background(), CreateStreamCommand{
		Namespace: "ns", Name: "", ThresholdBytes: 64,
	}); !domain.IsCode(err, domain.CodeInvalidInput) {
		t.Fatalf("validation err = %v, want invalid_input", err)
	}

	streamID := createTestStream(t, service)
	if _, err := service.AppendRecords(context.Background(), AppendRecordsCommand{
		StreamID: streamID, ExpectedRevision: 99, RecordCount: 1, PayloadBytes: 10,
	}); !domain.IsCode(err, domain.CodeStaleRevision) {
		t.Fatalf("conflict err = %v, want stale_revision", err)
	}
}

// lateCommitRepository persists successfully but returns the expired
// context error, modeling a commit that lands just after the deadline.
type lateCommitRepository struct {
	*storage.InMemoryRepository
	commitLate bool
}

func (r *lateCommitRepository) Save(ctx context.Context, state *domain.State) error {
	if r.commitLate {
		<-ctx.Done()
		if err := r.InMemoryRepository.Save(context.Background(), state); err != nil {
			return err
		}
		return ctx.Err()
	}
	return r.InMemoryRepository.Save(ctx, state)
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before deadline")
}
