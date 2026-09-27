package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

type Limits struct {
	MaxAppendRecords uint64
	MaxAppendBytes   int64
}

type Service struct {
	// mutationGate serializes all state-changing workflows at one mutation
	// gate. It is distinct from mu so a request can wait for its turn without
	// holding the state lock, and so the wait is interruptible by the request
	// deadline.
	mutationGate sync.Mutex
	mu           sync.RWMutex
	state        *domain.State
	repo         storage.Repository
	ids          idgen.Generator
	clock        clock.Clock
	logger       observability.Logger
	limits       Limits
}

func NewService(
	repository storage.Repository,
	identifiers idgen.Generator,
	timeSource clock.Clock,
	logger observability.Logger,
	limits Limits,
) (*Service, error) {
	if repository == nil {
		return nil, domain.NewError(domain.CodeInvalidInput, "repository is required")
	}
	if identifiers == nil {
		return nil, domain.NewError(domain.CodeInvalidInput, "identifier generator is required")
	}
	if timeSource == nil {
		return nil, domain.NewError(domain.CodeInvalidInput, "time source is required")
	}
	if logger == nil {
		logger = observability.Discard{}
	}
	if limits.MaxAppendRecords == 0 {
		limits.MaxAppendRecords = 10_000
	}
	if limits.MaxAppendBytes <= 0 {
		limits.MaxAppendBytes = 16 << 20
	}

	loaded, err := repository.Load()
	if err != nil {
		return nil, err
	}
	report, err := recoverState(loaded, timeSource.Now())
	if err != nil {
		return nil, err
	}
	if report.Changed {
		if err := repository.Save(loaded); err != nil {
			return nil, err
		}
		logger.Info("startup.recovered", map[string]any{
			"aborted_plans":      report.AbortedPlanIDs,
			"recomputed_streams": report.RecomputedStreamIDs,
		})
	}

	return &Service{
		state:  loaded,
		repo:   repository,
		ids:    identifiers,
		clock:  timeSource,
		logger: logger,
		limits: limits,
	}, nil
}

func (s *Service) RepositoryPath() string {
	if s == nil || s.repo == nil {
		return ""
	}
	return s.repo.Path()
}

func (s *Service) readState(ctx context.Context) (*domain.State, error) {
	if err := waitForRLock(ctx, &s.mu); err != nil {
		return nil, err
	}
	defer s.mu.RUnlock()
	return domain.CloneState(s.state), nil
}

// gateAcquire blocks until the single mutation gate is free or the context is
// canceled / past its deadline. A request that loses the race here has not
// touched any state and returns a stable retryable cancellation/timeout error.
func gateAcquire(ctx context.Context, gate *sync.Mutex) error {
	return waitForLock(ctx, gate.TryLock)
}

// waitForLock waits for an exclusive lock through an interruptible polling
// loop, so a deadline aborts the wait instead of queuing a mutation that runs
// after the caller has already given up.
func waitForLock(ctx context.Context, tryLock func() bool) error {
	for {
		if err := contextError(ctx); err != nil {
			return err
		}
		if tryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return contextError(ctx)
		case <-time.After(lockPollInterval):
		}
	}
}

func waitForRLock(ctx context.Context, lock *sync.RWMutex) error {
	return waitForLock(ctx, lock.TryRLock)
}

const lockPollInterval = 2 * time.Millisecond

func (s *Service) mutate(
	ctx context.Context,
	action string,
	change func(*domain.State, domainTimeline) error,
) error {
	// Boundary 1: processing must not start for an already canceled request.
	if err := contextError(ctx); err != nil {
		return err
	}

	// Boundary 2: queue behind the serialized commit without swallowing the
	// deadline; a timeout while waiting must not run the mutation afterwards.
	if err := gateAcquire(ctx, &s.mutationGate); err != nil {
		return err
	}
	defer s.mutationGate.Unlock()

	if err := waitForLock(ctx, s.mu.TryLock); err != nil {
		return err
	}
	defer s.mu.Unlock()

	// Re-check after acquiring the gate: state may have moved while queued and
	// the deadline may already have passed.
	if err := contextError(ctx); err != nil {
		return err
	}
	next := domain.CloneState(s.state)
	timeline := domainTimeline{current: s.clock.Now().UTC()}
	if err := change(next, timeline); err != nil {
		return err
	}
	if err := domain.ValidateState(next); err != nil {
		return err
	}
	// Boundary 3a: do not begin durable publication once the deadline has
	// already passed; the cloned state is discarded and nothing is published.
	if err := contextError(ctx); err != nil {
		return err
	}
	saveErr := s.repo.SaveContext(ctx, next)
	if saveErr != nil {
		// SaveContext returns the raw context error only when the snapshot did
		// not commit. If the repository reports any other failure the prior
		// state remains in place.
		if errors.Is(saveErr, context.Canceled) || errors.Is(saveErr, context.DeadlineExceeded) {
			return saveErr
		}
		s.logger.Error("state.commit.failed", saveErr, map[string]any{
			"action": action,
			"path":   s.repo.Path(),
		})
		return saveErr
	}
	// Boundary 3b: the snapshot is durably published. Even if the context
	// expired during the write, the committed result wins: publish it in
	// memory and report success so a client retry sees the idempotent result
	// instead of a fabricated timeout that could drive a double apply.
	s.state = next
	s.logger.Info("state.committed", map[string]any{
		"action":          action,
		"path":            s.repo.Path(),
		"context_expired": ctx.Err() != nil,
	})
	return nil
}

type domainTimeline struct {
	current time.Time
}

func (t domainTimeline) Now() time.Time {
	return t.current
}

// contextError translates the context lifecycle into stable domain errors that
// survive crossing into the transport layer. Cancellation and timeout are
// never reported as invalid input: a caller must be able to distinguish a
// malformed request (400, do not retry) from a deadline it can retry.
func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	switch ctx.Err() {
	case context.Canceled:
		return domain.WrapError(domain.CodeRequestCanceled, "request was canceled", context.Canceled)
	case context.DeadlineExceeded:
		return domain.WrapError(domain.CodeRequestTimeout, "request deadline was exceeded", context.DeadlineExceeded)
	default:
		return nil
	}
}

func streamSegments(state *domain.State, streamID string) []*domain.Segment {
	result := make([]*domain.Segment, 0)
	for _, segment := range state.Segments {
		if segment != nil && segment.StreamID == streamID {
			result = append(result, segment)
		}
	}
	return result
}

func normalizedIdentifier(value string) string {
	return strings.TrimSpace(value)
}

func resourceNotFound(kind string, id string) error {
	return domain.NewErrorf(domain.CodeNotFound, "%s was not found", kind).
		WithDetail("id", id)
}

func validateLimits(limits Limits) error {
	if limits.MaxAppendRecords == 0 {
		return fmt.Errorf("append record limit must be positive")
	}
	if limits.MaxAppendBytes <= 0 {
		return fmt.Errorf("append byte limit must be positive")
	}
	return nil
}
