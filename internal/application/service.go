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
	mu           sync.RWMutex
	mutationGate chan struct{}
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
		if err := repository.Save(context.Background(), loaded); err != nil {
			return nil, err
		}
		logger.Info("startup.recovered", map[string]any{
			"aborted_plans":      report.AbortedPlanIDs,
			"recomputed_streams": report.RecomputedStreamIDs,
		})
	}

	return &Service{
		mutationGate: make(chan struct{}, 1),
		state:        loaded,
		repo:         repository,
		ids:          identifiers,
		clock:        timeSource,
		logger:       logger,
		limits:       limits,
	}, nil
}

func (s *Service) RepositoryPath() string {
	if s == nil || s.repo == nil {
		return ""
	}
	return s.repo.Path()
}

func (s *Service) readState() *domain.State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return domain.CloneState(s.state)
}

// mutate runs one state-changing workflow under the single serialization
// boundary. The request deadline is honored at three points: before any
// work starts, while waiting for the serialization gate, and while waiting
// for persistence. Once persistence has committed the snapshot the result
// is published and the request succeeds even if the deadline elapsed during
// the write, so a retried request can never create a second result or be
// told a committed mutation timed out.
func (s *Service) mutate(
	ctx context.Context,
	action string,
	change func(*domain.State, domainTimeline) error,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}

	if err := s.acquireMutation(ctx); err != nil {
		return err
	}
	defer func() { <-s.mutationGate }()

	s.mu.Lock()
	defer s.mu.Unlock()

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
	if err := contextError(ctx); err != nil {
		return err
	}

	if err := s.repo.Save(ctx, next); err != nil {
		// The write may have committed durably (for example a successful
		// rename followed by a directory sync failure). If the deadline is
		// what stopped the request but the snapshot is already on disk,
		// publish it and report success rather than advertising a timeout
		// for a result the caller will replay.
		if ctxErr := ctx.Err(); ctxErr != nil && s.isCommitted(next) {
			s.state = next
			s.logger.Info("state.committed", map[string]any{
				"action":          action,
				"path":            s.repo.Path(),
				"published_after": contextKind(ctxErr),
			})
			return nil
		}
		s.logger.Error("state.commit.failed", err, map[string]any{
			"action": action,
			"path":   s.repo.Path(),
		})
		return err
	}
	s.state = next
	s.logger.Info("state.committed", map[string]any{
		"action": action,
		"path":   s.repo.Path(),
	})
	return nil
}

// acquireMutation waits for the serialization gate while remaining
// interruptible by the request deadline or cancellation.
func (s *Service) acquireMutation(ctx context.Context) error {
	select {
	case s.mutationGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return contextError(ctx)
	}
}

// isCommitted reports whether the durable snapshot on disk already contains
// the candidate state. It performs a read-only reload and never mutates
// service state.
func (s *Service) isCommitted(candidate *domain.State) bool {
	persisted, err := s.repo.Load()
	if err != nil {
		return false
	}
	return stateSnapshotCommitted(persisted, candidate)
}

// stateSnapshotCommitted recognizes the candidate snapshot on disk. Every
// mutation adds entities and/or advances a stream revision, and the
// serialization gate guarantees no other commit can interleave, so equal
// revisions and the presence of every candidate entity identify this commit.
func stateSnapshotCommitted(persisted, candidate *domain.State) bool {
	if persisted == nil || candidate == nil {
		return false
	}
	for id, stream := range candidate.Streams {
		if stream == nil {
			continue
		}
		saved, ok := persisted.Streams[id]
		if !ok || saved == nil || saved.Revision != stream.Revision {
			return false
		}
	}
	for id := range candidate.Segments {
		if _, ok := persisted.Segments[id]; !ok {
			return false
		}
	}
	for id := range candidate.Plans {
		if _, ok := persisted.Plans[id]; !ok {
			return false
		}
	}
	return persisted.AuditSequence >= candidate.AuditSequence
}

type domainTimeline struct {
	current time.Time
}

func (t domainTimeline) Now() time.Time {
	return t.current
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return domain.FromContextError(ctx.Err())
}

func contextKind(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "context_done"
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
