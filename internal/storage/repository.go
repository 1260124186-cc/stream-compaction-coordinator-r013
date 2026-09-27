package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

const DefaultMaximumSnapshotBytes = int64(64 << 20)

// Repository is the durable boundary used by the application package.
//
// SaveContext is the deadline-aware variant used by request processing. A
// context error returned before the snapshot is durably committed means no
// state was published. Once the atomic commit lands, the method returns nil
// even if the context has since expired, because an already committed result
// must be reported as success so retries stay idempotent instead of racing a
// second result into place.
type Repository interface {
	Load() (*domain.State, error)
	Save(*domain.State) error
	SaveContext(ctx context.Context, state *domain.State) error
	Path() string
}

type FileRepository struct {
	path           string
	maximumBytes   int64
	marshalState   func(*domain.State) ([]byte, error)
	unmarshalState func([]byte) (*domain.State, error)
}

func NewFileRepository(path string) (*FileRepository, error) {
	cleanPath := strings.TrimSpace(path)
	if cleanPath == "" {
		return nil, domain.NewError(domain.CodeInvalidInput, "state path must not be empty")
	}
	cleanPath = filepath.Clean(cleanPath)
	return &FileRepository{
		path:           cleanPath,
		maximumBytes:   DefaultMaximumSnapshotBytes,
		marshalState:   MarshalState,
		unmarshalState: UnmarshalState,
	}, nil
}

func (r *FileRepository) Path() string {
	if r == nil {
		return ""
	}
	return r.path
}

func (r *FileRepository) Load() (*domain.State, error) {
	if r == nil {
		return nil, domain.NewError(domain.CodePersistenceFailure, "file repository is nil")
	}
	data, err := os.ReadFile(r.path)
	if errors.Is(err, fs.ErrNotExist) {
		return domain.NewState(domainTimeZero()), nil
	}
	if err != nil {
		return nil, domain.WrapError(
			domain.CodePersistenceFailure,
			"cannot read state snapshot",
			err,
		).WithDetail("path", r.path)
	}
	if int64(len(data)) > r.maximumBytes {
		return nil, domain.NewError(domain.CodeLimitExceeded, "state snapshot exceeds the configured maximum").
			WithDetail("path", r.path).
			WithDetail("maximum_bytes", r.maximumBytes).
			WithDetail("actual_bytes", len(data))
	}

	state, err := r.unmarshalState(data)
	if err != nil {
		return nil, domain.WrapError(
			domain.CodePersistenceFailure,
			"cannot decode state snapshot",
			err,
		).WithDetail("path", r.path)
	}
	if err := domain.ValidateState(state); err != nil {
		return nil, domain.WrapError(
			domain.CodeInconsistentState,
			"state snapshot failed validation",
			err,
		).WithDetail("path", r.path)
	}
	return state, nil
}

func (r *FileRepository) Save(state *domain.State) error {
	return r.SaveContext(context.Background(), state)
}

func (r *FileRepository) SaveContext(ctx context.Context, state *domain.State) error {
	if r == nil {
		return domain.NewError(domain.CodePersistenceFailure, "file repository is nil")
	}
	if state == nil {
		return domain.NewError(domain.CodePersistenceFailure, "cannot persist a nil state")
	}
	if err := domain.ValidateState(state); err != nil {
		return domain.WrapError(
			domain.CodeInconsistentState,
			"refusing to persist invalid state",
			err,
		)
	}

	data, err := r.marshalState(state)
	if err != nil {
		return domain.WrapError(
			domain.CodePersistenceFailure,
			"cannot encode state snapshot",
			err,
		)
	}
	if int64(len(data)) > r.maximumBytes {
		return domain.NewError(domain.CodeLimitExceeded, "encoded state snapshot exceeds the configured maximum").
			WithDetail("maximum_bytes", r.maximumBytes).
			WithDetail("actual_bytes", len(data))
	}
	if err := WriteAtomicallyContext(ctx, r.path, data); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return domain.WrapError(
			domain.CodePersistenceFailure,
			"cannot commit state snapshot",
			err,
		).WithDetail("path", r.path)
	}
	return nil
}

// InMemoryRepository is useful to workflow checks and tests without weakening
// the production file-oriented implementation.
type InMemoryRepository struct {
	path  string
	state *domain.State
}

func NewInMemoryRepository(path string, initial *domain.State) *InMemoryRepository {
	if initial == nil {
		initial = domain.NewState(domainTimeZero())
	}
	return &InMemoryRepository{
		path:  path,
		state: domain.CloneState(initial),
	}
}

func (r *InMemoryRepository) Path() string {
	if r == nil {
		return ""
	}
	return r.path
}

func (r *InMemoryRepository) Load() (*domain.State, error) {
	if r == nil {
		return nil, domain.NewError(domain.CodePersistenceFailure, "memory repository is nil")
	}
	return domain.CloneState(r.state), nil
}

func (r *InMemoryRepository) Save(state *domain.State) error {
	return r.SaveContext(context.Background(), state)
}

func (r *InMemoryRepository) SaveContext(ctx context.Context, state *domain.State) error {
	if r == nil {
		return domain.NewError(domain.CodePersistenceFailure, "memory repository is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := domain.ValidateState(state); err != nil {
		return err
	}
	r.state = domain.CloneState(state)
	return nil
}

func (r *FileRepository) String() string {
	if r == nil {
		return "<nil>"
	}
	return fmt.Sprintf("file repository(%s)", r.path)
}
