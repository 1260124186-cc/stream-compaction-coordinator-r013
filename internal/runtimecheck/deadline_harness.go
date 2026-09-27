package runtimecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// deadlineHarness is a self-contained fixture for the deadline-semantics
// check. It owns a file-backed repository wrapped by a controllable slow
// repository, a live httptest server for wire-level status codes, an
// in-process handler for canceled contexts that never reach a socket, and
// the current application service for direct persistence assertions.
type deadlineHarness struct {
	root      string
	statePath string
	fileRepo  *storage.FileRepository
	slow      *slowRepository
	ids       *idgen.Counter
	clock     *clock.Fixed
	timeout   time.Duration
	server    *httptest.Server
	service   *application.Service
	client    *http.Client
}

func newDeadlineHarness(timeout time.Duration) (*deadlineHarness, error) {
	root, err := os.MkdirTemp("", "segmentd-deadline-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary root: %w", err)
	}
	fileRepo, err := storage.NewFileRepository(filepath.Join(root, "state.json"))
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	h := &deadlineHarness{
		root:      root,
		statePath: filepath.Join(root, "state.json"),
		fileRepo:  fileRepo,
		slow:      newSlowRepository(fileRepo),
		ids:       &idgen.Counter{},
		clock:     clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Second),
		timeout:   timeout,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
	if err := h.restart(); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	return h, nil
}

func (h *deadlineHarness) handler() http.Handler {
	return httpapi.New(h.service, httpapi.Options{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  h.timeout,
		Logger:          observability.Discard{},
	}).Handler()
}

// restart rebuilds the service from disk while keeping the same slow
// repository wrapper, so timing configuration and counters survive a
// simulated process restart.
func (h *deadlineHarness) restart() error {
	if h.server != nil {
		h.server.Close()
		h.server = nil
	}
	service, err := application.NewService(
		h.slow,
		h.ids,
		h.clock,
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		return fmt.Errorf("construct service: %w", err)
	}
	h.service = service
	h.server = httptest.NewServer(h.handler())
	return nil
}

func (h *deadlineHarness) close() {
	if h.server != nil {
		h.server.Close()
	}
	_ = os.RemoveAll(h.root)
}

// setRequestTimeout rebuilds the live server with a new per-request
// deadline while keeping the current service and durable state. It lets a
// scenario impose a very short server-side deadline without disconnecting
// the client, so the response status is still observable on the wire.
func (h *deadlineHarness) setRequestTimeout(timeout time.Duration) {
	h.timeout = timeout
	if h.server != nil {
		h.server.Close()
	}
	h.server = httptest.NewServer(h.handler())
}

type httpResult struct {
	status int
	body   []byte
	envelope
}

type envelope struct {
	Error wireErrorBody `json:"error"`
}

type wireErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// do issues one request over the live test server.
func (h *deadlineHarness) do(
	ctx context.Context,
	method string,
	path string,
	body any,
) (*httpResult, error) {
	request, err := h.newRequest(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	response, err := h.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	return parseResult(response.StatusCode, data)
}

// record issues one request in-process against the real middleware stack so
// responses are observable even when the supplied context is already
// canceled and would never reach a network client.
func (h *deadlineHarness) record(
	ctx context.Context,
	method string,
	path string,
	body any,
) (*httpResult, error) {
	request, err := h.newRequest(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	recorder := httptest.NewRecorder()
	h.handler().ServeHTTP(recorder, request)
	return parseResult(recorder.Code, recorder.Body.Bytes())
}

func (h *deadlineHarness) newRequest(
	ctx context.Context,
	method string,
	path string,
	body any,
) (*http.Request, error) {
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, h.server.URL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

func parseResult(status int, data []byte) (*httpResult, error) {
	result := &httpResult{status: status, body: data}
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &result.envelope); err != nil {
			return nil, fmt.Errorf("decode error envelope %q: %w", string(data), err)
		}
	}
	return result, nil
}

// loadedState reads the snapshot straight from the durable repository,
// bypassing the in-memory service cache.
func (h *deadlineHarness) loadedState() (*domain.State, error) {
	return h.fileRepo.Load()
}

func jsonDecode(data []byte, target any) error {
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode response %q: %w", string(data), err)
	}
	return nil
}
