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
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// deadlineShortTimeout is the per-request deadline on the "fast deadline"
// API instance. It is intentionally far smaller than the storage hook delays
// used by the scenarios.
const deadlineShortTimeout = 25 * time.Millisecond

// slowRepository is a controllable, file-backed Repository used to exercise
// the deadline boundary around durable publication. Hooks run either
// immediately before the snapshot is durably committed (preCommit) or
// immediately afterwards, before the in-memory publish (postCommit).
type slowRepository struct {
	mu         sync.Mutex
	fileRepo   *storage.FileRepository
	path       string
	preCommit  func(context.Context)
	postCommit func(context.Context)
}

func newSlowRepository(path string) (*slowRepository, error) {
	fileRepo, err := storage.NewFileRepository(path)
	if err != nil {
		return nil, err
	}
	return &slowRepository{fileRepo: fileRepo, path: path}, nil
}

func (r *slowRepository) setPreCommit(hook func(context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.preCommit = hook
}

func (r *slowRepository) setPostCommit(hook func(context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.postCommit = hook
}

func (r *slowRepository) Load() (*domain.State, error) {
	return r.fileRepo.Load()
}

func (r *slowRepository) Path() string { return r.path }

func (r *slowRepository) Save(state *domain.State) error {
	return r.SaveContext(context.Background(), state)
}

func (r *slowRepository) SaveContext(ctx context.Context, state *domain.State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := domain.ValidateState(state); err != nil {
		return err
	}

	r.mu.Lock()
	before := r.preCommit
	r.mu.Unlock()
	if before != nil {
		before(ctx)
	}
	// The deadline gets its last vote before atomic publication: an expired
	// context abandons the snapshot and nothing becomes observable.
	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := storage.MarshalState(state)
	if err != nil {
		return domain.WrapError(domain.CodePersistenceFailure, "cannot encode state snapshot", err)
	}
	if err := storage.WriteAtomicallyContext(ctx, r.path, data); err != nil {
		return err
	}

	r.mu.Lock()
	after := r.postCommit
	r.mu.Unlock()
	if after != nil {
		// The snapshot is already durable here. The hook simulates slow
		// post-commit work and observes the request context, so the service
		// can reconcile the committed result instead of stalling forever.
		after(ctx)
	}
	return nil
}

type deadlineHarness struct {
	root    string
	repo    *slowRepository
	ids     *idgen.Counter
	clock   *clock.Fixed
	service *application.Service
	normal  *httptest.Server // generous per-request timeout
	short   *httptest.Server // very short per-request timeout
	client  *http.Client
}

func newDeadlineHarness() (*deadlineHarness, error) {
	root, err := os.MkdirTemp("", "segmentd-deadline-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary root: %w", err)
	}
	repo, err := newSlowRepository(filepath.Join(root, "state.json"))
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	h := &deadlineHarness{
		root:   root,
		repo:   repo,
		ids:    &idgen.Counter{},
		clock:  clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Second),
		client: &http.Client{},
	}
	if err := h.start(); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	return h, nil
}

func (h *deadlineHarness) start() error {
	service, err := application.NewService(
		h.repo,
		h.ids,
		h.clock,
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		return fmt.Errorf("construct service: %w", err)
	}
	h.service = service
	h.normal = httptest.NewServer(httpapi.New(service, httpapi.Options{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  5 * time.Second,
		Logger:          observability.Discard{},
	}).Handler())
	h.short = httptest.NewServer(httpapi.New(service, httpapi.Options{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  deadlineShortTimeout,
		Logger:          observability.Discard{},
	}).Handler())
	return nil
}

// restart rebuilds the service from the durable snapshot, like a process
// restart.
func (h *deadlineHarness) restart() error {
	h.normal.Close()
	h.short.Close()
	return h.start()
}

func (h *deadlineHarness) close() {
	if h.normal != nil {
		h.normal.Close()
	}
	if h.short != nil {
		h.short.Close()
	}
	_ = os.RemoveAll(h.root)
}

type rawResponse struct {
	status int
	body   []byte
}

func (h *deadlineHarness) raw(
	ctx context.Context,
	server *httptest.Server,
	method string,
	path string,
	body any,
) (rawResponse, error) {
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return rawResponse{}, fmt.Errorf("encode body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, server.URL+path, reader)
	if err != nil {
		return rawResponse{}, fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return rawResponse{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return rawResponse{}, fmt.Errorf("read response: %w", err)
	}
	return rawResponse{status: response.StatusCode, body: data}, nil
}

// serveInProcess dispatches a request straight through the handler without a
// network connection. It deterministically observes responses to contexts
// that are already canceled, where a real HTTP client would only see a closed
// connection.
func serveInProcess(
	server *httptest.Server,
	method string,
	target string,
	body any,
	ctx context.Context,
) rawResponse {
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	}
	request := httptest.NewRequestWithContext(ctx, method, target, reader)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	server.Config.Handler.ServeHTTP(recorder, request)
	return rawResponse{status: recorder.Code, body: recorder.Body.Bytes()}
}

func errorEnvelopeFrom(raw rawResponse) (errorBodyEnvelope, error) {
	var envelope errorBodyEnvelope
	if err := json.Unmarshal(raw.body, &envelope); err != nil {
		return errorBodyEnvelope{}, fmt.Errorf("decode error envelope %q: %w", string(raw.body), err)
	}
	return envelope, nil
}

func createDeadlineStream(
	ctx context.Context,
	h *deadlineHarness,
	server *httptest.Server,
	name string,
	threshold int64,
) (application.CreateStreamResult, rawResponse, error) {
	var result application.CreateStreamResult
	raw, err := h.raw(ctx, server, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       "field-lab",
		"name":            name,
		"threshold_bytes": threshold,
	})
	if err != nil {
		return result, raw, err
	}
	if raw.status == http.StatusCreated {
		if err := json.Unmarshal(raw.body, &result); err != nil {
			return result, raw, fmt.Errorf("decode create result: %w", err)
		}
	}
	return result, raw, nil
}

func appendDeadlineBatch(
	ctx context.Context,
	h *deadlineHarness,
	server *httptest.Server,
	streamID string,
	revision uint64,
	payload int64,
) (application.AppendRecordsResult, rawResponse, error) {
	var result application.AppendRecordsResult
	raw, err := h.raw(ctx, server, http.MethodPost, "/v1/streams/"+streamID+"/records", map[string]any{
		"expected_revision": revision,
		"record_count":      1,
		"payload_bytes":     payload,
	})
	if err != nil {
		return result, raw, err
	}
	if raw.status == http.StatusOK {
		if err := json.Unmarshal(raw.body, &result); err != nil {
			return result, raw, fmt.Errorf("decode append result: %w", err)
		}
	}
	return result, raw, nil
}

func readDeadlineStream(
	ctx context.Context,
	h *deadlineHarness,
	streamID string,
) (domain.Stream, error) {
	raw, err := h.raw(ctx, h.normal, http.MethodGet, "/v1/streams/"+streamID, nil)
	if err != nil {
		return domain.Stream{}, err
	}
	var stream domain.Stream
	if err := json.Unmarshal(raw.body, &stream); err != nil {
		return domain.Stream{}, fmt.Errorf("decode stream: %w", err)
	}
	return stream, nil
}

func readDeadlineWatermark(
	ctx context.Context,
	h *deadlineHarness,
	streamID string,
) (application.WatermarkResult, rawResponse, error) {
	var result application.WatermarkResult
	raw, err := h.raw(ctx, h.normal, http.MethodGet, "/v1/streams/"+streamID+"/watermark", nil)
	if err != nil {
		return result, raw, err
	}
	if raw.status == http.StatusOK {
		if err := json.Unmarshal(raw.body, &result); err != nil {
			return result, raw, fmt.Errorf("decode watermark: %w", err)
		}
	}
	return result, raw, nil
}

// runRequestDeadlines verifies one external contract for request cancellation
// and timeout across every phase of request processing.
func runRequestDeadlines(ctx context.Context, assertions *counterAssertions) error {
	h, err := newDeadlineHarness()
	if err != nil {
		return err
	}
	defer h.close()

	// ---- Scenario 1: deadline already elapsed before processing starts.
	// An already-expired client context cannot carry a response back over a
	// real socket, so the start boundary is asserted in process; later
	// scenarios observe the same codes over HTTP once the request is in
	// flight.
	expired, cancelExpired := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancelExpired()
	raw := serveInProcess(h.short, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       "field-lab",
		"name":            "already-expired",
		"threshold_bytes": 64,
	}, expired)
	if err := assertions.require(
		raw.status == http.StatusGatewayTimeout,
		"pre-expired deadline returned status %d, want 504: %s",
		raw.status, raw.body,
	); err != nil {
		return err
	}
	envelope, err := errorEnvelopeFrom(raw)
	if err != nil {
		return err
	}
	if err := assertions.require(
		envelope.Error.Code == string(domain.CodeRequestTimeout),
		"pre-expired deadline error code is %q, want request_timeout",
		envelope.Error.Code,
	); err != nil {
		return err
	}
	// Nothing may have been published: the first identifier was never spent.
	if probe, probeErr := h.raw(ctx, h.normal, http.MethodGet, "/v1/streams/str_000001", nil); probeErr != nil {
		return probeErr
	} else if err := assertions.require(
		probe.status == http.StatusNotFound,
		"pre-expired request published a stream: GET status %d",
		probe.status,
	); err != nil {
		return err
	}

	// The same elapsed boundary for an explicit cancellation keeps cancel
	// semantics: 408 request_canceled, never 400 invalid_input.
	canceled, cancelBeforeStart := context.WithCancel(ctx)
	cancelBeforeStart()
	cancelRaw := serveInProcess(h.short, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       "field-lab",
		"name":            "canceled-before-start",
		"threshold_bytes": 64,
	}, canceled)
	if err := assertions.require(
		cancelRaw.status == http.StatusRequestTimeout,
		"pre-canceled request returned status %d, want 408: %s",
		cancelRaw.status, cancelRaw.body,
	); err != nil {
		return err
	}
	cancelEnvelope, err := errorEnvelopeFrom(cancelRaw)
	if err != nil {
		return err
	}
	if err := assertions.require(
		cancelEnvelope.Error.Code == string(domain.CodeRequestCanceled),
		"pre-canceled request error code is %q, want request_canceled",
		cancelEnvelope.Error.Code,
	); err != nil {
		return err
	}

	// ---- Baseline: normal requests succeed on the generous-deadline API.
	created, raw, err := createDeadlineStream(ctx, h, h.normal, "deadline-flow", 1000)
	if err != nil {
		return err
	}
	if err := assertions.require(raw.status == http.StatusCreated,
		"normal create returned %d: %s", raw.status, raw.body); err != nil {
		return err
	}
	streamID := created.Stream.ID
	setup, raw, err := appendDeadlineBatch(ctx, h, h.normal, streamID, 1, 10)
	if err != nil {
		return err
	}
	if err := assertions.require(
		raw.status == http.StatusOK && setup.Stream.Revision == 2,
		"normal append returned status %d revision %d",
		raw.status, setup.Stream.Revision,
	); err != nil {
		return err
	}

	// ---- Scenario 2: deadline elapses while waiting for serialized commit.
	gateAcquired := make(chan struct{})
	releaseHolder := make(chan struct{})
	holderDone := make(chan struct{})
	var holderErr error
	go func() {
		defer close(holderDone)
		h.repo.setPreCommit(func(commitCtx context.Context) {
			close(gateAcquired)
			select {
			case <-releaseHolder:
			case <-commitCtx.Done():
			}
		})
		_, _, holderErr = appendDeadlineBatch(ctx, h, h.normal, streamID, 2, 10)
	}()
	select {
	case <-gateAcquired:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("holder never reached the serialized commit gate")
	}

	queued, cancelQueued := context.WithTimeout(ctx, 30*time.Millisecond)
	queuedRaw := serveInProcess(h.normal, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{"expected_revision": 3, "record_count": 1, "payload_bytes": 10},
		queued)
	cancelQueued()
	if err := assertions.require(
		queuedRaw.status == http.StatusGatewayTimeout,
		"queued request returned %d, want 504: %s",
		queuedRaw.status, queuedRaw.body,
	); err != nil {
		return err
	}
	queuedEnvelope, err := errorEnvelopeFrom(queuedRaw)
	if err != nil {
		return err
	}
	if err := assertions.require(
		queuedEnvelope.Error.Code == string(domain.CodeRequestTimeout),
		"queued request code is %q, want request_timeout",
		queuedEnvelope.Error.Code,
	); err != nil {
		return err
	}

	// Release the in-flight commit; only it may land.
	close(releaseHolder)
	select {
	case <-holderDone:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("in-flight commit never finished")
	}
	if holderErr != nil {
		return fmt.Errorf("in-flight append failed: %w", holderErr)
	}
	h.repo.setPreCommit(nil)

	current, err := readDeadlineStream(ctx, h, streamID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		current.Revision == 3,
		"timed-out queued request published revision %d, want 3",
		current.Revision,
	); err != nil {
		return err
	}

	// ---- Scenario 3: deadline during persistence, before publication.
	blocked := make(chan struct{})
	h.repo.setPreCommit(func(commitCtx context.Context) {
		select {
		case <-blocked:
		case <-commitCtx.Done():
		}
	})
	persistTimed, cancelPersist := context.WithTimeout(ctx, 30*time.Millisecond)
	timedRaw := serveInProcess(h.normal, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{"expected_revision": 3, "record_count": 1, "payload_bytes": 10},
		persistTimed)
	cancelPersist()
	if err := assertions.require(
		timedRaw.status == http.StatusGatewayTimeout,
		"pre-persist deadline returned %d, want 504: %s",
		timedRaw.status, timedRaw.body,
	); err != nil {
		return err
	}
	close(blocked)
	// Let the abandoned attempt unwind without a hook.
	time.Sleep(20 * time.Millisecond)
	h.repo.setPreCommit(nil)

	stalled, err := readDeadlineStream(ctx, h, streamID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		stalled.Revision == 3,
		"pre-persist timeout published revision %d, want 3",
		stalled.Revision,
	); err != nil {
		return err
	}

	// Replay the same logical append with storage fast again: it commits
	// exactly once, proving the timeout was safely retryable.
	replay, replayRaw, err := appendDeadlineBatch(ctx, h, h.normal, streamID, 3, 10)
	if err != nil {
		return err
	}
	if err := assertions.require(
		replayRaw.status == http.StatusOK && replay.Stream.Revision == 4,
		"replay after pre-persist timeout returned status %d revision %d",
		replayRaw.status, replay.Stream.Revision,
	); err != nil {
		return err
	}

	// ---- Scenario 4: commit wins against a deadline during persistence.
	planStreamID, planID, err := prepareDeadlinePlan(ctx, h)
	if err != nil {
		return err
	}
	postGate := make(chan struct{})
	h.repo.setPostCommit(func(commitCtx context.Context) {
		// Snapshot already durable. Outlive the short server-side deadline so
		// its context ends, then let the service reconcile the commit.
		select {
		case <-time.After(200 * time.Millisecond):
		case <-commitCtx.Done():
		}
		<-commitCtx.Done()
		close(postGate)
	})
	var finish application.CompactionPlanResult
	finishStarted := time.Now()
	finishRaw, err := h.raw(ctx, h.short, http.MethodPost,
		"/v1/compaction-plans/"+planID+"/finish",
		map[string]any{"result_checksum": "sha256:compacted", "result_size_bytes": 160})
	if err != nil {
		return fmt.Errorf("commit-wins finish transport error: %w", err)
	}
	if err := assertions.require(
		finishRaw.status == http.StatusOK,
		"committed finish was misreported as %d: %s",
		finishRaw.status, finishRaw.body,
	); err != nil {
		return err
	}
	if err := assertions.require(
		time.Since(finishStarted) < 2*time.Second,
		"committed finish stalled instead of reconciling",
	); err != nil {
		return err
	}
	if err := json.Unmarshal(finishRaw.body, &finish); err != nil {
		return fmt.Errorf("decode committed finish: %w", err)
	}
	if err := assertions.require(
		finish.Plan.State == domain.PlanStateCompleted && finish.ResultSegment != nil,
		"committed plan result is incomplete: %+v", finish,
	); err != nil {
		return err
	}
	resultSegmentID := finish.ResultSegment.ID
	select {
	case <-postGate:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("post-commit hook never observed context end")
	}
	h.repo.setPostCommit(nil)

	// Current read observes the committed result.
	currentWatermark, watermarkRaw, err := readDeadlineWatermark(ctx, h, planStreamID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		watermarkRaw.status == http.StatusOK && currentWatermark.CompactionFloor == 40,
		"current read after raced commit is %+v", currentWatermark,
	); err != nil {
		return err
	}

	// Retry the identical finish: idempotent replay, same result segment, no
	// fabricated timeout and no second result set.
	var repeated application.CompactionPlanResult
	repeatedRaw, err := h.raw(ctx, h.normal, http.MethodPost,
		"/v1/compaction-plans/"+planID+"/finish",
		map[string]any{"result_checksum": "sha256:compacted", "result_size_bytes": 160})
	if err != nil {
		return err
	}
	if err := assertions.require(repeatedRaw.status == http.StatusOK,
		"idempotent finish retry returned %d: %s", repeatedRaw.status, repeatedRaw.body); err != nil {
		return err
	}
	if err := json.Unmarshal(repeatedRaw.body, &repeated); err != nil {
		return fmt.Errorf("decode repeated finish: %w", err)
	}
	if err := assertions.require(
		repeated.AlreadyFinished && repeated.ResultSegment.ID == resultSegmentID,
		"finish retry was not idempotent: %+v", repeated,
	); err != nil {
		return err
	}

	// Restart read: exactly one committed result survives.
	if err := h.restart(); err != nil {
		return err
	}
	restartPlanRaw, err := h.raw(ctx, h.normal, http.MethodGet,
		"/v1/compaction-plans/"+planID, nil)
	if err != nil {
		return err
	}
	var restartPlan domain.CompactionPlan
	if err := json.Unmarshal(restartPlanRaw.body, &restartPlan); err != nil {
		return fmt.Errorf("decode restarted plan: %w", err)
	}
	if err := assertions.require(
		restartPlanRaw.status == http.StatusOK &&
			restartPlan.State == domain.PlanStateCompleted &&
			restartPlan.ResultSegmentID == resultSegmentID,
		"restart lost or duplicated the raced commit: %+v", restartPlan,
	); err != nil {
		return err
	}
	restartWatermark, restartWatermarkRaw, err := readDeadlineWatermark(ctx, h, planStreamID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		restartWatermarkRaw.status == http.StatusOK &&
			restartWatermark.CompactionFloor == 40 &&
			restartWatermark.Watermark == 40,
		"restart watermark after raced commit is %+v", restartWatermark,
	); err != nil {
		return err
	}

	// ---- Scenario 5: client-side cancellation keeps cancellation semantics.
	cancelStream, cancelCreateRaw, err := createDeadlineStream(
		ctx, h, h.normal, "cancel-holder", 1000,
	)
	if err != nil {
		return err
	}
	if err := assertions.require(cancelCreateRaw.status == http.StatusCreated,
		"cancel-holder create returned %d", cancelCreateRaw.status); err != nil {
		return err
	}
	cancelGate := make(chan struct{})
	releaseCancel := make(chan struct{})
	h.repo.setPreCommit(func(commitCtx context.Context) {
		close(cancelGate)
		select {
		case <-releaseCancel:
		case <-commitCtx.Done():
		}
	})
	clientCancel, cancelClient := context.WithTimeout(ctx, 30*time.Millisecond)
	_, cancelHTTPErr := h.raw(clientCancel, h.normal, http.MethodPost,
		"/v1/streams/"+cancelStream.Stream.ID+"/records",
		map[string]any{"expected_revision": 1, "record_count": 1, "payload_bytes": 5})
	cancelClient()
	select {
	case <-cancelGate:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("cancel holder never blocked")
	}
	if err := assertions.require(
		cancelHTTPErr != nil,
		"client-canceled request unexpectedly returned a response: %v",
		cancelHTTPErr,
	); err != nil {
		return err
	}
	close(releaseCancel)
	time.Sleep(20 * time.Millisecond)
	h.repo.setPreCommit(nil)

	probed, err := readDeadlineStream(ctx, h, cancelStream.Stream.ID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		probed.Revision == 1,
		"canceled request published revision %d, want 1",
		probed.Revision,
	); err != nil {
		return err
	}

	// The in-process path proves a request whose context is canceled while
	// queued behind the serialized commit gets 408 request_canceled rather
	// than a 400.
	queuedStream, queuedCreateRaw, err := createDeadlineStream(
		ctx, h, h.normal, "cancel-queued", 1000,
	)
	if err != nil {
		return err
	}
	if err := assertions.require(queuedCreateRaw.status == http.StatusCreated,
		"queued-cancel stream create returned %d", queuedCreateRaw.status); err != nil {
		return err
	}
	holderStarted := make(chan struct{})
	releaseQueuedHolder := make(chan struct{})
	h.repo.setPreCommit(func(commitCtx context.Context) {
		close(holderStarted)
		select {
		case <-releaseQueuedHolder:
		case <-commitCtx.Done():
		}
	})
	holderResult := make(chan struct {
		raw rawResponse
		err error
	}, 1)
	go func() {
		raw, err := h.raw(ctx, h.normal, http.MethodPost,
			"/v1/streams/"+queuedStream.Stream.ID+"/records",
			map[string]any{"expected_revision": 1, "record_count": 1, "payload_bytes": 1})
		holderResult <- struct {
			raw rawResponse
			err error
		}{raw, err}
	}()
	select {
	case <-holderStarted:
	case <-time.After(2 * time.Second):
		return fmt.Errorf("queued-cancel holder never blocked")
	}

	queuedCancel, doCancel := context.WithCancel(ctx)
	cancelDone := make(chan rawResponse, 1)
	go func() {
		cancelDone <- serveInProcess(h.normal, http.MethodPost,
			"/v1/streams/"+queuedStream.Stream.ID+"/records",
			map[string]any{"expected_revision": 2, "record_count": 1, "payload_bytes": 1},
			queuedCancel)
	}()
	time.Sleep(20 * time.Millisecond)
	doCancel()
	queuedCancelRaw := <-cancelDone
	close(releaseQueuedHolder)
	holder := <-holderResult
	if err := assertions.require(
		holder.err == nil && holder.raw.status == http.StatusOK,
		"queued-cancel holder did not commit: %v %s", holder.err, holder.raw.body,
	); err != nil {
		return err
	}
	h.repo.setPreCommit(nil)
	if err := assertions.require(
		queuedCancelRaw.status == http.StatusRequestTimeout,
		"canceled-while-queued returned %d, want 408: %s",
		queuedCancelRaw.status, queuedCancelRaw.body,
	); err != nil {
		return err
	}
	queuedCancelEnvelope, err := errorEnvelopeFrom(queuedCancelRaw)
	if err != nil {
		return err
	}
	if err := assertions.require(
		queuedCancelEnvelope.Error.Code == string(domain.CodeRequestCanceled),
		"canceled-while-queued code is %q, want request_canceled",
		queuedCancelEnvelope.Error.Code,
	); err != nil {
		return err
	}
	// The queued cancel landed nothing: only the holder's revision 2 exists.
	queuedRead, err := readDeadlineStream(ctx, h, queuedStream.Stream.ID)
	if err != nil {
		return err
	}
	if err := assertions.require(
		queuedRead.Revision == 2,
		"canceled queued request published revision %d, want 2",
		queuedRead.Revision,
	); err != nil {
		return err
	}

	// ---- Ordinary validation and conflict classifications are unchanged.
	invalid, err := h.raw(ctx, h.normal, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       "field-lab",
		"name":            "bad name!",
		"threshold_bytes": 64,
	})
	if err != nil {
		return err
	}
	invalidEnvelope, err := errorEnvelopeFrom(invalid)
	if err != nil {
		return err
	}
	if err := assertions.require(
		invalid.status == http.StatusBadRequest &&
			invalidEnvelope.Error.Code == string(domain.CodeInvalidInput),
		"validation error classified as %d/%q, want 400/invalid_input",
		invalid.status, invalidEnvelope.Error.Code,
	); err != nil {
		return err
	}

	stale, err := h.raw(ctx, h.normal, http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{"expected_revision": 1, "record_count": 1, "payload_bytes": 1})
	if err != nil {
		return err
	}
	staleEnvelope, err := errorEnvelopeFrom(stale)
	if err != nil {
		return err
	}
	if err := assertions.require(
		stale.status == http.StatusConflict &&
			staleEnvelope.Error.Code == string(domain.CodeStaleRevision),
		"stale revision classified as %d/%q, want 409/stale_revision",
		stale.status, staleEnvelope.Error.Code,
	); err != nil {
		return err
	}

	return nil
}

// prepareDeadlinePlan creates a stream with two sealed source segments and a
// started compaction plan, returning the stream and plan identifiers.
func prepareDeadlinePlan(
	ctx context.Context,
	h *deadlineHarness,
) (string, string, error) {
	planStream, raw, err := createDeadlineStream(ctx, h, h.normal, "plan-raced-commit", 20)
	if err != nil {
		return "", "", err
	}
	if raw.status != http.StatusCreated {
		return "", "", fmt.Errorf("plan stream create returned %d: %s", raw.status, raw.body)
	}
	revision := uint64(1)
	for _, payload := range []int64{20, 20, 20, 20} {
		appended, appendedRaw, appendErr := appendDeadlineBatch(
			ctx, h, h.normal, planStream.Stream.ID, revision, payload,
		)
		if appendErr != nil {
			return "", "", appendErr
		}
		if appendedRaw.status != http.StatusOK {
			return "", "", fmt.Errorf("plan setup append returned %d: %s",
				appendedRaw.status, appendedRaw.body)
		}
		revision = appended.Stream.Revision
	}
	segmentsRaw, err := h.raw(ctx, h.normal, http.MethodGet,
		"/v1/streams/"+planStream.Stream.ID+"/segments?limit=100", nil)
	if err != nil {
		return "", "", err
	}
	var listing segmentList
	if err := json.Unmarshal(segmentsRaw.body, &listing); err != nil {
		return "", "", fmt.Errorf("decode segments: %w", err)
	}
	if len(listing.Items) < 3 {
		return "", "", fmt.Errorf("plan setup produced %d segments, want >= 3", len(listing.Items))
	}
	sourceIDs := []string{listing.Items[0].ID, listing.Items[1].ID}

	var planResult application.CompactionPlanResult
	planRaw, err := h.raw(ctx, h.normal, http.MethodPost,
		"/v1/streams/"+planStream.Stream.ID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       sourceIDs,
			"expected_stream_revision": revision,
		})
	if err != nil {
		return "", "", err
	}
	if planRaw.status != http.StatusCreated {
		return "", "", fmt.Errorf("plan create returned %d: %s", planRaw.status, planRaw.body)
	}
	if err := json.Unmarshal(planRaw.body, &planResult); err != nil {
		return "", "", err
	}
	startRaw, err := h.raw(ctx, h.normal, http.MethodPost,
		"/v1/compaction-plans/"+planResult.Plan.ID+"/start", map[string]any{})
	if err != nil {
		return "", "", err
	}
	if startRaw.status != http.StatusOK {
		return "", "", fmt.Errorf("plan start returned %d: %s", startRaw.status, startRaw.body)
	}
	return planStream.Stream.ID, planResult.Plan.ID, nil
}
