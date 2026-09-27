package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

func testAPI(t *testing.T, timeout time.Duration) http.Handler {
	t.Helper()
	repo := storage.NewInMemoryRepository(t.TempDir(), nil)
	service, err := application.NewService(
		repo,
		&idgen.Counter{},
		clock.System{},
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return New(service, Options{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  timeout,
		Logger:          observability.Discard{},
	}).Handler()
}

func TestMapErrorContract(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"timeout domain", domain.WrapError(domain.CodeRequestTimeout, "x", context.DeadlineExceeded),
			http.StatusGatewayTimeout, "request_timeout"},
		{"canceled domain", domain.WrapError(domain.CodeRequestCanceled, "x", context.Canceled),
			http.StatusRequestTimeout, "request_canceled"},
		{"raw deadline", context.DeadlineExceeded,
			http.StatusGatewayTimeout, "request_timeout"},
		{"raw canceled", context.Canceled,
			http.StatusRequestTimeout, "request_canceled"},
		{"invalid input", domain.NewError(domain.CodeInvalidInput, "bad"),
			http.StatusBadRequest, "invalid_input"},
		{"stale revision", domain.NewError(domain.CodeStaleRevision, "stale"),
			http.StatusConflict, "stale_revision"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := mapError(tc.err)
			if status != tc.status || body.Code != tc.code {
				t.Fatalf("mapError(%v) = %d/%q, want %d/%q", tc.err, status, body.Code, tc.status, tc.code)
			}
		})
	}
}

func TestElapsedDeadlineReturnsGatewayTimeoutNot400(t *testing.T) {
	handler := testAPI(t, time.Second)
	past, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	request := httptest.NewRequestWithContext(past, http.MethodPost, "/v1/streams",
		jsonReader(map[string]any{"namespace": "ns", "name": "late", "threshold_bytes": 64}))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body %s", recorder.Code, recorder.Body.String())
	}
	envelope := decodeEnvelope(t, recorder.Body.Bytes())
	if envelope.Error.Code != string(domain.CodeRequestTimeout) {
		t.Fatalf("code = %q, want request_timeout", envelope.Error.Code)
	}
}

func TestCanceledContextKeepsCancellationSemantics(t *testing.T) {
	handler := testAPI(t, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/streams",
		jsonReader(map[string]any{"namespace": "ns", "name": "canceled", "threshold_bytes": 64}))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408; body %s", recorder.Code, recorder.Body.String())
	}
	envelope := decodeEnvelope(t, recorder.Body.Bytes())
	if envelope.Error.Code != string(domain.CodeRequestCanceled) {
		t.Fatalf("code = %q, want request_canceled", envelope.Error.Code)
	}
}

func TestNormalAndConflictClassificationsUnchanged(t *testing.T) {
	handler := testAPI(t, time.Second)

	// Validation error stays 400 invalid_input.
	request := httptest.NewRequest(http.MethodPost, "/v1/streams",
		jsonReader(map[string]any{"namespace": "ns", "name": "bad name!", "threshold_bytes": 64}))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, want 400", recorder.Code)
	}
	if decodeEnvelope(t, recorder.Body.Bytes()).Error.Code != string(domain.CodeInvalidInput) {
		t.Fatalf("invalid code mismatch: %s", recorder.Body.String())
	}

	// Normal create then stale append yields 409 stale_revision.
	createReq := httptest.NewRequest(http.MethodPost, "/v1/streams",
		jsonReader(map[string]any{"namespace": "ns", "name": "ok", "threshold_bytes": 1000}))
	createReq.Header.Set("Content-Type", "application/json")
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, createReq)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d: %s", created.Code, created.Body.String())
	}
	var createdBody application.CreateStreamResult
	if err := json.Unmarshal(created.Body.Bytes(), &createdBody); err != nil {
		t.Fatalf("decode create: %v", err)
	}

	staleReq := httptest.NewRequest(http.MethodPost,
		"/v1/streams/"+createdBody.Stream.ID+"/records",
		jsonReader(map[string]any{"expected_revision": 99, "record_count": 1, "payload_bytes": 1}))
	staleReq.Header.Set("Content-Type", "application/json")
	staleRec := httptest.NewRecorder()
	handler.ServeHTTP(staleRec, staleReq)
	if staleRec.Code != http.StatusConflict {
		t.Fatalf("stale status = %d, want 409", staleRec.Code)
	}
	if decodeEnvelope(t, staleRec.Body.Bytes()).Error.Code != string(domain.CodeStaleRevision) {
		t.Fatalf("stale code mismatch: %s", staleRec.Body.String())
	}
}
