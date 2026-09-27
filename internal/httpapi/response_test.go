package httpapi

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func TestMapErrorDeadlineSemantics(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{
			name:       "deadline exceeded domain error",
			err:        domain.WrapError(domain.CodeRequestTimeout, "request deadline was exceeded", context.DeadlineExceeded),
			wantStatus: http.StatusGatewayTimeout,
			wantCode:   string(domain.CodeRequestTimeout),
		},
		{
			name:       "canceled domain error",
			err:        domain.WrapError(domain.CodeRequestCanceled, "request context was canceled", context.Canceled),
			wantStatus: http.StatusRequestTimeout,
			wantCode:   string(domain.CodeRequestCanceled),
		},
		{
			name:       "raw deadline exceeded",
			err:        context.DeadlineExceeded,
			wantStatus: http.StatusGatewayTimeout,
			wantCode:   "request_timeout",
		},
		{
			name:       "raw canceled",
			err:        context.Canceled,
			wantStatus: http.StatusRequestTimeout,
			wantCode:   "request_canceled",
		},
		{
			name:       "wrapped deadline remains detectable",
			err:        domain.FromContextError(context.DeadlineExceeded),
			wantStatus: http.StatusGatewayTimeout,
			wantCode:   string(domain.CodeRequestTimeout),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := mapError(tc.err)
			if status != tc.wantStatus {
				t.Fatalf("status = %d, want %d", status, tc.wantStatus)
			}
			if body.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", body.Code, tc.wantCode)
			}
		})
	}
}

func TestMapErrorPreservesExistingClassification(t *testing.T) {
	cases := []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{domain.NewError(domain.CodeInvalidInput, "bad"), http.StatusBadRequest, "invalid_input"},
		{domain.NewError(domain.CodeInvalidRange, "bad range"), http.StatusBadRequest, "invalid_range"},
		{domain.NewError(domain.CodeNotFound, "missing"), http.StatusNotFound, "not_found"},
		{domain.NewError(domain.CodeStaleRevision, "stale"), http.StatusConflict, "stale_revision"},
		{domain.NewError(domain.CodeConflict, "conflict"), http.StatusConflict, "conflict"},
		{domain.NewError(domain.CodeDuplicate, "dup"), http.StatusConflict, "duplicate"},
	}
	for _, tc := range cases {
		status, body := mapError(tc.err)
		if status != tc.wantStatus || body.Code != tc.wantCode {
			t.Fatalf("mapError(%v) = (%d, %q), want (%d, %q)",
				tc.err, status, body.Code, tc.wantStatus, tc.wantCode)
		}
	}
}

func TestFromContextErrorKeepsCause(t *testing.T) {
	if err := domain.FromContextError(nil); err != nil {
		t.Fatalf("nil context error mapped to %v", err)
	}
	if !errors.Is(domain.FromContextError(context.DeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("deadline error lost its cause")
	}
	if !errors.Is(domain.FromContextError(context.Canceled), context.Canceled) {
		t.Fatal("canceled error lost its cause")
	}
	other := errors.New("other")
	if domain.FromContextError(other) != other {
		t.Fatal("non-context error was rewritten")
	}
}
