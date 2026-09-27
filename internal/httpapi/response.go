package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func writeError(writer http.ResponseWriter, err error) {
	status, body := mapError(err)
	writeJSON(writer, status, errorEnvelope{Error: body})
}

func mapError(err error) (int, errorBody) {
	var domainErr *domain.Error
	if errors.As(err, &domainErr) {
		status := http.StatusInternalServerError
		switch domainErr.Code {
		case domain.CodeInvalidInput, domain.CodeInvalidRange, domain.CodeOverflow:
			status = http.StatusBadRequest
		case domain.CodeNotFound:
			status = http.StatusNotFound
		case domain.CodeConflict, domain.CodeStaleRevision, domain.CodeDuplicate,
			domain.CodeInvalidState:
			status = http.StatusConflict
		case domain.CodeLimitExceeded:
			status = http.StatusRequestEntityTooLarge
		case domain.CodeRequestCanceled:
			status = http.StatusRequestTimeout
		case domain.CodeRequestTimeout:
			status = http.StatusGatewayTimeout
		case domain.CodePersistenceFailure, domain.CodeInconsistentState:
			status = http.StatusInternalServerError
		}
		details := domainErr.Details
		if details == nil {
			details = map[string]any{}
		}
		return status, errorBody{
			Code:    string(domainErr.Code),
			Message: domainErr.Message,
			Details: details,
		}
	}
	if errors.Is(err, context.Canceled) {
		return http.StatusRequestTimeout, errorBody{
			Code:    "request_canceled",
			Message: "request context was canceled",
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, errorBody{
			Code:    "request_timeout",
			Message: "request deadline was exceeded",
		}
	}
	return http.StatusInternalServerError, errorBody{
		Code:    "internal_error",
		Message: "the request could not be completed",
	}
}
