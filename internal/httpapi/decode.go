package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func decodeJSON(
	writer http.ResponseWriter,
	request *http.Request,
	maximumBytes int64,
	target any,
) error {
	contentType := request.Header.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return domain.NewError(
			domain.CodeInvalidInput,
			"Content-Type must be application/json",
		)
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maximumBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return decodeError(err)
	}
	if err := ensureNoExtraJSON(decoder); err != nil {
		return err
	}
	return nil
}

func decodeError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return domain.FromContextError(err)
	}
	var maximumErr *http.MaxBytesError
	if errors.As(err, &maximumErr) {
		return domain.NewError(domain.CodeLimitExceeded, "request body exceeds the configured limit").
			WithDetail("maximum_bytes", maximumErr.Limit)
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return domain.NewError(domain.CodeInvalidInput, "request body is not valid JSON").
			WithDetail("offset", syntaxErr.Offset)
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return domain.NewError(domain.CodeInvalidInput, "request field has the wrong JSON type").
			WithDetail("field", typeErr.Field).
			WithDetail("offset", typeErr.Offset)
	}
	if strings.Contains(err.Error(), "unknown field") {
		return domain.NewError(domain.CodeInvalidInput, err.Error())
	}
	return domain.NewError(domain.CodeInvalidInput, "request body could not be decoded")
}

func ensureNoExtraJSON(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return domain.NewError(domain.CodeInvalidInput, "request body contains trailing JSON content")
	}
	return domain.NewError(
		domain.CodeInvalidInput,
		fmt.Sprintf("request body could not be decoded: %v", err),
	)
}
