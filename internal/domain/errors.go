package domain

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	CodeInvalidInput       ErrorCode = "invalid_input"
	CodeInvalidRange       ErrorCode = "invalid_range"
	CodeNotFound           ErrorCode = "not_found"
	CodeConflict           ErrorCode = "conflict"
	CodeStaleRevision      ErrorCode = "stale_revision"
	CodeInvalidState       ErrorCode = "invalid_state"
	CodeDuplicate          ErrorCode = "duplicate"
	CodeOverflow           ErrorCode = "overflow"
	CodeLimitExceeded      ErrorCode = "limit_exceeded"
	CodePersistenceFailure ErrorCode = "persistence_failure"
	CodeInconsistentState  ErrorCode = "inconsistent_state"
	CodeRequestTimeout     ErrorCode = "request_timeout"
	CodeRequestCanceled    ErrorCode = "request_canceled"
)

// Error is a stable domain error that can cross application and transport
// boundaries without exposing storage implementation details.
type Error struct {
	Code    ErrorCode
	Message string
	Details map[string]any
	Cause   error
}

func NewError(code ErrorCode, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Details: map[string]any{},
	}
}

func NewErrorf(code ErrorCode, format string, args ...any) *Error {
	return NewError(code, fmt.Sprintf(format, args...))
}

func WrapError(code ErrorCode, message string, cause error) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Details: map[string]any{},
		Cause:   cause,
	}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause == nil {
		return e.Message
	}
	return e.Message + ": " + e.Cause.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *Error) WithDetail(key string, value any) *Error {
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	e.Details[key] = value
	return e
}

func IsCode(err error, code ErrorCode) bool {
	var domainErr *Error
	return errors.As(err, &domainErr) && domainErr.Code == code
}
