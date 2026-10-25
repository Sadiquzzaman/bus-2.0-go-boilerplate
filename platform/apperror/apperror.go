// Package apperror defines the single error type every service returns.
// The HTTP layer renders it as:
//
//	{"error": {"code": "NOT_FOUND", "message": "...", "details": ..., "request_id": "..."}}
package apperror

import (
	"errors"
	"fmt"
	"net/http"
)

// Code is a stable, machine-readable error code. Clients switch on it.
type Code string

const (
	CodeBadRequest   Code = "BAD_REQUEST"
	CodeValidation   Code = "VALIDATION_FAILED"
	CodeUnauthorized Code = "UNAUTHORIZED"
	CodeForbidden    Code = "FORBIDDEN"
	CodeNotFound     Code = "NOT_FOUND"
	CodeConflict     Code = "CONFLICT"
	CodeRateLimited  Code = "RATE_LIMITED"
	CodeUnavailable  Code = "SERVICE_UNAVAILABLE"
	CodeTimeout      Code = "TIMEOUT"
	CodeInternal     Code = "INTERNAL"
)

// Error is the application error.
type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
	Status  int    `json:"-"`
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

// Wrap attaches an underlying cause (logged, never sent to clients).
func (e *Error) Wrap(cause error) *Error { c := *e; c.cause = cause; return &c }

// WithDetails attaches client-visible details.
func (e *Error) WithDetails(d any) *Error { c := *e; c.Details = d; return &c }

func New(code Code, status int, msg string) *Error {
	return &Error{Code: code, Status: status, Message: msg}
}

func BadRequest(msg string) *Error { return New(CodeBadRequest, http.StatusBadRequest, msg) }
func Unauthorized(msg string) *Error {
	return New(CodeUnauthorized, http.StatusUnauthorized, msg)
}
func Forbidden(msg string) *Error   { return New(CodeForbidden, http.StatusForbidden, msg) }
func NotFound(msg string) *Error    { return New(CodeNotFound, http.StatusNotFound, msg) }
func Conflict(msg string) *Error    { return New(CodeConflict, http.StatusConflict, msg) }
func RateLimited(msg string) *Error { return New(CodeRateLimited, http.StatusTooManyRequests, msg) }
func Unavailable(msg string) *Error {
	return New(CodeUnavailable, http.StatusServiceUnavailable, msg)
}
func Timeout(msg string) *Error  { return New(CodeTimeout, http.StatusGatewayTimeout, msg) }
func Internal(msg string) *Error { return New(CodeInternal, http.StatusInternalServerError, msg) }

// FieldError describes one invalid input field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// Validation builds a 422 with per-field details.
func Validation(fields ...FieldError) *Error {
	return New(CodeValidation, http.StatusUnprocessableEntity, "request validation failed").WithDetails(fields)
}

// From converts any error into *Error. Unknown errors become INTERNAL with
// the original kept as cause (so it is logged but not leaked).
func From(err error) *Error {
	if err == nil {
		return nil
	}
	var ae *Error
	if errors.As(err, &ae) {
		return ae
	}
	return Internal("internal error").Wrap(err)
}

// Is reports whether err is an *Error with the given code.
func Is(err error, code Code) bool {
	var ae *Error
	return errors.As(err, &ae) && ae.Code == code
}
