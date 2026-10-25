package httpserver

import (
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

// Response shapes (identical in every service):
//
//	single:  {"data": {...}}
//	list:    {"data": [...], "meta": {"page": 1, "limit": 20, "total": 134}}
//	error:   {"error": {"code": "...", "message": "...", "details": ..., "request_id": "..."}}

type envelope struct {
	Data any       `json:"data"`
	Meta *PageMeta `json:"meta,omitempty"`
}

type errorBody struct {
	Error errorPayload `json:"error"`
}

type errorPayload struct {
	Code      apperror.Code `json:"code"`
	Message   string        `json:"message"`
	Details   any           `json:"details,omitempty"`
	RequestID string        `json:"request_id,omitempty"`
}

// OK writes 200 {"data": v}.
func OK(c *gin.Context, v any) { c.JSON(http.StatusOK, envelope{Data: v}) }

// Created writes 201 {"data": v}.
func Created(c *gin.Context, v any) { c.JSON(http.StatusCreated, envelope{Data: v}) }

// Accepted writes 202 {"data": v}.
func Accepted(c *gin.Context, v any) { c.JSON(http.StatusAccepted, envelope{Data: v}) }

// NoContent writes 204.
func NoContent(c *gin.Context) { c.Status(http.StatusNoContent) }

// List writes 200 {"data": items, "meta": page}.
func List(c *gin.Context, items any, meta PageMeta) {
	c.JSON(http.StatusOK, envelope{Data: items, Meta: &meta})
}

// Fail renders err as the standard error body and aborts the chain.
// 5xx errors are logged with their cause; the cause is never sent to clients.
func Fail(c *gin.Context, err error) {
	ae := apperror.From(err)
	if ae.Status >= 500 {
		slog.ErrorContext(c.Request.Context(), "request failed", slog.String("code", string(ae.Code)), slog.String("error", ae.Error()))
	}
	_ = c.Error(err)
	c.AbortWithStatusJSON(ae.Status, errorBody{Error: errorPayload{
		Code:      ae.Code,
		Message:   ae.Message,
		Details:   ae.Details,
		RequestID: reqctx.RequestID(c.Request.Context()),
	}})
}

// BindJSON decodes and validates the body (gin `binding:"..."` tags).
// On failure it writes a 400/422 and returns false.
func BindJSON(c *gin.Context, dst any) bool {
	if err := c.ShouldBindJSON(dst); err != nil {
		Fail(c, bindError(err))
		return false
	}
	return true
}

// BindQuery decodes and validates query parameters (`form:"..."` tags).
func BindQuery(c *gin.Context, dst any) bool {
	if err := c.ShouldBindQuery(dst); err != nil {
		Fail(c, bindError(err))
		return false
	}
	return true
}

func bindError(err error) error {
	var ve validator.ValidationErrors
	if errors.As(err, &ve) {
		fields := make([]apperror.FieldError, 0, len(ve))
		for _, fe := range ve {
			fields = append(fields, apperror.FieldError{
				Field:   jsonFieldName(fe.Namespace()),
				Message: validationMessage(fe),
			})
		}
		return apperror.Validation(fields...)
	}
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		return apperror.New("PAYLOAD_TOO_LARGE", http.StatusRequestEntityTooLarge, "request body too large")
	}
	return apperror.BadRequest("malformed request body").Wrap(err)
}

// jsonFieldName turns "CreateItemRequest.name" into "name". Field names are
// the json tags because New() registers a tag-name function on the validator.
func jsonFieldName(ns string) string {
	if i := strings.Index(ns, "."); i >= 0 {
		return ns[i+1:]
	}
	return ns
}

func init() {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterTagNameFunc(func(f reflect.StructField) string {
			for _, tag := range []string{"json", "form", "uri"} {
				name := strings.SplitN(f.Tag.Get(tag), ",", 2)[0]
				if name == "-" {
					return ""
				}
				if name != "" {
					return name
				}
			}
			return f.Name
		})
	}
}

func validationMessage(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "min":
		return "must be at least " + fe.Param()
	case "max":
		return "must be at most " + fe.Param()
	case "oneof":
		return "must be one of: " + fe.Param()
	case "uuid", "uuid4", "uuid7":
		return "must be a valid UUID"
	case "email":
		return "must be a valid email"
	default:
		return "failed '" + fe.Tag() + "' validation"
	}
}

// Page is parsed offset pagination.
type Page struct {
	Page  int
	Limit int
}

// Offset returns the SQL/Mongo skip value.
func (p Page) Offset() int { return (p.Page - 1) * p.Limit }

// PageMeta is returned in list responses.
type PageMeta struct {
	Page  int `json:"page"`
	Limit int `json:"limit"`
	Total int `json:"total"`
}

// Meta builds PageMeta for a total count.
func (p Page) Meta(total int) PageMeta { return PageMeta{Page: p.Page, Limit: p.Limit, Total: total} }

// ParsePage reads ?page=&limit= with defaults 1/20 and a maximum limit of 100.
func ParsePage(c *gin.Context) Page {
	page, _ := strconv.Atoi(c.Query("page"))
	limit, _ := strconv.Atoi(c.Query("limit"))
	if page < 1 {
		page = 1
	}
	switch {
	case limit < 1:
		limit = 20
	case limit > 100:
		limit = 100
	}
	return Page{Page: page, Limit: limit}
}
