package httpserver

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/apperror"
	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	s, err := New(Config{RequestTimeout: 0}, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func do(s *Server, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.Engine.ServeHTTP(w, req)
	return w
}

func TestErrorShapeAndRequestID(t *testing.T) {
	s := newTestServer(t)
	s.Engine.GET("/x", func(c *gin.Context) { Fail(c, apperror.NotFound("nope")) })

	w := do(s, http.MethodGet, "/x", "", map[string]string{reqctx.HeaderRequestID: "rid-1"})
	if w.Code != http.StatusNotFound || w.Header().Get(reqctx.HeaderRequestID) != "rid-1" {
		t.Fatalf("code=%d headers=%v", w.Code, w.Header())
	}
	var body struct {
		Error struct{ Code, Message, RequestID string } `json:"error"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body.Error.Code != "NOT_FOUND" || body.Error.Message != "nope" {
		t.Fatalf("body=%s", w.Body.String())
	}
}

func TestValidationUsesJSONFieldNames(t *testing.T) {
	s := newTestServer(t)
	type req struct {
		BusName string `json:"bus_name" binding:"required"`
	}
	s.Engine.POST("/x", func(c *gin.Context) {
		var r req
		if BindJSON(c, &r) {
			OK(c, r)
		}
	})
	w := do(s, http.MethodPost, "/x", `{}`, nil)
	if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), `"field":"bus_name"`) {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestScopeHeaders(t *testing.T) {
	s := newTestServer(t)
	var got reqctx.Scope
	s.Engine.GET("/x", Scope(), RequireCompany(), func(c *gin.Context) {
		got, _ = reqctx.ScopeFrom(c.Request.Context())
		NoContent(c)
	})

	const cid = "0190a8f0-0000-7000-8000-000000000001"
	if w := do(s, http.MethodGet, "/x", "", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("missing company: code=%d", w.Code)
	}
	if w := do(s, http.MethodGet, "/x", "", map[string]string{reqctx.HeaderCompanyID: "nope"}); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad uuid: code=%d", w.Code)
	}
	if w := do(s, http.MethodGet, "/x", "", map[string]string{reqctx.HeaderCompanyID: cid}); w.Code != http.StatusNoContent || got.CompanyID != cid {
		t.Fatalf("valid: code=%d scope=%+v", w.Code, got)
	}
}

func TestParsePage(t *testing.T) {
	s := newTestServer(t)
	var p Page
	s.Engine.GET("/x", func(c *gin.Context) { p = ParsePage(c); NoContent(c) })
	do(s, http.MethodGet, "/x?page=3&limit=500", "", nil)
	if p.Page != 3 || p.Limit != 100 || p.Offset() != 200 {
		t.Fatalf("%+v", p)
	}
}
