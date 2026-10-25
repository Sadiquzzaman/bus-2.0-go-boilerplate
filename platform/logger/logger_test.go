package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

func TestRedactsSensitiveKeysAndAddsContext(t *testing.T) {
	var buf bytes.Buffer
	l := NewWithWriter(&buf, Config{Level: "debug", RedactKeys: []string{"nid"}}, "svc", "v1", "test")

	ctx := reqctx.WithRequestID(context.Background(), "req-1")
	ctx = reqctx.WithScope(ctx, reqctx.Scope{CompanyID: "c1", TenantID: "t1"})
	l.InfoContext(ctx, "hello", slog.String("password", "p@ss"), slog.String("OTP", "123456"),
		slog.String("nid", "1990"), slog.String("name", "rahim"))

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"password", "OTP", "nid"} {
		if got[k] != Redacted {
			t.Errorf("%s = %v, want redacted", k, got[k])
		}
	}
	want := map[string]string{
		"name": "rahim", "message": "hello", "service": "svc", "request_id": "req-1",
		"correlation_id": "req-1", "company_id": "c1", "tenant_id": "t1",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
}
