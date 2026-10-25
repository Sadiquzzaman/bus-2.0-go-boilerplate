package kafka

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sadiquzzaman/bus-2.0-go-boilerplate/platform/reqctx"
)

func TestNewEnvelopeTakesContext(t *testing.T) {
	ctx := reqctx.WithCorrelationID(context.Background(), "corr-1")
	ctx = reqctx.WithScope(ctx, reqctx.Scope{CompanyID: "c1", VerticalID: "v1"})
	ctx = reqctx.WithActor(ctx, reqctx.Actor{Type: reqctx.ActorUser, ID: "u1"})

	env, err := NewEnvelope(ctx, Event{Type: "seat.reserved", AggregateType: "seat", AggregateID: "s1", Payload: map[string]int{"n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if env.EventVersion != 1 || env.CompanyID != "c1" || env.VerticalID != "v1" ||
		env.CorrelationID != "corr-1" || env.Actor == nil || env.Actor.ID != "u1" || env.Key() != "s1" {
		t.Fatalf("unexpected envelope: %+v", env)
	}

	rec, err := BuildRecord(ctx, "booking", "booking.seat.v1", env)
	if err != nil {
		t.Fatal(err)
	}
	if string(rec.Key) != "s1" || Header(rec, HeaderEventType) != "seat.reserved" {
		t.Fatalf("unexpected record: key=%s headers=%v", rec.Key, rec.Headers)
	}
}

func TestEnvelopeContextRoundTrip(t *testing.T) {
	env := Envelope{CompanyID: "c1", TenantID: "t1", CorrelationID: "corr", Actor: &reqctx.Actor{Type: reqctx.ActorService, ID: "svc"}}
	ctx := env.Context(context.Background())
	s, ok := reqctx.ScopeFrom(ctx)
	if !ok || s.CompanyID != "c1" || s.TenantID != "t1" || reqctx.CorrelationID(ctx) != "corr" {
		t.Fatalf("scope/correlation not restored")
	}
}

func TestDecodeErrorIsPermanent(t *testing.T) {
	env := Envelope{EventType: "x.y", EventVersion: 1, Payload: []byte(`"not an object"`)}
	var v struct{ A int }
	if err := env.Decode(&v); !IsPermanent(err) {
		t.Fatalf("want permanent error, got %v", err)
	}
	if IsPermanent(errors.New("x")) {
		t.Fatal("plain error must not be permanent")
	}
}

func TestTopicNames(t *testing.T) {
	if got := Topic("booking", "seat", 1); got != "booking.seat.v1" {
		t.Fatal(got)
	}
	got := ConsumerTopics("g", []string{"a.b.v1"}, RetryPolicy{})
	want := []string{"a.b.v1", "a.b.v1.g.retry.1", "a.b.v1.g.retry.2", "a.b.v1.g.retry.3", "a.b.v1.g.retry.4", "a.b.v1.g.dlt"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v", got)
	}
	inPlace := ConsumerTopics("g", []string{"a.b.v1"}, RetryPolicy{Mode: RetryInPlace})
	if !slices.Equal(inPlace, []string{"a.b.v1", "a.b.v1.g.dlt"}) {
		t.Fatalf("got %v", inPlace)
	}
}

func TestRouterSkipsUnknownTypes(t *testing.T) {
	called := false
	h := NewRouter().Handle("a.created", func(context.Context, *Message) error { called = true; return nil }).Handler()
	if err := h(context.Background(), &Message{Envelope: Envelope{EventType: "b.created"}, Record: &kgo.Record{}}); err != nil || called {
		t.Fatal("unknown type should be skipped")
	}
	if err := h(context.Background(), &Message{Envelope: Envelope{EventType: "a.created"}}); err != nil || !called {
		t.Fatal("known type should be handled")
	}
}
