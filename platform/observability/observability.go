// Package observability wires OpenTelemetry tracing and Prometheus metrics.
//
// Both are in the kit from day one but tracing export is OFF by default.
// Trace context (W3C traceparent) is always propagated over HTTP and Kafka
// headers, so turning on export later needs only config:
//
//	OTEL_TRACING_ENABLED=true
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
//
// Metrics are exposed on the admin server at /metrics.
package observability

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Config for tracing. The OTLP endpoint itself is read by the exporter from
// the standard OTEL_EXPORTER_OTLP_ENDPOINT variable.
type Config struct {
	TracingEnabled bool    `env:"TRACING_ENABLED" envDefault:"false"`
	SampleRatio    float64 `env:"TRACING_SAMPLE_RATIO" envDefault:"0.1"`
}

// Setup installs the global propagator and (optionally) a tracer provider.
// The returned function flushes spans on shutdown.
func Setup(ctx context.Context, cfg Config, service, version, env string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	if !cfg.TracingEnabled {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("observability: otlp exporter: %w", err)
	}
	// Plain attribute keys (OTel semantic conventions) keep us independent of
	// the semconv package version.
	res := resource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("service.version", version),
		attribute.String("deployment.environment.name", env),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// MetricsHandler serves Prometheus metrics from the default registry.
func MetricsHandler() http.Handler { return promhttp.Handler() }
