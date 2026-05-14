// Package apmcore is the framework-agnostic Elastic APM instrumentation
// engine behind the apmfiber (Fiber v2) and apmfiberv3 (Fiber v3) adapters.
//
// It exposes:
//
//   - SetupOTelSDK: bootstrap the Elastic APM Go agent together with an
//     OpenTelemetry TracerProvider/MeterProvider bridge so OTel-aware
//     libraries (e.g. redisotel) export spans/metrics through the APM
//     agent's transport.
//   - WrapHTTPTransport: wrap an http.RoundTripper so outgoing requests
//     produce APM spans and inject W3C traceparent headers.
//   - NewGormPlugin / RegisterDriver: GORM callback plugin + database/sql
//     driver wrapper that emit foldable spans (logical gorm op → underlying
//     prepare/exec/query/close roundtrips) for any database/sql driver.
//   - DBPoolMetrics: an apm.MetricsGatherer that publishes *sql.DB pool
//     stats on the agent's metrics tick.
//   - WrapZapCore / LogCtxFields: helpers to correlate zap logs with the
//     current APM trace via trace.id / transaction.id / span.id fields.
//
// The package is intentionally Fiber-agnostic. The HTTP middleware lives in
// the apmfiber/apmfiberv3 adapter packages.
package apmcore

import (
	"context"
	"errors"
	"net/http"

	apmhttp "go.elastic.co/apm/module/apmhttp/v2"
	apmotel "go.elastic.co/apm/module/apmotel/v2"
	apm "go.elastic.co/apm/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
)

// ShutdownFunc flushes and closes APM/OTel pipelines. It is safe to call
// multiple times; subsequent calls are no-ops.
type ShutdownFunc func(ctx context.Context) error

// SetupOTelSDK wires the Elastic APM Go agent into the OpenTelemetry global
// providers and registers the apmotel metrics gatherer with the APM agent.
//
// After this returns:
//   - otel.GetTracerProvider() routes spans through the APM agent.
//   - otel.GetMeterProvider() routes metrics through the APM agent.
//   - otel.GetTextMapPropagator() propagates W3C TraceContext + Baggage so
//     traces stitch across services.
//
// The returned ShutdownFunc closes the APM tracer (flushing buffered
// events) and shuts down the OTel MeterProvider. Call it from main after
// the server has finished draining.
func SetupOTelSDK(ctx context.Context) (ShutdownFunc, error) {
	var shutdownFns []ShutdownFunc
	shutdown := func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFns {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFns = nil
		return err
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracerProvider, err := apmotel.NewTracerProvider()
	if err != nil {
		return nil, err
	}
	otel.SetTracerProvider(tracerProvider)
	// apmotel.NewTracerProvider returns the bare TracerProvider interface
	// with no Shutdown — flush via the underlying APM tracer instead.
	shutdownFns = append(shutdownFns, func(context.Context) error {
		apm.DefaultTracer().Close()
		return nil
	})

	gatherer, err := apmotel.NewGatherer()
	if err != nil {
		_ = shutdown(ctx)
		return nil, err
	}
	meterProvider := metric.NewMeterProvider(metric.WithReader(gatherer))
	otel.SetMeterProvider(meterProvider)
	shutdownFns = append(shutdownFns, meterProvider.Shutdown)
	apm.DefaultTracer().RegisterMetricsGatherer(gatherer)

	return shutdown, nil
}

// WrapHTTPTransport wraps base with apmhttp.WrapRoundTripper so every
// outgoing request built with http.NewRequestWithContext gets its own
// APM span and the W3C traceparent header injected.
//
// If base is nil, http.DefaultTransport is wrapped.
func WrapHTTPTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return apmhttp.WrapRoundTripper(base)
}
