package apmcore

import (
	"context"

	apmzap "go.elastic.co/apm/module/apmzap/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// WrapZapCore decorates a zapcore.Core with apmzap.Core so that:
//   - Any Error/Fatal log line is auto-emitted as an APM error event,
//     visible in Kibana → APM → Errors.
//   - When used together with LogCtxFields, every log line carries the
//     current request's trace.id / transaction.id / span.id.
//
// Wire it into your zap logger with zap.WrapCore:
//
//	logger := zap.NewProductionConfig().Build(
//	    zap.WrapCore(func(c zapcore.Core) zapcore.Core { return apmcore.WrapZapCore(c) }),
//	)
//
// Note: at the central error-handler site, log at Warn level (not Error)
// to avoid double-reporting: apm.CaptureError already records the
// exception; an Error log here would create a second "log" error doc in
// Kibana with empty error.exception.type.
func WrapZapCore(c zapcore.Core) zapcore.Core {
	return (&apmzap.Core{}).WrapCore(c)
}

// LogCtxFields returns the trace.id / transaction.id / span.id zap fields
// for the active APM transaction (or nil if ctx has none).
//
// Usage in request handlers:
//
//	logger.With(apmcore.LogCtxFields(ctx)...).Info("processed request")
//
// Returns an empty slice when ctx is nil or has no active transaction.
func LogCtxFields(ctx context.Context) []zap.Field {
	if ctx == nil {
		return nil
	}
	return apmzap.TraceContext(ctx)
}
