package apmcore

import (
	"context"

	apm "go.elastic.co/apm/v2"
)

// SetLabel attaches a business identifier to the APM transaction in ctx.
// In Kibana the value appears under `labels.<key>` and can be filtered on.
// No-op if ctx has no active transaction or value is empty.
//
// Useful for late-binding identifiers that only become available after the
// handler ran (e.g. a generated transaction ID returned from a domain call).
func SetLabel(ctx context.Context, key, value string) {
	if ctx == nil || key == "" || value == "" {
		return
	}
	tx := apm.TransactionFromContext(ctx)
	if tx == nil {
		return
	}
	tx.Context.SetLabel(key, value)
}

// SetLabels applies SetLabel for every non-empty entry in m. Convenience
// for middlewares that decode known identifiers from the request body and
// publish them in one pass.
func SetLabels(ctx context.Context, m map[string]string) {
	if len(m) == 0 {
		return
	}
	tx := apm.TransactionFromContext(ctx)
	if tx == nil {
		return
	}
	for k, v := range m {
		if k == "" || v == "" {
			continue
		}
		tx.Context.SetLabel(k, v)
	}
}

// CaptureError records err on the APM transaction in ctx and sends it to
// the agent. Safe to call with nil err (no-op) or with ctx that has no
// active transaction (drops the event).
//
// Use this from handlers that map errors inline (return ctx.Status(...).
// JSON(...)) so the error still appears in Kibana → APM → Errors with the
// request's trace.id attached.
func CaptureError(ctx context.Context, err error) {
	if err == nil || ctx == nil {
		return
	}
	if e := apm.CaptureError(ctx, err); e != nil {
		e.Send()
	}
}
