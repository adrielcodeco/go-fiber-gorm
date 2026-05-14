// Package apmfiber is the Fiber v2 adapter for the apmcore Elastic APM
// instrumentation engine. See the sibling apmfiberv3 package for Fiber v3.
//
// The adapter exposes three things on top of apmcore:
//
//  1. Middleware — wraps apmfiber.Middleware from the upstream Elastic
//     module so every request becomes an APM transaction named
//     "<METHOD> <route-template>".
//  2. Labels — a typed-body helper that decodes known business
//     identifiers from the request body/headers and attaches them as
//     transaction labels (filterable in Kibana as `labels.<key>`).
//  3. CaptureError — captures an error against the current request's
//     transaction. Use it from handlers that map errors inline (without
//     bubbling to Fiber's ErrorHandler).
//
// Pitfall: always read the active transaction via `c.Context()`, NOT
// `c.UserContext()`. apmfiber stores it on the underlying
// *fasthttp.RequestCtx; UserContext is a separate context.Background
// unless explicitly set elsewhere.
package apmfiber

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	upstream "go.elastic.co/apm/module/apmfiber/v2"
	apm "go.elastic.co/apm/v2"

	"github.com/adrielcodeco/go-fiber-gorm/apmcore"
)

// Middleware returns the upstream apmfiber middleware. It must be the
// FIRST middleware on the Fiber app so subsequent middlewares can see
// the transaction on the request context.
func Middleware() fiber.Handler {
	return upstream.Middleware()
}

// LabelExtractor returns key/value pairs to attach to the transaction.
// Return empty values to skip; the labels middleware filters them out.
type LabelExtractor func(c *fiber.Ctx) map[string]string

// LabelsConfig configures the Labels middleware.
type LabelsConfig struct {
	// Headers maps an incoming HTTP header name to the label key it
	// should publish under. Example: {"X-Wallet-Id": "wallet_id"}.
	Headers map[string]string

	// BodyTarget is a pointer to a struct that the middleware will
	// json-unmarshal the request body into to extract typed fields. The
	// extractor is then called with that decoded value. Use a small
	// dedicated struct with only the fields you care about — this avoids
	// allocating the full request body twice.
	//
	// BodyTarget MUST be a function that returns a fresh target per
	// request, otherwise concurrent requests would race on the same
	// pointer.
	BodyTarget func() any

	// BodyExtractor is called with the decoded BodyTarget value. Return
	// the map of label key → value to publish.
	BodyExtractor func(decoded any) map[string]string

	// Extra runs after Headers and BodyExtractor and lets the caller
	// publish labels computed from anything else on the request.
	Extra LabelExtractor
}

// Labels returns a Fiber middleware that publishes transaction labels
// based on cfg. It must run AFTER Middleware() so the transaction is
// already on the context.
//
// Reads `c.Body()` which is safe — fasthttp keeps the body buffer
// around for subsequent BodyParser calls, so the handler still sees it.
func Labels(cfg LabelsConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		ctx := c.Context()
		tx := apm.TransactionFromContext(ctx)
		if tx != nil {
			for header, key := range cfg.Headers {
				if v := string(c.Request().Header.Peek(header)); v != "" {
					tx.Context.SetLabel(key, v)
				}
			}
			if cfg.BodyTarget != nil && cfg.BodyExtractor != nil {
				target := cfg.BodyTarget()
				if target != nil && len(c.Body()) > 0 {
					if err := json.Unmarshal(c.Body(), target); err == nil {
						apmcore.SetLabels(ctx, cfg.BodyExtractor(target))
					}
				}
			}
			if cfg.Extra != nil {
				apmcore.SetLabels(ctx, cfg.Extra(c))
			}
		}
		return c.Next()
	}
}

// SetLabel publishes a single label on the active transaction. Use it
// when an identifier only becomes available after the handler ran
// (e.g. a generated ID returned from a domain call).
func SetLabel(c *fiber.Ctx, key, value string) {
	apmcore.SetLabel(c.Context(), key, value)
}

// CaptureError records err on the active APM transaction. Call it from
// handlers that convert errors inline (return ctx.Status(...).JSON(...))
// so the error still appears in Kibana → APM → Errors.
//
// IMPORTANT: pair with-or-without the central Fiber ErrorHandler, never
// both for the same error path — capture-once or you'll see duplicate
// error docs in Kibana.
func CaptureError(c *fiber.Ctx, err error) {
	apmcore.CaptureError(c.Context(), err)
}
