// Package apmfiberv3 is the Fiber v3 adapter for the apmcore Elastic APM
// instrumentation engine.
//
// Fiber v3 has no upstream apmfiber module (only v2 is published by
// Elastic), so this package implements a minimal middleware that mirrors
// the v2 one: it starts an APM transaction on the underlying
// *fasthttp.RequestCtx, names it "<METHOD> <route-template>" after the
// handler chain runs, recovers panics, and captures bubbled errors.
//
// All trace plumbing happens via c.RequestCtx(); APM stores the
// transaction there (matching the v2 behavior). Code that reads the
// transaction inside handlers/middlewares should use c.RequestCtx() too.
package apmfiberv3

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"
	apmfasthttp "go.elastic.co/apm/module/apmfasthttp/v2"
	apmhttp "go.elastic.co/apm/module/apmhttp/v2"
	apm "go.elastic.co/apm/v2"

	"github.com/adrielcodeco/go-fiber-gorm/apmcore"
)

// Option mutates middleware configuration. Mirrors the apmfiber v2 API.
type Option func(*config)

type config struct {
	tracer         *apm.Tracer
	requestIgnorer apmfasthttp.RequestIgnorerFunc
	panicProp      bool
}

// WithTracer overrides the tracer (default apm.DefaultTracer()).
func WithTracer(t *apm.Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithPanicPropagation makes the middleware re-panic after capturing the
// panic as an APM error (default is to swallow and respond 500).
func WithPanicPropagation() Option {
	return func(c *config) { c.panicProp = true }
}

// WithRequestIgnorer overrides the request-ignorer function used to skip
// tracing for matching requests (e.g. health checks).
func WithRequestIgnorer(fn apmfasthttp.RequestIgnorerFunc) Option {
	return func(c *config) {
		if fn != nil {
			c.requestIgnorer = fn
		}
	}
}

// Middleware returns a Fiber v3 handler that wraps each request in an
// APM transaction. Register it FIRST so subsequent middlewares observe
// the transaction on c.RequestCtx().
func Middleware(opts ...Option) fiber.Handler {
	cfg := &config{
		tracer:         apm.DefaultTracer(),
		requestIgnorer: apmfasthttp.NewDynamicServerRequestIgnorer(apm.DefaultTracer()),
	}
	for _, o := range opts {
		o(cfg)
	}

	return func(c fiber.Ctx) (result error) {
		reqCtx := c.RequestCtx()
		if !cfg.tracer.Recording() || cfg.requestIgnorer(reqCtx) {
			return c.Next()
		}

		name := string(reqCtx.Method()) + " " + c.Path()
		tx, body, err := apmfasthttp.StartTransactionWithBody(reqCtx, cfg.tracer, name)
		if err != nil {
			reqCtx.Error(err.Error(), http.StatusInternalServerError)
			return err
		}

		defer func() {
			resp := &reqCtx.Response
			route := c.Route()

			var fiberErr *fiber.Error
			if route != nil && route.Path == "/" && errors.As(result, &fiberErr) && fiberErr.Code == http.StatusNotFound {
				tx.Name = string(reqCtx.Method()) + " unknown route"
			} else if route != nil {
				tx.Name = string(reqCtx.Method()) + " " + route.Path
			}

			if v := recover(); v != nil {
				if cfg.panicProp {
					defer panic(v)
				}
				e := cfg.tracer.Recovered(v)
				e.SetTransaction(tx)
				setRespContext(&e.Context, resp)
				e.Send()
				c.Status(http.StatusInternalServerError)
			}

			statusCode := resp.StatusCode()
			tx.Result = apmhttp.StatusCodeResult(statusCode)
			if tx.Sampled() {
				setRespContext(&tx.Context, resp)
			}
			body.Discard()
		}()

		result = c.Next()
		if result != nil {
			resp := &reqCtx.Response
			e := cfg.tracer.NewError(result)
			e.Handled = true
			e.SetTransaction(tx)
			setRespContext(&e.Context, resp)
			e.Send()
		}
		return result
	}
}

func setRespContext(ctx *apm.Context, resp *fasthttp.Response) {
	ctx.SetFramework("fiber", fiber.Version)
	ctx.SetHTTPStatusCode(resp.StatusCode())

	headers := make(http.Header)
	resp.Header.VisitAll(func(k, v []byte) {
		headers.Set(string(k), string(v))
	})
	ctx.SetHTTPResponseHeaders(headers)
}

// LabelExtractor — see the apmfiber package for semantics. Returns
// label key/value pairs to attach to the transaction.
type LabelExtractor func(c fiber.Ctx) map[string]string

// LabelsConfig mirrors apmfiber.LabelsConfig for Fiber v3.
type LabelsConfig struct {
	Headers       map[string]string
	BodyTarget    func() any
	BodyExtractor func(decoded any) map[string]string
	Extra         LabelExtractor
}

// Labels returns a Fiber v3 middleware that publishes transaction labels
// based on cfg. Must run AFTER Middleware().
func Labels(cfg LabelsConfig) fiber.Handler {
	return func(c fiber.Ctx) error {
		reqCtx := c.RequestCtx()
		tx := apm.TransactionFromContext(reqCtx)
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
						apmcore.SetLabels(reqCtx, cfg.BodyExtractor(target))
					}
				}
			}
			if cfg.Extra != nil {
				apmcore.SetLabels(reqCtx, cfg.Extra(c))
			}
		}
		return c.Next()
	}
}

// SetLabel publishes a single label on the active transaction. Useful
// for identifiers that only materialize after the handler ran.
func SetLabel(c fiber.Ctx, key, value string) {
	apmcore.SetLabel(c.RequestCtx(), key, value)
}

// CaptureError records err on the active APM transaction. Use it from
// handlers that map errors inline so the error still appears in Kibana.
func CaptureError(c fiber.Ctx, err error) {
	apmcore.CaptureError(c.RequestCtx(), err)
}
