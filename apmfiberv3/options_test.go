package apmfiberv3_test

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"
	apm "go.elastic.co/apm/v2"

	"github.com/adrielcodeco/go-fiber-gorm/apmfiberv3"
)

func TestOptionsAreApplied(t *testing.T) {
	customTracer, err := apm.NewTracer("test", "0.0.1")
	if err != nil {
		t.Fatalf("NewTracer: %v", err)
	}
	defer customTracer.Close()

	ignored := false
	ignorer := func(*fasthttp.RequestCtx) bool {
		ignored = true
		return true
	}

	app := fiber.New()
	app.Use(apmfiberv3.Middleware(
		apmfiberv3.WithTracer(customTracer),
		apmfiberv3.WithTracer(nil),                  // exercise the nil-guard branch
		apmfiberv3.WithRequestIgnorer(ignorer),
		apmfiberv3.WithRequestIgnorer(nil),          // exercise the nil-guard branch
		apmfiberv3.WithPanicPropagation(),
	))
	app.Get("/ok", func(c fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })

	resp, err := app.Test(httptest.NewRequest("GET", "/ok", nil))
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !ignored {
		t.Fatal("expected request ignorer to be invoked")
	}
}

func TestMiddlewareCapturesBubbledError(t *testing.T) {
	app := fiber.New()
	app.Use(apmfiberv3.Middleware())
	app.Get("/boom", func(fiber.Ctx) error { return errors.New("boom") })

	resp, err := app.Test(httptest.NewRequest("GET", "/boom", nil))
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	// Fiber's default ErrorHandler responds 500 for non-*fiber.Error errors.
	if resp.StatusCode == fiber.StatusOK {
		t.Fatalf("expected error status, got %d", resp.StatusCode)
	}
}

func TestMiddlewareRecoversPanic(t *testing.T) {
	app := fiber.New()
	app.Use(apmfiberv3.Middleware()) // panic propagation OFF by default
	app.Get("/panic", func(fiber.Ctx) error {
		panic("kaboom")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/panic", nil))
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500 after recover, got %d", resp.StatusCode)
	}
}

func TestMiddlewareLabelsExtraExtractor(t *testing.T) {
	app := fiber.New()
	app.Use(apmfiberv3.Middleware())
	app.Use(apmfiberv3.Labels(apmfiberv3.LabelsConfig{
		Extra: func(c fiber.Ctx) map[string]string {
			return map[string]string{"path": c.Path()}
		},
	}))
	app.Get("/x", func(c fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })

	if _, err := app.Test(httptest.NewRequest("GET", "/x", nil)); err != nil {
		t.Fatalf("Test: %v", err)
	}
}
