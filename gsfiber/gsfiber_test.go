package gsfiber

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestReadinessHandlerReflectsManager(t *testing.T) {
	mgr := New(Config{})
	app := fiber.New()
	app.Get("/ready", ReadinessHandler(mgr))

	// Ready → 200.
	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("ready: got %d want 200", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	// Trigger → 503.
	mgr.Trigger()
	_ = mgr.Wait()

	resp, err = app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("Test post-trigger: %v", err)
	}
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("after trigger: got %d want 503", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func TestRegisterAppDrainsViaManager(t *testing.T) {
	app := fiber.New()
	mgr := New(Config{})
	RegisterApp(mgr, app)

	// Sanity: Trigger completes without error even though app was never
	// Listen()ed. Fiber's ShutdownWithContext is a no-op in that case.
	mgr.Trigger()
	if err := mgr.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}
}
