package gsfiberv3

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
)

func TestReadinessHandlerReflectsManager(t *testing.T) {
	mgr := New(Config{})
	app := fiber.New()
	app.Get("/ready", ReadinessHandler(mgr))

	resp, err := app.Test(httptest.NewRequest("GET", "/ready", nil))
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("ready: got %d want 200", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

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
	mgr.Trigger()
	if err := mgr.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}
}
