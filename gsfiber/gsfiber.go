// Package gsfiber is the Fiber v2 adapter for the gscore graceful-shutdown
// engine. See the sibling gsfiberv3 package for Fiber v3.
package gsfiber

import (
	"context"

	"github.com/gofiber/fiber/v2"

	"github.com/adrielcodeco/go-fiber-gorm/gscore"
)

// Manager re-exports the core Manager so callers do not need a second
// import for the common case.
type Manager = gscore.Manager

// Config re-exports the core Config.
type Config = gscore.Config

// Hook re-exports the core Hook.
type Hook = gscore.Hook

// Phase enum re-exports for ergonomic registration.
const (
	PhasePreStop   = gscore.PhasePreStop
	PhaseDrain     = gscore.PhaseDrain
	PhasePostDrain = gscore.PhasePostDrain
	PhaseDB        = gscore.PhaseDB
	PhasePostDB    = gscore.PhasePostDB
)

// New constructs a Manager with the given Config.
func New(cfg Config) *Manager { return gscore.New(cfg) }

// fiberShutdowner adapts *fiber.App to gscore.Shutdowner. Fiber v2's
// ShutdownWithContext already matches the signature, but we wrap it to
// keep the adapter package import-free of gscore-internal types at the
// call site.
type fiberShutdowner struct{ app *fiber.App }

func (s fiberShutdowner) ShutdownWithContext(ctx context.Context) error {
	return s.app.ShutdownWithContext(ctx)
}

// RegisterApp adds a Fiber v2 app to be drained during the drain phase.
func RegisterApp(m *Manager, app *fiber.App) {
	m.RegisterServer(fiberShutdowner{app: app})
}

// ReadinessHandler returns a Fiber v2 handler suitable for a Kubernetes
// readiness probe. It returns 200 while the Manager is ready and 503
// once shutdown has begun, so kube-proxy can remove the pod from
// service endpoints before in-flight requests are drained.
func ReadinessHandler(m *Manager) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if m.IsReady() {
			return c.SendStatus(fiber.StatusOK)
		}
		return c.SendStatus(fiber.StatusServiceUnavailable)
	}
}
