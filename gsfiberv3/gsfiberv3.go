// Package gsfiberv3 is the Fiber v3 adapter for the gscore graceful-shutdown
// engine. See the sibling gsfiber package for Fiber v2.
package gsfiberv3

import (
	"context"

	"github.com/gofiber/fiber/v3"

	"github.com/adrielcodeco/go-fiber-gorm/gscore"
)

// Manager re-exports the core Manager.
type Manager = gscore.Manager

// Config re-exports the core Config.
type Config = gscore.Config

// Hook re-exports the core Hook.
type Hook = gscore.Hook

// Phase enum re-exports.
const (
	PhasePreStop   = gscore.PhasePreStop
	PhaseDrain     = gscore.PhaseDrain
	PhasePostDrain = gscore.PhasePostDrain
	PhaseDB        = gscore.PhaseDB
	PhasePostDB    = gscore.PhasePostDB
)

// New constructs a Manager with the given Config.
func New(cfg Config) *Manager { return gscore.New(cfg) }

type fiberShutdowner struct{ app *fiber.App }

func (s fiberShutdowner) ShutdownWithContext(ctx context.Context) error {
	return s.app.ShutdownWithContext(ctx)
}

// RegisterApp adds a Fiber v3 app to be drained during the drain phase.
func RegisterApp(m *Manager, app *fiber.App) {
	m.RegisterServer(fiberShutdowner{app: app})
}

// ReadinessHandler returns a Fiber v3 handler suitable for a Kubernetes
// readiness probe. It returns 200 while the Manager is ready and 503
// once shutdown has begun.
func ReadinessHandler(m *Manager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if m.IsReady() {
			return c.SendStatus(fiber.StatusOK)
		}
		return c.SendStatus(fiber.StatusServiceUnavailable)
	}
}
