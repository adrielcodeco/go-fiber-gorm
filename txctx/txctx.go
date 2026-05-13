// Package txctx is a Fiber v2 + GORM middleware that manages a request-scoped
// database transaction. See txctxv3 for the Fiber v3 variant.
package txctx

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"

	"github.com/adrielcodeco/go-fiber-gorm/txcore"
)

// Config re-exports the shared core configuration.
type Config = txcore.Config

// BoolPtr is a helper for setting Config.LazyTx inline.
func BoolPtr(v bool) *bool { return txcore.BoolPtr(v) }

// Middleware returns a Fiber v2 middleware that manages a request-scoped GORM
// transaction with timeout-triggered rollback and commit/rollback callbacks.
func Middleware(db *gorm.DB, cfg Config) fiber.Handler {
	cfg = cfg.WithDefaults()
	lazy := *cfg.LazyTx

	return func(c *fiber.Ctx) error {
		reqCtx, cancel := context.WithTimeout(c.UserContext(), cfg.Timeout)
		defer cancel()

		holder := txcore.NewHolder(db, cfg.CompensationCtx, lazy, cfg.OnCallbackError)
		ctx := txcore.Inject(reqCtx, holder)
		c.SetUserContext(ctx)

		if !lazy {
			holder.Begin(ctx)
		}

		defer func() {
			if r := recover(); r != nil {
				holder.Rollback()
				panic(r)
			}
		}()

		handlerErr := c.Next()

		if handlerErr != nil || ctx.Err() != nil {
			holder.Rollback()
			return handlerErr
		}

		commitErr, postErr := holder.Commit()
		if commitErr != nil {
			holder.Rollback()
			return commitErr
		}
		return postErr
	}
}

// DB returns the request-scoped *gorm.DB. With LazyTx the first write call
// transparently opens BEGIN; reads stay outside any transaction.
func DB(c *fiber.Ctx) *gorm.DB { return DBFromCtx(c.UserContext()) }

// DBFromCtx is the context.Context variant of DB.
func DBFromCtx(ctx context.Context) *gorm.DB {
	return txcore.MustFromCtx(ctx).DB(ctx)
}

// Outside returns a *gorm.DB whose context is decoupled from the request
// cancellation (so it survives request timeout) but preserves request values
// for tracing/logging propagation.
func Outside(c *fiber.Ctx) *gorm.DB { return OutsideCtx(c.UserContext()) }

// OutsideCtx is the context.Context variant of Outside.
func OutsideCtx(ctx context.Context) *gorm.DB {
	return txcore.MustFromCtx(ctx).Outside(ctx)
}

// OnRollback registers fn to run if the transaction rolls back.
func OnRollback(c *fiber.Ctx, fn func(*gorm.DB) error) { OnRollbackCtx(c.UserContext(), fn) }

// OnRollbackCtx is the context.Context variant of OnRollback.
func OnRollbackCtx(ctx context.Context, fn func(*gorm.DB) error) {
	txcore.MustFromCtx(ctx).AppendOnRollback(fn)
}

// OnCommit registers fn to run only if the transaction commits successfully.
// Callbacks run in registration order; the first error stops the chain and
// is reported via Config.OnCallbackError (the commit itself is not undone).
func OnCommit(c *fiber.Ctx, fn func(*gorm.DB) error) { OnCommitCtx(c.UserContext(), fn) }

// OnCommitCtx is the context.Context variant of OnCommit.
func OnCommitCtx(ctx context.Context, fn func(*gorm.DB) error) {
	txcore.MustFromCtx(ctx).AppendOnCommit(fn)
}
