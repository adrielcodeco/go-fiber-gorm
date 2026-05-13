package txcore

import (
	"context"
	"database/sql"
	"strings"

	"gorm.io/gorm"
)

// lazyDB returns a *gorm.DB that defers BEGIN until the first write reaches
// the connection pool. It works by wrapping the base ConnPool with a pool
// proxy that, on the first write statement, asks the holder to begin and
// then forwards every subsequent call to the tx's pool.
func lazyDB(ctx context.Context, h *Holder) *gorm.DB {
	pool := &lazyPool{h: h, base: h.db.ConnPool}
	session := h.db.Session(&gorm.Session{
		Context:                  ctx,
		SkipDefaultTransaction:   true,
		DisableNestedTransaction: true,
	})
	session.Config.ConnPool = pool
	session.Statement.ConnPool = pool
	return session
}

// lazyPool implements gorm.ConnPool. Reads (SELECT) route to the base pool,
// writes upgrade to a real *sql.Tx via Holder.Begin.
//
// Detection is statement-based: anything that isn't a SELECT is treated as a
// write. False positives only mean a transaction opens slightly earlier than
// strictly necessary, never a correctness issue. False negatives (missed
// writes) would break atomicity, so we err on the side of opening too eagerly.
type lazyPool struct {
	h    *Holder
	base gorm.ConnPool
}

func isWrite(query string) bool {
	q := strings.TrimLeft(query, " \t\n\r(")
	if len(q) < 6 {
		return true
	}
	if strings.ToUpper(q[:6]) == "SELECT" {
		return false
	}
	return true
}

func (p *lazyPool) active(ctx context.Context, write bool) gorm.ConnPool {
	if !write {
		return p.base
	}
	p.h.mu.Lock()
	if p.h.started {
		pool := p.h.tx.Statement.ConnPool
		p.h.mu.Unlock()
		return pool
	}
	p.h.mu.Unlock()

	p.h.Begin(ctx)

	p.h.mu.Lock()
	pool := p.h.tx.Statement.ConnPool
	p.h.mu.Unlock()
	return pool
}

func (p *lazyPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return p.active(ctx, isWrite(query)).(interface {
		PrepareContext(context.Context, string) (*sql.Stmt, error)
	}).PrepareContext(ctx, query)
}

func (p *lazyPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return p.active(ctx, isWrite(query)).ExecContext(ctx, query, args...)
}

func (p *lazyPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return p.active(ctx, isWrite(query)).QueryContext(ctx, query, args...)
}

func (p *lazyPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return p.active(ctx, isWrite(query)).QueryRowContext(ctx, query, args...)
}
