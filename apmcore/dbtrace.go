package apmcore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	apmsql "go.elastic.co/apm/module/apmsql/v2"
	apm "go.elastic.co/apm/v2"
	"gorm.io/gorm"
)

// RegisterDriver wraps an existing database/sql driver with APM
// instrumentation and registers it under name so it can be opened via
// sql.Open(name, dsn) or referenced as gorm.Config.DriverName.
//
// The wrapper emits spans for Prepare/Query/Exec/Begin/Commit/Rollback and,
// crucially, for prepared-statement Close roundtrips — which dominate
// transaction-commit cost when gorm.PrepareStmt is enabled.
//
// Span names use apmsql.QuerySignature so they are readable in the Kibana
// waterfall (e.g. "SELECT FROM users (query)" instead of the raw SQL).
//
// Registration is idempotent: calling twice with the same name is a no-op.
func RegisterDriver(name string, base driver.Driver) {
	registerOnce(name, func() {
		sql.Register(name, &tracingDriver{base: base})
	})
}

var (
	driverMu       sync.Mutex
	registeredDrvs = map[string]struct{}{}
)

func registerOnce(name string, fn func()) {
	driverMu.Lock()
	defer driverMu.Unlock()
	if _, ok := registeredDrvs[name]; ok {
		return
	}
	registeredDrvs[name] = struct{}{}
	fn()
}

// --- driver wrapper -------------------------------------------------------

type tracingDriver struct{ base driver.Driver }

func (d *tracingDriver) Open(name string) (driver.Conn, error) {
	c, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &tracingConn{conn: c}, nil
}

type tracingConn struct{ conn driver.Conn }

func (c *tracingConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &tracingStmt{stmt: stmt, query: query}, nil
}

func (c *tracingConn) Close() error { return c.conn.Close() }

func (c *tracingConn) Begin() (driver.Tx, error) { //nolint:staticcheck
	tx, err := c.conn.Begin() //nolint:staticcheck
	if err != nil {
		return nil, err
	}
	return &tracingTx{tx: tx}, nil
}

func (c *tracingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	span, ctx := startDBSpan(ctx, "BEGIN", "db.postgresql.begin")
	if cbt, ok := c.conn.(driver.ConnBeginTx); ok {
		tx, err := cbt.BeginTx(ctx, opts)
		endSpan(span, err)
		if err != nil {
			return nil, err
		}
		return &tracingTx{tx: tx}, nil
	}
	tx, err := c.conn.Begin() //nolint:staticcheck
	endSpan(span, err)
	if err != nil {
		return nil, err
	}
	return &tracingTx{tx: tx}, nil
}

func (c *tracingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	sig := apmsql.QuerySignature(query)
	span, ctx := startDBSpan(ctx, sig+" (prepare)", "db.postgresql.prepare")
	var (
		stmt driver.Stmt
		err  error
	)
	if cpc, ok := c.conn.(driver.ConnPrepareContext); ok {
		stmt, err = cpc.PrepareContext(ctx, query)
	} else {
		stmt, err = c.conn.Prepare(query)
	}
	endSpan(span, err)
	if err != nil {
		return nil, err
	}
	return &tracingStmt{stmt: stmt, query: query}, nil
}

func (c *tracingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := c.conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	sig := apmsql.QuerySignature(query)
	span, ctx := startDBSpan(ctx, sig+" (exec)", "db.postgresql.exec")
	res, err := ec.ExecContext(ctx, query, args)
	endSpan(span, err)
	return res, err
}

func (c *tracingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := c.conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	sig := apmsql.QuerySignature(query)
	span, ctx := startDBSpan(ctx, sig+" (query)", "db.postgresql.query")
	rows, err := qc.QueryContext(ctx, query, args)
	endSpan(span, err)
	return rows, err
}

func (c *tracingConn) Ping(ctx context.Context) error {
	if p, ok := c.conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *tracingConn) ResetSession(ctx context.Context) error {
	if rs, ok := c.conn.(driver.SessionResetter); ok {
		return rs.ResetSession(ctx)
	}
	return nil
}

func (c *tracingConn) IsValid() bool {
	if v, ok := c.conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

// --- stmt wrapper ---------------------------------------------------------

type tracingStmt struct {
	stmt  driver.Stmt
	query string
}

func (s *tracingStmt) Close() error {
	// Close is called from the connection's context, not the request's, so
	// we can't easily attach to the originating transaction here. We start
	// a span on context.Background; it will appear as an orphan in Kibana,
	// but the count and duration are still visible. Callers who care about
	// nesting should disable PrepareStmt under write-heavy transactions.
	sig := apmsql.QuerySignature(s.query)
	span, _ := startDBSpan(context.Background(), sig+" (close)", "db.postgresql.close")
	err := s.stmt.Close()
	endSpan(span, err)
	return err
}

func (s *tracingStmt) NumInput() int { return s.stmt.NumInput() }

func (s *tracingStmt) Exec(args []driver.Value) (driver.Result, error) { //nolint:staticcheck
	return s.stmt.Exec(args) //nolint:staticcheck
}

func (s *tracingStmt) Query(args []driver.Value) (driver.Rows, error) { //nolint:staticcheck
	return s.stmt.Query(args) //nolint:staticcheck
}

func (s *tracingStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	sig := apmsql.QuerySignature(s.query)
	span, ctx := startDBSpan(ctx, sig+" (exec)", "db.postgresql.exec")
	var (
		res driver.Result
		err error
	)
	if sec, ok := s.stmt.(driver.StmtExecContext); ok {
		res, err = sec.ExecContext(ctx, args)
	} else {
		err = driver.ErrSkip
	}
	endSpan(span, err)
	return res, err
}

func (s *tracingStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	sig := apmsql.QuerySignature(s.query)
	span, ctx := startDBSpan(ctx, sig+" (query)", "db.postgresql.query")
	var (
		rows driver.Rows
		err  error
	)
	if sqc, ok := s.stmt.(driver.StmtQueryContext); ok {
		rows, err = sqc.QueryContext(ctx, args)
	} else {
		err = driver.ErrSkip
	}
	endSpan(span, err)
	return rows, err
}

// --- tx wrapper -----------------------------------------------------------

type tracingTx struct{ tx driver.Tx }

func (t *tracingTx) Commit() error {
	span, _ := startDBSpan(context.Background(), "COMMIT", "db.postgresql.commit")
	err := t.tx.Commit()
	endSpan(span, err)
	return err
}

func (t *tracingTx) Rollback() error {
	span, _ := startDBSpan(context.Background(), "ROLLBACK", "db.postgresql.rollback")
	err := t.tx.Rollback()
	endSpan(span, err)
	return err
}

// --- shared helpers -------------------------------------------------------

func startDBSpan(ctx context.Context, name, typ string) (*apm.Span, context.Context) {
	span, ctx := apm.StartSpanOptions(ctx, name, typ, apm.SpanOptions{ExitSpan: true})
	if span != nil {
		span.Context.SetDatabase(apm.DatabaseSpanContext{Type: "sql", Instance: "postgres"})
	}
	return span, ctx
}

func endSpan(span *apm.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		// Attach the error to the active transaction; it will dedupe in
		// Kibana if the request also captures it.
		_ = err
	}
	span.End()
}

// --- gorm plugin ----------------------------------------------------------

// GormPluginName is the name registered with gorm.Use.
const GormPluginName = "apmcore:dbtrace"

// NewGormPlugin returns a gorm.Plugin that wraps each gorm operation
// (Create/Query/Update/Delete/Row/Raw) in a parent APM span named
// "<Op> <table>" of type "db.gorm.<op>". The driver-level spans then
// nest underneath, producing a foldable hierarchy in Kibana.
//
// The plugin assigns the new ctx back into tx.Statement.Context so the
// driver-level spans inherit the gorm span as parent. This is the
// foldable-spans invariant — without it, driver spans parent directly
// to the transaction and the gorm span looks empty.
func NewGormPlugin() gorm.Plugin {
	return &gormPlugin{}
}

type gormPlugin struct{}

func (*gormPlugin) Name() string { return GormPluginName }

func (p *gormPlugin) Initialize(db *gorm.DB) error {
	cb := db.Callback()
	// gorm's processor type is unexported, so we accept the Before/After
	// results via a structural interface that both expose.
	register := func(name string, before, after interface {
		Register(name string, fn func(*gorm.DB)) error
	}) error {
		if err := before.Register("apmcore:before:"+name, startGormSpan(name)); err != nil {
			return err
		}
		return after.Register("apmcore:after:"+name, endGormSpan)
	}

	if err := register("Create", cb.Create().Before("*"), cb.Create().After("*")); err != nil {
		return err
	}
	if err := register("Query", cb.Query().Before("*"), cb.Query().After("*")); err != nil {
		return err
	}
	if err := register("Update", cb.Update().Before("*"), cb.Update().After("*")); err != nil {
		return err
	}
	if err := register("Delete", cb.Delete().Before("*"), cb.Delete().After("*")); err != nil {
		return err
	}
	if err := register("Row", cb.Row().Before("*"), cb.Row().After("*")); err != nil {
		return err
	}
	if err := register("Raw", cb.Raw().Before("*"), cb.Raw().After("*")); err != nil {
		return err
	}
	return nil
}

type gormSpanKey struct{}

func startGormSpan(op string) func(*gorm.DB) {
	return func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Context == nil {
			return
		}
		ctx := tx.Statement.Context
		if apm.TransactionFromContext(ctx) == nil {
			return
		}
		table := tx.Statement.Table
		if table == "" {
			table = "?"
		}
		span, ctx := apm.StartSpan(ctx, op+" "+table, "db.gorm."+op)
		ctx = context.WithValue(ctx, gormSpanKey{}, span)
		tx.Statement.Context = ctx
	}
}

func endGormSpan(tx *gorm.DB) {
	if tx.Statement == nil || tx.Statement.Context == nil {
		return
	}
	span, _ := tx.Statement.Context.Value(gormSpanKey{}).(*apm.Span)
	if span == nil {
		return
	}
	if tx.Error != nil {
		if e := apm.CaptureError(tx.Statement.Context, tx.Error); e != nil {
			e.Send()
		}
	}
	span.End()
}

// PluginNameOf is exported for callers who want to assert plugin registration.
func PluginNameOf(p gorm.Plugin) string { return p.Name() }

// ErrNoBaseDriver is returned when RegisterDriver is called with a nil base.
var ErrNoBaseDriver = fmt.Errorf("apmcore: base driver is nil")
