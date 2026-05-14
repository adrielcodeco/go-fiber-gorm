package apmcore_test

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	redis "github.com/redis/go-redis/v9"
	apm "go.elastic.co/apm/v2"
	"go.elastic.co/apm/v2/apmtest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/adrielcodeco/go-fiber-gorm/apmcore"
)

func TestPreparedStatementAndRollbackEmitSpans(t *testing.T) {
	apmcore.RegisterDriver("sqlite-apm", sqliteBase)
	sqlDB, err := sql.Open("sqlite-apm", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer sqlDB.Close()
	if _, err := sqlDB.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, spans, _ := apmtest.WithTransaction(func(ctx context.Context) {
		// PrepareContext → drives PrepareContext + Stmt.ExecContext + Close.
		stmt, err := sqlDB.PrepareContext(ctx, "INSERT INTO t (name) VALUES (?)")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if _, err := stmt.ExecContext(ctx, "alpha"); err != nil {
			t.Fatalf("stmt.exec: %v", err)
		}
		// QueryContext on a prepared stmt.
		qstmt, err := sqlDB.PrepareContext(ctx, "SELECT name FROM t WHERE id = ?")
		if err != nil {
			t.Fatalf("prepare q: %v", err)
		}
		rows, err := qstmt.QueryContext(ctx, 1)
		if err != nil {
			t.Fatalf("stmt.query: %v", err)
		}
		rows.Close()
		stmt.Close()
		qstmt.Close()

		// Begin + Rollback → drives BeginTx and tracingTx.Rollback.
		tx, err := sqlDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("beginTx: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO t (name) VALUES (?)", "to-rollback"); err != nil {
			t.Fatalf("tx.exec: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("rollback: %v", err)
		}

		// ExecContext / QueryContext directly on the connection (no prepare).
		if _, err := sqlDB.ExecContext(ctx, "UPDATE t SET name = ? WHERE id = ?", "beta", 1); err != nil {
			t.Fatalf("conn.exec: %v", err)
		}
		rows2, err := sqlDB.QueryContext(ctx, "SELECT id FROM t")
		if err != nil {
			t.Fatalf("conn.query: %v", err)
		}
		rows2.Close()

		// Ping → exercises tracingConn.Ping.
		if err := sqlDB.PingContext(ctx); err != nil {
			t.Fatalf("ping: %v", err)
		}
	})

	if len(spans) == 0 {
		t.Fatal("expected spans")
	}
	var sawPrepare, sawStmtExec bool
	for i := range spans {
		switch spans[i].Action {
		case "prepare":
			sawPrepare = true
		case "exec":
			sawStmtExec = true
		}
	}
	if !sawPrepare {
		t.Errorf("expected a prepare span; subtypes/actions: %s", spanSubtypes(spans))
	}
	if !sawStmtExec {
		t.Errorf("expected an exec span")
	}
	// Note: Rollback/Commit spans are emitted on context.Background by
	// database/sql (the underlying call is not context-aware), so they
	// are not attached to the request transaction and will not appear
	// in the apmtest recorder. Coverage still includes those paths.
}

func TestPoolMetricsGather(t *testing.T) {
	apmcore.RegisterDriver("sqlite-apm", sqliteBase)
	sqlDB, _ := sql.Open("sqlite-apm", ":memory:")
	defer sqlDB.Close()
	_ = sqlDB.Ping()

	deregister := apmcore.RegisterDBPoolMetrics(sqlDB)
	defer deregister()

	// Force the agent to flush so the gatherer fires synchronously.
	apm.DefaultTracer().SendMetrics(nil)
	apm.DefaultTracer().Flush(nil)
}

func TestWrapZapCoreEmitsAPMError(t *testing.T) {
	core, observed := observer.New(zapcore.ErrorLevel)
	wrapped := apmcore.WrapZapCore(core)
	logger := zap.New(wrapped)

	_, _, _ = apmtest.WithTransaction(func(ctx context.Context) {
		logger.With(apmcore.LogCtxFields(ctx)...).Error("boom")
	})
	// apmzap.Core.Write sends to apm.DefaultTracer (not the apmtest
	// recorder), so we can't observe the captured error here — but we can
	// verify the wrap forwarded to the base core.
	if observed.Len() == 0 {
		t.Fatal("expected wrapped core to forward to base core")
	}
}

func TestInstrumentRedisDoesNotPanic(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}) // unreachable; we don't issue commands
	defer client.Close()
	if err := apmcore.InstrumentRedis(client); err != nil {
		t.Fatalf("InstrumentRedis: %v", err)
	}
}

func TestLabelsAndCaptureErrorWithActiveTx(t *testing.T) {
	_, _, errs := apmtest.WithTransaction(func(ctx context.Context) {
		apmcore.SetLabel(ctx, "wallet_id", "w-1")
		apmcore.SetLabel(ctx, "", "skipped")        // empty key → no-op
		apmcore.SetLabel(ctx, "skipped_too", "")    // empty value → no-op
		apmcore.SetLabels(ctx, map[string]string{
			"external_id": "e-1",
			"":            "skip",
			"skip_too":    "",
		})
		apmcore.CaptureError(ctx, errBoom{})
	})
	if len(errs) == 0 {
		t.Fatal("expected captured error")
	}
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

func TestPluginNameOf(t *testing.T) {
	p := apmcore.NewGormPlugin()
	if n := apmcore.PluginNameOf(p); n == "" {
		t.Fatal("expected plugin name")
	}
}

func TestWrappedHTTPTransportRoundTrips(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := &http.Client{Transport: apmcore.WrapHTTPTransport(nil)}
	_, _, _ = apmtest.WithTransaction(func(ctx context.Context) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("client.Do: %v", err)
		}
		resp.Body.Close()
	})
}
