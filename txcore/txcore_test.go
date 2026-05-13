package txcore

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type item struct {
	ID   uint   `gorm:"primarykey"`
	Name string `gorm:"not null"`
}

func (item) TableName() string { return "items" }

var testDBCounter atomic.Int64

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	id := testDBCounter.Add(1)
	dsn := fmt.Sprintf("file:txcore%d?mode=memory&cache=shared", id)
	sqlDB, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db, err := gorm.Open(sqlite.Dialector{DSN: dsn, Conn: sqlDB}, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if err := db.AutoMigrate(&item{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func injectHolder(db *gorm.DB) (context.Context, *Holder) {
	h := NewHolder(db, 5*time.Second, true, nil)
	return Inject(context.Background(), h), h
}

func TestFromCtxNil(t *testing.T) {
	if FromCtx(context.Background()) != nil {
		t.Error("expected nil holder for plain context")
	}
}

func TestMustFromCtxPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustFromCtx should panic when no holder")
		}
	}()
	MustFromCtx(context.Background())
}

func TestHolderBeginIdempotent(t *testing.T) {
	db := setupTestDB(t)
	ctx, h := injectHolder(db)

	h.Begin(ctx)
	first := h.Tx()
	h.Begin(ctx)
	second := h.Tx()
	if first != second {
		t.Error("Begin must be idempotent")
	}
}

func TestHolderCommitNoTx(t *testing.T) {
	db := setupTestDB(t)
	_, h := injectHolder(db)

	var fired int32
	h.AppendOnCommit(func(_ *gorm.DB) error {
		atomic.StoreInt32(&fired, 1)
		return nil
	})
	commitErr, postErr := h.Commit()
	if commitErr != nil || postErr != nil {
		t.Fatalf("unexpected: %v / %v", commitErr, postErr)
	}
	if atomic.LoadInt32(&fired) == 0 {
		t.Error("OnCommit must fire even with no tx")
	}
}

func TestHolderRollbackNoTx(t *testing.T) {
	db := setupTestDB(t)
	_, h := injectHolder(db)

	var fired int32
	h.AppendOnRollback(func(_ *gorm.DB) error {
		atomic.StoreInt32(&fired, 1)
		return nil
	})
	h.Rollback()
	if atomic.LoadInt32(&fired) == 0 {
		t.Error("OnRollback must fire even with no tx")
	}
}

func TestOutsideIndependentOfTx(t *testing.T) {
	db := setupTestDB(t)
	ctx, h := injectHolder(db)

	out := h.Outside(ctx)
	if err := out.Create(&item{Name: "outside-unit"}).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	var got item
	if err := db.Where("name = ?", "outside-unit").First(&got).Error; err != nil {
		t.Errorf("record should be visible: %v", err)
	}
}

func TestOnCallbackErrorCapturesRollback(t *testing.T) {
	db := setupTestDB(t)
	var captured error
	h := NewHolder(db, 1*time.Second, true, func(err error) { captured = err })

	h.AppendOnRollback(func(_ *gorm.DB) error {
		return fmt.Errorf("compensation boom")
	})
	h.Rollback()
	if captured == nil {
		t.Error("OnCallbackError should have captured the OnRollback error")
	}
}

func TestDBLazyReturnsBaseWhenNotStarted(t *testing.T) {
	db := setupTestDB(t)
	ctx, h := injectHolder(db)

	got := h.DB(ctx)
	if got == nil {
		t.Fatal("DB returned nil")
	}
}

func TestDBEagerReturnsBaseWhenSomehowNotStarted(t *testing.T) {
	db := setupTestDB(t)
	h := NewHolder(db, 1*time.Second, false, nil)
	ctx := Inject(context.Background(), h)
	got := h.DB(ctx)
	if got == nil {
		t.Fatal("DB returned nil")
	}
}

func TestConfigWithDefaults(t *testing.T) {
	cfg := Config{}.WithDefaults()
	if cfg.Timeout != 30*time.Second {
		t.Errorf("default Timeout: want 30s, got %v", cfg.Timeout)
	}
	if cfg.CompensationCtx != 5*time.Second {
		t.Errorf("default CompensationCtx: want 5s, got %v", cfg.CompensationCtx)
	}
	if cfg.LazyTx == nil || !*cfg.LazyTx {
		t.Errorf("default LazyTx should be true, got %v", cfg.LazyTx)
	}
}

func TestConfigPreservesExplicit(t *testing.T) {
	cfg := Config{
		Timeout:         10 * time.Second,
		CompensationCtx: 2 * time.Second,
		LazyTx:          BoolPtr(false),
	}.WithDefaults()
	if cfg.Timeout != 10*time.Second {
		t.Errorf("Timeout not preserved")
	}
	if cfg.CompensationCtx != 2*time.Second {
		t.Errorf("CompensationCtx not preserved")
	}
	if cfg.LazyTx == nil || *cfg.LazyTx {
		t.Errorf("LazyTx should stay false")
	}
}

func TestIsWriteHeuristic(t *testing.T) {
	cases := []struct {
		q       string
		isWrite bool
	}{
		{"SELECT * FROM t", false},
		{"  select * from t", false},
		{"INSERT INTO t VALUES (1)", true},
		{"UPDATE t SET x=1", true},
		{"DELETE FROM t", true},
		{"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", true},
		{"", true},
	}
	for _, c := range cases {
		if got := isWrite(c.q); got != c.isWrite {
			t.Errorf("isWrite(%q) = %v, want %v", c.q, got, c.isWrite)
		}
	}
}

func TestLazyPoolDirect(t *testing.T) {
	db := setupTestDB(t)
	ctx, h := injectHolder(db)

	pool := &lazyPool{h: h, base: db.ConnPool}

	row := pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM items")
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("QueryRowContext: %v", err)
	}

	stmt, err := pool.PrepareContext(ctx, "INSERT INTO items(name) VALUES (?)")
	if err != nil {
		t.Fatalf("PrepareContext: %v", err)
	}
	_ = stmt.Close()

	if _, err := pool.ExecContext(ctx, "INSERT INTO items(name) VALUES (?)", "exec-row"); err != nil {
		t.Fatalf("ExecContext: %v", err)
	}
	if !h.Started() {
		t.Error("writes through lazyPool should have started the tx")
	}

	rows, err := pool.QueryContext(ctx, "SELECT name FROM items WHERE name = ?", "exec-row")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	_ = rows.Close()
}

func TestAppendCounters(t *testing.T) {
	db := setupTestDB(t)
	_, h := injectHolder(db)

	h.AppendOnRollback(func(_ *gorm.DB) error { return nil })
	h.AppendOnRollback(func(_ *gorm.DB) error { return nil })
	if h.OnRollbackLen() != 2 {
		t.Errorf("want 2 rollback, got %d", h.OnRollbackLen())
	}
	h.AppendOnCommit(func(_ *gorm.DB) error { return nil })
	h.AppendOnCommit(func(_ *gorm.DB) error { return nil })
	h.AppendOnCommit(func(_ *gorm.DB) error { return nil })
	if h.OnCommitLen() != 3 {
		t.Errorf("want 3 commit, got %d", h.OnCommitLen())
	}
}

func TestBaseDB(t *testing.T) {
	db := setupTestDB(t)
	_, h := injectHolder(db)
	if h.BaseDB() != db {
		t.Error("BaseDB should return the original *gorm.DB")
	}
}
