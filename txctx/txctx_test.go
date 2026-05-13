package txctx_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gofiber/fiber/v2"
	fiberrecover "github.com/gofiber/fiber/v2/middleware/recover"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/adrielcodeco/go-fiber-gorm/txcore"
	"github.com/adrielcodeco/go-fiber-gorm/txctx"
)

type Item struct {
	ID   uint   `gorm:"primarykey"`
	Name string `gorm:"not null"`
}

var testDBCounter atomic.Int64

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	id := testDBCounter.Add(1)
	dsn := fmt.Sprintf("file:testdb%d?mode=memory&cache=shared", id)

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
	if err := db.AutoMigrate(&Item{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func newApp(db *gorm.DB, cfg txctx.Config, method string, handler fiber.Handler) *fiber.App {
	if method == "" {
		method = http.MethodPost
	}
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		},
	})
	app.Add(method, "/test", txctx.Middleware(db, cfg), handler)
	return app
}

func newAppWithRecover(db *gorm.DB, cfg txctx.Config, method string, handler fiber.Handler) *fiber.App {
	if method == "" {
		method = http.MethodPost
	}
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		},
	})
	app.Add(method, "/test", fiberrecover.New(), txctx.Middleware(db, cfg), handler)
	return app
}

func doRequest(app *fiber.App, method string) *http.Response {
	req := httptest.NewRequest(method, "/test", nil)
	resp, _ := app.Test(req, 5000)
	return resp
}

func itemExists(t *testing.T, db *gorm.DB, name string) bool {
	t.Helper()
	var item Item
	return db.Where("name = ?", name).First(&item).Error == nil
}

func countItems(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	if err := db.Model(&Item{}).Count(&count).Error; err != nil {
		t.Fatalf("countItems: %v", err)
	}
	return count
}

func TestReadNoTx(t *testing.T) {
	db := setupTestDB(t)
	if err := db.Create(&Item{Name: "seed"}).Error; err != nil {
		t.Fatal(err)
	}

	var holder *txcore.Holder

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(true)}, http.MethodGet, func(c *fiber.Ctx) error {
		holder = txcore.FromCtx(c.UserContext())
		var count int64
		_ = txctx.DBFromCtx(c.UserContext()).Model(&Item{}).Count(&count)
		return c.SendStatus(fiber.StatusOK)
	})

	resp := doRequest(app, http.MethodGet)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	if holder == nil || holder.Started() {
		t.Error("no transaction should be started for a read-only handler with LazyTx:true")
	}
}

func TestLazyTxOpensOnWrite(t *testing.T) {
	db := setupTestDB(t)

	var holder *txcore.Holder

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(true)}, http.MethodPost, func(c *fiber.Ctx) error {
		holder = txcore.FromCtx(c.UserContext())
		return txctx.DBFromCtx(c.UserContext()).Create(&Item{Name: "lazy-write"}).Error
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	if holder == nil || !holder.Started() {
		t.Error("expected lazy tx to be started after a write")
	}
	if !itemExists(t, db, "lazy-write") {
		t.Error("expected record to persist after successful handler")
	}
}

// TestLazyTxRollbackOnError is the regression test for the original LazyTx
// rollback bug.
func TestLazyTxRollbackOnError(t *testing.T) {
	db := setupTestDB(t)

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(true)}, http.MethodPost, func(c *fiber.Ctx) error {
		if err := txctx.DBFromCtx(c.UserContext()).Create(&Item{Name: "lazy-rollback"}).Error; err != nil {
			return err
		}
		return errors.New("fail")
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if itemExists(t, db, "lazy-rollback") {
		t.Error("lazy-mode write must roll back on handler error")
	}
}

func TestHandlerErrorTriggersRollback(t *testing.T) {
	db := setupTestDB(t)

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		if err := txctx.DBFromCtx(c.UserContext()).Create(&Item{Name: "should-rollback"}).Error; err != nil {
			return err
		}
		return errors.New("fail")
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if itemExists(t, db, "should-rollback") {
		t.Error("record should have been rolled back")
	}
}

func TestOnRollbackCalledOnError(t *testing.T) {
	db := setupTestDB(t)

	var fired int32

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		txctx.OnRollbackCtx(c.UserContext(), func(_ *gorm.DB) error {
			atomic.StoreInt32(&fired, 1)
			return nil
		})
		return errors.New("trigger rollback")
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&fired) == 0 {
		t.Error("OnRollback callback was not called")
	}
}

func TestOnCommitCalledOnSuccess(t *testing.T) {
	db := setupTestDB(t)

	var fired int32

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		if err := txctx.DBFromCtx(c.UserContext()).Create(&Item{Name: "on-commit"}).Error; err != nil {
			return err
		}
		txctx.OnCommitCtx(c.UserContext(), func(_ *gorm.DB) error {
			atomic.StoreInt32(&fired, 1)
			return nil
		})
		return nil
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&fired) == 0 {
		t.Error("OnCommit callback was not called")
	}
	if !itemExists(t, db, "on-commit") {
		t.Error("record should persist after successful commit")
	}
}

func TestOnCommitErrorDoesNotRollback(t *testing.T) {
	db := setupTestDB(t)

	var rollbackFired int32
	var observed error

	cfg := txctx.Config{
		LazyTx:          txctx.BoolPtr(false),
		OnCallbackError: func(err error) { observed = err },
	}

	app := newApp(db, cfg, http.MethodPost, func(c *fiber.Ctx) error {
		if err := txctx.DBFromCtx(c.UserContext()).Create(&Item{Name: "post-commit"}).Error; err != nil {
			return err
		}
		txctx.OnRollbackCtx(c.UserContext(), func(_ *gorm.DB) error {
			atomic.StoreInt32(&rollbackFired, 1)
			return nil
		})
		txctx.OnCommitCtx(c.UserContext(), func(_ *gorm.DB) error {
			return errors.New("publish failed")
		})
		return nil
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500 from post-commit error, got %d", resp.StatusCode)
	}
	if !itemExists(t, db, "post-commit") {
		t.Error("commit already succeeded; record must persist")
	}
	if atomic.LoadInt32(&rollbackFired) != 0 {
		t.Error("OnRollback must NOT fire after a successful commit")
	}
	if observed == nil {
		t.Error("OnCallbackError should have observed the OnCommit error")
	}
}

func TestOutsideCtxPersistsDespiteRollback(t *testing.T) {
	db := setupTestDB(t)

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		if err := txctx.OutsideCtx(c.UserContext()).Create(&Item{Name: "outside-record"}).Error; err != nil {
			return err
		}
		if err := txctx.DBFromCtx(c.UserContext()).Create(&Item{Name: "inside-record"}).Error; err != nil {
			return err
		}
		return errors.New("force rollback")
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if !itemExists(t, db, "outside-record") {
		t.Error("outside-record should persist despite rollback")
	}
	if itemExists(t, db, "inside-record") {
		t.Error("inside-record should have been rolled back")
	}
}

func TestPanicTriggersRollbackAndOnRollback(t *testing.T) {
	db := setupTestDB(t)

	var rollbackFired int32

	app := newAppWithRecover(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		txctx.OnRollbackCtx(c.UserContext(), func(_ *gorm.DB) error {
			atomic.StoreInt32(&rollbackFired, 1)
			return nil
		})
		if err := txctx.DBFromCtx(c.UserContext()).Create(&Item{Name: "panic-record"}).Error; err != nil {
			return err
		}
		panic("intentional panic")
	})

	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	resp, _ := app.Test(req, 5000)

	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500 from panic, got %d", resp.StatusCode)
	}
	if itemExists(t, db, "panic-record") {
		t.Error("panic-record should have been rolled back")
	}
	if atomic.LoadInt32(&rollbackFired) == 0 {
		t.Error("OnRollback hook should have been called after panic")
	}
}

func TestNonLazyTxBeginsImmediately(t *testing.T) {
	db := setupTestDB(t)

	var holder *txcore.Holder

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodGet, func(c *fiber.Ctx) error {
		holder = txcore.FromCtx(c.UserContext())
		return c.SendStatus(fiber.StatusOK)
	})

	resp := doRequest(app, http.MethodGet)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	if holder == nil || !holder.Started() {
		t.Error("expected transaction to be started immediately with LazyTx:false")
	}
}

func TestMultipleWritesSameTxAllCommit(t *testing.T) {
	db := setupTestDB(t)

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		txDB := txctx.DBFromCtx(c.UserContext())
		for i := 0; i < 3; i++ {
			if err := txDB.Create(&Item{Name: fmt.Sprintf("multi-%d", i)}).Error; err != nil {
				return err
			}
		}
		return nil
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	if count := countItems(t, db); count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}
}

func TestMultipleWritesSameTxAllRollback(t *testing.T) {
	db := setupTestDB(t)

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		txDB := txctx.DBFromCtx(c.UserContext())
		for i := 0; i < 3; i++ {
			if err := txDB.Create(&Item{Name: fmt.Sprintf("rollback-multi-%d", i)}).Error; err != nil {
				return err
			}
		}
		return errors.New("rollback all three")
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if count := countItems(t, db); count != 0 {
		t.Errorf("expected 0 records after rollback, got %d", count)
	}
}

func TestUserContextPropagated(t *testing.T) {
	db := setupTestDB(t)

	type keyT struct{}
	const want = "tracing-id-42"

	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		},
	})
	app.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(context.WithValue(c.UserContext(), keyT{}, want))
		return c.Next()
	})
	app.Add(http.MethodGet, "/test", txctx.Middleware(db, txctx.Config{}), func(c *fiber.Ctx) error {
		got, _ := c.UserContext().Value(keyT{}).(string)
		if got != want {
			return fmt.Errorf("upstream context value lost: got %q want %q", got, want)
		}
		return c.SendStatus(fiber.StatusOK)
	})

	resp := doRequest(app, http.MethodGet)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("upstream ctx not propagated, status=%d", resp.StatusCode)
	}
}

func TestMustHolderPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("DBFromCtx should panic when context has no holder")
		}
	}()
	_ = txctx.DBFromCtx(context.Background())
}

func TestFiberCtxWrappers(t *testing.T) {
	db := setupTestDB(t)

	var (
		dbGot         *gorm.DB
		outsideGot    *gorm.DB
		rollbackFired int32
		commitFired   int32
	)

	app := newApp(db, txctx.Config{LazyTx: txctx.BoolPtr(false)}, http.MethodPost, func(c *fiber.Ctx) error {
		dbGot = txctx.DB(c)
		outsideGot = txctx.Outside(c)
		txctx.OnRollback(c, func(_ *gorm.DB) error {
			atomic.StoreInt32(&rollbackFired, 1)
			return nil
		})
		txctx.OnCommit(c, func(_ *gorm.DB) error {
			atomic.StoreInt32(&commitFired, 1)
			return nil
		})
		return txctx.DB(c).Create(&Item{Name: "wrappers"}).Error
	})

	resp := doRequest(app, http.MethodPost)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	if dbGot == nil || outsideGot == nil {
		t.Fatal("DB or Outside returned nil")
	}
	if atomic.LoadInt32(&commitFired) == 0 {
		t.Error("OnCommit wrapper did not fire")
	}
	if atomic.LoadInt32(&rollbackFired) != 0 {
		t.Error("OnRollback wrapper must not fire on success")
	}
}
