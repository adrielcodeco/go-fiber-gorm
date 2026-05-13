package txctxv3_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gofiber/fiber/v3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/adrielcodeco/go-fiber-gorm/txctxv3"
)

type Item struct {
	ID   uint   `gorm:"primarykey"`
	Name string `gorm:"not null"`
}

var testDBCounter atomic.Int64

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	id := testDBCounter.Add(1)
	dsn := fmt.Sprintf("file:v3db%d?mode=memory&cache=shared", id)
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

func itemExists(t *testing.T, db *gorm.DB, name string) bool {
	t.Helper()
	var item Item
	return db.Where("name = ?", name).First(&item).Error == nil
}

func TestV3LazyCommit(t *testing.T) {
	db := setupTestDB(t)

	app := fiber.New()
	app.Post("/test", txctxv3.Middleware(db, txctxv3.Config{}), func(c fiber.Ctx) error {
		return txctxv3.DB(c).Create(&Item{Name: "v3-ok"}).Error
	})

	req := httptest.NewRequest("POST", "/test", nil)
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if !itemExists(t, db, "v3-ok") {
		t.Error("record should persist after successful handler")
	}
}

func TestV3LazyRollback(t *testing.T) {
	db := setupTestDB(t)

	app := fiber.New()
	app.Post("/test", txctxv3.Middleware(db, txctxv3.Config{}), func(c fiber.Ctx) error {
		if err := txctxv3.DB(c).Create(&Item{Name: "v3-rollback"}).Error; err != nil {
			return err
		}
		return errors.New("fail")
	})

	req := httptest.NewRequest("POST", "/test", nil)
	resp, _ := app.Test(req, fiber.TestConfig{Timeout: 0})
	if resp.StatusCode != 500 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if itemExists(t, db, "v3-rollback") {
		t.Error("record must have been rolled back")
	}
}

func TestV3OutsideAndCallbacks(t *testing.T) {
	db := setupTestDB(t)

	var (
		commitFired   int32
		rollbackFired int32
	)

	app := fiber.New()
	app.Post("/test", txctxv3.Middleware(db, txctxv3.Config{LazyTx: txctxv3.BoolPtr(false)}), func(c fiber.Ctx) error {
		_ = txctxv3.Outside(c).Create(&Item{Name: "v3-outside"}).Error
		txctxv3.OnRollback(c, func(_ *gorm.DB) error {
			atomic.StoreInt32(&rollbackFired, 1)
			return nil
		})
		txctxv3.OnCommit(c, func(_ *gorm.DB) error {
			atomic.StoreInt32(&commitFired, 1)
			return nil
		})
		return txctxv3.DB(c).Create(&Item{Name: "v3-inside"}).Error
	})

	req := httptest.NewRequest("POST", "/test", nil)
	resp, _ := app.Test(req, fiber.TestConfig{Timeout: 0})
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if !itemExists(t, db, "v3-outside") || !itemExists(t, db, "v3-inside") {
		t.Error("both records should persist")
	}
	if atomic.LoadInt32(&commitFired) == 0 {
		t.Error("OnCommit should have fired")
	}
	if atomic.LoadInt32(&rollbackFired) != 0 {
		t.Error("OnRollback must NOT fire on success")
	}
}

func TestV3CtxVariants(t *testing.T) {
	db := setupTestDB(t)
	app := fiber.New()
	app.Post("/test", txctxv3.Middleware(db, txctxv3.Config{}), func(c fiber.Ctx) error {
		ctx := c.Context()
		if d := txctxv3.DBFromCtx(ctx); d == nil {
			t.Error("DBFromCtx nil")
		}
		if d := txctxv3.OutsideCtx(ctx); d == nil {
			t.Error("OutsideCtx nil")
		}
		txctxv3.OnRollbackCtx(ctx, func(_ *gorm.DB) error { return nil })
		txctxv3.OnCommitCtx(ctx, func(_ *gorm.DB) error { return nil })
		return nil
	})
	req := httptest.NewRequest("POST", "/test", nil)
	resp, _ := app.Test(req, fiber.TestConfig{Timeout: 0})
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestV3UpstreamContextPropagated(t *testing.T) {
	db := setupTestDB(t)

	type keyT struct{}
	const want = "trace-v3"

	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(context.WithValue(c.Context(), keyT{}, want))
		return c.Next()
	})
	app.Get("/test", txctxv3.Middleware(db, txctxv3.Config{}), func(c fiber.Ctx) error {
		got, _ := c.Context().Value(keyT{}).(string)
		if got != want {
			return fmt.Errorf("upstream ctx lost: got %q", got)
		}
		return nil
	})

	req := httptest.NewRequest("GET", "/test", nil)
	resp, _ := app.Test(req, fiber.TestConfig{Timeout: 0})
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
