package apmfiberv3_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"

	"github.com/adrielcodeco/go-fiber-gorm/apmfiberv3"
)

type body struct {
	WalletId string `json:"walletId"`
}

func TestMiddlewareAndLabelsDoNotPanic(t *testing.T) {
	app := fiber.New()
	app.Use(apmfiberv3.Middleware())
	app.Use(apmfiberv3.Labels(apmfiberv3.LabelsConfig{
		Headers:    map[string]string{"X-Origin": "origin"},
		BodyTarget: func() any { return new(body) },
		BodyExtractor: func(d any) map[string]string {
			return map[string]string{"wallet_id": d.(*body).WalletId}
		},
	}))
	app.Post("/ping", func(c fiber.Ctx) error {
		apmfiberv3.SetLabel(c, "late", "v")
		apmfiberv3.CaptureError(c, nil)
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/ping", bytes.NewReader([]byte(`{"walletId":"w-1"}`)))
	req.Header.Set("X-Origin", "test")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
