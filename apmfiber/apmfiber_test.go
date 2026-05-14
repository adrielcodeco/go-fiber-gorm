package apmfiber_test

import (
	"bytes"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/adrielcodeco/go-fiber-gorm/apmfiber"
)

type bodyShape struct {
	WalletId   string `json:"walletId"`
	ExternalId string `json:"externalId"`
}

func TestMiddlewareAndLabelsDoNotPanic(t *testing.T) {
	app := fiber.New()
	app.Use(apmfiber.Middleware())
	app.Use(apmfiber.Labels(apmfiber.LabelsConfig{
		Headers:    map[string]string{"X-Origin": "origin"},
		BodyTarget: func() any { return new(bodyShape) },
		BodyExtractor: func(decoded any) map[string]string {
			b := decoded.(*bodyShape)
			return map[string]string{
				"wallet_id":   b.WalletId,
				"external_id": b.ExternalId,
			}
		},
		Extra: func(c *fiber.Ctx) map[string]string {
			return map[string]string{"path": c.Path()}
		},
	}))
	app.Post("/ping", func(c *fiber.Ctx) error {
		apmfiber.SetLabel(c, "late_label", "value")
		apmfiber.CaptureError(c, nil)
		return c.SendStatus(fiber.StatusOK)
	})

	req := httptest.NewRequest("POST", "/ping", bytes.NewReader([]byte(`{"walletId":"w-1","externalId":"e-1"}`)))
	req.Header.Set("X-Origin", "test")
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
}
