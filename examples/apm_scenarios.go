// APM scenarios — examples of wiring the apmcore + apmfiber packages
// into a Fiber v2 application. These are documentation-only snippets
// (compile-checked by the examples module) — no executable main here.
package examples

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net/http"

	"github.com/gofiber/fiber/v2"

	"github.com/adrielcodeco/go-fiber-gorm/apmcore"
	"github.com/adrielcodeco/go-fiber-gorm/apmfiber"
)

// apmScenarioBootstrap wires the APM agent + OTel bridge at process start
// and returns the shutdown func to call from main after wg.Wait().
//
// Crucial ordering: SetupOTelSDK must run BEFORE any DB/Redis loaders so
// they see the global TracerProvider configured by apmcore.
func apmScenarioBootstrap() (apmcore.ShutdownFunc, error) {
	shutdown, err := apmcore.SetupOTelSDK(context.Background())
	if err != nil {
		return nil, err
	}

	// Wrap http.DefaultTransport so every outbound request built via
	// http.NewRequestWithContext(ctx, ...) gets its own APM span + W3C
	// traceparent header.
	http.DefaultTransport = apmcore.WrapHTTPTransport(http.DefaultTransport)
	http.DefaultClient.Transport = http.DefaultTransport

	return shutdown, nil
}

// apmScenarioFiberApp shows the middleware order: apmfiber.Middleware
// MUST be the first middleware so the transaction is on the request
// context before anything else runs. Labels go right after.
func apmScenarioFiberApp() *fiber.App {
	app := fiber.New()
	app.Use(apmfiber.Middleware())
	app.Use(apmfiber.Labels(apmfiber.LabelsConfig{
		Headers: map[string]string{
			"X-Product-Type":       "product_type",
			"X-Product-Payment-Id": "product_payment_id",
			"X-Origin":             "origin",
		},
		BodyTarget: func() any { return new(businessIDs) },
		BodyExtractor: func(decoded any) map[string]string {
			b := decoded.(*businessIDs)
			return map[string]string{
				"wallet_id":          b.WalletId,
				"external_id":        b.ExternalId,
				"end_to_end_id":      b.EndToEndId,
				"pix_charge_id":      b.PixChargeId,
				"pix_transaction_id": b.PixTransactionId,
			}
		},
	}))
	app.Post("/charge", apmExampleHandler)
	return app
}

type businessIDs struct {
	WalletId         string `json:"walletId"`
	ExternalId       string `json:"externalId"`
	EndToEndId       string `json:"endToEndId"`
	PixChargeId      string `json:"pixChargeId"`
	PixTransactionId string `json:"pixTransactionId"`
}

// apmExampleHandler illustrates the in-handler concerns:
//
//   - Late-bound labels via apmfiber.SetLabel (identifiers that only
//     become available after the domain call ran).
//   - Inline error mapping with apmfiber.CaptureError — without this
//     call the error never reaches Kibana → APM → Errors, because
//     Fiber's central ErrorHandler is not invoked when the handler
//     responds directly.
func apmExampleHandler(c *fiber.Ctx) error {
	id, err := doDomainWork(c.Context())
	if err != nil {
		apmfiber.CaptureError(c, err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	apmfiber.SetLabel(c, "generated_id", id)
	return c.SendStatus(fiber.StatusOK)
}

func doDomainWork(_ context.Context) (string, error) { return "id-123", nil }

// apmScenarioDB illustrates the DB wiring. RegisterDriver must be
// called BEFORE sql.Open / gorm.Open; the gorm plugin is attached
// after gorm.Open. RegisterDBPoolMetrics is registered once per pool.
func apmScenarioDB(baseDriver driver.Driver) (*sql.DB, error) {
	apmcore.RegisterDriver("pgx-apm", baseDriver)
	db, err := sql.Open("pgx-apm", "host=localhost user=app dbname=app sslmode=disable")
	if err != nil {
		return nil, err
	}
	apmcore.RegisterDBPoolMetrics(db)
	// db.Use(apmcore.NewGormPlugin())  // when wrapping via gorm.Open with DriverName="pgx-apm"
	return db, nil
}
