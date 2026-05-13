// Exemplos de uso do package gsfiber (graceful shutdown para Fiber v2 + GORM
// + chamadas externas), e do core gscore.
//
// Pattern recomendado num main():
//
//	mgr := gsfiber.New(gsfiber.Config{
//	    PreStopDelay:   5 * time.Second,  // tempo p/ kube-proxy remover endpoint
//	    DrainTimeout:   25 * time.Second, // aguarda requests in-flight
//	    DBCloseTimeout: 5 * time.Second,
//	    ForceKillAfter: 55 * time.Second, // < terminationGracePeriodSeconds
//	    Logger:         myLogger,
//	})
//	gsfiber.RegisterApp(mgr, app)
//	mgr.RegisterDB(db)
//
//	// Readiness probe vira 503 assim que o shutdown começa.
//	app.Get("/healthz/ready", gsfiber.ReadinessHandler(mgr))
//
//	// Outbound calls derivam contexto do manager → cancelam no shutdown.
//	httpClient := &http.Client{Timeout: 10 * time.Second}
//	go pollExternalAPI(mgr.RootContext(), httpClient)
//
//	go func() {
//	    if err := app.Listen(":8080"); err != nil { mgr.Trigger() }
//	}()
//
//	if err := mgr.ListenAndWait(); err != nil { log.Fatal(err) }
package examples

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"

	"github.com/adrielcodeco/go-fiber-gorm/gscore"
	"github.com/adrielcodeco/go-fiber-gorm/gsfiber"
)

// ShutdownScenarioBasic mostra o setup mínimo: drain do servidor HTTP +
// fechamento do pool GORM + readiness probe que reflete o estado.
func ShutdownScenarioBasic(app *fiber.App, db *gorm.DB) error {
	mgr := gsfiber.New(gsfiber.Config{
		PreStopDelay:   2 * time.Second,
		DrainTimeout:   20 * time.Second,
		DBCloseTimeout: 3 * time.Second,
		ForceKillAfter: 30 * time.Second,
	})
	gsfiber.RegisterApp(mgr, app)
	mgr.RegisterDB(db)
	app.Get("/healthz/ready", gsfiber.ReadinessHandler(mgr))
	return mgr.ListenAndWait()
}

// ShutdownScenarioOutboundCancel mostra como cancelar chamadas externas
// em andamento no momento do shutdown: derive o contexto do
// mgr.RootContext().
func ShutdownScenarioOutboundCancel(mgr *gsfiber.Manager, client *http.Client) error {
	req, err := http.NewRequestWithContext(mgr.RootContext(),
		http.MethodGet, "https://example.invalid/things", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("outbound call cancelled or failed: %w", err)
	}
	defer resp.Body.Close()
	return nil
}

// ShutdownScenarioHooks mostra hooks ordenados em múltiplas fases:
// flush de outbox antes do drain, cleanup de Redis depois do DB.
func ShutdownScenarioHooks(mgr *gsfiber.Manager, flushOutbox, closeRedis func(context.Context) error) {
	mgr.AddHook(gsfiber.Hook{
		Name:     "outbox-flush",
		Phase:    gsfiber.PhasePreStop,
		Priority: 0,
		Run:      flushOutbox,
	})
	mgr.AddHook(gsfiber.Hook{
		Name:     "redis-close",
		Phase:    gsfiber.PhasePostDB,
		Priority: 0,
		Run:      closeRedis,
	})
}

// ShutdownScenarioCustomLogger mostra integração com um logger estruturado
// (qualquer tipo que satisfaça gscore.Logger).
type stdLogger struct{}

func (stdLogger) Info(msg string, kv ...any)  { fmt.Println("INFO", msg, kv) }
func (stdLogger) Warn(msg string, kv ...any)  { fmt.Println("WARN", msg, kv) }
func (stdLogger) Error(msg string, kv ...any) { fmt.Println("ERROR", msg, kv) }

func ShutdownScenarioCustomLogger() *gscore.Manager {
	return gscore.New(gscore.Config{Logger: stdLogger{}})
}
