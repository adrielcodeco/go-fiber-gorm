// Exemplos da API proposta para o package `txctx` (fiber + gorm).
//
// Exemplos de uso do package txctx.
//
// API proposta (resumo):
//
//   app.Use(txctx.Middleware(db, txctx.Config{
//       Timeout:         5 * time.Second,
//       LazyTx:          true,            // só abre tx na 1ª escrita
//       CompensationCtx: 3 * time.Second, // ctx novo p/ OnRollback callbacks
//   }))
//
//   txctx.DB(c)               -> *gorm.DB lazy-tx (vira tx na 1ª escrita)
//   txctx.Outside(c)          -> *gorm.DB fora da tx, ctx independente
//   txctx.OnRollback(c, fn)   -> registra compensação se houver rollback
//   txctx.OnCommit(c, fn)     -> registra ação se o commit suceder
//
package examples

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"

	"github.com/adrielcodeco/go-fiber-gorm/txctx"
)

// ---------------------------------------------------------------------------
// Models de exemplo
// ---------------------------------------------------------------------------

type User struct {
	ID    uint
	Email string
	Name  string
}

type Order struct {
	ID     uint
	UserID uint
	Total  int
}

type AuditLog struct {
	ID      uint
	Action  string
	Payload string
}

type FailedSignup struct {
	ID    uint
	Email string
	Error string
}

type OutboxEvent struct {
	ID      uint
	Topic   string
	Payload string
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

func setup(db *gorm.DB) *fiber.App {
	app := fiber.New()

	app.Use(txctx.Middleware(db, txctx.Config{
		Timeout:         5 * time.Second,
		LazyTx:          txctx.BoolPtr(true),
		CompensationCtx: 3 * time.Second,
	}))

	app.Get("/users/:id", getUser)             // cenário 1: só leitura, sem tx
	app.Post("/users", createUser)             // cenário 2: escrita simples, tx lazy
	app.Post("/orders", createOrderMulti)      // cenário 3: várias escritas na mesma tx
	app.Post("/signup", signupWithAudit)       // cenário 4: ignoreTimeout / Outside
	app.Post("/payment", paymentCompensation)  // cenário 5: OnRollback compensatório
	app.Post("/publish", commitThenPublish)    // cenário 6: OnCommit (outbox-ish)
	app.Post("/mixed", mixedScenario)          // cenário 7: tudo junto
	app.Post("/manual", manualRollback)        // cenário 8: erro explícito do handler
	app.Post("/panic", panicScenario)          // cenário 9: panic dispara rollback
	return app
}

// ---------------------------------------------------------------------------
// Cenário 1 — GET puro, nenhuma transação é aberta.
// LazyTx garante que não há custo de BEGIN/COMMIT em leituras.
// ---------------------------------------------------------------------------

func getUser(c *fiber.Ctx) error {
	var u User
	// txctx.DB(c) ainda não abriu tx; como é Find (read), continua sem tx.
	if err := txctx.DB(c).First(&u, c.Params("id")).Error; err != nil {
		return err
	}
	return c.JSON(u)
}

// ---------------------------------------------------------------------------
// Cenário 2 — Escrita simples. A tx abre na 1ª chamada de Create.
// Se o handler retornar nil e o ctx não estourar, commit. Senão, rollback.
// ---------------------------------------------------------------------------

func createUser(c *fiber.Ctx) error {
	var u User
	if err := c.BodyParser(&u); err != nil {
		return err
	}
	// 1ª escrita: middleware abre BEGIN aqui, transparente.
	if err := txctx.DB(c).Create(&u).Error; err != nil {
		return err
	}
	return c.JSON(u)
}

// ---------------------------------------------------------------------------
// Cenário 3 — Múltiplas escritas atômicas. Tudo na mesma tx aberta lazy.
// Se qualquer uma falhar (ou o handler devolver erro), rollback de todas.
// ---------------------------------------------------------------------------

func createOrderMulti(c *fiber.Ctx) error {
	db := txctx.DB(c)

	user := User{Email: "a@b.com"}
	if err := db.Create(&user).Error; err != nil { // abre tx aqui
		return err
	}

	order := Order{UserID: user.ID, Total: 100}
	if err := db.Create(&order).Error; err != nil { // mesma tx
		return err
	}

	if err := db.Model(&user).Update("Name", "updated").Error; err != nil {
		return err
	}

	return c.JSON(order)
}

// ---------------------------------------------------------------------------
// Cenário 4 — "ignoreTimeout" via txctx.Outside(c).
// O AuditLog precisa persistir MESMO se o restante do request der rollback
// (ex: registro de tentativa de signup que falhou por timeout).
// ---------------------------------------------------------------------------

func signupWithAudit(c *fiber.Ctx) error {
	var u User
	if err := c.BodyParser(&u); err != nil {
		return err
	}

	// Audit ANTES da operação principal — sobrevive a qualquer rollback,
	// porque roda numa conexão fora da tx, com ctx.Background() interno.
	_ = txctx.Outside(c).Create(&AuditLog{
		Action:  "signup_attempt",
		Payload: u.Email,
	}).Error

	// Operação principal: dentro da tx do request.
	// Se der timeout aqui, o User some, mas o AuditLog acima permanece.
	if err := txctx.DB(c).Create(&u).Error; err != nil {
		return err
	}

	return c.JSON(u)
}

// ---------------------------------------------------------------------------
// Cenário 5 — OnRollback: se a tx for revertida, inserir um registro
// compensatório em OUTRA tabela (FailedSignup).
//
// Diferença para o cenário 4:
//   - Outside grava INCONDICIONALMENTE (commit ou rollback).
//   - OnRollback grava SÓ se houver rollback.
//
// O callback roda em um ctx novo (CompensationCtx), pois o ctx do request
// já está cancelado quando o rollback acontece por timeout.
// ---------------------------------------------------------------------------

func paymentCompensation(c *fiber.Ctx) error {
	var u User
	if err := c.BodyParser(&u); err != nil {
		return err
	}

	if err := txctx.DB(c).Create(&u).Error; err != nil {
		return err
	}

	// Se a tx der rollback (timeout, erro no handler, panic, 5xx),
	// este callback roda com uma *gorm.DB nova, fora da tx morta.
	txctx.OnRollback(c, func(bg *gorm.DB) error {
		return bg.Create(&FailedSignup{
			Email: u.Email,
			Error: "rolled back",
		}).Error
	})

	// Simula uma chamada externa lenta que pode estourar o timeout do middleware.
	if err := chargeExternal(c.UserContext(), u.ID); err != nil {
		return err // dispara rollback -> OnRollback acima é executado
	}

	return c.JSON(u)
}

// ---------------------------------------------------------------------------
// Cenário 6 — OnCommit: publicar um evento SÓ se o commit suceder.
// Padrão "outbox light": evita publicar evento de algo que não persistiu.
// ---------------------------------------------------------------------------

func commitThenPublish(c *fiber.Ctx) error {
	var o Order
	if err := c.BodyParser(&o); err != nil {
		return err
	}

	if err := txctx.DB(c).Create(&o).Error; err != nil {
		return err
	}

	txctx.OnCommit(c, func(bg *gorm.DB) error {
		// poderia ser publish em fila externa; usando DB como exemplo
		return bg.Create(&OutboxEvent{
			Topic:   "order.created",
			Payload: fmt.Sprintf("%d", o.ID),
		}).Error
	})

	return c.JSON(o)
}

// ---------------------------------------------------------------------------
// Cenário 7 — Combinação completa em um único handler.
//
// - Outside: log de tentativa (sempre persiste).
// - DB (tx lazy): user + order.
// - OnRollback: marca FailedSignup se algo der errado.
// - OnCommit: publica evento se tudo der certo.
// ---------------------------------------------------------------------------

func mixedScenario(c *fiber.Ctx) error {
	var u User
	if err := c.BodyParser(&u); err != nil {
		return err
	}

	// Sempre persiste.
	_ = txctx.Outside(c).Create(&AuditLog{Action: "mixed_attempt", Payload: u.Email}).Error

	tx := txctx.DB(c)
	if err := tx.Create(&u).Error; err != nil {
		return err
	}

	order := Order{UserID: u.ID, Total: 250}
	if err := tx.Create(&order).Error; err != nil {
		return err
	}

	// Compensação se rolar rollback.
	txctx.OnRollback(c, func(bg *gorm.DB) error {
		return bg.Create(&FailedSignup{Email: u.Email, Error: "mixed rollback"}).Error
	})

	// Publica só se commitar.
	txctx.OnCommit(c, func(bg *gorm.DB) error {
		return bg.Create(&OutboxEvent{Topic: "order.created", Payload: u.Email}).Error
	})

	return c.JSON(fiber.Map{"user": u, "order": order})
}

// ---------------------------------------------------------------------------
// Cenário 8 — Handler decide fazer rollback retornando erro.
// Mostra que rollback não precisa vir de timeout; qualquer erro retornado
// pelo handler dispara rollback + OnRollback callbacks.
// ---------------------------------------------------------------------------

func manualRollback(c *fiber.Ctx) error {
	var u User
	if err := txctx.DB(c).Create(&u).Error; err != nil {
		return err
	}

	txctx.OnRollback(c, func(bg *gorm.DB) error {
		return bg.Create(&FailedSignup{Email: u.Email, Error: "manual"}).Error
	})

	// validação de negócio falhou após escrita -> rollback
	if u.Email == "" {
		return errors.New("email required")
	}
	return c.JSON(u)
}

// ---------------------------------------------------------------------------
// Cenário 9 — Panic em handler dispara rollback via recover() no middleware.
// O panic é re-lançado após o rollback (para o ErrorHandler do Fiber tratar).
// ---------------------------------------------------------------------------

func panicScenario(c *fiber.Ctx) error {
	_ = txctx.DB(c).Create(&User{Email: "boom"}).Error

	txctx.OnRollback(c, func(bg *gorm.DB) error {
		return bg.Create(&AuditLog{Action: "panicked"}).Error
	})

	panic("something went very wrong")
}

// ---------------------------------------------------------------------------
// Helper fake só para o exemplo compilar mentalmente.
// ---------------------------------------------------------------------------

func chargeExternal(ctx context.Context, userID uint) error {
	select {
	case <-time.After(10 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err() // timeout do middleware -> rollback
	}
}
