# go-fiber-gorm

A [Fiber](https://github.com/gofiber/fiber) + [GORM](https://gorm.io) middleware that manages database transactions transparently at the request level. Supports both Fiber **v2** and **v3**.

**Module:** `github.com/adrielcodeco/go-fiber-gorm`

| Fiber | Import path | Go min |
|---|---|---|
| v2 | `github.com/adrielcodeco/go-fiber-gorm/txctx` | 1.22 |
| v3 | `github.com/adrielcodeco/go-fiber-gorm/txctxv3` | 1.25 |

The two adapters share a framework-agnostic engine (`txcore`) so both have identical semantics.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Public API](#public-api)
- [Usage](#usage)
  - [1. Setup](#1-setup)
  - [2. Read-only handler](#2-read-only-handler)
  - [3. Simple write (lazy tx)](#3-simple-write-lazy-tx)
  - [4. Multiple writes in the same transaction](#4-multiple-writes-in-the-same-transaction)
  - [5. Outside — write that survives rollback](#5-outside--write-that-survives-rollback)
  - [6. OnRollback — compensating transaction](#6-onrollback--compensating-transaction)
  - [7. OnCommit — outbox / post-commit event](#7-oncommit--outbox--post-commit-event)
  - [8. Handler returns error → rollback](#8-handler-returns-error--rollback)
  - [9. Panic → rollback + re-panic](#9-panic--rollback--re-panic)
  - [10. Layered architecture](#10-layered-architecture)
- [Commit / Rollback Decision Table](#commit--rollback-decision-table)

---

## Features

- **Lazy transactions** — a DB transaction is only opened when the first write (`Create`/`Update`/`Delete`) occurs in a request. Pure read requests never touch a transaction.
- **Timeout-triggered rollback** — each request gets a configurable context timeout. If it expires before the handler finishes, the transaction is rolled back automatically.
- **`Outside(c)`** — returns a `*gorm.DB` connected to `context.Background()`, completely outside the request transaction. Writes via `Outside` persist even if the main transaction rolls back.
- **`OnRollback(c, fn)`** — registers a compensating callback that runs if the transaction rolls back (timeout, error, or panic). Runs with a fresh context (`CompensationCtx` duration) because the request context is already cancelled.
- **`OnCommit(c, fn)`** — registers a callback that runs only after a successful commit. Useful for the outbox pattern (publish events only after the DB write is confirmed).
- **Panic recovery** — the middleware recovers panics, rolls back the transaction, runs `OnRollback` callbacks, then re-panics so Fiber's `ErrorHandler` can still handle it.
- **Context propagation** — all public functions have both `*fiber.Ctx` and `context.Context` variants, so repository and service layers stay framework-agnostic.

---

## Installation

For Fiber v2:
```bash
go get github.com/adrielcodeco/go-fiber-gorm/txctx
```

For Fiber v3:
```bash
go get github.com/adrielcodeco/go-fiber-gorm/txctxv3
```

The v3 adapter has the same surface as v2; just swap `txctx` → `txctxv3` and replace `*fiber.Ctx` with `fiber.Ctx` in your handler signatures.

---

## Public API

```go
// Middleware
txctx.Middleware(db *gorm.DB, cfg txctx.Config) fiber.Handler

// Config
type Config struct {
    Timeout         time.Duration   // request deadline (default: 30s)
    LazyTx          *bool           // open tx only on first write (default: true; use txctx.BoolPtr to set)
    CompensationCtx time.Duration   // timeout for OnRollback callbacks (default: 5s)
    OnCallbackError func(error)     // optional sink for errors from OnCommit/OnRollback callbacks and from rollback/commit
}

// DB access
txctx.DB(c *fiber.Ctx) *gorm.DB
txctx.DBFromCtx(ctx context.Context) *gorm.DB

// Outside-tx access
txctx.Outside(c *fiber.Ctx) *gorm.DB
txctx.OutsideCtx(ctx context.Context) *gorm.DB

// Callbacks
txctx.OnRollback(c *fiber.Ctx, fn func(*gorm.DB) error)
txctx.OnRollbackCtx(ctx context.Context, fn func(*gorm.DB) error)
txctx.OnCommit(c *fiber.Ctx, fn func(*gorm.DB) error)
txctx.OnCommitCtx(ctx context.Context, fn func(*gorm.DB) error)
```

---

## Usage

### 1. Setup

Register the middleware once, globally or on a route group:

```go
app.Use(txctx.Middleware(db, txctx.Config{
    Timeout:         5 * time.Second,
    LazyTx:          txctx.BoolPtr(true),
    CompensationCtx: 3 * time.Second,
}))
```

- `Timeout` — maximum duration allowed for a single request before the context is cancelled and any open transaction is rolled back.
- `LazyTx` — when `true`, `BEGIN` is deferred until the first write operation. Read-only requests skip transactions entirely.
- `CompensationCtx` — timeout granted to `OnRollback` callbacks. Because the original request context is already cancelled at rollback time, each callback receives a fresh context with this duration.

---

### 2. Read-only handler

When `LazyTx` is `true` and no write happens, `DB(c)` returns a plain `*gorm.DB` without ever opening a transaction.

```go
func getUser(c *fiber.Ctx) error {
    var u User
    if err := txctx.DB(c).First(&u, c.Params("id")).Error; err != nil {
        return err
    }
    return c.JSON(u)
}
```

---

### 3. Simple write (lazy tx)

The first call to a write operation (`Create`, `Save`, `Update`, `Delete`) transparently triggers `BEGIN`.

```go
func createUser(c *fiber.Ctx) error {
    var u User
    c.BodyParser(&u)
    // First write: middleware transparently opens BEGIN here
    if err := txctx.DB(c).Create(&u).Error; err != nil {
        return err
    }
    return c.JSON(u) // handler returns nil → COMMIT
}
```

---

### 4. Multiple writes in the same transaction

All calls to `DB(c)` within the same request share the same underlying transaction.

```go
func createOrder(c *fiber.Ctx) error {
    db := txctx.DB(c)
    user := User{Email: "a@b.com"}
    db.Create(&user)                                        // opens tx
    db.Create(&Order{UserID: user.ID, Total: 100})          // same tx
    db.Model(&user).Update("Name", "updated")               // same tx
    return c.JSON(user)                                     // COMMIT — all three writes atomic
}
```

---

### 5. `Outside` — write that survives rollback

`Outside(c)` returns a `*gorm.DB` backed by `context.Background()`, completely independent of the request transaction. Writes via `Outside` are committed immediately and are not affected by a subsequent rollback of the main transaction.

```go
func signupWithAudit(c *fiber.Ctx) error {
    var u User
    c.BodyParser(&u)

    // Persists regardless of what happens to the main tx
    txctx.Outside(c).Create(&AuditLog{Action: "signup_attempt", Payload: u.Email})

    if err := txctx.DB(c).Create(&u).Error; err != nil {
        return err // rollback of User, but AuditLog stays
    }
    return c.JSON(u)
}
```

---

### 6. `OnRollback` — compensating transaction

`OnRollback` registers a function that runs only if the transaction is rolled back (due to a handler error, timeout, or panic). The callback receives a `*gorm.DB` with a fresh context whose deadline is `CompensationCtx`.

```go
func paymentHandler(c *fiber.Ctx) error {
    var u User
    c.BodyParser(&u)
    txctx.DB(c).Create(&u)

    txctx.OnRollback(c, func(bg *gorm.DB) error {
        return bg.Create(&FailedSignup{Email: u.Email, Error: "rolled back"}).Error
    })

    if err := chargeExternal(c.UserContext(), u.ID); err != nil {
        return err // triggers rollback → OnRollback callback fires
    }
    return c.JSON(u)
}
```

---

### 7. `OnCommit` — outbox / post-commit event

`OnCommit` registers a function that runs only after a successful commit. This is the recommended pattern for publishing domain events (outbox pattern): the event is only dispatched once the DB write is durably confirmed.

```go
func createOrder(c *fiber.Ctx) error {
    var o Order
    c.BodyParser(&o)
    txctx.DB(c).Create(&o)

    txctx.OnCommit(c, func(bg *gorm.DB) error {
        return publishEvent("order.created", o.ID)
    })
    return c.JSON(o)
}
```

---

### 8. Handler returns error → rollback

Any non-nil error returned by the handler causes the middleware to roll back the active transaction before passing the error to Fiber's error handler.

```go
func manualRollback(c *fiber.Ctx) error {
    var u User
    txctx.DB(c).Create(&u)
    if u.Email == "" {
        return errors.New("email required") // rollback triggered
    }
    return c.JSON(u)
}
```

---

### 9. Panic → rollback + re-panic

The middleware recovers from panics, rolls back the transaction (running any registered `OnRollback` callbacks), and then re-panics so that Fiber's `ErrorHandler` or `RecoverHandler` can process it normally.

```go
func panicHandler(c *fiber.Ctx) error {
    txctx.DB(c).Create(&User{Email: "boom"})
    txctx.OnRollback(c, func(bg *gorm.DB) error {
        return bg.Create(&AuditLog{Action: "panicked"}).Error
    })
    panic("something went very wrong") // middleware: recover → rollback → re-panic
}
```

---

### 10. Layered architecture

The `*Ctx` variants (`DBFromCtx`, `OutsideCtx`, `OnRollbackCtx`, `OnCommitCtx`) accept a `context.Context` instead of a `*fiber.Ctx`. This allows repository and service layers to remain completely framework-agnostic while still participating in the request-scoped transaction.

```go
// handler — Fiber layer
func createUserHandler(c *fiber.Ctx) error {
    var u User
    c.BodyParser(&u)
    if err := userService.Create(c.UserContext(), &u); err != nil {
        return err
    }
    return c.JSON(u)
}

// service — no Fiber dependency
func (s *UserService) Create(ctx context.Context, u *User) error {
    if err := s.repo.Insert(ctx, u); err != nil {
        return err
    }
    txctx.OnCommitCtx(ctx, func(db *gorm.DB) error {
        return s.events.Publish("user.created", u.ID)
    })
    return nil
}

// repository — no Fiber dependency
func (r *UserRepository) Insert(ctx context.Context, u *User) error {
    return txctx.DBFromCtx(ctx).Create(u).Error
}
```

---

## Commit / Rollback Decision Table

| Situation | Result |
|---|---|
| Handler returns `nil` | COMMIT → `OnCommit` callbacks run |
| Handler returns `error` | ROLLBACK → `OnRollback` callbacks run |
| Request context timeout | ROLLBACK → `OnRollback` callbacks run |
| Panic in handler | ROLLBACK → `OnRollback` callbacks run → re-panic |
| `tx.Commit()` itself fails | ROLLBACK → `OnRollback` callbacks run → commit error returned |
| `OnCommit` callback fails (after successful commit) | Tx stays committed; error surfaced via `OnCallbackError` + returned to Fiber. `OnRollback` does **not** fire. |
| Write via `Outside` | Always persists, independent of tx. Context cancellation is decoupled but values (request-id, tracing) are preserved. |

### Concurrency notes

The request-scoped `*gorm.DB` is safe for sequential use within the handler.
If you spawn goroutines from the handler, do **not** use `DB(c)` from them
after the handler returns — the middleware will commit/rollback as soon as
the handler returns, and the underlying `*sql.Tx` becomes invalid. Use
`Outside(c)` for fire-and-forget work, or wait for the goroutine before
returning from the handler.

---

## Propagating cancellation to outbound calls

The middleware wraps `c.UserContext()` with the configured `Timeout`. **Any
outbound call (HTTP, gRPC, Redis, message broker, etc.) that receives this
context will be cancelled automatically when the request times out, errors,
or the client disconnects** — Go's standard libraries already implement this:
`net/http` aborts the in-flight TCP request, `database/sql` interrupts the
query, gRPC closes the stream, and so on.

For this to work you must **thread the context through every outbound call**.
The package can't do this for you — it would require wrapping every client
type in the ecosystem. The discipline is:

```go
func chargeExternal(c *fiber.Ctx, userID uint) error {
    // ✅ Pass the request context — cancels on Fiber timeout/error/panic.
    req, err := http.NewRequestWithContext(c.UserContext(),
        http.MethodPost, "https://payments.example/charge", body)
    if err != nil {
        return err
    }
    resp, err := http.DefaultClient.Do(req)
    // ...
}

func chargeExternalBAD(userID uint) error {
    // ❌ No context: the call will keep running after the request times out,
    //    burning a goroutine and a connection until the remote replies.
    resp, err := http.Post("https://payments.example/charge", "...", body)
    // ...
}
```

The same applies to gRPC (`grpc.Invoke(ctx, ...)`), Redis
(`rdb.Get(ctx, ...)`), AWS SDK v2 (`client.GetItem(ctx, ...)`), and any other
client that accepts a `context.Context` as its first argument.

**Service / repository layers:** use the `*Ctx` variants
(`DBFromCtx`, `OutsideCtx`, `OnRollbackCtx`, `OnCommitCtx`) so the same
`context.Context` flows through the whole call chain — DB, HTTP, gRPC, queue
publishes, etc. — and a single cancellation point unwinds everything.

**When you need to escape cancellation** (e.g. publishing a "request-failed"
event to a queue from `OnRollback` callbacks), `Outside(c)` already gives you
a context decoupled from the request cancellation while preserving values
like request-id and trace headers — use the same pattern for outbound HTTP
in that scenario:

```go
txctx.OnRollback(c, func(_ *gorm.DB) error {
    // Need a fresh ctx because c.UserContext() is already cancelled here.
    ctx, cancel := context.WithTimeout(
        context.WithoutCancel(c.UserContext()), 3*time.Second)
    defer cancel()
    req, _ := http.NewRequestWithContext(ctx, http.MethodPost, alertURL, body)
    _, _ = http.DefaultClient.Do(req)
    return nil
})
```
