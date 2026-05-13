# go-fiber-gorm

A toolbox for production [Fiber](https://github.com/gofiber/fiber) + [GORM](https://gorm.io) services. It ships two complementary primitives that share the same design philosophy (framework-agnostic core + thin Fiber adapters for v2 and v3):

1. **`txctx` / `txctxv3`** — request-scoped database transactions with lazy `BEGIN`, automatic rollback on error/timeout/panic, and commit/rollback callbacks.
2. **`gsfiber` / `gsfiberv3`** — Kubernetes-aware graceful shutdown for Fiber + GORM + outbound calls, with ordered phases, hooks, readiness probe, and force-kill ceiling.

**Module:** `github.com/adrielcodeco/go-fiber-gorm`

| Feature | Fiber v2 | Fiber v3 | Go min |
|---|---|---|---|
| Request-scoped transactions | `…/txctx` | `…/txctxv3` | 1.22 / 1.25 |
| Graceful shutdown | `…/gsfiber` | `…/gsfiberv3` | 1.22 / 1.25 |

Each pair shares a framework-agnostic engine (`txcore`, `gscore`) so both Fiber versions have identical semantics.

---

## Table of Contents

- [Transactions (`txctx` / `txctxv3`)](#transactions-txctx--txctxv3)
  - [Features](#features)
  - [Installation](#installation)
  - [Public API](#public-api)
  - [Usage](#usage)
  - [Commit / Rollback Decision Table](#commit--rollback-decision-table)
  - [Propagating cancellation to outbound calls](#propagating-cancellation-to-outbound-calls)
- [Graceful Shutdown (`gsfiber` / `gsfiberv3`)](#graceful-shutdown-gsfiber--gsfiberv3)
  - [Features](#features-1)
  - [Installation](#installation-1)
  - [Public API](#public-api-1)
  - [Phases](#phases)
  - [Usage](#usage-1)
  - [Kubernetes integration](#kubernetes-integration)

---

## Transactions (`txctx` / `txctxv3`)

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

---

## Graceful Shutdown (`gsfiber` / `gsfiberv3`)

A coordinator for the full shutdown sequence of a Fiber + GORM service:
**drain in-flight HTTP requests**, **cancel outbound calls**, **flush
application state**, **close the database pool**, all bounded by per-phase
and global timeouts. Designed around the Kubernetes pod lifecycle.

### Features

- **Phased sequence** — `PreStop → Drain → PostDrain → DB → PostDB`, so each
  resource is cleaned up at the right moment (e.g. flush outbox *before*
  closing the DB; close Redis *after*).
- **Ordered hooks** — each phase runs registered hooks sorted by `Priority`;
  a failing hook is logged but does not stop the sequence.
- **`RootContext()`** — a `context.Context` that is cancelled the moment
  shutdown begins. Derive outbound HTTP/gRPC/queue calls from it and they
  abort cleanly on SIGTERM.
- **Readiness flip** — `IsReady()` (and the provided `ReadinessHandler`)
  returns `200` while serving and `503` once shutdown begins, so kube-proxy
  can remove the pod from service endpoints before any request is dropped.
- **Configurable timeouts** — independent `PreStopDelay`, `DrainTimeout`,
  `HookTimeout`, `DBCloseTimeout`, plus a global `ForceKillAfter` that
  `os.Exit(1)`s if the whole sequence overshoots
  `terminationGracePeriodSeconds`.
- **Configurable signals** — defaults to `SIGINT` + `SIGTERM`, override via
  `Config.Signals`.
- **Structured logging** — every phase logs begin/end with duration; plug
  any logger that implements the 3-method `Logger` interface.
- **GORM-aware** — closes the underlying `*sql.DB` of each registered
  `*gorm.DB` with a deadline (avoids hanging on a stuck pool).
- **Concurrent drain** — multiple `*fiber.App` instances (or anything
  implementing `Shutdowner`) are drained in parallel under a shared
  deadline.

### Installation

For Fiber v2:
```bash
go get github.com/adrielcodeco/go-fiber-gorm/gsfiber
```

For Fiber v3:
```bash
go get github.com/adrielcodeco/go-fiber-gorm/gsfiberv3
```

The two adapters share an engine (`gscore`); the public surface is
identical apart from `*fiber.App` vs `fiber.App` and `*fiber.Ctx` vs
`fiber.Ctx` in the readiness handler.

### Public API

```go
// Manager
gsfiber.New(cfg gsfiber.Config) *gsfiber.Manager

// Registration
gsfiber.RegisterApp(m *Manager, app *fiber.App)    // one or more
mgr.RegisterDB(db *gorm.DB)                        // one or more
mgr.AddHook(gsfiber.Hook{Name, Phase, Priority, Run})

// Lifecycle
mgr.RootContext() context.Context                  // cancelled on shutdown
mgr.IsReady() bool                                 // false once shutdown began
mgr.Trigger()                                      // start sequence programmatically
mgr.ListenAndWait() error                          // block on signals + run
mgr.Wait() error                                   // block until sequence done

// Readiness probe
gsfiber.ReadinessHandler(mgr) fiber.Handler

// Phases (re-exported on the adapter package)
gsfiber.PhasePreStop
gsfiber.PhaseDrain
gsfiber.PhasePostDrain
gsfiber.PhaseDB
gsfiber.PhasePostDB

// Config
type Config struct {
    Signals        []os.Signal     // default: SIGINT, SIGTERM
    PreStopDelay   time.Duration   // wait before any phase runs (default: 0)
    DrainTimeout   time.Duration   // bound on HTTP drain (default: 25s)
    HookTimeout    time.Duration   // bound per phase (default: 10s)
    DBCloseTimeout time.Duration   // bound on each gorm.DB close (default: 5s)
    ForceKillAfter time.Duration   // global ceiling, os.Exit(1) (default: 60s)
    Logger         gscore.Logger   // structured logger; nil = silent
    OnHookError    func(name string, phase gscore.Phase, err error)
}
```

### Phases

| Phase | Purpose |
|---|---|
| `PhasePreStop` | Runs first, while the server is still serving. Use for actions that need the HTTP layer alive (signal in-flight workers, flush in-memory queue). |
| `PhaseDrain` | Drains all registered Fiber apps concurrently with `DrainTimeout`. |
| `PhasePostDrain` | Runs after HTTP is fully drained, before DB close. Best place for outbound-call cleanups, worker pool waits, etc. |
| `PhaseDB` | Closes each registered `*gorm.DB`'s underlying `*sql.DB` with `DBCloseTimeout`. |
| `PhasePostDB` | Last phase. Use for resources that do not depend on the DB: Kafka producers, log flushers, metric exporters. |

### Usage

#### 1. Minimum setup

```go
func main() {
    db := openGORM()
    app := fiber.New()
    app.Use(txctx.Middleware(db, txctx.Config{Timeout: 5 * time.Second}))
    registerRoutes(app)

    mgr := gsfiber.New(gsfiber.Config{
        PreStopDelay:   5 * time.Second,  // give kube-proxy time to drop the endpoint
        DrainTimeout:   25 * time.Second,
        DBCloseTimeout: 5 * time.Second,
        ForceKillAfter: 55 * time.Second, // < terminationGracePeriodSeconds
    })
    gsfiber.RegisterApp(mgr, app)
    mgr.RegisterDB(db)

    // Readiness probe flips to 503 the instant SIGTERM arrives.
    app.Get("/healthz/ready", gsfiber.ReadinessHandler(mgr))

    go func() {
        if err := app.Listen(":8080"); err != nil {
            mgr.Trigger() // server failed → start shutdown
        }
    }()

    if err := mgr.ListenAndWait(); err != nil {
        log.Fatal(err)
    }
}
```

#### 2. Cancel outbound calls on shutdown

Derive any long-running outbound call from `mgr.RootContext()`. It is
cancelled the moment SIGTERM is observed, so the call aborts cleanly
during the drain phase.

```go
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-mgr.RootContext().Done():
            return
        case <-ticker.C:
            req, _ := http.NewRequestWithContext(mgr.RootContext(),
                http.MethodGet, "https://api.example/poll", nil)
            _, _ = http.DefaultClient.Do(req)
        }
    }
}()
```

For per-request outbound calls inside a handler, keep using
`c.UserContext()` — `txctx` already wires its cancellation.

#### 3. Ordered hooks across phases

```go
mgr.AddHook(gsfiber.Hook{
    Name:     "outbox-flush",
    Phase:    gsfiber.PhasePreStop, // before we stop accepting requests
    Priority: 0,
    Run: func(ctx context.Context) error {
        return outbox.FlushAll(ctx)
    },
})

mgr.AddHook(gsfiber.Hook{
    Name:     "kafka-close",
    Phase:    gsfiber.PhasePostDB,  // after DB is closed
    Priority: 10,
    Run: func(ctx context.Context) error {
        return kafkaProducer.Close()
    },
})

mgr.AddHook(gsfiber.Hook{
    Name:     "redis-close",
    Phase:    gsfiber.PhasePostDB,
    Priority: 0, // runs before kafka-close (lower priority first)
    Run: func(ctx context.Context) error {
        return redisClient.Close()
    },
})
```

Lower `Priority` runs first within the same phase; equal priorities run in
registration order.

#### 4. Custom logger

Any type that satisfies the three-method `gscore.Logger` interface works
(slog, zap, zerolog, logrus, etc.).

```go
type slogAdapter struct{ l *slog.Logger }

func (s slogAdapter) Info(msg string, kv ...any)  { s.l.Info(msg, kv...) }
func (s slogAdapter) Warn(msg string, kv ...any)  { s.l.Warn(msg, kv...) }
func (s slogAdapter) Error(msg string, kv ...any) { s.l.Error(msg, kv...) }

mgr := gsfiber.New(gsfiber.Config{
    Logger: slogAdapter{l: slog.Default()},
})
```

#### 5. Triggering shutdown programmatically

`mgr.Trigger()` starts the sequence from anywhere — useful for fatal
errors caught outside the HTTP layer (e.g. a background worker losing a
critical connection).

```go
if err := kafkaConsumer.Run(mgr.RootContext()); err != nil && !errors.Is(err, context.Canceled) {
    log.Printf("consumer fatal: %v", err)
    mgr.Trigger()
}
```

`Trigger` is idempotent — the sequence runs exactly once regardless of
how many times it is called or whether a signal also arrives.

### Kubernetes integration

A typical deployment lines up cleanly with the Manager's phases:

```yaml
spec:
  terminationGracePeriodSeconds: 60   # > ForceKillAfter (55s in example above)
  containers:
  - name: api
    readinessProbe:
      httpGet:
        path: /healthz/ready          # gsfiber.ReadinessHandler
        port: 8080
      periodSeconds: 2
      failureThreshold: 1
    lifecycle:
      preStop:
        exec:
          # Optional: belt-and-suspenders if PreStopDelay isn't enough.
          # The Manager already handles SIGTERM directly.
          command: ["sleep", "5"]
```

The sequence on `kubectl delete pod`:

1. Kubernetes sends `SIGTERM` and starts the `preStop` hook (in parallel).
2. The Manager observes the signal → flips readiness to `503` → starts
   `PreStopDelay`.
3. kube-proxy sees the failing readiness probe and removes the pod from
   service endpoints → no new requests arrive.
4. `PreStopDelay` elapses → hooks run → HTTP drain → DB close → post-DB
   hooks.
5. Process exits cleanly, well before
   `terminationGracePeriodSeconds`.

Keep `ForceKillAfter` strictly **less than** `terminationGracePeriodSeconds`
so the Manager's own ceiling fires first, with logs you can read, instead
of an abrupt `SIGKILL` from the kubelet.
