// Package gscore is the framework-agnostic engine behind the gsfiber (Fiber v2)
// and gsfiberv3 (Fiber v3) graceful-shutdown adapters. It orchestrates the
// shutdown lifecycle: signal capture, pre-stop delay, readiness flip, HTTP
// drain, ordered hooks, GORM pool close, and a force-kill ceiling.
//
// The public adapter packages are thin wrappers that translate a framework's
// Shutdown method into the generic Shutdowner interface accepted here.
package gscore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gorm.io/gorm"
)

// Shutdowner is the minimal contract an HTTP server (or anything similar)
// must satisfy to be drained by the Manager. Both Fiber v2 (*fiber.App) and
// Fiber v3 (*fiber.App) implement Shutdown / ShutdownWithContext directly;
// the adapter packages adapt the exact signature.
type Shutdowner interface {
	// ShutdownWithContext must stop accepting new connections and wait for
	// in-flight requests to finish, returning when ctx is done or all
	// requests drained.
	ShutdownWithContext(ctx context.Context) error
}

// Phase identifies a stage of the shutdown sequence. Hooks are grouped by
// phase; phases run sequentially in the order below.
type Phase int

const (
	// PhasePreStop runs first, before HTTP drain. Use it for actions that
	// must happen while the server is still serving (e.g. flushing an
	// in-memory queue back to a durable store).
	PhasePreStop Phase = iota
	// PhaseDrain runs the HTTP server drain.
	PhaseDrain
	// PhasePostDrain runs after the HTTP server is fully drained but before
	// the database pool is closed. Most outbound-call cleanups belong here.
	PhasePostDrain
	// PhaseDB runs the GORM pool close.
	PhaseDB
	// PhasePostDB runs last, after the database is closed. Use it for
	// resources that do not depend on the DB (Kafka producers, log
	// flushers, metric exporters).
	PhasePostDB
)

func (p Phase) String() string {
	switch p {
	case PhasePreStop:
		return "pre-stop"
	case PhaseDrain:
		return "drain"
	case PhasePostDrain:
		return "post-drain"
	case PhaseDB:
		return "db"
	case PhasePostDB:
		return "post-db"
	default:
		return fmt.Sprintf("phase(%d)", int(p))
	}
}

// Hook is a user-registered shutdown action. Lower Priority runs first
// within the same phase; equal priorities run in registration order.
type Hook struct {
	Name     string
	Priority int
	Phase    Phase
	Run      func(ctx context.Context) error
}

// Logger is the structured-logging surface the Manager calls during the
// shutdown sequence. Implementations should be safe for concurrent use.
type Logger interface {
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// nopLogger is the default when Config.Logger is nil.
type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// Config configures a Manager. All durations are independent; the global
// ForceKillAfter bounds the entire sequence as a hard ceiling.
type Config struct {
	// Signals that trigger Start to return and shutdown to begin. Defaults
	// to SIGINT + SIGTERM when nil.
	Signals []os.Signal

	// PreStopDelay is a sleep injected after a signal is received but
	// before any phase runs. Gives kube-proxy / load balancers time to
	// observe the failing readiness probe and stop routing new traffic.
	// Zero disables the delay.
	PreStopDelay time.Duration

	// Per-phase timeouts. Zero means "use ForceKillAfter as a soft cap"
	// (i.e. no per-phase deadline beyond the global one).
	DrainTimeout     time.Duration
	HookTimeout      time.Duration // applied per phase, not per hook
	DBCloseTimeout   time.Duration

	// ForceKillAfter bounds the whole shutdown sequence. When exceeded,
	// the Manager logs an error and calls os.Exit(1). Zero disables.
	ForceKillAfter time.Duration

	// Logger receives per-phase events. Nil = silent.
	Logger Logger

	// OnHookError is invoked whenever a hook returns a non-nil error.
	// It does not stop the sequence. Nil = errors only go to the logger.
	OnHookError func(name string, phase Phase, err error)
}

// WithDefaults returns cfg with zero-valued fields filled in.
func (cfg Config) WithDefaults() Config {
	if len(cfg.Signals) == 0 {
		cfg.Signals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 25 * time.Second
	}
	if cfg.HookTimeout == 0 {
		cfg.HookTimeout = 10 * time.Second
	}
	if cfg.DBCloseTimeout == 0 {
		cfg.DBCloseTimeout = 5 * time.Second
	}
	if cfg.ForceKillAfter == 0 {
		cfg.ForceKillAfter = 60 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = nopLogger{}
	}
	return cfg
}

// Manager coordinates the shutdown sequence. It is safe to construct once at
// boot and share across goroutines; Trigger and Wait may be called
// concurrently.
type Manager struct {
	cfg Config

	servers []Shutdowner
	dbs     []*gorm.DB
	hooks   []Hook

	rootCtx    context.Context
	rootCancel context.CancelFunc

	ready atomic.Bool

	once     sync.Once
	doneCh   chan struct{}
	exitCode atomic.Int32

	// exitFn is os.Exit by default; tests override it.
	exitFn func(int)
}

// New constructs a Manager. The returned Manager's RootContext is live
// until Trigger fires.
func New(cfg Config) *Manager {
	cfg = cfg.WithDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:        cfg,
		rootCtx:    ctx,
		rootCancel: cancel,
		doneCh:     make(chan struct{}),
		exitFn:     os.Exit,
	}
	m.ready.Store(true)
	return m
}

// RegisterServer adds an HTTP-like server to be drained during PhaseDrain.
// Multiple servers are drained concurrently with a shared deadline.
func (m *Manager) RegisterServer(s Shutdowner) {
	m.servers = append(m.servers, s)
}

// RegisterDB adds a GORM DB whose underlying *sql.DB will be Close()'d
// during PhaseDB. Multiple DBs are closed sequentially in registration
// order.
func (m *Manager) RegisterDB(db *gorm.DB) {
	m.dbs = append(m.dbs, db)
}

// AddHook registers a hook to run during phase h.Phase.
func (m *Manager) AddHook(h Hook) {
	if h.Run == nil {
		return
	}
	if h.Name == "" {
		h.Name = fmt.Sprintf("hook-%d", len(m.hooks))
	}
	m.hooks = append(m.hooks, h)
}

// RootContext returns a context.Context that is cancelled the moment the
// shutdown sequence begins. Outbound HTTP clients, workers, and any
// long-running operation should derive their context from this so they
// observe shutdown via ctx.Done().
func (m *Manager) RootContext() context.Context {
	return m.rootCtx
}

// IsReady reports the readiness flag. It is true at construction and
// flipped to false the moment a shutdown signal is observed. Kubernetes
// readiness probes should reflect this value.
func (m *Manager) IsReady() bool {
	return m.ready.Load()
}

// SetReady forces the readiness flag. Useful for tests or for marking
// "not ready" before the process is even started.
func (m *Manager) SetReady(v bool) {
	m.ready.Store(v)
}

// Trigger initiates the shutdown sequence programmatically (e.g. from a
// health-check failure or an unrecoverable error). Subsequent calls are
// no-ops. It returns immediately; callers should Wait for completion.
func (m *Manager) Trigger() {
	m.once.Do(func() {
		m.ready.Store(false)
		m.rootCancel()
		go m.run()
	})
}

// Wait blocks until the shutdown sequence has finished (or os.Exit was
// invoked by the force-kill ceiling). The returned error is the first
// fatal error encountered, or nil on a clean shutdown.
func (m *Manager) Wait() error {
	<-m.doneCh
	if code := m.exitCode.Load(); code != 0 {
		return fmt.Errorf("shutdown completed with exit code %d", code)
	}
	return nil
}

// ListenAndWait blocks until one of cfg.Signals is received OR Trigger is
// called from elsewhere, then runs the shutdown sequence and returns.
// Typical use is `defer mgr.ListenAndWait()` at the end of main, after
// starting the server in a goroutine.
func (m *Manager) ListenAndWait() error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, m.cfg.Signals...)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		m.cfg.Logger.Info("shutdown signal received", "signal", sig.String())
		m.Trigger()
	case <-m.doneCh:
		// Trigger was called from elsewhere; run() has already started or
		// finished. Fall through to Wait.
	}
	return m.Wait()
}

// run executes the full sequence. Called from a goroutine inside Trigger.
func (m *Manager) run() {
	defer close(m.doneCh)

	if m.cfg.ForceKillAfter > 0 {
		t := time.AfterFunc(m.cfg.ForceKillAfter, func() {
			m.cfg.Logger.Error("force-kill ceiling exceeded", "after", m.cfg.ForceKillAfter)
			m.exitCode.Store(1)
			m.exitFn(1)
		})
		defer t.Stop()
	}

	if d := m.cfg.PreStopDelay; d > 0 {
		m.cfg.Logger.Info("pre-stop delay", "duration", d)
		time.Sleep(d)
	}

	m.runHooks(PhasePreStop)
	m.runDrain()
	m.runHooks(PhasePostDrain)
	m.runDB()
	m.runHooks(PhasePostDB)

	m.cfg.Logger.Info("shutdown complete")
}

func (m *Manager) phaseHooks(p Phase) []Hook {
	var out []Hook
	for _, h := range m.hooks {
		if h.Phase == p {
			out = append(out, h)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Priority < out[j].Priority
	})
	return out
}

func (m *Manager) runHooks(p Phase) {
	hooks := m.phaseHooks(p)
	if len(hooks) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.HookTimeout)
	defer cancel()

	m.cfg.Logger.Info("phase begin", "phase", p.String(), "hooks", len(hooks))
	start := time.Now()
	for _, h := range hooks {
		hStart := time.Now()
		err := h.Run(ctx)
		dur := time.Since(hStart)
		if err != nil {
			m.cfg.Logger.Error("hook failed", "phase", p.String(), "name", h.Name, "duration", dur, "err", err)
			if m.cfg.OnHookError != nil {
				m.cfg.OnHookError(h.Name, p, err)
			}
			continue
		}
		m.cfg.Logger.Info("hook ok", "phase", p.String(), "name", h.Name, "duration", dur)
	}
	m.cfg.Logger.Info("phase end", "phase", p.String(), "duration", time.Since(start))
}

func (m *Manager) runDrain() {
	if len(m.servers) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.DrainTimeout)
	defer cancel()

	m.cfg.Logger.Info("phase begin", "phase", PhaseDrain.String(), "servers", len(m.servers))
	start := time.Now()

	var wg sync.WaitGroup
	errs := make([]error, len(m.servers))
	for i, s := range m.servers {
		wg.Add(1)
		go func(i int, s Shutdowner) {
			defer wg.Done()
			errs[i] = s.ShutdownWithContext(ctx)
		}(i, s)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			m.cfg.Logger.Error("server drain failed", "index", i, "err", err)
		}
	}
	m.cfg.Logger.Info("phase end", "phase", PhaseDrain.String(), "duration", time.Since(start))
}

func (m *Manager) runDB() {
	if len(m.dbs) == 0 {
		return
	}
	m.cfg.Logger.Info("phase begin", "phase", PhaseDB.String(), "dbs", len(m.dbs))
	start := time.Now()

	for i, db := range m.dbs {
		if err := closeDB(db, m.cfg.DBCloseTimeout); err != nil {
			m.cfg.Logger.Error("db close failed", "index", i, "err", err)
		}
	}
	m.cfg.Logger.Info("phase end", "phase", PhaseDB.String(), "duration", time.Since(start))
}

// closeDB closes the underlying *sql.DB with a deadline. GORM does not
// expose a context-aware close, so we run it in a goroutine and bound it
// with timeout to avoid hanging on a stuck pool.
func closeDB(db *gorm.DB, timeout time.Duration) error {
	if db == nil {
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("gorm.DB(): %w", err)
	}
	return closeSQLDB(sqlDB, timeout)
}

func closeSQLDB(sqlDB *sql.DB, timeout time.Duration) error {
	return closeWithTimeout(sqlDB.Close, timeout)
}

// closeWithTimeout runs close() in a goroutine bounded by timeout. Extracted
// so it can be exercised without a real *sql.DB.
func closeWithTimeout(close func() error, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- close() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return errors.New("db close timed out")
	}
}
