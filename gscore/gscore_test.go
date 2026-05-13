package gscore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var testDBCounter atomic.Int64

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	id := testDBCounter.Add(1)
	dsn := fmt.Sprintf("file:gscore%d?mode=memory&cache=shared", id)
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
	return db
}

type fakeServer struct {
	called   atomic.Int32
	blockFor time.Duration
	err      error
}

func (s *fakeServer) ShutdownWithContext(ctx context.Context) error {
	s.called.Add(1)
	if s.blockFor > 0 {
		select {
		case <-time.After(s.blockFor):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.err
}

func newTestManager(cfg Config) *Manager {
	m := New(cfg)
	m.exitFn = func(int) {} // never actually exit in tests
	return m
}

func TestTriggerCancelsRootContext(t *testing.T) {
	m := newTestManager(Config{})
	ctx := m.RootContext()
	if ctx.Err() != nil {
		t.Fatalf("root ctx cancelled before trigger: %v", ctx.Err())
	}
	m.Trigger()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("root ctx not cancelled after Trigger")
	}
	if err := m.Wait(); err != nil {
		t.Fatalf("unexpected wait err: %v", err)
	}
}

func TestReadinessFlipsOnTrigger(t *testing.T) {
	m := newTestManager(Config{})
	if !m.IsReady() {
		t.Fatal("expected ready at construction")
	}
	m.Trigger()
	_ = m.Wait()
	if m.IsReady() {
		t.Fatal("expected not ready after trigger")
	}
}

func TestHooksRunInPhaseAndPriorityOrder(t *testing.T) {
	m := newTestManager(Config{HookTimeout: time.Second})

	var mu sync.Mutex
	var order []string
	add := func(name string, phase Phase, prio int) {
		m.AddHook(Hook{
			Name:     name,
			Phase:    phase,
			Priority: prio,
			Run: func(ctx context.Context) error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				return nil
			},
		})
	}
	// Intentionally out of order.
	add("post-db-a", PhasePostDB, 0)
	add("pre-stop-b", PhasePreStop, 10)
	add("pre-stop-a", PhasePreStop, 0)
	add("post-drain-a", PhasePostDrain, 0)

	m.Trigger()
	if err := m.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}
	want := []string{"pre-stop-a", "pre-stop-b", "post-drain-a", "post-db-a"}
	if len(order) != len(want) {
		t.Fatalf("order len: got %v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d]: got %s want %s (full %v)", i, order[i], want[i], order)
		}
	}
}

func TestHookErrorContinuesAndReports(t *testing.T) {
	var reported []string
	m := newTestManager(Config{
		HookTimeout: time.Second,
		OnHookError: func(name string, _ Phase, _ error) {
			reported = append(reported, name)
		},
	})
	m.AddHook(Hook{Name: "bad", Phase: PhasePreStop, Run: func(ctx context.Context) error {
		return errors.New("boom")
	}})
	ran := false
	m.AddHook(Hook{Name: "good", Phase: PhasePreStop, Priority: 1, Run: func(ctx context.Context) error {
		ran = true
		return nil
	}})
	m.Trigger()
	_ = m.Wait()
	if !ran {
		t.Fatal("second hook did not run after first failed")
	}
	if len(reported) != 1 || reported[0] != "bad" {
		t.Fatalf("OnHookError reports: %v", reported)
	}
}

func TestDrainRunsConcurrently(t *testing.T) {
	m := newTestManager(Config{DrainTimeout: 2 * time.Second})
	a := &fakeServer{blockFor: 200 * time.Millisecond}
	b := &fakeServer{blockFor: 200 * time.Millisecond}
	m.RegisterServer(a)
	m.RegisterServer(b)

	start := time.Now()
	m.Trigger()
	_ = m.Wait()
	elapsed := time.Since(start)

	if a.called.Load() != 1 || b.called.Load() != 1 {
		t.Fatalf("both servers must be drained: a=%d b=%d", a.called.Load(), b.called.Load())
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("drain ran sequentially (elapsed=%s)", elapsed)
	}
}

func TestDrainRespectsTimeout(t *testing.T) {
	m := newTestManager(Config{DrainTimeout: 100 * time.Millisecond})
	slow := &fakeServer{blockFor: time.Second}
	m.RegisterServer(slow)

	start := time.Now()
	m.Trigger()
	_ = m.Wait()
	elapsed := time.Since(start)
	if elapsed > 400*time.Millisecond {
		t.Fatalf("drain did not honor timeout (elapsed=%s)", elapsed)
	}
}

func TestPreStopDelay(t *testing.T) {
	m := newTestManager(Config{PreStopDelay: 150 * time.Millisecond})
	start := time.Now()
	m.Trigger()
	_ = m.Wait()
	if elapsed := time.Since(start); elapsed < 140*time.Millisecond {
		t.Fatalf("pre-stop delay not honored (elapsed=%s)", elapsed)
	}
}

func TestTriggerIsIdempotent(t *testing.T) {
	m := newTestManager(Config{})
	calls := atomic.Int32{}
	m.AddHook(Hook{Name: "once", Phase: PhasePreStop, Run: func(ctx context.Context) error {
		calls.Add(1)
		return nil
	}})
	m.Trigger()
	m.Trigger()
	m.Trigger()
	_ = m.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("hook ran %d times, want 1", got)
	}
}

func TestDBCloseClosesUnderlyingPool(t *testing.T) {
	db := openTestDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("DB(): %v", err)
	}

	m := newTestManager(Config{DBCloseTimeout: 2 * time.Second})
	m.RegisterDB(db)
	m.Trigger()
	if err := m.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}

	// After close, Ping must fail — that's how we know the pool is gone.
	if err := sqlDB.Ping(); err == nil {
		t.Fatal("expected Ping to fail after pool close")
	}
}

func TestCloseWithTimeoutFires(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	err := closeWithTimeout(func() error {
		<-block
		return nil
	}, 50*time.Millisecond)
	if err == nil || err.Error() != "db close timed out" {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestCloseWithTimeoutPropagatesError(t *testing.T) {
	want := errors.New("boom")
	if got := closeWithTimeout(func() error { return want }, time.Second); !errors.Is(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestListenAndWaitRunsOnSignal(t *testing.T) {
	m := newTestManager(Config{
		Signals:        []os.Signal{syscall.SIGUSR1},
		HookTimeout:    time.Second,
		ForceKillAfter: 5 * time.Second,
	})
	ran := atomic.Bool{}
	m.AddHook(Hook{Name: "h", Phase: PhasePreStop, Run: func(ctx context.Context) error {
		ran.Store(true)
		return nil
	}})

	done := make(chan error, 1)
	go func() { done <- m.ListenAndWait() }()

	// Give ListenAndWait a moment to install its signal handler.
	time.Sleep(50 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ListenAndWait: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ListenAndWait did not return after signal")
	}
	if !ran.Load() {
		t.Fatal("hook did not run via signal-driven shutdown")
	}
}

func TestListenAndWaitReturnsIfAlreadyTriggered(t *testing.T) {
	m := newTestManager(Config{Signals: []os.Signal{syscall.SIGUSR2}})
	m.Trigger()
	// run() finishes quickly with no servers/hooks; ListenAndWait should
	// then fall through to Wait and return promptly.
	done := make(chan error, 1)
	go func() { done <- m.ListenAndWait() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ListenAndWait did not return for already-triggered manager")
	}
}

func TestSetReady(t *testing.T) {
	m := newTestManager(Config{})
	if !m.IsReady() {
		t.Fatal("expected ready at construction")
	}
	m.SetReady(false)
	if m.IsReady() {
		t.Fatal("SetReady(false) had no effect")
	}
	m.SetReady(true)
	if !m.IsReady() {
		t.Fatal("SetReady(true) had no effect")
	}
}

func TestAddHookNilRunIsNoOp(t *testing.T) {
	m := newTestManager(Config{})
	m.AddHook(Hook{Name: "nil", Phase: PhasePreStop, Run: nil})
	if got := len(m.phaseHooks(PhasePreStop)); got != 0 {
		t.Fatalf("nil-run hook was registered: got %d hooks", got)
	}
}

func TestNopLoggerSilent(t *testing.T) {
	// Just exercises the no-op logger methods used by default.
	l := nopLogger{}
	l.Info("x", "k", "v")
	l.Warn("x")
	l.Error("x")
}

func TestPhaseString(t *testing.T) {
	cases := map[Phase]string{
		PhasePreStop:   "pre-stop",
		PhaseDrain:     "drain",
		PhasePostDrain: "post-drain",
		PhaseDB:        "db",
		PhasePostDB:    "post-db",
		Phase(99):      "phase(99)",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("Phase(%d).String() = %q want %q", int(p), got, want)
		}
	}
}

func TestForceKillCeiling(t *testing.T) {
	var killed atomic.Bool
	m := New(Config{
		ForceKillAfter: 100 * time.Millisecond,
		HookTimeout:    time.Second,
	})
	m.exitFn = func(int) { killed.Store(true) }
	m.AddHook(Hook{Name: "slow", Phase: PhasePreStop, Run: func(ctx context.Context) error {
		// Sleep longer than ForceKillAfter to ensure the timer fires.
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil
	}})
	m.Trigger()
	_ = m.Wait()
	if !killed.Load() {
		t.Fatal("expected force-kill to fire")
	}
}
