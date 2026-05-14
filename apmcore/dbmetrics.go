package apmcore

import (
	"context"
	"database/sql"

	apm "go.elastic.co/apm/v2"
)

// RegisterDBPoolMetrics registers an apm.MetricsGatherer that publishes
// *sql.DB pool statistics on the agent's metrics tick (default 30s,
// configurable via ELASTIC_APM_METRICS_INTERVAL).
//
// Emitted metrics:
//
//	db.pool.max_open
//	db.pool.open
//	db.pool.in_use
//	db.pool.idle
//	db.pool.wait_count
//	db.pool.wait_duration_ms
//	db.pool.max_idle_closed
//	db.pool.max_idle_time_closed
//	db.pool.max_lifetime_closed
//
// They land in the metrics-apm.app.<service>-default data stream and are
// chartable from Kibana → Observability → Infrastructure → Metrics Explorer.
//
// The returned function deregisters the gatherer; call it when the pool
// is closed (e.g. from a graceful-shutdown PhasePostDB hook).
func RegisterDBPoolMetrics(db *sql.DB) func() {
	return apm.DefaultTracer().RegisterMetricsGatherer(&poolGatherer{db: db})
}

type poolGatherer struct{ db *sql.DB }

func (g *poolGatherer) GatherMetrics(_ context.Context, m *apm.Metrics) error {
	if g.db == nil {
		return nil
	}
	s := g.db.Stats()
	m.Add("db.pool.max_open", nil, float64(s.MaxOpenConnections))
	m.Add("db.pool.open", nil, float64(s.OpenConnections))
	m.Add("db.pool.in_use", nil, float64(s.InUse))
	m.Add("db.pool.idle", nil, float64(s.Idle))
	m.Add("db.pool.wait_count", nil, float64(s.WaitCount))
	m.Add("db.pool.wait_duration_ms", nil, float64(s.WaitDuration.Milliseconds()))
	m.Add("db.pool.max_idle_closed", nil, float64(s.MaxIdleClosed))
	m.Add("db.pool.max_idle_time_closed", nil, float64(s.MaxIdleTimeClosed))
	m.Add("db.pool.max_lifetime_closed", nil, float64(s.MaxLifetimeClosed))
	return nil
}
