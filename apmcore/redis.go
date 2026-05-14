package apmcore

import (
	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	redis "github.com/redis/go-redis/v9"
)

// InstrumentRedis attaches the redisotel tracing + metrics hooks to client.
// Spans/metrics flow through the OTel global providers configured by
// SetupOTelSDK and end up in the APM agent's transport.
//
// Pass any redis.UniversalClient (Client, ClusterClient, Ring, etc.).
func InstrumentRedis(client redis.UniversalClient) error {
	if err := redisotel.InstrumentTracing(client); err != nil {
		return err
	}
	return redisotel.InstrumentMetrics(client)
}
