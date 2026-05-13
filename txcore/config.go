package txcore

import "time"

// Config holds the shared middleware options for both the v2 and v3 Fiber
// adapters. LazyTx is *bool so the zero value can be distinguished from an
// explicit false; when nil, the default is true (lazy mode).
type Config struct {
	Timeout         time.Duration
	LazyTx          *bool
	CompensationCtx time.Duration
	OnCallbackError func(error)
}

// BoolPtr is a helper for setting Config.LazyTx inline.
func BoolPtr(v bool) *bool { return &v }

// WithDefaults returns cfg with zero-valued fields filled in.
func (cfg Config) WithDefaults() Config {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.CompensationCtx == 0 {
		cfg.CompensationCtx = 5 * time.Second
	}
	if cfg.LazyTx == nil {
		cfg.LazyTx = BoolPtr(true)
	}
	return cfg
}
