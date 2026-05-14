package apmcore_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/adrielcodeco/go-fiber-gorm/apmcore"
)

func TestSetupAndShutdown(t *testing.T) {
	shutdown, err := apmcore.SetupOTelSDK(context.Background())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestWrapHTTPTransport_NilBaseUsesDefault(t *testing.T) {
	rt := apmcore.WrapHTTPTransport(nil)
	if rt == nil {
		t.Fatal("expected non-nil round tripper")
	}
	// Ensure the wrapper preserves the http.RoundTripper contract.
	var _ http.RoundTripper = rt
}

func TestSetLabel_NoTransactionIsNoop(t *testing.T) {
	// Just ensures no panic when ctx has no APM transaction.
	apmcore.SetLabel(context.Background(), "wallet_id", "abc")
	apmcore.SetLabels(context.Background(), map[string]string{"k": "v"})
	apmcore.CaptureError(context.Background(), nil)
	apmcore.CaptureError(nil, nil)
}

func TestLogCtxFields_NilCtx(t *testing.T) {
	if f := apmcore.LogCtxFields(nil); f != nil {
		t.Fatalf("expected nil fields, got %v", f)
	}
}
