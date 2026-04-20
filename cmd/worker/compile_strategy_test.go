package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/cpclient"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
)

// v1StrategyJSON is the minimal v1 document dispatch.Parse accepts. Kept
// identical in spirit to dslcompile.minimalV1 so that a CP response carrying
// this payload drives compileStrategy down the v1 branch.
const v1StrategyJSON = `{
  "schema_version": "1.0.0",
  "strategy_code": "compile_strategy_v1",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "rsi_14_lt_30000" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": false }
}`

// newCPServer wires a test control-plane that serves exactly one strategy
// version record. Helper intentionally mirrors cpclient/client_test.go's style
// so future fixtures can be shared without coupling back to the real CP.
func newCPServer(t *testing.T, id string, dsl json.RawMessage) *cpclient.Client {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/strategy-versions/"+id, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method %s", r.Method)
		}
		_ = json.NewEncoder(w).Encode(cpclient.StrategyVersion{
			ID:                 id,
			StrategyTemplateID: "tpl-1",
			Version:            1,
			DSLJSON:            dsl,
			CreatedAt:          time.Now().UTC(),
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return cpclient.New(srv.URL)
}

func TestCompileStrategy_V1_HappyPath(t *testing.T) {
	id := "sv-1"
	cli := newCPServer(t, id, json.RawMessage(v1StrategyJSON))
	plan, err := compileStrategy(context.Background(), cli, id)
	if err != nil {
		t.Fatalf("compileStrategy: %v", err)
	}
	if plan.Major != dslcompile.MajorV1 {
		t.Fatalf("Major=%v, want v1", plan.Major)
	}
	if plan.StrategyCode != "compile_strategy_v1" {
		t.Fatalf("StrategyCode=%q", plan.StrategyCode)
	}
	if len(plan.RequiredColumns) == 0 {
		t.Fatalf("v1 RequiredColumns must be populated for runtime executor")
	}
	if plan.V1 == nil {
		t.Fatal("expected typed V1 plan")
	}
}

func TestCompileStrategy_EmptyDSLJSON_Rejected(t *testing.T) {
	id := "sv-2"
	cli := newCPServer(t, id, json.RawMessage(nil))
	_, err := compileStrategy(context.Background(), cli, id)
	if err == nil {
		t.Fatal("expected error for empty DSLJSON")
	}
}

func TestCompileStrategy_CPNotFound_WrapsAPIError(t *testing.T) {
	// No handler registered → default 404.
	srv := httptest.NewServer(http.NewServeMux())
	t.Cleanup(srv.Close)
	cli := cpclient.New(srv.URL)

	_, err := compileStrategy(context.Background(), cli, "missing-id")
	if err == nil {
		t.Fatal("expected error for missing strategy_version")
	}
	if !errors.Is(err, cpclient.ErrNotFound) {
		t.Fatalf("expected wrapped cpclient.ErrNotFound, got %v", err)
	}
}

func TestCompileStrategy_InvalidDSL_ReturnsParseError(t *testing.T) {
	id := "sv-3"
	cli := newCPServer(t, id, json.RawMessage(`{"schema_version":"2.0.0"}`))
	_, err := compileStrategy(context.Background(), cli, id)
	if err == nil {
		t.Fatal("expected ParseError")
	}
	var pe *dslcompile.ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *dslcompile.ParseError, got %T: %v", err, err)
	}
}
