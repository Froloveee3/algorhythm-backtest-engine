package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleStrategyRuntimePreflight_V1HappyPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/preflight/strategy-runtime", bytes.NewBufferString(`{
		"dsl_json": {
			"schema_version": "1.0.0",
			"strategy_code": "rt_preflight_v1",
			"instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
			"entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
			"exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
			"filters": [],
			"risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
			"execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": false }
		},
		"feature_set_code": "btcusdt_futures_mvp",
		"feature_set_version": 1
	}`))
	rr := httptest.NewRecorder()

	handleStrategyRuntimePreflight(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var body runtimePreflightResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.RuntimeSupported {
		t.Fatalf("expected runtime_supported=true, got %#v", body)
	}
	if len(body.RequiredColumns) == 0 {
		t.Fatal("expected required columns")
	}
}

func TestHandleStrategyRuntimePreflight_RequiresFeatureSetBinding(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/preflight/strategy-runtime", bytes.NewBufferString(`{
		"dsl_json": {
			"schema_version": "1.0.0",
			"strategy_code": "rt_preflight_v1_no_fs",
			"instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
			"entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
			"exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
			"filters": [],
			"risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
			"execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": false }
		}
	}`))
	rr := httptest.NewRecorder()

	handleStrategyRuntimePreflight(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var body runtimePreflightResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.RuntimeSupported {
		t.Fatalf("expected runtime_supported=false without feature set, got %#v", body)
	}
	if len(body.UnsupportedReasons) == 0 {
		t.Fatal("expected unsupported reasons")
	}
}

func TestHandleStrategyRuntimePreflight_ReportsUnsupportedFeatureSet(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/preflight/strategy-runtime", bytes.NewBufferString(`{
		"dsl_json": {
			"schema_version": "1.0.0",
			"strategy_code": "rt_preflight_v1_unsupported_feature_set",
			"instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
			"entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
			"exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
			"filters": [],
			"risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
			"execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": false }
		},
		"feature_set_code": "unknown_feature_set",
		"feature_set_version": 999
	}`))
	rr := httptest.NewRecorder()

	handleStrategyRuntimePreflight(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var body runtimePreflightResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.RuntimeSupported {
		t.Fatalf("expected runtime_supported=false, got %#v", body)
	}
	if len(body.UnsupportedReasons) == 0 {
		t.Fatal("expected unsupported reasons")
	}
}

func TestHandleStrategyRuntimePreflight_V1CloseSidesSupported(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/preflight/strategy-runtime", bytes.NewBufferString(`{
		"dsl_json": {
			"schema_version": "1.1.0",
			"strategy_code": "rt_preflight_v1_close_sides",
			"instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
			"entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
			"entry_short": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
			"close_long": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
			"close_short": { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
			"exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
			"filters": [],
			"risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
			"execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": true }
		},
		"feature_set_code": "btcusdt_futures_mvp",
		"feature_set_version": 1
	}`))
	rr := httptest.NewRecorder()

	handleStrategyRuntimePreflight(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var body runtimePreflightResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.RuntimeSupported {
		t.Fatalf("expected runtime_supported=true, got %#v", body)
	}
}

func TestHandleStrategyRuntimePreflight_V1SignalOnlySupported(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/preflight/strategy-runtime", bytes.NewBufferString(`{
		"dsl_json": {
			"schema_version": "1.1.0",
			"strategy_code": "rt_preflight_v1_signal_only",
			"instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
			"entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
			"exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
			"filters": [],
			"risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
			"execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": false, "signal_only": true }
		},
		"feature_set_code": "btcusdt_futures_mvp",
		"feature_set_version": 1
	}`))
	rr := httptest.NewRecorder()

	handleStrategyRuntimePreflight(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var body runtimePreflightResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.RuntimeSupported {
		t.Fatalf("expected runtime_supported=true, got %#v", body)
	}
}
