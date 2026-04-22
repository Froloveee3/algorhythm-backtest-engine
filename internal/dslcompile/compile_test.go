package dslcompile

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
	"github.com/algorhythm-llc/strategy-dsl/dispatch"
)

// minimalV1 is the smallest v1 document dispatch.Parse accepts. Kept inline so
// the test does not transitively couple to strategy-dsl testdata fixtures.
const minimalV1 = `{
  "schema_version": "1.0.0",
  "strategy_code": "ema_rsi_breakout",
  "instrument_scope": {
    "exchange": "binance",
    "symbols": ["BTCUSDT", "ETHUSDT"]
  },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "rsi_14_lt_30000" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
  "filters": [ { "type": "regime_filter", "params": { "allowed": ["trend_up", "trend_down"] } } ],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": true }
}`

// v2DocWithFeatures builds a minimal-yet-valid v2 document whose
// feature_requirements we can control per test. features is the slice of
// required_features[] objects to inline.
//
// The surrounding blocks are the minimum the JSON Schema + semantic hard
// gates accept. Any change in strategy-dsl semantic rules that invalidates
// this template is a hard test failure by design — we want those signals.
func v2DocWithFeatures(t *testing.T, features ...map[string]any) []byte {
	t.Helper()
	doc := map[string]any{
		"schema_version": "2.0.0",
		"strategy_code":  "compile_test",
		"instrument_scope": map[string]any{
			"exchange":      "binance",
			"symbols":       []string{"BTCUSDT"},
			"market_type":   "futures",
			"contract_type": "perpetual",
			"interval":      "1m",
		},
		"feature_requirements": map[string]any{
			"required_features": features,
		},
		"entries": []map[string]any{
			{
				"id":   "e1",
				"side": "long",
				"when": map[string]any{
					"feature": map[string]any{"name": "ema_20", "timeframe": "1m"},
					"cmp":     "gt",
					"value":   0,
				},
				"order":         map[string]any{"type": "market"},
				"priority":      10,
				"cooldown_bars": 0,
			},
		},
		"exits": []map[string]any{
			{"id": "x1", "kind": "time_stop", "params": map[string]any{"max_holding_bars": 240}, "applies_to": "all"},
		},
		"risk_management": map[string]any{
			"default_size": map[string]any{
				"kind":   "fixed_fraction",
				"params": map[string]any{"fraction_ppm": 100000},
			},
		},
		"valuation": map[string]any{
			"entry_trigger_price_source":  "trade",
			"exit_trigger_price_source":   "mark",
			"mark_to_market_price_source": "mark",
			"funding_application":         "enabled",
			"funding_price_source":        "mark",
		},
		"execution": map[string]any{
			"fee_model":      map[string]any{"kind": "bps_flat", "params": map[string]any{"fee_bps": 10}},
			"slippage_model": map[string]any{"kind": "fixed_bps", "params": map[string]any{"slippage_bps": 5}},
			"fill_model":     map[string]any{"kind": "next_bar_open"},
			"allow_short":    false,
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal v2 template: %v", err)
	}
	return raw
}

func TestCompile_V1_ProducesTypedPlanAndColumns(t *testing.T) {
	plan, err := Compile([]byte(minimalV1))
	if err != nil {
		t.Fatalf("Compile(v1): %v", err)
	}
	if plan.Major != MajorV1 {
		t.Fatalf("Major=%v, want v1", plan.Major)
	}
	if plan.StrategyCode != "ema_rsi_breakout" {
		t.Fatalf("StrategyCode=%q", plan.StrategyCode)
	}
	if plan.Exchange != "binance" {
		t.Fatalf("Exchange=%q", plan.Exchange)
	}
	if !equalStrings(plan.Symbols, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Fatalf("Symbols=%v", plan.Symbols)
	}
	if !plan.AllowShort {
		t.Fatal("AllowShort should follow execution.allow_short=true")
	}
	wantCols := []featuredata.ColumnName{
		featuredata.ColEMA20,
		featuredata.ColEMA50,
		featuredata.ColRSI14,
		featuredata.ColTrendUp,
		featuredata.ColTrendDown,
		featuredata.ColCloseTradeI64,
	}
	if !equalColumns(plan.RequiredColumns, wantCols) {
		t.Fatalf("v1 RequiredColumns=%v, want %v", plan.RequiredColumns, wantCols)
	}
	if plan.V1Raw == nil || plan.V2Doc != nil || plan.V1 == nil {
		t.Fatalf("v1 plan must have V1Raw and V1 set, and V2Doc nil (have V1Raw=%v V1=%v V2Doc=%v)", plan.V1Raw != nil, plan.V1 != nil, plan.V2Doc != nil)
	}
	if plan.FillModelKind != "same_bar_close" {
		t.Fatalf("FillModelKind=%q", plan.FillModelKind)
	}
	if plan.V1.Exit.TakeProfitBps != 400 || plan.V1.Exit.StopLossBps != 150 {
		t.Fatalf("unexpected v1 exit plan: %+v", plan.V1.Exit)
	}
	if plan.V1.Risk.Type != "fixed_fraction" || plan.V1.Risk.RiskBps != 100 {
		t.Fatalf("unexpected v1 risk plan: %+v", plan.V1.Risk)
	}
	if len(plan.V1.Entry.IndicatorConditions) != 2 {
		t.Fatalf("expected 2 indicator predicates, got %+v", plan.V1.Entry.IndicatorConditions)
	}
	if !plan.V1.SymmetricShortEntry {
		t.Fatal("expected SymmetricShortEntry=true when entry_short is omitted but allow_short=true")
	}
	if len(plan.V1.EntryShort.IndicatorConditions) != len(plan.V1.Entry.IndicatorConditions) {
		t.Fatalf("expected EntryShort to mirror Entry for legacy symmetric v1, got long=%d short=%d",
			len(plan.V1.Entry.IndicatorConditions), len(plan.V1.EntryShort.IndicatorConditions))
	}
}

func TestCompile_V1_EntryShortAndCloseSides_Compiles(t *testing.T) {
	raw := []byte(`{
  "schema_version": "1.1.0",
  "strategy_code": "dual_side_close_compile",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry": { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "entry_short": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "close_long": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "close_short": { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "exit": { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
  "filters": [],
  "risk": { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 10, "slippage_bps": 5, "allow_short": true }
}`)
	plan, err := Compile(raw)
	if err != nil {
		t.Fatalf("Compile(v1): %v", err)
	}
	if plan.V1.SymmetricShortEntry {
		t.Fatal("expected SymmetricShortEntry=false when entry_short is present")
	}
	wantCols := []featuredata.ColumnName{
		featuredata.ColEMA20,
		featuredata.ColEMA50,
		featuredata.ColRSI14,
		featuredata.ColCloseTradeI64,
	}
	if !equalColumns(plan.RequiredColumns, wantCols) {
		t.Fatalf("RequiredColumns=%v, want %v", plan.RequiredColumns, wantCols)
	}
}

func TestCompile_V1_SignalOnlyExecutionFlag(t *testing.T) {
	raw := []byte(`{
  "schema_version": "1.1.0",
  "strategy_code": "signal_only_flag",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry": { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "exit": { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
  "filters": [],
  "risk": { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": false, "signal_only": true }
}`)
	plan, err := Compile(raw)
	if err != nil {
		t.Fatalf("Compile(v1): %v", err)
	}
	if plan.V1 == nil || !plan.V1.Execution.SignalOnly {
		t.Fatalf("expected Execution.SignalOnly=true, got %+v", plan.V1)
	}
}

func TestCompile_V2_HappyPath_ExtractsDedupedOrderedColumns(t *testing.T) {
	// Include a duplicate resolved column (ema_20 twice) to prove dedupe keeps
	// first-seen ordering. mark_close_i64 / funding_rate_current round out
	// the mapping coverage.
	raw := v2DocWithFeatures(t,
		map[string]any{"name": "ema_20", "timeframe": "1m"},
		map[string]any{"name": "rsi_14", "timeframe": "1m"},
		map[string]any{"name": "ema_20", "timeframe": "5m"}, // same column post-mapping
		map[string]any{"name": "mark_close_i64", "namespace": "mark", "timeframe": "1m"},
		map[string]any{"name": "funding_rate_current", "namespace": "funding"},
	)
	plan, err := Compile(raw)
	if err != nil {
		t.Fatalf("Compile(v2): %v", err)
	}
	if plan.Major != MajorV2 {
		t.Fatalf("Major=%v, want v2", plan.Major)
	}
	if plan.Interval != "1m" {
		t.Fatalf("Interval=%q", plan.Interval)
	}
	if plan.FillModelKind != "next_bar_open" {
		t.Fatalf("FillModelKind=%q", plan.FillModelKind)
	}
	if plan.V2Doc == nil || plan.V1Raw != nil {
		t.Fatalf("v2 plan must have V2Doc set and V1Raw nil")
	}
	want := []featuredata.ColumnName{
		featuredata.ColEMA20,
		featuredata.ColRSI14,
		featuredata.ColMarkCloseI64,
		featuredata.ColFundingRateCurrent,
	}
	if !equalColumns(plan.RequiredColumns, want) {
		t.Fatalf("RequiredColumns=%v, want %v", plan.RequiredColumns, want)
	}
}

func TestCompile_V2_UnknownFeatureRejectsWithColumnError(t *testing.T) {
	raw := v2DocWithFeatures(t,
		map[string]any{"name": "ema_20", "timeframe": "1m"},
		map[string]any{"name": "not_a_real_feature", "timeframe": "1m"},
	)
	_, err := Compile(raw)
	if err == nil {
		t.Fatal("expected ColumnError, got nil")
	}
	var ce *ColumnError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ColumnError, got %T: %v", err, err)
	}
	if ce.Index != 1 || ce.Optional {
		t.Fatalf("ColumnError Index=%d Optional=%v", ce.Index, ce.Optional)
	}
	if ce.Name != "not_a_real_feature" {
		t.Fatalf("ColumnError Name=%q", ce.Name)
	}
}

func TestCompile_EmptyPayload(t *testing.T) {
	_, err := Compile(nil)
	if !errors.Is(err, ErrEmptyPayload) {
		t.Fatalf("expected ErrEmptyPayload, got %v", err)
	}
}

func TestCompile_UnsupportedVersion_WrapsSentinel(t *testing.T) {
	raw := []byte(`{"schema_version":"9.9.9"}`)
	_, err := Compile(raw)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
	if !errors.Is(err, dispatch.ErrUnsupportedVersion) {
		t.Fatalf("expected wrapped dispatch.ErrUnsupportedVersion, got %v", err)
	}
}

func TestCompile_SchemaRejection_ReturnedAsParseError(t *testing.T) {
	// v2 schema_version but missing required fields → JSON Schema hard failure.
	raw := []byte(`{"schema_version":"2.0.0"}`)
	_, err := Compile(raw)
	if err == nil {
		t.Fatal("expected ParseError, got nil")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *ParseError, got %T: %v", err, err)
	}
	if pe.Unwrap() == nil {
		t.Fatal("ParseError must wrap the underlying dispatch error")
	}
}

func TestCompiledPlan_NewReadRequest_CopiesColumns(t *testing.T) {
	raw := v2DocWithFeatures(t,
		map[string]any{"name": "ema_20", "timeframe": "1m"},
		map[string]any{"name": "rsi_14", "timeframe": "1m"},
	)
	plan, err := Compile(raw)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	fs := featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1}
	parts := []featuredata.Partition{{Year: 2024, Month: 1, ObjectKey: "a/b.parquet"}}
	req := plan.NewReadRequest(fs, "BTCUSDT", parts)
	if req.FeatureSet != fs || req.ExpectedSymbol != "BTCUSDT" {
		t.Fatalf("ReadRequest header mismatch: %+v", req)
	}
	if len(req.Partitions) != 1 || req.Partitions[0].ObjectKey != "a/b.parquet" {
		t.Fatalf("Partitions passthrough failed: %+v", req.Partitions)
	}
	// Mutating the plan's slice after the fact must not affect the request
	// copy — guards against a subtle aliasing bug that would let the worker
	// silently reconfigure an in-flight read.
	plan.RequiredColumns[0] = "mutated"
	if strings.HasPrefix(string(req.RequiredColumns[0]), "mutated") {
		t.Fatal("NewReadRequest must not alias CompiledPlan.RequiredColumns")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalColumns(a, b []featuredata.ColumnName) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
