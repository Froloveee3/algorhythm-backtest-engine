package runtime

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

const longDSL = `{
  "schema_version": "1.0.0",
  "strategy_code": "long_test",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "rsi_14_lt_30000" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 200, "stop_loss_bps": 100 } },
  "filters": [ { "type": "regime_filter", "params": { "allowed": ["trend_up"] } } ],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": false }
}`

const shortDSL = `{
  "schema_version": "1.0.0",
  "strategy_code": "short_test",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 200, "stop_loss_bps": 100 } },
  "filters": [ { "type": "regime_filter", "params": { "allowed": ["trend_down"] } } ],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": true }
}`

func TestRunV1_LongScenario_OpensAndCloses(t *testing.T) {
	plan := mustCompileV1(t, longDSL)
	frame := featureFrameForLong()
	res, err := RunV1(context.Background(), RunMetadata{
		RunID:             "run-long",
		StrategyVersionID: "sv-1",
		Symbol:            "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("trades=%d, want 1", len(res.Trades))
	}
	tr := res.Trades[0]
	if tr.Side != "LONG" {
		t.Fatalf("side=%q, want LONG", tr.Side)
	}
	if tr.RegimeCode != "trend_up_low_vol" {
		t.Fatalf("RegimeCode=%q, want trend_up_low_vol", tr.RegimeCode)
	}
	if !tr.EntryTime.Equal(tsAt(1)) || !tr.ExitTime.Equal(tsAt(3)) {
		t.Fatalf("unexpected trade times: %+v", tr)
	}
	if !(tr.EntryPrice > 0 && tr.ExitPrice > tr.EntryPrice && tr.PnLAbs > 0) {
		t.Fatalf("unexpected long trade values: %+v", tr)
	}
	if res.Metrics.TradesTotal != 1 || res.Metrics.TradesWon != 1 || res.Metrics.TradesLost != 0 {
		t.Fatalf("unexpected metrics: %+v", res.Metrics)
	}
	if got := res.Metrics.RegimeBreakdown["trend_up_low_vol"]; got.Trades != 1 || got.Wins != 1 {
		t.Fatalf("unexpected regime breakdown: %+v", res.Metrics.RegimeBreakdown)
	}
}

func TestRunV1_ShortScenario_RespectsAllowShort(t *testing.T) {
	plan := mustCompileV1(t, shortDSL)
	frame := featureFrameForShort()
	res, err := RunV1(context.Background(), RunMetadata{
		RunID:             "run-short",
		StrategyVersionID: "sv-2",
		Symbol:            "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("trades=%d, want 1", len(res.Trades))
	}
	tr := res.Trades[0]
	if tr.Side != "SHORT" {
		t.Fatalf("side=%q, want SHORT", tr.Side)
	}
	if !(tr.ExitPrice < tr.EntryPrice && tr.PnLAbs > 0) {
		t.Fatalf("unexpected short trade values: %+v", tr)
	}
}

func TestRunV1_InvalidFeatureSuppressesEntry(t *testing.T) {
	plan := mustCompileV1(t, longDSL)
	frame := featureFrameForLong()
	// row 1 technically matches by values, but make RSI invalid there so entry
	// must be delayed until row 2 instead of crashing or treating invalid as 0.
	frame.Floats[featuredata.ColRSI14].Valid = []uint64{0b11101}

	res, err := RunV1(context.Background(), RunMetadata{
		RunID:             "run-null",
		StrategyVersionID: "sv-3",
		Symbol:            "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("trades=%d, want 1", len(res.Trades))
	}
	if got := res.Trades[0].EntryTime; !got.Equal(tsAt(2)) {
		t.Fatalf("entry delayed incorrectly: got %v, want %v", got, tsAt(2))
	}
}

func TestRunV1_UnsupportedFillModel(t *testing.T) {
	plan := mustCompileV1(t, longDSL)
	plan.FillModelKind = "next_bar_open"
	_, err := RunV1(context.Background(), RunMetadata{RunID: "run-bad", Symbol: "BTCUSDT"}, plan, featureFrameForLong())
	if err == nil {
		t.Fatal("expected unsupported runtime error")
	}
}

func TestRunV1_IsDeterministic(t *testing.T) {
	plan := mustCompileV1(t, longDSL)
	frame := featureFrameForLong()
	meta := RunMetadata{RunID: "run-det", StrategyVersionID: "sv-det", Symbol: "BTCUSDT"}
	a, err := RunV1(context.Background(), meta, plan, frame)
	if err != nil {
		t.Fatalf("RunV1(first): %v", err)
	}
	b, err := RunV1(context.Background(), meta, plan, frame)
	if err != nil {
		t.Fatalf("RunV1(second): %v", err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("RunV1 not deterministic:\nA=%+v\nB=%+v", a, b)
	}
}

func mustCompileV1(t *testing.T, raw string) *dslcompile.CompiledPlan {
	t.Helper()
	p, err := dslcompile.Compile([]byte(raw))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return p
}

func featureFrameForLong() *featuredata.FeatureFrame {
	return &featuredata.FeatureFrame{
		FeatureSet: featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1},
		Symbol:     "BTCUSDT",
		PriceScale: 0,
		RowCount:   5,
		Timestamps: []int64{
			tsAt(0).UnixMilli(),
			tsAt(1).UnixMilli(),
			tsAt(2).UnixMilli(),
			tsAt(3).UnixMilli(),
			tsAt(4).UnixMilli(),
		},
		Int64s: map[featuredata.ColumnName]*featuredata.Int64Column{
			featuredata.ColCloseTradeI64: {Name: featuredata.ColCloseTradeI64, Values: []int64{100, 101, 102, 104, 103}},
		},
		Floats: map[featuredata.ColumnName]*featuredata.Float64Column{
			featuredata.ColEMA20: {Name: featuredata.ColEMA20, Values: []float64{99, 102, 103, 104, 101}, Valid: []uint64{0b11111}},
			featuredata.ColEMA50: {Name: featuredata.ColEMA50, Values: []float64{100, 100, 100, 100, 100}, Valid: []uint64{0b11111}},
			featuredata.ColRSI14: {Name: featuredata.ColRSI14, Values: []float64{32000, 29000, 28000, 31000, 33000}, Valid: []uint64{0b11111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{
			featuredata.ColTrendUp: {Name: featuredata.ColTrendUp, Values: []uint8{0, 1, 1, 1, 0}, Valid: []uint64{0b11111}},
			featuredata.ColLowVol:  {Name: featuredata.ColLowVol, Values: []uint8{1, 1, 1, 0, 1}, Valid: []uint64{0b11111}},
		},
	}
}

func featureFrameForShort() *featuredata.FeatureFrame {
	return &featuredata.FeatureFrame{
		FeatureSet: featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1},
		Symbol:     "BTCUSDT",
		PriceScale: 0,
		RowCount:   4,
		Timestamps: []int64{
			tsAt(0).UnixMilli(),
			tsAt(1).UnixMilli(),
			tsAt(2).UnixMilli(),
			tsAt(3).UnixMilli(),
		},
		Int64s: map[featuredata.ColumnName]*featuredata.Int64Column{
			featuredata.ColCloseTradeI64: {Name: featuredata.ColCloseTradeI64, Values: []int64{100, 100, 97, 96}},
		},
		Floats: map[featuredata.ColumnName]*featuredata.Float64Column{
			featuredata.ColRSI14: {Name: featuredata.ColRSI14, Values: []float64{32000, 29000, 28000, 31000}, Valid: []uint64{0b0111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{
			featuredata.ColTrendDown: {Name: featuredata.ColTrendDown, Values: []uint8{0, 1, 1, 1}, Valid: []uint64{0b1111}},
		},
	}
}

func tsAt(minute int) time.Time {
	return time.UnixMilli(int64(minute) * 60_000).UTC()
}
