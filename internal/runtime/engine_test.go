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

const closeLongDSL = `{
  "schema_version": "1.1.0",
  "strategy_code": "close_long_test",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "close_long": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 100000, "stop_loss_bps": 100000 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": false }
}`

func TestRunV1_CloseLongExitsBeforeMechanicalTP(t *testing.T) {
	plan := mustCompileV1(t, closeLongDSL)
	frame := &featuredata.FeatureFrame{
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
			// Entry at bar1; close signal at bar3 when RSI dips below 30000.
			featuredata.ColRSI14: {Name: featuredata.ColRSI14, Values: []float64{32000, 29000, 31000, 29000, 33000}, Valid: []uint64{0b11111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{},
	}
	res, err := RunV1(context.Background(), RunMetadata{
		RunID:             "run-close-long",
		StrategyVersionID: "sv-close-long",
		Symbol:            "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("trades=%d, want 1: %+v", len(res.Trades), res.Trades)
	}
	tr := res.Trades[0]
	if tr.Side != "LONG" {
		t.Fatalf("side=%q, want LONG", tr.Side)
	}
	if !tr.EntryTime.Equal(tsAt(1)) || !tr.ExitTime.Equal(tsAt(3)) {
		t.Fatalf("unexpected trade times: %+v", tr)
	}
}

const signalOnlyLongDSL = `{
  "schema_version": "1.1.0",
  "strategy_code": "signal_only_long",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 400, "stop_loss_bps": 150 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": false, "signal_only": true }
}`

func TestRunV1_SignalOnlyHoldsThroughMechanicalTP(t *testing.T) {
	plan := mustCompileV1(t, signalOnlyLongDSL)
	frame := &featuredata.FeatureFrame{
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
			featuredata.ColCloseTradeI64: {Name: featuredata.ColCloseTradeI64, Values: []int64{100, 101, 102, 130, 103}},
		},
		Floats: map[featuredata.ColumnName]*featuredata.Float64Column{
			featuredata.ColEMA20: {Name: featuredata.ColEMA20, Values: []float64{99, 102, 103, 104, 99}, Valid: []uint64{0b11111}},
			featuredata.ColEMA50: {Name: featuredata.ColEMA50, Values: []float64{100, 100, 100, 100, 100}, Valid: []uint64{0b11111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{},
	}
	res, err := RunV1(context.Background(), RunMetadata{
		RunID:             "run-signal-only",
		StrategyVersionID: "sv-signal-only",
		Symbol:            "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("trades=%d, want 1: %+v", len(res.Trades), res.Trades)
	}
	tr := res.Trades[0]
	if !tr.EntryTime.Equal(tsAt(1)) || !tr.ExitTime.Equal(tsAt(4)) {
		t.Fatalf("want entry bar1 exit bar4 when TP would hit bar3; got %+v", tr)
	}
}

func TestRunV1_SignalOnlyCloseLongStillExitsEarly(t *testing.T) {
	dsl := `{
  "schema_version": "1.1.0",
  "strategy_code": "signal_only_close_long",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "close_long": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 100000, "stop_loss_bps": 100000 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": false, "signal_only": true }
}`
	plan := mustCompileV1(t, dsl)
	frame := &featuredata.FeatureFrame{
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
			featuredata.ColRSI14: {Name: featuredata.ColRSI14, Values: []float64{32000, 29000, 31000, 29000, 33000}, Valid: []uint64{0b11111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{},
	}
	res, err := RunV1(context.Background(), RunMetadata{
		RunID:             "run-signal-only-close",
		StrategyVersionID: "sv-soc",
		Symbol:            "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("trades=%d, want 1", len(res.Trades))
	}
	tr := res.Trades[0]
	if !tr.EntryTime.Equal(tsAt(1)) || !tr.ExitTime.Equal(tsAt(3)) {
		t.Fatalf("close_long should win at bar3; got %+v", tr)
	}
}

const dualEntryDSL = `{
  "schema_version": "1.1.0",
  "strategy_code": "dual_entry_tiebreak",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "entry_short": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 100000, "stop_loss_bps": 100000 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": true }
}`

func TestRunV1_DualEntryTieBreakPrefersLong(t *testing.T) {
	plan := mustCompileV1(t, dualEntryDSL)
	frame := &featuredata.FeatureFrame{
		FeatureSet: featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1},
		Symbol:     "BTCUSDT",
		PriceScale: 0,
		RowCount:   3,
		Timestamps: []int64{
			tsAt(0).UnixMilli(),
			tsAt(1).UnixMilli(),
			tsAt(2).UnixMilli(),
		},
		Int64s: map[featuredata.ColumnName]*featuredata.Int64Column{
			featuredata.ColCloseTradeI64: {Name: featuredata.ColCloseTradeI64, Values: []int64{100, 101, 102}},
		},
		Floats: map[featuredata.ColumnName]*featuredata.Float64Column{
			featuredata.ColEMA20: {Name: featuredata.ColEMA20, Values: []float64{100, 100, 100}, Valid: []uint64{0b111}},
			featuredata.ColEMA50: {Name: featuredata.ColEMA50, Values: []float64{100, 100, 100}, Valid: []uint64{0b111}},
			featuredata.ColRSI14: {Name: featuredata.ColRSI14, Values: []float64{32000, 29000, 31000}, Valid: []uint64{0b111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{},
	}
	res, err := RunV1(context.Background(), RunMetadata{
		RunID:             "run-dual-entry",
		StrategyVersionID: "sv-dual-entry",
		Symbol:            "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 1 {
		t.Fatalf("trades=%d, want 1", len(res.Trades))
	}
	if got := res.Trades[0].Side; got != "LONG" {
		t.Fatalf("side=%q, want LONG", got)
	}
}

// PR-09 continuous: no 2-bar cooldown — after close on bar i, re-entry on
// bar i+1 is allowed if entry signal persists.
func TestRunV1_ReentryContinuous_AllowsNextBarReentry(t *testing.T) {
	// entry=ema_20_gt_ema_50 always true; close_long=rsi_14_lt_30000 fires only
	// at bar2; after close we want re-entry at bar3 (which continuous allows).
	dsl := `{
  "schema_version": "1.2.0",
  "strategy_code": "continuous_reentry",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "close_long": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 100000, "stop_loss_bps": 100000 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": false, "reentry_mode": "continuous" }
}`
	plan := mustCompileV1(t, dsl)
	frame := &featuredata.FeatureFrame{
		FeatureSet: featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1},
		Symbol:     "BTCUSDT",
		PriceScale: 0,
		RowCount:   6,
		Timestamps: []int64{
			tsAt(0).UnixMilli(), tsAt(1).UnixMilli(), tsAt(2).UnixMilli(),
			tsAt(3).UnixMilli(), tsAt(4).UnixMilli(), tsAt(5).UnixMilli(),
		},
		Int64s: map[featuredata.ColumnName]*featuredata.Int64Column{
			featuredata.ColCloseTradeI64: {Name: featuredata.ColCloseTradeI64, Values: []int64{100, 101, 100, 102, 103, 104}},
		},
		Floats: map[featuredata.ColumnName]*featuredata.Float64Column{
			featuredata.ColEMA20: {Name: featuredata.ColEMA20, Values: []float64{102, 103, 103, 103, 104, 105}, Valid: []uint64{0b111111}},
			featuredata.ColEMA50: {Name: featuredata.ColEMA50, Values: []float64{100, 100, 100, 100, 100, 100}, Valid: []uint64{0b111111}},
			// rsi drops below 30000 only at bar2 → close_long fires, then rsi recovers.
			featuredata.ColRSI14: {Name: featuredata.ColRSI14, Values: []float64{35000, 33000, 28000, 32000, 34000, 36000}, Valid: []uint64{0b111111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{},
	}
	res, err := RunV1(context.Background(), RunMetadata{
		RunID: "run-cont", StrategyVersionID: "sv-cont", Symbol: "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 2 {
		t.Fatalf("continuous: trades=%d, want 2; %+v", len(res.Trades), res.Trades)
	}
	if !res.Trades[0].ExitTime.Equal(tsAt(2)) {
		t.Fatalf("first trade must close at bar2 via close_long; got %+v", res.Trades[0])
	}
	if !res.Trades[1].EntryTime.Equal(tsAt(3)) {
		t.Fatalf("second trade must re-enter at bar3 (continuous, no 2-bar cooldown); got %+v", res.Trades[1])
	}
}

// PR-09 single (default): 2-bar cooldown preserved as a regression lock.
func TestRunV1_ReentrySingle_Default_KeepsTwoBarCooldown(t *testing.T) {
	dsl := `{
  "schema_version": "1.2.0",
  "strategy_code": "single_reentry",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":   { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "close_long": { "type": "indicator_condition", "params": { "left": "rsi_14_lt_30000", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 100000, "stop_loss_bps": 100000 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": false }
}`
	plan := mustCompileV1(t, dsl)
	frame := &featuredata.FeatureFrame{
		FeatureSet: featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1},
		Symbol:     "BTCUSDT",
		PriceScale: 0,
		RowCount:   6,
		Timestamps: []int64{
			tsAt(0).UnixMilli(), tsAt(1).UnixMilli(), tsAt(2).UnixMilli(),
			tsAt(3).UnixMilli(), tsAt(4).UnixMilli(), tsAt(5).UnixMilli(),
		},
		Int64s: map[featuredata.ColumnName]*featuredata.Int64Column{
			featuredata.ColCloseTradeI64: {Name: featuredata.ColCloseTradeI64, Values: []int64{100, 101, 100, 102, 103, 104}},
		},
		Floats: map[featuredata.ColumnName]*featuredata.Float64Column{
			featuredata.ColEMA20: {Name: featuredata.ColEMA20, Values: []float64{102, 103, 103, 103, 104, 105}, Valid: []uint64{0b111111}},
			featuredata.ColEMA50: {Name: featuredata.ColEMA50, Values: []float64{100, 100, 100, 100, 100, 100}, Valid: []uint64{0b111111}},
			featuredata.ColRSI14: {Name: featuredata.ColRSI14, Values: []float64{35000, 33000, 28000, 32000, 34000, 36000}, Valid: []uint64{0b111111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{},
	}
	res, err := RunV1(context.Background(), RunMetadata{
		RunID: "run-single", StrategyVersionID: "sv-single", Symbol: "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 2 {
		t.Fatalf("single: trades=%d, want 2 (close at bar2 + eol close); %+v", len(res.Trades), res.Trades)
	}
	// With 2-bar cooldown, re-entry at bar3 is blocked; re-entry can only happen at bar4;
	// so the second trade must start at bar4 (not bar3).
	if !res.Trades[1].EntryTime.Equal(tsAt(4)) {
		t.Fatalf("single mode: re-entry must wait until bar4 (cooldown = bar+2); got %+v", res.Trades[1])
	}
}

// PR-09 flip: opposite-side entry signal on same bar closes current and opens reverse.
func TestRunV1_ReentryFlip_SameBarReversal(t *testing.T) {
	// Long entry: ema_20 > ema_50. Short entry: ema_20 < ema_50.
	// At bar1: long entry fires. At bar2: ema_20 < ema_50 → short signal fires → flip.
	dsl := `{
  "schema_version": "1.2.0",
  "strategy_code": "flip_reversal",
  "instrument_scope": { "exchange": "binance", "symbols": ["BTCUSDT"] },
  "entry":       { "type": "indicator_condition", "params": { "left": "ema_20_gt_ema_50", "right": "" } },
  "entry_short": { "type": "indicator_condition", "params": { "left": "ema_20_lt_ema_50", "right": "" } },
  "exit":    { "type": "tp_sl", "params": { "take_profit_bps": 100000, "stop_loss_bps": 100000 } },
  "filters": [],
  "risk":    { "type": "fixed_fraction", "params": { "risk_bps": 100 } },
  "execution": { "fee_bps": 0, "slippage_bps": 0, "allow_short": true, "reentry_mode": "flip" }
}`
	plan := mustCompileV1(t, dsl)
	frame := &featuredata.FeatureFrame{
		FeatureSet: featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1},
		Symbol:     "BTCUSDT",
		PriceScale: 0,
		RowCount:   4,
		Timestamps: []int64{
			tsAt(0).UnixMilli(), tsAt(1).UnixMilli(), tsAt(2).UnixMilli(), tsAt(3).UnixMilli(),
		},
		Int64s: map[featuredata.ColumnName]*featuredata.Int64Column{
			featuredata.ColCloseTradeI64: {Name: featuredata.ColCloseTradeI64, Values: []int64{100, 105, 95, 90}},
		},
		Floats: map[featuredata.ColumnName]*featuredata.Float64Column{
			// bar0: equal → neither long nor short fires (strict gt/lt).
			// bar1: long fires. bar2: short fires → flip. bar3: short holds.
			featuredata.ColEMA20: {Name: featuredata.ColEMA20, Values: []float64{100, 105, 95, 90}, Valid: []uint64{0b1111}},
			featuredata.ColEMA50: {Name: featuredata.ColEMA50, Values: []float64{100, 100, 100, 100}, Valid: []uint64{0b1111}},
		},
		Bools: map[featuredata.ColumnName]*featuredata.BoolColumn{},
	}
	res, err := RunV1(context.Background(), RunMetadata{
		RunID: "run-flip", StrategyVersionID: "sv-flip", Symbol: "BTCUSDT",
	}, plan, frame)
	if err != nil {
		t.Fatalf("RunV1: %v", err)
	}
	if len(res.Trades) != 2 {
		t.Fatalf("flip: trades=%d want 2 (long+short reversal, eol close); %+v", len(res.Trades), res.Trades)
	}
	if res.Trades[0].Side != "LONG" || !res.Trades[0].ExitTime.Equal(tsAt(2)) {
		t.Fatalf("flip: first trade must be LONG closed at bar2; got %+v", res.Trades[0])
	}
	if res.Trades[1].Side != "SHORT" || !res.Trades[1].EntryTime.Equal(tsAt(2)) {
		t.Fatalf("flip: second trade must be SHORT opened at bar2 (same-bar reversal); got %+v", res.Trades[1])
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
