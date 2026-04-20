package dslcompile

import "github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"

// featureNameToColumn maps a DSL v2 FeatureSelector.Name (snake_case) to the
// canonical feature-parquet-v1 column.
//
// Rules:
//
//   - Only FeatureSelector.Name is consulted here. namespace / timeframe /
//     symbol are part of the binding identity a future milestone (M5) will
//     validate against a *concrete* FeatureSet (code,version); at compile time
//     a name unknown to feature-parquet-v1 cannot possibly be satisfied by any
//     version, so we reject it now.
//   - The mark-vs-trade close pair (`mark_close_i64`, `close_trade_i64`) is
//     addressed by the fully-qualified physical name in the DSL v2 schema.
//     Canonical examples spell them that way, and the semantic validator
//     refuses any shorter / ambiguous form.
//   - timestamp_utc and symbol are always materialised by the reader; they
//     must not be asked for via feature_requirements (the schema does not
//     list them as indicator features).
//
// Keep this table in lock-step with internal/featuredata/contract.go.
var featureNameToColumn = map[string]featuredata.ColumnName{
	"close_trade_i64":        featuredata.ColCloseTradeI64,
	"mark_close_i64":         featuredata.ColMarkCloseI64,
	"returns_1m":             featuredata.ColReturns1m,
	"returns_5m":             featuredata.ColReturns5m,
	"returns_15m":            featuredata.ColReturns15m,
	"ema_20":                 featuredata.ColEMA20,
	"ema_50":                 featuredata.ColEMA50,
	"atr_14":                 featuredata.ColATR14,
	"rsi_14":                 featuredata.ColRSI14,
	"rolling_std_60":         featuredata.ColRollingStd60,
	"rolling_std_240":        featuredata.ColRollingStd240,
	"mark_trade_spread_bps":  featuredata.ColMarkTradeSpreadBps,
	"funding_rate_current":   featuredata.ColFundingRateCurrent,
	"funding_rate_rolling_3": featuredata.ColFundingRateRolling3,
	"funding_rate_rolling_9": featuredata.ColFundingRateRolling9,
	"funding_pressure_score": featuredata.ColFundingPressureScore,
	"trend_up":               featuredata.ColTrendUp,
	"trend_down":             featuredata.ColTrendDown,
	"flat":                   featuredata.ColFlat,
	"high_vol":               featuredata.ColHighVol,
	"low_vol":                featuredata.ColLowVol,
}

// resolveFeatureColumn returns the physical column for a DSL v2 feature name,
// or ("", false) if the name is not part of the feature-parquet-v1 contract.
func resolveFeatureColumn(name string) (featuredata.ColumnName, bool) {
	c, ok := featureNameToColumn[name]
	return c, ok
}
