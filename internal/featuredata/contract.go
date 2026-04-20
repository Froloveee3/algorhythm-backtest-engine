package featuredata

// This file pins the consumer-side view of the feature-parquet-v1 contract.
// The canonical contract is docs/contracts/feature-parquet-v1.md in the meta-repo.
// Any change to column names, types, or the price-scale registry must be driven
// by a contract bump (new feature_set_code or version), never a silent patch.

// ColumnName is the canonical physical column name as written by feature-builder.
type ColumnName string

const (
	ColSymbol       ColumnName = "symbol"
	ColTimestampUTC ColumnName = "timestamp_utc"

	ColCloseTradeI64 ColumnName = "close_trade_i64"
	ColMarkCloseI64  ColumnName = "mark_close_i64"

	ColReturns1m  ColumnName = "returns_1m"
	ColReturns5m  ColumnName = "returns_5m"
	ColReturns15m ColumnName = "returns_15m"

	ColEMA20 ColumnName = "ema_20"
	ColEMA50 ColumnName = "ema_50"
	ColATR14 ColumnName = "atr_14"
	ColRSI14 ColumnName = "rsi_14"

	ColRollingStd60  ColumnName = "rolling_std_60"
	ColRollingStd240 ColumnName = "rolling_std_240"

	ColMarkTradeSpreadBps   ColumnName = "mark_trade_spread_bps"
	ColFundingRateCurrent   ColumnName = "funding_rate_current"
	ColFundingRateRolling3  ColumnName = "funding_rate_rolling_3"
	ColFundingRateRolling9  ColumnName = "funding_rate_rolling_9"
	ColFundingPressureScore ColumnName = "funding_pressure_score"

	ColTrendUp   ColumnName = "trend_up"
	ColTrendDown ColumnName = "trend_down"
	ColFlat      ColumnName = "flat"
	ColHighVol   ColumnName = "high_vol"
	ColLowVol    ColumnName = "low_vol"
)

// int64Columns are required, non-nullable after the first row.
var int64Columns = map[ColumnName]struct{}{
	ColCloseTradeI64: {},
	ColMarkCloseI64:  {},
}

// floatColumns are optional (nullable during warmup or when upstream cannot compute).
var floatColumns = map[ColumnName]struct{}{
	ColReturns1m:            {},
	ColReturns5m:            {},
	ColReturns15m:           {},
	ColEMA20:                {},
	ColEMA50:                {},
	ColATR14:                {},
	ColRSI14:                {},
	ColRollingStd60:         {},
	ColRollingStd240:        {},
	ColMarkTradeSpreadBps:   {},
	ColFundingRateCurrent:   {},
	ColFundingRateRolling3:  {},
	ColFundingRateRolling9:  {},
	ColFundingPressureScore: {},
}

// boolColumns are optional; regime and volatility flags.
var boolColumns = map[ColumnName]struct{}{
	ColTrendUp:   {},
	ColTrendDown: {},
	ColFlat:      {},
	ColHighVol:   {},
	ColLowVol:    {},
}

// IsKnownColumn reports whether the name corresponds to a contract-v1 column
// (including timestamp_utc and symbol).
func IsKnownColumn(n ColumnName) bool {
	if n == ColSymbol || n == ColTimestampUTC {
		return true
	}
	if _, ok := int64Columns[n]; ok {
		return true
	}
	if _, ok := floatColumns[n]; ok {
		return true
	}
	if _, ok := boolColumns[n]; ok {
		return true
	}
	return false
}

// FeatureSetKey identifies a specific producer contract instance.
// Price scale and schema compatibility are tracked per (Code, Version) pair,
// never by Code alone - see feature-parquet-v1.md §4.
type FeatureSetKey struct {
	Code    string
	Version int
}

// PriceScaleRegistry maps known (code, version) pairs to their fixed price scale.
// Unknown pairs MUST be rejected with ErrReasonFeatureSetUnsupported at runtime.
//
// This is a deliberate v1 gap: feature-parquet files do not yet carry their own
// price_scale; the engine knows it out-of-band. The registry is the single
// source of truth on the consumer side.
type PriceScaleRegistry map[FeatureSetKey]int32

// DefaultPriceScaleRegistry returns the canonical v1 registry.
// Pinning this to a function (not a var) keeps callers from mutating it.
func DefaultPriceScaleRegistry() PriceScaleRegistry {
	return PriceScaleRegistry{
		{Code: "btcusdt_futures_mvp", Version: 1}: 8,
	}
}

// Lookup returns the price scale for the given key or false if unknown.
func (r PriceScaleRegistry) Lookup(k FeatureSetKey) (int32, bool) {
	s, ok := r[k]
	return s, ok
}
