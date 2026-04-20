package featurecompat

import (
	"errors"
	"testing"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

func TestCheck_HappyPath_KnownKeyAndColumns(t *testing.T) {
	key := featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1}
	cols := []featuredata.ColumnName{
		featuredata.ColEMA20,
		featuredata.ColRSI14,
		featuredata.ColMarkCloseI64,
	}
	if err := Check(key, cols, nil); err != nil {
		t.Fatalf("Check(happy): unexpected error %v", err)
	}
}

func TestCheck_EmptyRequiredIsAllowed(t *testing.T) {
	// v1 strategies compile with RequiredColumns empty; compat must still
	// accept them so legacy runs do not trip on the gate.
	key := featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1}
	if err := Check(key, nil, nil); err != nil {
		t.Fatalf("Check(empty cols): unexpected error %v", err)
	}
}

func TestCheck_TimestampAndSymbolAreAlwaysAccepted(t *testing.T) {
	// featuredata.IsKnownColumn treats symbol and timestamp_utc as known
	// even though they are not materialised as value columns on the frame;
	// compat must mirror that policy to stay consistent with the reader.
	key := featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1}
	cols := []featuredata.ColumnName{featuredata.ColSymbol, featuredata.ColTimestampUTC}
	if err := Check(key, cols, nil); err != nil {
		t.Fatalf("Check(symbol/ts): unexpected error %v", err)
	}
}

func TestCheck_UnsupportedFeatureSet_WrapsSentinel(t *testing.T) {
	key := featuredata.FeatureSetKey{Code: "unknown_set", Version: 7}
	err := Check(key, nil, nil)
	if err == nil {
		t.Fatal("Check(unknown key): expected error, got nil")
	}
	if !errors.Is(err, ErrUnsupportedFeatureSet) {
		t.Fatalf("expected wrapped ErrUnsupportedFeatureSet, got %v", err)
	}
	var typed *UnsupportedFeatureSetError
	if !errors.As(err, &typed) {
		t.Fatalf("expected *UnsupportedFeatureSetError in chain, got %T", err)
	}
	if typed.Key != key {
		t.Fatalf("Key=%+v, want %+v", typed.Key, key)
	}
}

func TestCheck_UnknownColumn_ReportsIndexAndName(t *testing.T) {
	key := featuredata.FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1}
	cols := []featuredata.ColumnName{
		featuredata.ColEMA20,
		featuredata.ColumnName("no_such_feature"),
		featuredata.ColRSI14,
	}
	err := Check(key, cols, nil)
	if err == nil {
		t.Fatal("Check(unknown col): expected error, got nil")
	}
	if !errors.Is(err, ErrUnknownColumn) {
		t.Fatalf("expected wrapped ErrUnknownColumn, got %v", err)
	}
	var typed *UnknownColumnError
	if !errors.As(err, &typed) {
		t.Fatalf("expected *UnknownColumnError in chain, got %T", err)
	}
	if typed.Index != 1 {
		t.Fatalf("Index=%d, want 1", typed.Index)
	}
	if typed.Column != "no_such_feature" {
		t.Fatalf("Column=%q, want %q", typed.Column, "no_such_feature")
	}
	if typed.Key != key {
		t.Fatalf("Key=%+v, want %+v", typed.Key, key)
	}
}

func TestCheck_CustomRegistry_IsRespected(t *testing.T) {
	// Passing a custom registry must take precedence over the default; this
	// is the hook future FeatureSet versions will use to install a tighter
	// allow-list without changing compat's call sites.
	customKey := featuredata.FeatureSetKey{Code: "test_set", Version: 2}
	reg := featuredata.PriceScaleRegistry{customKey: 4}
	if err := Check(customKey, []featuredata.ColumnName{featuredata.ColEMA20}, reg); err != nil {
		t.Fatalf("Check(custom registry hit): unexpected error %v", err)
	}
	// default registry should reject the same key
	if err := Check(customKey, nil, nil); err == nil {
		t.Fatal("Check(custom key on default registry): expected error, got nil")
	}
}
