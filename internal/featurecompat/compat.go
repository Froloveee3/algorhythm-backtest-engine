// Package featurecompat is the narrow "do the physical features this run
// asks for actually exist in the dataset we're about to read" checker. It
// runs once per run between the compile step (M4) and dataset resolution
// (M5), on the cold boundary, never in the executor's hot path.
//
// It is intentionally small: the authoritative column contract lives in
// featuredata.contract (feature-parquet-v1). This package only:
//
//  1. Asserts the bound feature-set (Code, Version) is one the engine knows.
//  2. Asserts every RequiredColumn from the compiled plan exists in that
//     set's runtime contract.
//
// Anything deeper — value ranges, upstream nullability policy, warmup row
// counts — belongs either to featuredata (per-row invariants) or to the
// DSL-driven executor (per-rule preconditions).
package featurecompat

import (
	"errors"
	"fmt"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

// ErrUnsupportedFeatureSet is returned when (Code, Version) is not in the
// engine's price-scale registry. This is the same sentinel
// featuredata.ReadFeatureFrame would return later, but we catch it before
// the first S3 round-trip.
var ErrUnsupportedFeatureSet = errors.New("featurecompat: unsupported feature set")

// ErrUnknownColumn is returned when a RequiredColumn is not part of the
// feature-set's runtime contract. For feature-parquet-v1 this means the
// column is not declared in featuredata.contract.go; future versions will
// hook in per-key allow-lists here.
var ErrUnknownColumn = errors.New("featurecompat: column not in feature-set contract")

// UnsupportedFeatureSetError decorates ErrUnsupportedFeatureSet with the
// offending key so callers can include it verbatim in bt.run.failed.
type UnsupportedFeatureSetError struct {
	Key featuredata.FeatureSetKey
}

func (e *UnsupportedFeatureSetError) Error() string {
	return fmt.Sprintf("%s: code=%q version=%d", ErrUnsupportedFeatureSet.Error(), e.Key.Code, e.Key.Version)
}

func (e *UnsupportedFeatureSetError) Unwrap() error { return ErrUnsupportedFeatureSet }

// UnknownColumnError decorates ErrUnknownColumn with the key and column
// name, plus the zero-based index into the plan's RequiredColumns slice so
// operators can point at the exact entry that blew up.
type UnknownColumnError struct {
	Key    featuredata.FeatureSetKey
	Index  int
	Column featuredata.ColumnName
}

func (e *UnknownColumnError) Error() string {
	return fmt.Sprintf(
		"%s: required_columns[%d]=%q not declared in feature-set code=%q version=%d",
		ErrUnknownColumn.Error(), e.Index, string(e.Column), e.Key.Code, e.Key.Version,
	)
}

func (e *UnknownColumnError) Unwrap() error { return ErrUnknownColumn }

// Check verifies that (a) the feature set is known and (b) every required
// column is part of its contract. Returns a typed error (one of
// *UnsupportedFeatureSetError, *UnknownColumnError) on failure; nil when the
// plan can safely be bound to the set.
//
// Passing a nil registry falls back to featuredata.DefaultPriceScaleRegistry.
// Empty required list is valid (v1 strategies compile with RequiredColumns
// empty — their legacy simulator does not touch feature parquet).
func Check(key featuredata.FeatureSetKey, required []featuredata.ColumnName, registry featuredata.PriceScaleRegistry) error {
	if registry == nil {
		registry = featuredata.DefaultPriceScaleRegistry()
	}
	if _, ok := registry.Lookup(key); !ok {
		return &UnsupportedFeatureSetError{Key: key}
	}
	for i, c := range required {
		if !featuredata.IsKnownColumn(c) {
			return &UnknownColumnError{Key: key, Index: i, Column: c}
		}
	}
	return nil
}
