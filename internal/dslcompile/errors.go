package dslcompile

import (
	"errors"
	"fmt"
	"strings"
)

// ErrUnsupportedVersion is returned when strategy-dsl/dispatch rejected the
// payload because schema_version is neither ^1. nor ^2. Surfacing it as a
// sentinel lets the worker map it onto a stable bt.run.failed reason.
var ErrUnsupportedVersion = errors.New("dslcompile: unsupported schema_version")

// ErrEmptyPayload is returned when the caller passes zero-length DSL bytes.
// dispatch.Parse would also reject this, but we surface our own sentinel so
// the call site does not need to unwrap a fmt.Errorf chain.
var ErrEmptyPayload = errors.New("dslcompile: empty DSL payload")

// ParseError wraps any error produced by strategy-dsl/dispatch.Parse (JSON
// Schema violation, v2 semantic hard error, unsupported schema_version, ...).
// The wrapped error preserves the original typing; callers can errors.Is /
// errors.As through it to reach e.g. *dispatch.SemanticHardError.
type ParseError struct {
	Err error
}

func (e *ParseError) Error() string {
	if e == nil || e.Err == nil {
		return "dslcompile: parse error"
	}
	return "dslcompile: parse: " + e.Err.Error()
}

// Unwrap exposes the wrapped dispatch error for errors.Is / errors.As.
func (e *ParseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ColumnError is returned when a v2 FeatureRequirements entry names a feature
// that does not map to any feature-parquet-v1 physical column. This is a
// compile-time rejection independent of any specific dataset: a name the
// producer contract does not recognise cannot be satisfied by any FeatureSet
// version.
type ColumnError struct {
	// Index is the position of the offending selector in required_features[]
	// (or optional_features[] when Optional == true).
	Index int
	// Optional reports whether the selector lived in optional_features[].
	Optional bool
	// Name is the FeatureSelector.Name the engine could not resolve.
	Name string
	// Namespace / Timeframe / Symbol are copied verbatim for diagnostics. They
	// are NOT used for resolution in M4 — see columns.go.
	Namespace string
	Timeframe string
	Symbol    string
}

func (e *ColumnError) Error() string {
	if e == nil {
		return "dslcompile: column error"
	}
	kind := "required_features"
	if e.Optional {
		kind = "optional_features"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "dslcompile: %s[%d]: feature name %q is not part of feature-parquet-v1", kind, e.Index, e.Name)
	if e.Namespace != "" || e.Timeframe != "" || e.Symbol != "" {
		fmt.Fprintf(&b, " (namespace=%q timeframe=%q symbol=%q)", e.Namespace, e.Timeframe, e.Symbol)
	}
	return b.String()
}
