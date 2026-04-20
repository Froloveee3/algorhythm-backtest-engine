// Package dslcompile turns a raw strategy-DSL JSON document into a typed,
// JSON-free CompiledPlan that the rest of backtest-engine can bind against.
//
// Placement in the pipeline (cold boundary, once per run):
//
//	cpclient.GetStrategyVersion -> dispatch.Parse -> dslcompile.Compile -> bar loop
//
// Design invariants (ADR-004 v2, `v2/README.md` §"Runtime contract"):
//
//   - dispatch.Parse lives on the compile boundary, not in the hot path.
//   - CompiledPlan never carries json.RawMessage, map[string]any or reflection-
//     driven data for the bar loop. v2 raw AST bytes (entries.when, exits.params,
//     etc.) are intentionally NOT exposed on this type: hot-path evaluators are
//     a later milestone and will hang off a dedicated field added here, not
//     decoded ad-hoc at iteration time.
//   - Feature *compatibility* against the bound dataset's FeatureSet is out of
//     scope for M4; that check is M5. Compile rejects only what strategy-dsl
//     itself rejects plus the mapping from FeatureSelector.Name to a physical
//     feature-parquet-v1 column (a name the producer does not know cannot be
//     "made compatible" by any dataset version).
package dslcompile

import (
	"encoding/json"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
	"github.com/algorhythm-llc/strategy-dsl/dispatch"
	dslv2 "github.com/algorhythm-llc/strategy-dsl/v2"
)

// MajorVersion re-exports dispatch.MajorVersion so callers can branch without
// importing strategy-dsl directly.
type MajorVersion = dispatch.MajorVersion

const (
	// MajorV1 is the legacy DSL contract (flat entry/exit/filters/risk).
	MajorV1 = dispatch.MajorV1
	// MajorV2 is the composable DSL contract (entries[]/exits[]/feature_requirements).
	MajorV2 = dispatch.MajorV2
)

// CompiledPlan is the cold-path handoff between compile and the rest of the
// engine (feature reader, then bar loop). Only fields needed by M4 consumers
// are materialised; later milestones will grow typed sub-plans (condition AST,
// per-exit param blocks, execution models) off this struct without re-parsing.
type CompiledPlan struct {
	// Major is 1 or 2 — ALWAYS check before touching V1Raw / V2Doc.
	Major MajorVersion

	// StrategyCode is a copy of the top-level strategy_code string (stable for
	// logs and for the run summary).
	StrategyCode string

	// Exchange / Symbols / Interval mirror instrument_scope. For v1 Interval is
	// empty (v1 did not carry an explicit interval; MVP assumed 1m).
	Exchange string
	Symbols  []string
	Interval string

	// AllowShort mirrors execution.allow_short / v1.execution.allow_short.
	AllowShort bool

	// FillModelKind is v2.execution.fill_model.kind ("next_bar_open" |
	// "same_bar_close"). Empty for v1.
	FillModelKind string

	// RequiredColumns is the set of physical feature-parquet-v1 columns the
	// engine must request from the reader (M3). For v2 this is derived from
	// feature_requirements.required_features; for v1 it is empty (the v1 legacy
	// executor does not consume the feature reader — see simulator.go).
	//
	// The slice is deduplicated and stable-ordered (insertion order of the
	// first occurrence of each column name).
	RequiredColumns []featuredata.ColumnName

	// SemanticWarnings propagates v2 semantic warnings from dispatch. For v1 it
	// is nil.
	SemanticWarnings []Warning

	// V1Raw is the validated v1 JSON, kept verbatim for the legacy v1 execution
	// path. Nil for Major == MajorV2.
	V1Raw json.RawMessage

	// V1 is the typed runtime-facing copy of the v1 DSL. Nil for MajorV2.
	// M7 executor binds against this instead of decoding V1Raw in the hot path.
	V1 *V1Plan

	// V2Doc is the decoded v2 Document (schema + semantic hard gates passed).
	// Nil for Major == MajorV1.
	V2Doc *dslv2.Document
}

// Warning is the compile-layer copy of dslv2.SemanticIssue limited to fields
// callers actually surface. Kept separate from dslv2.SemanticIssue so that
// downstream code does not grow a transitive import of strategy-dsl.
type Warning struct {
	Rule    string
	Path    string
	Message string
}
