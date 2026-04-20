package dslcompile

import (
	"errors"
	"fmt"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
	"github.com/algorhythm-llc/strategy-dsl/dispatch"
	dslv2 "github.com/algorhythm-llc/strategy-dsl/v2"
)

// Compile turns a raw DSL JSON document (typically cpclient.StrategyVersion.DSLJSON)
// into a typed CompiledPlan. It is the single M4 entry point for the worker:
//
//  1. Validate (defence-in-depth) via dispatch.Parse. ADR-004 v2 requires the
//     engine re-runs the same validator pair on consume even though
//     control-plane already validated on write.
//  2. Branch on Major.
//  3. Normalise into CompiledPlan with no JSON-carrying fields for the hot
//     loop to touch.
//  4. Extract RequiredColumns in a form featuredata.ReadFeatureFrame accepts
//     verbatim (via CompiledPlan.NewReadRequest).
//
// Errors:
//
//   - ErrEmptyPayload when raw is empty.
//   - *ParseError wrapping any error from dispatch.Parse (schema violation,
//     v2 semantic hard error, dispatch.ErrUnsupportedVersion). The underlying
//     error is preserved for errors.Is / errors.As.
//   - *ColumnError when a v2 required_features / optional_features entry
//     references a feature name not part of feature-parquet-v1.
func Compile(raw []byte) (*CompiledPlan, error) {
	if len(raw) == 0 {
		return nil, ErrEmptyPayload
	}
	res, err := dispatch.Parse(raw)
	if err != nil {
		if errors.Is(err, dispatch.ErrUnsupportedVersion) {
			return nil, fmt.Errorf("%w: %w", ErrUnsupportedVersion, err)
		}
		return nil, &ParseError{Err: err}
	}
	switch res.Major {
	case dispatch.MajorV1:
		return compileV1(res.V1JSON)
	case dispatch.MajorV2:
		return compileV2(res.V2, res.V2SemanticWarnings)
	default:
		return nil, ErrUnsupportedVersion
	}
}

func compileV2(doc *dslv2.Document, semWarns []dslv2.SemanticIssue) (*CompiledPlan, error) {
	if doc == nil {
		return nil, &ParseError{Err: errors.New("dispatch returned nil v2 document")}
	}
	cols, err := extractRequiredColumns(doc.FeatureRequirements)
	if err != nil {
		return nil, err
	}
	return &CompiledPlan{
		Major:            dispatch.MajorV2,
		StrategyCode:     doc.StrategyCode,
		Exchange:         doc.InstrumentScope.Exchange,
		Symbols:          append([]string(nil), doc.InstrumentScope.Symbols...),
		Interval:         doc.InstrumentScope.Interval,
		AllowShort:       doc.Execution.AllowShort,
		FillModelKind:    doc.Execution.FillModel.Kind,
		RequiredColumns:  cols,
		SemanticWarnings: copyWarnings(semWarns),
		V2Doc:            doc,
	}, nil
}

// extractRequiredColumns walks feature_requirements.required_features (the only
// mandatory binding source; optional_features carry a documentation warning
// already surfaced by the semantic layer, see v2/README.md §"Warnings S2").
// Columns are deduplicated while preserving first-seen order so the reader's
// downstream materialisation order is stable across runs.
func extractRequiredColumns(fr dslv2.FeatureRequirements) ([]featuredata.ColumnName, error) {
	seen := make(map[featuredata.ColumnName]struct{}, len(fr.RequiredFeatures))
	out := make([]featuredata.ColumnName, 0, len(fr.RequiredFeatures))
	for i, f := range fr.RequiredFeatures {
		col, ok := resolveFeatureColumn(f.Name)
		if !ok {
			return nil, &ColumnError{
				Index:     i,
				Optional:  false,
				Name:      f.Name,
				Namespace: f.Namespace,
				Timeframe: f.Timeframe,
				Symbol:    f.Symbol,
			}
		}
		if _, dup := seen[col]; dup {
			// A duplicate resolved column can legitimately happen if two
			// required_features entries differ only by a field we do not yet
			// bind on (e.g. namespace / timeframe). The v2 semantic validator
			// already rejects duplicates *after* default-filling (hard rule
			// §9); anything that survives is a distinct binding at the DSL
			// level but maps to the same physical column under v1 features.
			// Drop silently at compile time — binding nuance is M5.
			continue
		}
		seen[col] = struct{}{}
		out = append(out, col)
	}
	return out, nil
}

func copyWarnings(in []dslv2.SemanticIssue) []Warning {
	if len(in) == 0 {
		return nil
	}
	out := make([]Warning, 0, len(in))
	for _, w := range in {
		out = append(out, Warning{Rule: w.Rule, Path: w.Path, Message: w.Message})
	}
	return out
}

// NewReadRequest builds a featuredata.ReadRequest from the compiled plan and
// the externally-resolved dataset bindings (FeatureSet + partitions).
//
// This keeps dslcompile ignorant of how the engine chooses the dataset (via
// control-plane experiment_batch → feature_set_version → dataset), while
// still producing the exact ReadRequest the reader expects. ExpectedSymbol
// is taken from the caller (control-plane run row), not the DSL — the run
// binds a single concrete symbol per the stage-3 contract.
func (p *CompiledPlan) NewReadRequest(fs featuredata.FeatureSetKey, symbol string, partitions []featuredata.Partition) featuredata.ReadRequest {
	return featuredata.ReadRequest{
		FeatureSet:      fs,
		ExpectedSymbol:  symbol,
		Partitions:      partitions,
		RequiredColumns: append([]featuredata.ColumnName(nil), p.RequiredColumns...),
	}
}
