package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featurecompat"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

type runtimePreflightRequest struct {
	DSLJSON           json.RawMessage `json:"dsl_json"`
	FeatureSetCode    string          `json:"feature_set_code,omitempty"`
	FeatureSetVersion int             `json:"feature_set_version,omitempty"`
}

type runtimePreflightIssue struct {
	Kind    string `json:"kind,omitempty"`
	Path    string `json:"path,omitempty"`
	Rule    string `json:"rule,omitempty"`
	Message string `json:"message,omitempty"`
}

type runtimeCompiledSummary struct {
	StrategyCode  string   `json:"strategy_code"`
	SchemaVersion string   `json:"schema_version,omitempty"`
	RuntimeMajor  int      `json:"runtime_major,omitempty"`
	Exchange      string   `json:"exchange,omitempty"`
	Symbols       []string `json:"symbols,omitempty"`
	Interval      string   `json:"interval,omitempty"`
	AllowShort    bool     `json:"allow_short,omitempty"`
	FillModelKind string   `json:"fill_model_kind,omitempty"`
}

type runtimePreflightResponse struct {
	RuntimeSupported   bool                    `json:"runtime_supported"`
	RequiredColumns    []string                `json:"required_columns,omitempty"`
	UnsupportedReasons []string                `json:"unsupported_reasons,omitempty"`
	Warnings           []runtimePreflightIssue `json:"warnings,omitempty"`
	CompiledSummary    *runtimeCompiledSummary `json:"compiled_summary,omitempty"`
	EngineVersion      string                  `json:"engine_version,omitempty"`
}

func registerPreflightRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/preflight/strategy-runtime", handleStrategyRuntimePreflight)
}

func handleStrategyRuntimePreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req runtimePreflightRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	plan, err := dslcompile.Compile(req.DSLJSON)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	resp := runtimePreflightResponse{
		RuntimeSupported: true,
		RequiredColumns:  stringifyColumns(plan.RequiredColumns),
		CompiledSummary: &runtimeCompiledSummary{
			StrategyCode:  plan.StrategyCode,
			RuntimeMajor:  int(plan.Major),
			Exchange:      plan.Exchange,
			Symbols:       append([]string(nil), plan.Symbols...),
			Interval:      plan.Interval,
			AllowShort:    plan.AllowShort,
			FillModelKind: plan.FillModelKind,
			SchemaVersion: schemaVersionFromPlan(plan),
		},
		EngineVersion: engineVersion,
	}
	for _, wItem := range plan.SemanticWarnings {
		resp.Warnings = append(resp.Warnings, runtimePreflightIssue{
			Kind:    "semantic_warning",
			Path:    wItem.Path,
			Rule:    wItem.Rule,
			Message: wItem.Message,
		})
	}
	if plan.Major != dslcompile.MajorV1 {
		resp.RuntimeSupported = false
		resp.UnsupportedReasons = append(resp.UnsupportedReasons, "runtime preflight currently supports executable v1 plans only; v2 executor is not implemented yet")
	}
	if strings.TrimSpace(req.FeatureSetCode) == "" || req.FeatureSetVersion <= 0 {
		resp.RuntimeSupported = false
		resp.UnsupportedReasons = append(resp.UnsupportedReasons, "feature_set_code and feature_set_version are required for truthful runtime preflight (feature contract compatibility must be checked)")
	} else {
		key := featuredata.FeatureSetKey{Code: req.FeatureSetCode, Version: req.FeatureSetVersion}
		if err := featurecompat.Check(key, plan.RequiredColumns, nil); err != nil {
			resp.RuntimeSupported = false
			resp.UnsupportedReasons = append(resp.UnsupportedReasons, err.Error())
		}
	}
	if plan.FillModelKind != "" && plan.FillModelKind != "same_bar_close" {
		resp.RuntimeSupported = false
		resp.UnsupportedReasons = append(resp.UnsupportedReasons, "only fill_model_kind=same_bar_close is executable on the current runtime")
	}
	writeJSON(w, http.StatusOK, resp)
}

func stringifyColumns(in []featuredata.ColumnName) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, c := range in {
		out = append(out, string(c))
	}
	return out
}

func schemaVersionFromPlan(plan *dslcompile.CompiledPlan) string {
	if plan == nil {
		return ""
	}
	switch plan.Major {
	case dslcompile.MajorV1:
		return "1.x"
	case dslcompile.MajorV2:
		return "2.x"
	default:
		return ""
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func isCompatError(err error) bool {
	return errors.Is(err, featurecompat.ErrUnsupportedFeatureSet) || errors.Is(err, featurecompat.ErrUnknownColumn)
}
