package dslcompile

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

// V1Plan is the typed, runtime-facing copy of the v1 DSL contract. It is the
// executor input for M7; V1Raw stays on CompiledPlan only for diagnostics /
// parity with older tests, not for hot-path interpretation.
type V1Plan struct {
	Entry     V1EntryPlan
	Exit      V1ExitPlan
	Filters   []V1FilterPlan
	Risk      V1RiskPlan
	Execution V1ExecutionPlan
}

type V1EntryPlan struct {
	Type                string
	IndicatorConditions []V1Predicate
}

type V1ExitPlan struct {
	Type            string
	TakeProfitBps   int
	StopLossBps     int
	TrailingStopBps int
	MaxHoldingBars  int
}

type V1FilterPlan struct {
	Type    string
	Allowed []string
}

type V1RiskPlan struct {
	Type          string
	RiskBps       int
	Amount        float64
	FixedNotional float64
}

type V1ExecutionPlan struct {
	FeeBps       int
	SlippageBps  int
	AllowShort   bool
	FillModelKind string
}

type V1PredicateOp string

const (
	V1PredicateGT V1PredicateOp = "gt"
	V1PredicateLT V1PredicateOp = "lt"
)

type V1PredicateRightKind string

const (
	V1PredicateRightColumn V1PredicateRightKind = "column"
	V1PredicateRightNumber V1PredicateRightKind = "number"
)

// V1Predicate is the minimal executable unit for `indicator_condition`.
// Example tokens:
//   - "ema_20_gt_ema_50"
//   - "rsi_14_lt_30000"
type V1Predicate struct {
	Token       string
	LeftColumn  featuredata.ColumnName
	Op          V1PredicateOp
	RightKind   V1PredicateRightKind
	RightColumn featuredata.ColumnName
	RightNumber float64
}

type v1TypedBlock struct {
	Type   string          `json:"type"`
	Params json.RawMessage `json:"params"`
}

type v1Doc struct {
	SchemaVersion   string         `json:"schema_version"`
	StrategyCode    string         `json:"strategy_code"`
	InstrumentScope v1Instrument   `json:"instrument_scope"`
	Entry           v1TypedBlock   `json:"entry"`
	Exit            v1TypedBlock   `json:"exit"`
	Filters         []v1TypedBlock `json:"filters"`
	Risk            v1TypedBlock   `json:"risk"`
	Execution       struct {
		FeeBps      int  `json:"fee_bps"`
		SlippageBps int  `json:"slippage_bps"`
		AllowShort  bool `json:"allow_short"`
	} `json:"execution"`
}

type v1Instrument struct {
	Exchange string   `json:"exchange"`
	Symbols  []string `json:"symbols"`
}

func compileV1(raw json.RawMessage) (*CompiledPlan, error) {
	var doc v1Doc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, &ParseError{Err: fmt.Errorf("decode v1 shape: %w", err)}
	}
	v1Plan, cols, err := parseV1Plan(doc)
	if err != nil {
		return nil, &ParseError{Err: fmt.Errorf("parse v1 runtime plan: %w", err)}
	}
	return &CompiledPlan{
		Major:           MajorV1,
		StrategyCode:    doc.StrategyCode,
		Exchange:        doc.InstrumentScope.Exchange,
		Symbols:         append([]string(nil), doc.InstrumentScope.Symbols...),
		Interval:        "",
		AllowShort:      doc.Execution.AllowShort,
		FillModelKind:   v1Plan.Execution.FillModelKind,
		RequiredColumns: cols,
		V1Raw:           append(json.RawMessage(nil), raw...),
		V1:              v1Plan,
	}, nil
}

func parseV1Plan(doc v1Doc) (*V1Plan, []featuredata.ColumnName, error) {
	var cols []featuredata.ColumnName
	seen := map[featuredata.ColumnName]struct{}{}
	addCol := func(c featuredata.ColumnName) {
		if _, ok := seen[c]; ok {
			return
		}
		seen[c] = struct{}{}
		cols = append(cols, c)
	}

	plan := &V1Plan{
		Execution: V1ExecutionPlan{
			FeeBps:        doc.Execution.FeeBps,
			SlippageBps:   doc.Execution.SlippageBps,
			AllowShort:    doc.Execution.AllowShort,
			FillModelKind: "same_bar_close",
		},
	}
	entry, err := parseV1Entry(doc.Entry, addCol)
	if err != nil {
		return nil, nil, err
	}
	plan.Entry = entry

	exit, err := parseV1Exit(doc.Exit)
	if err != nil {
		return nil, nil, err
	}
	plan.Exit = exit

	filters, err := parseV1Filters(doc.Filters, addCol)
	if err != nil {
		return nil, nil, err
	}
	plan.Filters = filters

	risk, err := parseV1Risk(doc.Risk)
	if err != nil {
		return nil, nil, err
	}
	plan.Risk = risk

	// v1 market execution always needs the trade close for fill / PnL.
	addCol(featuredata.ColCloseTradeI64)

	return plan, cols, nil
}

func parseV1Entry(in v1TypedBlock, addCol func(featuredata.ColumnName)) (V1EntryPlan, error) {
	out := V1EntryPlan{Type: in.Type}
	switch in.Type {
	case "indicator_condition":
		var params struct {
			Left  string `json:"left"`
			Right string `json:"right"`
		}
		if err := json.Unmarshal(in.Params, &params); err != nil {
			return out, err
		}
		for _, token := range []string{params.Left, params.Right} {
			if strings.TrimSpace(token) == "" {
				continue
			}
			p, err := parseV1Predicate(token)
			if err != nil {
				return out, err
			}
			addCol(p.LeftColumn)
			if p.RightKind == V1PredicateRightColumn {
				addCol(p.RightColumn)
			}
			out.IndicatorConditions = append(out.IndicatorConditions, p)
		}
	}
	return out, nil
}

func parseV1Exit(in v1TypedBlock) (V1ExitPlan, error) {
	out := V1ExitPlan{Type: in.Type}
	switch in.Type {
	case "tp_sl":
		var params struct {
			TakeProfitBps int `json:"take_profit_bps"`
			StopLossBps   int `json:"stop_loss_bps"`
		}
		if err := json.Unmarshal(in.Params, &params); err != nil {
			return out, err
		}
		out.TakeProfitBps = params.TakeProfitBps
		out.StopLossBps = params.StopLossBps
	case "trailing_stop":
		var params struct {
			TrailingStopBps int `json:"trailing_stop_bps"`
		}
		if err := json.Unmarshal(in.Params, &params); err != nil {
			return out, err
		}
		out.TrailingStopBps = params.TrailingStopBps
	case "time_based":
		var params struct {
			MaxHoldingBars int `json:"max_holding_bars"`
		}
		if err := json.Unmarshal(in.Params, &params); err != nil {
			return out, err
		}
		out.MaxHoldingBars = params.MaxHoldingBars
	}
	return out, nil
}

func parseV1Filters(in []v1TypedBlock, addCol func(featuredata.ColumnName)) ([]V1FilterPlan, error) {
	out := make([]V1FilterPlan, 0, len(in))
	for _, block := range in {
		fp := V1FilterPlan{Type: block.Type}
		switch block.Type {
		case "regime_filter", "volatility_filter":
			var params struct {
				Allowed []string `json:"allowed"`
			}
			if err := json.Unmarshal(block.Params, &params); err != nil {
				return nil, err
			}
			fp.Allowed = append([]string(nil), params.Allowed...)
			for _, name := range fp.Allowed {
				col, ok := resolveFeatureColumn(name)
				if !ok {
					return nil, fmt.Errorf("unsupported filter token %q", name)
				}
				addCol(col)
			}
		}
		out = append(out, fp)
	}
	return out, nil
}

func parseV1Risk(in v1TypedBlock) (V1RiskPlan, error) {
	out := V1RiskPlan{Type: in.Type}
	switch in.Type {
	case "fixed_fraction":
		var params struct {
			RiskBps int `json:"risk_bps"`
		}
		if err := json.Unmarshal(in.Params, &params); err != nil {
			return out, err
		}
		out.RiskBps = params.RiskBps
	case "fixed_amount":
		var params struct {
			Amount        float64 `json:"amount"`
			FixedNotional float64 `json:"fixed_notional"`
		}
		if err := json.Unmarshal(in.Params, &params); err != nil {
			return out, err
		}
		out.Amount = params.Amount
		out.FixedNotional = params.FixedNotional
	}
	return out, nil
}

func parseV1Predicate(token string) (V1Predicate, error) {
	if left, right, ok := strings.Cut(token, "_gt_"); ok {
		return buildV1Predicate(token, left, V1PredicateGT, right)
	}
	if left, right, ok := strings.Cut(token, "_lt_"); ok {
		return buildV1Predicate(token, left, V1PredicateLT, right)
	}
	return V1Predicate{}, fmt.Errorf("unsupported v1 predicate %q", token)
}

func buildV1Predicate(token, left string, op V1PredicateOp, right string) (V1Predicate, error) {
	leftCol, ok := resolveFeatureColumn(left)
	if !ok {
		return V1Predicate{}, fmt.Errorf("unsupported left feature %q in %q", left, token)
	}
	out := V1Predicate{Token: token, LeftColumn: leftCol, Op: op}
	if rightCol, ok := resolveFeatureColumn(right); ok {
		out.RightKind = V1PredicateRightColumn
		out.RightColumn = rightCol
		return out, nil
	}
	n, err := strconv.ParseFloat(right, 64)
	if err != nil {
		return V1Predicate{}, fmt.Errorf("unsupported right operand %q in %q", right, token)
	}
	out.RightKind = V1PredicateRightNumber
	out.RightNumber = n
	return out, nil
}
