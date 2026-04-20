package runtime

import (
	"fmt"
	"time"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

type floatColumnBinding struct {
	values []float64
	valid  func(int) bool
}

type boolColumnBinding struct {
	values []uint8
	valid  func(int) bool
}

type v1Bindings struct {
	tradeClose *featuredata.Int64Column
	floats     map[featuredata.ColumnName]floatColumnBinding
	bools      map[featuredata.ColumnName]boolColumnBinding
}

func bindV1Frame(frame *featuredata.FeatureFrame, plan *dslcompile.V1Plan) (*v1Bindings, error) {
	if frame == nil {
		return nil, fmt.Errorf("runtime: feature frame is nil")
	}
	if plan == nil {
		return nil, fmt.Errorf("runtime: v1 plan is nil")
	}
	tradeClose := frame.Int64s[featuredata.ColCloseTradeI64]
	if tradeClose == nil {
		return nil, fmt.Errorf("runtime: missing required trade_close column")
	}
	b := &v1Bindings{
		tradeClose: tradeClose,
		floats:     map[featuredata.ColumnName]floatColumnBinding{},
		bools:      map[featuredata.ColumnName]boolColumnBinding{},
	}
	for _, p := range plan.Entry.IndicatorConditions {
		if err := b.ensurePredicate(p, frame); err != nil {
			return nil, err
		}
	}
	for _, f := range plan.Filters {
		for _, name := range f.Allowed {
			col, ok := resolveV1FilterColumn(name)
			if !ok {
				return nil, fmt.Errorf("runtime: unsupported filter token %q", name)
			}
			if err := b.ensureBool(col, frame); err != nil {
				return nil, err
			}
		}
	}
	for _, col := range []featuredata.ColumnName{
		featuredata.ColTrendUp,
		featuredata.ColTrendDown,
		featuredata.ColFlat,
		featuredata.ColHighVol,
		featuredata.ColLowVol,
	} {
		_ = b.tryEnsureBool(col, frame)
	}
	return b, nil
}

func (b *v1Bindings) ensurePredicate(p dslcompile.V1Predicate, frame *featuredata.FeatureFrame) error {
	if err := b.ensureFloat(p.LeftColumn, frame); err != nil {
		return err
	}
	if p.RightKind == dslcompile.V1PredicateRightColumn {
		return b.ensureFloat(p.RightColumn, frame)
	}
	return nil
}

func (b *v1Bindings) ensureFloat(col featuredata.ColumnName, frame *featuredata.FeatureFrame) error {
	if _, ok := b.floats[col]; ok {
		return nil
	}
	fc := frame.Floats[col]
	if fc == nil {
		return fmt.Errorf("runtime: float column %q missing from FeatureFrame", col)
	}
	b.floats[col] = floatColumnBinding{values: fc.Values, valid: fc.IsValid}
	return nil
}

func (b *v1Bindings) ensureBool(col featuredata.ColumnName, frame *featuredata.FeatureFrame) error {
	if _, ok := b.bools[col]; ok {
		return nil
	}
	bc := frame.Bools[col]
	if bc == nil {
		return fmt.Errorf("runtime: bool column %q missing from FeatureFrame", col)
	}
	b.bools[col] = boolColumnBinding{values: bc.Values, valid: bc.IsValid}
	return nil
}

func (b *v1Bindings) tryEnsureBool(col featuredata.ColumnName, frame *featuredata.FeatureFrame) bool {
	if _, ok := b.bools[col]; ok {
		return true
	}
	bc := frame.Bools[col]
	if bc == nil {
		return false
	}
	b.bools[col] = boolColumnBinding{values: bc.Values, valid: bc.IsValid}
	return true
}

func barViewAt(frame *featuredata.FeatureFrame, b *v1Bindings, idx int) BarView {
	ts := time.UnixMilli(frame.Timestamps[idx]).UTC()
	ok := b.tradeClose != nil && idx < len(b.tradeClose.Values)
	var px float64
	if ok {
		px = float64(b.tradeClose.Values[idx]) / float64(pow10(frame.PriceScale))
	}
	return BarView{
		Index:        idx,
		Timestamp:    ts,
		TradeClose:   px,
		TradeCloseOK: ok,
	}
}

func evaluateV1Entry(plan *dslcompile.V1Plan, b *v1Bindings, idx int) bool {
	if plan == nil {
		return false
	}
	if !evaluateV1Filters(plan, b, idx) {
		return false
	}
	switch plan.Entry.Type {
	case "indicator_condition":
		if len(plan.Entry.IndicatorConditions) == 0 {
			return false
		}
		for _, p := range plan.Entry.IndicatorConditions {
			ok, valid := evaluatePredicate(p, b, idx)
			if !valid || !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func evaluateV1Filters(plan *dslcompile.V1Plan, b *v1Bindings, idx int) bool {
	for _, filter := range plan.Filters {
		switch filter.Type {
		case "regime_filter", "volatility_filter":
			match := false
			seenValid := false
			for _, allowed := range filter.Allowed {
				col, ok := resolveV1FilterColumn(allowed)
				if !ok {
					return false
				}
				bound, ok := b.bools[col]
				if !ok || !bound.valid(idx) {
					continue
				}
				seenValid = true
				if bound.values[idx] == 1 {
					match = true
					break
				}
			}
			if !seenValid || !match {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func evaluatePredicate(p dslcompile.V1Predicate, b *v1Bindings, idx int) (match bool, valid bool) {
	left, ok := b.floats[p.LeftColumn]
	if !ok || !left.valid(idx) {
		return false, false
	}
	lv := left.values[idx]
	var rv float64
	switch p.RightKind {
	case dslcompile.V1PredicateRightNumber:
		rv = p.RightNumber
	case dslcompile.V1PredicateRightColumn:
		right, ok := b.floats[p.RightColumn]
		if !ok || !right.valid(idx) {
			return false, false
		}
		rv = right.values[idx]
	default:
		return false, false
	}
	switch p.Op {
	case dslcompile.V1PredicateGT:
		return lv > rv, true
	case dslcompile.V1PredicateLT:
		return lv < rv, true
	default:
		return false, false
	}
}

func resolveV1FilterColumn(name string) (featuredata.ColumnName, bool) {
	switch name {
	case "trend_up":
		return featuredata.ColTrendUp, true
	case "trend_down":
		return featuredata.ColTrendDown, true
	case "flat":
		return featuredata.ColFlat, true
	case "high_vol":
		return featuredata.ColHighVol, true
	case "low_vol":
		return featuredata.ColLowVol, true
	default:
		return "", false
	}
}

func inferRegimeCode(b *v1Bindings, idx int) string {
	if b == nil {
		return ""
	}
	trend := ""
	switch {
	case boolColumnTrueAt(b, featuredata.ColTrendUp, idx):
		trend = "trend_up"
	case boolColumnTrueAt(b, featuredata.ColTrendDown, idx):
		trend = "trend_down"
	case boolColumnTrueAt(b, featuredata.ColFlat, idx):
		trend = "flat"
	}
	vol := ""
	switch {
	case boolColumnTrueAt(b, featuredata.ColHighVol, idx):
		vol = "high_vol"
	case boolColumnTrueAt(b, featuredata.ColLowVol, idx):
		vol = "low_vol"
	}
	switch {
	case trend != "" && vol != "":
		return trend + "_" + vol
	case trend != "":
		return trend
	case vol != "":
		return vol
	default:
		return ""
	}
}

func boolColumnTrueAt(b *v1Bindings, col featuredata.ColumnName, idx int) bool {
	bound, ok := b.bools[col]
	if !ok || !bound.valid(idx) {
		return false
	}
	return bound.values[idx] == 1
}

func pow10(n int32) int64 {
	v := int64(1)
	for i := int32(0); i < n; i++ {
		v *= 10
	}
	return v
}
