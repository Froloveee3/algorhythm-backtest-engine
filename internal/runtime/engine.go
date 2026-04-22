package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/execution"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/results"
)

var (
	ErrUnsupportedRuntime = errors.New("runtime: unsupported strategy runtime")
	ErrInvalidData        = errors.New("runtime: invalid runtime data")
	ErrExecutionFailed    = errors.New("runtime: execution failed")
)

func RunV1(ctx context.Context, meta RunMetadata, plan *dslcompile.CompiledPlan, frame *featuredata.FeatureFrame) (*results.RunResult, error) {
	if plan == nil || plan.V1 == nil {
		return nil, fmt.Errorf("%w: missing typed v1 plan", ErrUnsupportedRuntime)
	}
	if plan.Major != dslcompile.MajorV1 {
		return nil, fmt.Errorf("%w: major=%d", ErrUnsupportedRuntime, plan.Major)
	}
	if frame == nil {
		return nil, fmt.Errorf("%w: FeatureFrame is nil", ErrInvalidData)
	}
	if plan.FillModelKind != "" && plan.FillModelKind != "same_bar_close" {
		return nil, fmt.Errorf("%w: fill_model=%q", ErrUnsupportedRuntime, plan.FillModelKind)
	}
	if frame.RowCount == 0 {
		return nil, fmt.Errorf("%w: empty FeatureFrame", ErrInvalidData)
	}
	bindings, err := bindV1Frame(frame, plan.V1)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidData, err)
	}
	state := NewState()
	equity := make([]results.EquityPoint, 0, frame.RowCount+1)
	equity = append(equity, results.EquityPoint{
		RunID:       meta.RunID,
		TS:          barViewAt(frame, bindings, 0).Timestamp,
		Equity:      state.Equity,
		DrawdownAbs: 0,
		DrawdownPct: 0,
	})
	trades := make([]results.Trade, 0)
	returns := make([]float64, 0)

	for i := 0; i < frame.RowCount; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		bar := barViewAt(frame, bindings, i)
		state.CurrentBarIndex = i
		state.CurrentTime = bar.Timestamp
		if !bar.TradeCloseOK || bar.TradeClose <= 0 {
			continue
		}

		exitedThisBar := false
		flipRequested := false
		if state.Position.Open {
			reason, ok := exitReason(plan.V1, plan.AllowShort, &state.Position, bindings, i, bar)
			if ok {
				trade, ret, err := closePosition(meta, &state, bar, plan.V1.Execution)
				if err != nil {
					return nil, fmt.Errorf("%w: %v", ErrExecutionFailed, err)
				}
				trade.TradeIndex = uint32(len(trades))
				trades = append(trades, trade)
				returns = append(returns, ret)
				exitedThisBar = true
				// PR-07/09: default close → suppress both same-bar reopen and
				// next-bar re-entry (2-bar cooldown). `continuous`/`flip` allow
				// next-bar re-entry; `flip` additionally permits same-bar open
				// of the opposite side below.
				state.BlockedEntryUntilBar = nextEntryBarAfterExit(plan.V1, bar.Index)
				if reason == exitReasonFlip {
					flipRequested = true
				}
			}
		}
		if flipRequested {
			if sideOK, side := flipTargetSide(plan.V1, plan.AllowShort, bindings, i); sideOK {
				if err := openPosition(meta, &state, bar, plan.V1, side, inferRegimeCode(bindings, i)); err != nil {
					return nil, fmt.Errorf("%w: %v", ErrExecutionFailed, err)
				}
				// Same-bar reversal: the new position is now active and no
				// cooldown applies to it.
				state.BlockedEntryUntilBar = -1
			}
		} else if !exitedThisBar && !state.Position.Open && i >= state.BlockedEntryUntilBar {
			if side, ok := pickV1EntrySide(plan.V1, plan.AllowShort, bindings, i); ok {
				if err := openPosition(meta, &state, bar, plan.V1, side, inferRegimeCode(bindings, i)); err != nil {
					return nil, fmt.Errorf("%w: %v", ErrExecutionFailed, err)
				}
			}
		}

		markEquityPoint(meta, &state, &equity, bar)
	}
	if state.Position.Open {
		bar := barViewAt(frame, bindings, frame.RowCount-1)
		trade, ret, err := closePosition(meta, &state, bar, plan.V1.Execution)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrExecutionFailed, err)
		}
		trade.TradeIndex = uint32(len(trades))
		trades = append(trades, trade)
		returns = append(returns, ret)
		markEquityPoint(meta, &state, &equity, bar)
	}

	metrics := results.RunMetrics{
		RunID:             meta.RunID,
		StrategyVersionID: meta.StrategyVersionID,
		Symbol:            meta.Symbol,
		PeriodFrom:        equity[0].TS,
		PeriodTo:          equity[len(equity)-1].TS,
		PnLAbs:            state.Equity - StartingEquity,
		PnLPct:            float32((state.Equity - StartingEquity) / StartingEquity * 100.0),
		SharpeRatio:       float32(results.Sharpe(returns)),
		SortinoRatio:      float32(results.Sortino(returns)),
		MaxDrawdownAbs:    maxDrawdownAbs(equity),
		MaxDrawdownPct:    maxDrawdownPct(equity),
		TradesTotal:       state.TradesTotal,
		TradesWon:         state.TradesWon,
		TradesLost:        state.TradesLost,
		ProfitFactor:      results.ProfitFactor(state.GrossProfit, state.GrossLoss),
		Expectancy:        results.Expectancy(state.Equity-StartingEquity, state.TradesTotal),
		RegimeBreakdown:   aggregateRegimeBreakdown(trades),
	}
	return &results.RunResult{Trades: trades, Equity: equity, Metrics: metrics}, nil
}

func inferEntrySide(plan *dslcompile.V1Plan, b *v1Bindings, idx int) execution.Side {
	if plan == nil || len(plan.Entry.IndicatorConditions) == 0 {
		return execution.SideLong
	}
	first := plan.Entry.IndicatorConditions[0]
	if first.Op == dslcompile.V1PredicateLT {
		return execution.SideShort
	}
	return execution.SideLong
}

func pickV1EntrySide(plan *dslcompile.V1Plan, allowShort bool, b *v1Bindings, idx int) (execution.Side, bool) {
	if plan == nil || b == nil {
		return execution.SideLong, false
	}
	if allowShort && plan.SymmetricShortEntry {
		if !evaluateV1Entry(plan, b, idx) {
			return execution.SideLong, false
		}
		return inferEntrySide(plan, b, idx), true
	}

	longOK := evaluateV1IndicatorEntry(plan, plan.Entry, b, idx)
	shortOK := false
	if allowShort {
		shortOK = evaluateV1IndicatorEntry(plan, plan.EntryShort, b, idx)
	}
	switch {
	case longOK && shortOK:
		// PR-07 deterministic tie-break: long wins; no same-bar flip/reopen.
		return execution.SideLong, true
	case longOK:
		return execution.SideLong, true
	case shortOK:
		return execution.SideShort, true
	default:
		return execution.SideLong, false
	}
}

// exitReasonCode enumerates the PR-07/08/09 exit triggers. The runtime uses
// it to decide whether a same-bar reverse open is permitted (flip only).
type exitReasonCode int

const (
	exitReasonNone exitReasonCode = iota
	exitReasonCloseSide
	exitReasonMechanical
	exitReasonSignalHold
	exitReasonFlip
)

// exitReason computes the ordered precedence from PR-09 §2:
//  1. close_* (PR-07) — wins over all.
//  2. mechanical exit (tp_sl/trailing_stop/time_based) — only if !signal_only.
//  3. flip-close — only if reentry_mode=flip and opposite-side entry fires.
//  4. signal-hold exit — only if signal_only (PR-08).
//
// Returns (reason, true) when the position should be closed at this bar.
func exitReason(plan *dslcompile.V1Plan, allowShort bool, pos *PositionState, b *v1Bindings, idx int, bar BarView) (exitReasonCode, bool) {
	if plan == nil || pos == nil || !pos.Open {
		return exitReasonNone, false
	}
	if shouldExitCloseSide(plan, pos, b, idx) {
		return exitReasonCloseSide, true
	}
	if !plan.Execution.SignalOnly {
		if shouldExitMechanical(plan, pos, bar) {
			return exitReasonMechanical, true
		}
		if plan.Execution.ReentryMode == dslcompile.V1ReentryFlip && flipOppositeSignalFires(plan, allowShort, pos, b, idx) {
			return exitReasonFlip, true
		}
		return exitReasonNone, false
	}
	if plan.Execution.ReentryMode == dslcompile.V1ReentryFlip && flipOppositeSignalFires(plan, allowShort, pos, b, idx) {
		return exitReasonFlip, true
	}
	if shouldExitSignalHold(plan, allowShort, pos, b, idx) {
		return exitReasonSignalHold, true
	}
	return exitReasonNone, false
}

// flipOppositeSignalFires returns true when an entry signal for the side
// OPPOSITE to the current position is active at idx (and that side is allowed).
// Only called under reentry_mode=flip.
func flipOppositeSignalFires(plan *dslcompile.V1Plan, allowShort bool, pos *PositionState, b *v1Bindings, idx int) bool {
	if plan == nil || pos == nil || !pos.Open {
		return false
	}
	switch pos.Side {
	case execution.SideLong:
		if !allowShort {
			return false
		}
		// Symmetric-short shape cannot reliably report "short signal" without
		// also implying long; flip requires explicit entry_short to avoid
		// ambiguity.
		if plan.SymmetricShortEntry {
			return false
		}
		return evaluateV1IndicatorEntry(plan, plan.EntryShort, b, idx)
	case execution.SideShort:
		return evaluateV1IndicatorEntry(plan, plan.Entry, b, idx)
	default:
		return false
	}
}

// flipTargetSide derives the reverse-side to open when a flip-close fires at
// bar idx. The opposite-side signal must still be valid at idx; if both sides
// also fire, long wins (same tie-break as flat entry).
func flipTargetSide(plan *dslcompile.V1Plan, allowShort bool, b *v1Bindings, idx int) (bool, execution.Side) {
	if plan == nil {
		return false, execution.SideLong
	}
	longOK := evaluateV1IndicatorEntry(plan, plan.Entry, b, idx)
	shortOK := false
	if allowShort && !plan.SymmetricShortEntry {
		shortOK = evaluateV1IndicatorEntry(plan, plan.EntryShort, b, idx)
	}
	switch {
	case longOK:
		return true, execution.SideLong
	case shortOK:
		return true, execution.SideShort
	default:
		return false, execution.SideLong
	}
}

func oppositeSide(s execution.Side) execution.Side {
	if s == execution.SideLong {
		return execution.SideShort
	}
	return execution.SideLong
}

// nextEntryBarAfterExit encodes PR-09 cooldown: `single` → bar+2; otherwise
// (continuous/flip) → bar+1.
func nextEntryBarAfterExit(plan *dslcompile.V1Plan, barIndex int) int {
	if plan != nil && (plan.Execution.ReentryMode == dslcompile.V1ReentryContinuous || plan.Execution.ReentryMode == dslcompile.V1ReentryFlip) {
		return barIndex + 1
	}
	return barIndex + 2
}

func openPosition(meta RunMetadata, state *RuntimeState, bar BarView, plan *dslcompile.V1Plan, side execution.Side, regimeCode string) error {
	qty := positionQuantity(plan, bar.TradeClose)
	fill, err := execution.ApplyMarketFill(side, bar.TradeClose, qty, execution.FillConfig{
		FeeBps:      plan.Execution.FeeBps,
		SlippageBps: plan.Execution.SlippageBps,
	}, true)
	if err != nil {
		return err
	}
	state.Position = PositionState{
		Open:          true,
		Side:          side,
		EntryBarIndex: bar.Index,
		EntryTime:     bar.Timestamp,
		EntryPrice:    fill.Price,
		Quantity:      fill.Quantity,
		EntryFeesAbs:  fill.FeesAbs,
		PeakPrice:     fill.Price,
		TroughPrice:   fill.Price,
		RegimeCode:    regimeCode,
	}
	state.FeesPaid += fill.FeesAbs
	_ = meta
	return nil
}

func closePosition(meta RunMetadata, state *RuntimeState, bar BarView, execPlan dslcompile.V1ExecutionPlan) (results.Trade, float64, error) {
	pos := state.Position
	fill, err := execution.ApplyMarketFill(pos.Side, bar.TradeClose, pos.Quantity, execution.FillConfig{
		FeeBps:      execPlan.FeeBps,
		SlippageBps: execPlan.SlippageBps,
	}, false)
	if err != nil {
		return results.Trade{}, 0, err
	}
	pnlGross := signedPnL(pos.Side, pos.EntryPrice, fill.Price, fill.Quantity)
	fees := pos.EntryFeesAbs + fill.FeesAbs
	pnlNet := pnlGross - fees
	state.RealizedPnL += pnlNet
	state.FeesPaid += fill.FeesAbs
	state.TradesTotal++
	if pnlNet >= 0 {
		state.TradesWon++
		state.GrossProfit += pnlNet
	} else {
		state.TradesLost++
		state.GrossLoss += -pnlNet
	}
	state.Position = PositionState{}
	equity := StartingEquity + state.RealizedPnL
	state.MarkEquity(equity)
	ret := pnlNet / StartingEquity
	trade := results.Trade{
		RunID:       meta.RunID,
		Symbol:      meta.Symbol,
		Side:        sideString(pos.Side),
		EntryTime:   pos.EntryTime,
		ExitTime:    bar.Timestamp,
		EntryPrice:  pos.EntryPrice,
		ExitPrice:   fill.Price,
		Quantity:    fill.Quantity,
		PnLAbs:      pnlNet,
		PnLBps:      toPnLBps(pnlNet, fill.Notional),
		FeesAbs:     fees,
		SlippageBps: fill.SlippageBps,
		RegimeCode:  pos.RegimeCode,
	}
	return trade, ret, nil
}

func shouldExitMechanical(plan *dslcompile.V1Plan, pos *PositionState, bar BarView) bool {
	if pos == nil || !pos.Open {
		return false
	}
	switch plan.Exit.Type {
	case "tp_sl":
		moveBps := sideMoveBps(pos.Side, pos.EntryPrice, bar.TradeClose)
		return moveBps >= float64(plan.Exit.TakeProfitBps) || moveBps <= -float64(plan.Exit.StopLossBps)
	case "time_based":
		if plan.Exit.MaxHoldingBars <= 0 {
			return false
		}
		return bar.Index-pos.EntryBarIndex >= plan.Exit.MaxHoldingBars
	case "trailing_stop":
		if plan.Exit.TrailingStopBps <= 0 {
			return false
		}
		if pos.Side == execution.SideLong {
			if bar.TradeClose > pos.PeakPrice {
				pos.PeakPrice = bar.TradeClose
			}
			return ((pos.PeakPrice - bar.TradeClose) / pos.PeakPrice * 10_000.0) >= float64(plan.Exit.TrailingStopBps)
		}
		if pos.TroughPrice == 0 || bar.TradeClose < pos.TroughPrice {
			pos.TroughPrice = bar.TradeClose
		}
		return ((bar.TradeClose - pos.TroughPrice) / pos.TroughPrice * 10_000.0) >= float64(plan.Exit.TrailingStopBps)
	default:
		return false
	}
}

func shouldExitCloseSide(plan *dslcompile.V1Plan, pos *PositionState, b *v1Bindings, idx int) bool {
	if plan == nil || pos == nil || !pos.Open || b == nil {
		return false
	}
	switch pos.Side {
	case execution.SideLong:
		return evaluateV1IndicatorEntry(plan, plan.CloseLong, b, idx)
	case execution.SideShort:
		return evaluateV1IndicatorEntry(plan, plan.CloseShort, b, idx)
	default:
		return false
	}
}

// shouldExitSignalHold is used when execution.signal_only is true: exit when
// the side's entry "hold" signal is no longer true (see stage-6-1-pr-08 doc).
func shouldExitSignalHold(plan *dslcompile.V1Plan, allowShort bool, pos *PositionState, b *v1Bindings, idx int) bool {
	if plan == nil || pos == nil || !pos.Open || b == nil {
		return false
	}
	switch pos.Side {
	case execution.SideLong:
		return !evaluateV1IndicatorEntry(plan, plan.Entry, b, idx)
	case execution.SideShort:
		if plan.SymmetricShortEntry && allowShort {
			if !evaluateV1Entry(plan, b, idx) {
				return true
			}
			return inferEntrySide(plan, b, idx) != execution.SideShort
		}
		return !evaluateV1IndicatorEntry(plan, plan.EntryShort, b, idx)
	default:
		return false
	}
}

func positionQuantity(plan *dslcompile.V1Plan, price float64) float64 {
	switch plan.Risk.Type {
	case "fixed_amount":
		if plan.Risk.Amount > 0 && price > 0 {
			return plan.Risk.Amount / price
		}
		if plan.Risk.FixedNotional > 0 && price > 0 {
			return plan.Risk.FixedNotional / price
		}
	}
	riskFrac := float64(plan.Risk.RiskBps) / 10_000.0
	if riskFrac <= 0 {
		riskFrac = 0.01
	}
	notional := StartingEquity * riskFrac
	q := notional / price
	if q <= 0 {
		return 0.001
	}
	return q
}

func signedPnL(side execution.Side, entryPrice, exitPrice, quantity float64) float64 {
	if side == execution.SideShort {
		return (entryPrice - exitPrice) * quantity
	}
	return (exitPrice - entryPrice) * quantity
}

func sideMoveBps(side execution.Side, entryPrice, currentPrice float64) float64 {
	if entryPrice <= 0 {
		return 0
	}
	if side == execution.SideShort {
		return (entryPrice - currentPrice) / entryPrice * 10_000.0
	}
	return (currentPrice - entryPrice) / entryPrice * 10_000.0
}

func markEquityPoint(meta RunMetadata, state *RuntimeState, points *[]results.EquityPoint, bar BarView) {
	equity := StartingEquity + state.RealizedPnL
	if state.Position.Open {
		equity += signedPnL(state.Position.Side, state.Position.EntryPrice, bar.TradeClose, state.Position.Quantity) - state.Position.EntryFeesAbs
	}
	ddAbs, ddPct := state.MarkEquity(equity)
	*points = append(*points, results.EquityPoint{
		RunID:       meta.RunID,
		TS:          bar.Timestamp,
		Equity:      equity,
		DrawdownAbs: ddAbs,
		DrawdownPct: ddPct,
	})
}

func maxDrawdownAbs(points []results.EquityPoint) float64 {
	var m float64
	for _, p := range points {
		if p.DrawdownAbs > m {
			m = p.DrawdownAbs
		}
	}
	return m
}

func maxDrawdownPct(points []results.EquityPoint) float32 {
	var m float32
	for _, p := range points {
		if p.DrawdownPct > m {
			m = p.DrawdownPct
		}
	}
	return m
}

func toPnLBps(pnlAbs, notional float64) int32 {
	if notional == 0 {
		return 0
	}
	return int32(pnlAbs / notional * 10_000.0)
}

func sideString(side execution.Side) string {
	if side == execution.SideShort {
		return "SHORT"
	}
	return "LONG"
}

func aggregateRegimeBreakdown(trades []results.Trade) map[string]results.RegimeStats {
	if len(trades) == 0 {
		return nil
	}
	out := make(map[string]results.RegimeStats, len(trades))
	for _, tr := range trades {
		key := tr.RegimeCode
		if key == "" {
			key = "unknown"
		}
		cur := out[key]
		cur.Trades++
		cur.PnLAbs += tr.PnLAbs
		if tr.PnLAbs >= 0 {
			cur.Wins++
		} else {
			cur.Losses++
		}
		out[key] = cur
	}
	return out
}
