package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/dslcompile"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/results"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/runtime"
)

func executeV1Run(
	ctx context.Context,
	conn driver.Conn,
	runID, strategyVersionID, symbol string,
	plan *dslcompile.CompiledPlan,
	frame *featuredata.FeatureFrame,
) (runMetrics, error) {
	if plan == nil || plan.V1 == nil {
		return runMetrics{}, fmt.Errorf("%w: runtime v1 typed plan missing", runtime.ErrUnsupportedRuntime)
	}
	if frame == nil {
		return runMetrics{}, fmt.Errorf("%w: runtime v1 FeatureFrame required", runtime.ErrInvalidData)
	}
	res, err := runtime.RunV1(ctx, runtime.RunMetadata{
		RunID:             runID,
		StrategyVersionID: strategyVersionID,
		Symbol:            symbol,
	}, plan, frame)
	if err != nil {
		return runMetrics{}, err
	}
	if err := writeRuntimeResult(ctx, conn, *res); err != nil {
		return runMetrics{}, fmt.Errorf("%w: %v", ErrClickHouseWriteFailed, err)
	}
	return toLegacyMetrics(res.Metrics), nil
}

func writeRuntimeResult(ctx context.Context, conn driver.Conn, res results.RunResult) error {
	if err := deleteExistingRunRows(ctx, conn, res.Metrics.RunID); err != nil {
		return fmt.Errorf("purge previous run rows: %w", err)
	}
	if err := insertRuntimeTrades(ctx, conn, res.Trades); err != nil {
		return fmt.Errorf("insert backtest_trades: %w", err)
	}
	if err := insertRuntimeEquityCurve(ctx, conn, res.Equity); err != nil {
		return fmt.Errorf("insert backtest_equity_curve: %w", err)
	}
	if err := insertRuntimeRunMetrics(ctx, conn, res.Metrics); err != nil {
		return fmt.Errorf("insert backtest_run_metrics: %w", err)
	}
	return nil
}

func insertRuntimeTrades(ctx context.Context, conn driver.Conn, trades []results.Trade) error {
	if len(trades) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, `
		INSERT INTO default.backtest_trades (
			run_id, trade_index, symbol, side,
			entry_time, exit_time, entry_price, exit_price,
			quantity, pnl_abs, pnl_bps, fees_abs,
			slippage_bps, regime_code
		)
	`)
	if err != nil {
		return err
	}
	for _, t := range trades {
		if err := batch.Append(
			t.RunID, t.TradeIndex, t.Symbol, t.Side,
			t.EntryTime, t.ExitTime, t.EntryPrice, t.ExitPrice,
			t.Quantity, t.PnLAbs, t.PnLBps, t.FeesAbs,
			t.SlippageBps, t.RegimeCode,
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func insertRuntimeEquityCurve(ctx context.Context, conn driver.Conn, points []results.EquityPoint) error {
	if len(points) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, `
		INSERT INTO default.backtest_equity_curve (
			run_id, ts, equity, drawdown_abs, drawdown_pct
		)
	`)
	if err != nil {
		return err
	}
	for _, p := range points {
		if err := batch.Append(p.RunID, p.TS, p.Equity, p.DrawdownAbs, p.DrawdownPct); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func insertRuntimeRunMetrics(ctx context.Context, conn driver.Conn, m results.RunMetrics) error {
	regimeBreakdownJSON, err := marshalRegimeBreakdown(m.RegimeBreakdown)
	if err != nil {
		return err
	}
	return conn.Exec(ctx, `
		INSERT INTO default.backtest_run_metrics (
			run_id, strategy_version_id, symbol,
			period_from, period_to,
			pnl_abs, pnl_pct, sharpe_ratio, sortino_ratio,
			max_drawdown_abs, max_drawdown_pct,
			trades_total, trades_won, trades_lost,
			profit_factor, expectancy,
			regime_breakdown_json
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		m.RunID, m.StrategyVersionID, m.Symbol,
		m.PeriodFrom, m.PeriodTo,
		m.PnLAbs, m.PnLPct, m.SharpeRatio, m.SortinoRatio,
		m.MaxDrawdownAbs, m.MaxDrawdownPct,
		m.TradesTotal, m.TradesWon, m.TradesLost,
		m.ProfitFactor, m.Expectancy,
		string(regimeBreakdownJSON),
	)
}

func marshalRegimeBreakdown(in map[string]results.RegimeStats) (json.RawMessage, error) {
	if len(in) == 0 {
		return json.Marshal(map[string]any{})
	}
	return json.Marshal(in)
}

func toLegacyMetrics(m results.RunMetrics) runMetrics {
	return runMetrics{
		RunID:             m.RunID,
		StrategyVersionID: m.StrategyVersionID,
		Symbol:            m.Symbol,
		PeriodFrom:        m.PeriodFrom,
		PeriodTo:          m.PeriodTo,
		PnLAbs:            m.PnLAbs,
		PnLPct:            m.PnLPct,
		SharpeRatio:       m.SharpeRatio,
		SortinoRatio:      m.SortinoRatio,
		MaxDrawdownAbs:    m.MaxDrawdownAbs,
		MaxDrawdownPct:    m.MaxDrawdownPct,
		TradesTotal:       m.TradesTotal,
		TradesWon:         m.TradesWon,
		TradesLost:        m.TradesLost,
		ProfitFactor:      m.ProfitFactor,
		Expectancy:        m.Expectancy,
	}
}
