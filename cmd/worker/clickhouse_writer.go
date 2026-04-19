package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// writeSimulationResult persists trades + equity + metrics for a run.
//
// Semantics:
//   - backtest_trades / backtest_equity_curve are plain MergeTree, so we delete
//     any prior rows for the same run_id before inserting to make reruns
//     idempotent from the reader's point of view.
//   - backtest_run_metrics is ReplacingMergeTree(version). We rely on its
//     built-in deduplication at merge time, but callers querying FINAL get the
//     latest row immediately.
//
// ALTER ... DELETE is a mutation in ClickHouse — it's async and heavy, but for
// the Stage 3 placeholder volume (tens of rows per run) it's cheap. Swap for
// ReplacingMergeTree on the detail tables once the engine writes many millions
// per run and the cost profile changes.
func writeSimulationResult(ctx context.Context, conn driver.Conn, res simulationResult) error {
	if err := deleteExistingRunRows(ctx, conn, res.Metrics.RunID); err != nil {
		return fmt.Errorf("purge previous run rows: %w", err)
	}
	if err := insertTrades(ctx, conn, res.Trades); err != nil {
		return fmt.Errorf("insert backtest_trades: %w", err)
	}
	if err := insertEquityCurve(ctx, conn, res.Equity); err != nil {
		return fmt.Errorf("insert backtest_equity_curve: %w", err)
	}
	if err := insertRunMetrics(ctx, conn, res.Metrics); err != nil {
		return fmt.Errorf("insert backtest_run_metrics: %w", err)
	}
	return nil
}

func deleteExistingRunRows(ctx context.Context, conn driver.Conn, runID string) error {
	for _, table := range []string{"backtest_trades", "backtest_equity_curve"} {
		q := fmt.Sprintf("ALTER TABLE default.%s DELETE WHERE run_id = ?", table)
		if err := conn.Exec(ctx, q, runID); err != nil {
			return fmt.Errorf("delete from %s: %w", table, err)
		}
	}
	return nil
}

func insertTrades(ctx context.Context, conn driver.Conn, trades []tradeRow) error {
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

func insertEquityCurve(ctx context.Context, conn driver.Conn, points []equityPoint) error {
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

func insertRunMetrics(ctx context.Context, conn driver.Conn, m runMetrics) error {
	return conn.Exec(ctx, `
		INSERT INTO default.backtest_run_metrics (
			run_id, strategy_version_id, symbol,
			period_from, period_to,
			pnl_abs, pnl_pct, sharpe_ratio, sortino_ratio,
			max_drawdown_abs, max_drawdown_pct,
			trades_total, trades_won, trades_lost,
			profit_factor, expectancy
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		m.RunID, m.StrategyVersionID, m.Symbol,
		m.PeriodFrom, m.PeriodTo,
		m.PnLAbs, m.PnLPct, m.SharpeRatio, m.SortinoRatio,
		m.MaxDrawdownAbs, m.MaxDrawdownPct,
		m.TradesTotal, m.TradesWon, m.TradesLost,
		m.ProfitFactor, m.Expectancy,
	)
}
