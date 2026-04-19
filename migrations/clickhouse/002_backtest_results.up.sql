-- Stage 3 skeleton: canonical ClickHouse tables for backtest results.
-- Source of truth: trading_platform_technical_charter.md §7.4 + ADR-002.
-- These tables are *not yet populated* by backtest-engine (cmd/worker today
-- inserts only into backtest_run_summaries from 001_init.sql). This migration
-- is checked in so downstream work (results-api / control-desktop) can code
-- against a stable schema contract.

-- ---------------------------------------------------------------------------
-- backtest_trades: one row per executed trade inside a run.
-- Partitioned by entry_time (YYYYMM) so per-month runs drop cleanly when runs
-- are discarded. Ordered by (run_id, trade_index) to make per-run scans local.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default.backtest_trades (
    run_id           String,
    trade_index      UInt32,
    symbol           LowCardinality(String),
    side             Enum8('LONG' = 1, 'SHORT' = 2),
    entry_time       DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    exit_time        DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    entry_price      Float64,
    exit_price       Float64,
    quantity         Float64,
    pnl_abs          Float64,
    pnl_bps          Int32,
    fees_abs         Float64,
    slippage_bps     Int32,
    regime_code      LowCardinality(String) DEFAULT '',
    created_at       DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(entry_time)
ORDER BY (run_id, trade_index)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- backtest_equity_curve: equity samples over time for a run.
-- Partitioned by ts (YYYYMM). Ordered by (run_id, ts) for time-series scans.
-- Drawdown fields are denormalised: engine computes on write, readers don't
-- recompute (per charter, results-api is read-only over CH).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default.backtest_equity_curve (
    run_id           String,
    ts               DateTime64(3) CODEC(DoubleDelta, ZSTD(3)),
    equity           Float64,
    drawdown_abs     Float64,
    drawdown_pct     Float32,
    created_at       DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (run_id, ts)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- backtest_run_metrics: denormalised aggregate metrics per run (one row).
-- Engine ReplacingMergeTree so reruns of the same run_id overwrite.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS default.backtest_run_metrics (
    run_id                String,
    strategy_version_id   String,
    symbol                LowCardinality(String),
    period_from           DateTime64(3),
    period_to             DateTime64(3),
    pnl_abs               Float64,
    pnl_pct               Float32,
    sharpe_ratio          Float32,
    sortino_ratio         Float32,
    max_drawdown_abs      Float64,
    max_drawdown_pct      Float32,
    trades_total          UInt32,
    trades_won            UInt32,
    trades_lost           UInt32,
    profit_factor         Float32,
    expectancy            Float64,
    regime_breakdown_json String DEFAULT '',
    created_at            DateTime64(3) DEFAULT now64(3),
    version               UInt64 DEFAULT toUnixTimestamp64Milli(now64(3))
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id)
SETTINGS index_granularity = 8192;
