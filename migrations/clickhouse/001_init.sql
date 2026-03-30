CREATE TABLE IF NOT EXISTS default.backtest_run_summaries (
    run_id String,
    symbol String,
    engine_version String,
    created_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (run_id, created_at);
