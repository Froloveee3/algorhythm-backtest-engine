//go:build chsmoke
// +build chsmoke

package main

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestChSmoke_WriteReal runs simulateRun + writeSimulationResult against a
// *live* ClickHouse instance and asserts that rows actually land in all three
// canonical tables.
//
// Gated by `-tags=chsmoke`; set BT_CLICKHOUSE_DSN if your stack is not on the
// default local port. Not wired into the default `go test ./...` run because
// normal CI runs without the dev-stack docker compose up.
func TestChSmoke_WriteReal(t *testing.T) {
	dsn := os.Getenv("BT_CLICKHOUSE_DSN")
	if dsn == "" {
		dsn = "clickhouse://default:clickhouse@localhost:9009/default"
	}
	conn, err := openClickHouse(dsn)
	if err != nil {
		t.Fatalf("openClickHouse: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("clickhouse ping: %v", err)
	}

	runID := "smoke-" + time.Now().UTC().Format("20060102T150405.000000000")
	sim := simulateRun(runID, "smoke-sv", "BTCUSDT")

	if err := writeSimulationResult(ctx, conn, sim); err != nil {
		t.Fatalf("writeSimulationResult: %v", err)
	}

	type row struct {
		Name  string
		Query string
		Min   uint64
	}
	checks := []row{
		{"backtest_trades", "SELECT count() FROM default.backtest_trades WHERE run_id = ?", uint64(len(sim.Trades))},
		{"backtest_equity_curve", "SELECT count() FROM default.backtest_equity_curve WHERE run_id = ?", uint64(len(sim.Equity))},
		{"backtest_run_metrics", "SELECT count() FROM default.backtest_run_metrics FINAL WHERE run_id = ?", 1},
	}
	for _, c := range checks {
		var cnt uint64
		if err := conn.QueryRow(ctx, c.Query, runID).Scan(&cnt); err != nil {
			t.Fatalf("%s query: %v", c.Name, err)
		}
		if cnt < c.Min {
			t.Fatalf("%s: got %d rows for run %s, want at least %d", c.Name, cnt, runID, c.Min)
		}
		t.Logf("%s: %d rows for run %s", c.Name, cnt, runID)
	}

	// Cleanup the smoke rows so we don't leave test garbage in the dev stack.
	for _, table := range []string{"backtest_trades", "backtest_equity_curve", "backtest_run_metrics"} {
		if err := conn.Exec(ctx, "ALTER TABLE default."+table+" DELETE WHERE run_id = ?", runID); err != nil {
			t.Logf("cleanup %s: %v (non-fatal)", table, err)
		}
	}
}
