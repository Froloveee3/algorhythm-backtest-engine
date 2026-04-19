package main

import (
	"testing"
)

func TestSimulateRun_Deterministic(t *testing.T) {
	a := simulateRun("run-abc", "strategy-v1", "BTCUSDT")
	b := simulateRun("run-abc", "strategy-v1", "BTCUSDT")

	if len(a.Trades) != len(b.Trades) {
		t.Fatalf("trade count drifts: a=%d b=%d", len(a.Trades), len(b.Trades))
	}
	for i := range a.Trades {
		if a.Trades[i] != b.Trades[i] {
			t.Fatalf("trade %d drifts between identical runs: a=%+v b=%+v", i, a.Trades[i], b.Trades[i])
		}
	}
	if a.Metrics != b.Metrics {
		t.Fatalf("metrics drift: a=%+v b=%+v", a.Metrics, b.Metrics)
	}
}

func TestSimulateRun_DifferentRunIDsDiverge(t *testing.T) {
	a := simulateRun("run-one", "sv1", "BTCUSDT")
	b := simulateRun("run-two", "sv1", "BTCUSDT")

	if a.Metrics.PnLAbs == b.Metrics.PnLAbs && a.Metrics.TradesWon == b.Metrics.TradesWon {
		t.Fatalf("different run_ids produced identical metrics; rng not really seeded by run_id")
	}
}

func TestSimulateRun_PopulatesAllSchemaFields(t *testing.T) {
	r := simulateRun("run-field-check", "sv1", "ETHUSDT")

	if len(r.Trades) != simTrades {
		t.Fatalf("expected %d trades, got %d", simTrades, len(r.Trades))
	}
	// equity has seed point + one point per trade
	if len(r.Equity) != simTrades+1 {
		t.Fatalf("expected %d equity points, got %d", simTrades+1, len(r.Equity))
	}

	for i, tr := range r.Trades {
		if tr.RunID == "" || tr.Symbol == "" || tr.Side == "" {
			t.Fatalf("trade %d missing mandatory string field: %+v", i, tr)
		}
		if tr.Side != "LONG" && tr.Side != "SHORT" {
			t.Fatalf("trade %d has invalid side %q", i, tr.Side)
		}
		if tr.EntryTime.After(tr.ExitTime) {
			t.Fatalf("trade %d: entry_time after exit_time", i)
		}
		if tr.Quantity <= 0 || tr.EntryPrice <= 0 || tr.ExitPrice <= 0 {
			t.Fatalf("trade %d: non-positive price/quantity: %+v", i, tr)
		}
	}

	// metrics sanity
	if r.Metrics.TradesTotal != uint32(simTrades) {
		t.Fatalf("metrics.TradesTotal = %d, want %d", r.Metrics.TradesTotal, simTrades)
	}
	if r.Metrics.TradesWon+r.Metrics.TradesLost != r.Metrics.TradesTotal {
		t.Fatalf("won+lost (%d+%d) != total (%d)",
			r.Metrics.TradesWon, r.Metrics.TradesLost, r.Metrics.TradesTotal)
	}
	if r.Metrics.PeriodTo.Before(r.Metrics.PeriodFrom) {
		t.Fatalf("period_to before period_from")
	}
	if r.Metrics.MaxDrawdownAbs < 0 {
		t.Fatalf("negative max drawdown: %f", r.Metrics.MaxDrawdownAbs)
	}
}
