package main

import (
	"encoding/json"
	"testing"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/results"
)

func TestMarshalRegimeBreakdown_Empty(t *testing.T) {
	raw, err := marshalRegimeBreakdown(nil)
	if err != nil {
		t.Fatalf("marshalRegimeBreakdown(nil): %v", err)
	}
	if string(raw) != "{}" {
		t.Fatalf("got %q, want {}", string(raw))
	}
}

func TestMarshalRegimeBreakdown_Content(t *testing.T) {
	raw, err := marshalRegimeBreakdown(map[string]results.RegimeStats{
		"trend_up_low_vol": {Trades: 2, PnLAbs: 123.45, Wins: 2, Losses: 0},
	})
	if err != nil {
		t.Fatalf("marshalRegimeBreakdown: %v", err)
	}
	var out map[string]results.RegimeStats
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal back: %v", err)
	}
	got := out["trend_up_low_vol"]
	if got.Trades != 2 || got.Wins != 2 || got.Losses != 0 {
		t.Fatalf("unexpected breakdown payload: %+v", out)
	}
}
