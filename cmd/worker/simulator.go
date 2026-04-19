package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// tradeRow is one executed trade inside a simulated run.
// Column types mirror default.backtest_trades.
type tradeRow struct {
	RunID       string
	TradeIndex  uint32
	Symbol      string
	Side        string // "LONG" | "SHORT"
	EntryTime   time.Time
	ExitTime    time.Time
	EntryPrice  float64
	ExitPrice   float64
	Quantity    float64
	PnLAbs      float64
	PnLBps      int32
	FeesAbs     float64
	SlippageBps int32
	RegimeCode  string
}

// equityPoint is a sample of portfolio equity at one instant.
// Column types mirror default.backtest_equity_curve.
type equityPoint struct {
	RunID       string
	TS          time.Time
	Equity      float64
	DrawdownAbs float64
	DrawdownPct float32
}

// runMetrics is the aggregate metrics row per run.
// Column types mirror default.backtest_run_metrics.
type runMetrics struct {
	RunID             string
	StrategyVersionID string
	Symbol            string
	PeriodFrom        time.Time
	PeriodTo          time.Time
	PnLAbs            float64
	PnLPct            float32
	SharpeRatio       float32
	SortinoRatio      float32
	MaxDrawdownAbs    float64
	MaxDrawdownPct    float32
	TradesTotal       uint32
	TradesWon         uint32
	TradesLost        uint32
	ProfitFactor      float32
	Expectancy        float64
}

// simulationResult bundles everything we persist for a run.
type simulationResult struct {
	Trades  []tradeRow
	Equity  []equityPoint
	Metrics runMetrics
}

// Simulator parameters are intentionally constants for the placeholder stage:
// the contract is determinism by run_id, not trading realism. Real strategy
// execution lives in a follow-up (feature parquet reader + DSL interpreter).
const (
	simStartingEquity   = 100_000.0
	simTrades           = 20
	simBasePrice        = 50_000.0
	simFeeBps           = 10
	simSlippageBps      = 5
	simPnLBpsMean       = 20.0
	simPnLBpsStdDev     = 150.0
	simTradeDurationH   = 4
	simBetweenTradesH   = 24
	simQuantityBaseUnit = 0.01
)

// simulateRun returns a deterministic synthetic backtest result for the given
// run_id/strategy/symbol. Same run_id always yields identical rows (including
// order), so reinserting with ReplacingMergeTree semantics is safe.
//
// This is a *placeholder*: trades are random-walky fake PnL, not the output of
// interpreting a DSL over feature data. Downstream consumers (results-api,
// control-desktop) can nevertheless code against the real canonical schema.
func simulateRun(runID, strategyVersionID, symbol string) simulationResult {
	seed := seedFromRunID(runID)
	rng := rand.New(rand.NewSource(int64(seed))) // #nosec G404 -- determinism, not crypto

	// Anchor to a fixed UTC window so rows partition cleanly and tests don't
	// drift with wall clock.
	periodFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	trades := make([]tradeRow, 0, simTrades)
	equity := make([]equityPoint, 0, simTrades+1)

	currentEquity := simStartingEquity
	peakEquity := currentEquity
	maxDD := 0.0
	maxDDPct := float32(0)

	// Seed point: starting equity at periodFrom.
	equity = append(equity, equityPoint{
		RunID:       runID,
		TS:          periodFrom,
		Equity:      currentEquity,
		DrawdownAbs: 0,
		DrawdownPct: 0,
	})

	var tradesWon, tradesLost uint32
	var grossProfit, grossLoss float64 // absolute magnitudes, for profit_factor
	pnlReturns := make([]float64, 0, simTrades)

	for i := 0; i < simTrades; i++ {
		entryTime := periodFrom.Add(time.Duration(i*simBetweenTradesH) * time.Hour)
		exitTime := entryTime.Add(time.Duration(simTradeDurationH) * time.Hour)

		side := "LONG"
		if rng.Float64() < 0.35 {
			side = "SHORT"
		}

		pnlBpsFloat := rng.NormFloat64()*simPnLBpsStdDev + simPnLBpsMean
		pnlBps := int32(math.Round(pnlBpsFloat))

		priceJitter := (rng.Float64() - 0.5) * 0.01
		entryPrice := simBasePrice * (1 + priceJitter)
		exitPrice := entryPrice * (1 + float64(pnlBps)/10_000.0)
		if side == "SHORT" {
			exitPrice = entryPrice * (1 - float64(pnlBps)/10_000.0)
		}

		quantity := simQuantityBaseUnit * (1 + rng.Float64())
		notional := entryPrice * quantity
		pnlAbs := notional * float64(pnlBps) / 10_000.0
		fees := notional * float64(simFeeBps) / 10_000.0 * 2 // entry + exit
		pnlAbs -= fees

		trades = append(trades, tradeRow{
			RunID:       runID,
			TradeIndex:  uint32(i),
			Symbol:      symbol,
			Side:        side,
			EntryTime:   entryTime,
			ExitTime:    exitTime,
			EntryPrice:  entryPrice,
			ExitPrice:   exitPrice,
			Quantity:    quantity,
			PnLAbs:      pnlAbs,
			PnLBps:      pnlBps,
			FeesAbs:     fees,
			SlippageBps: simSlippageBps,
			RegimeCode:  regimeCodeFor(i, rng),
		})

		currentEquity += pnlAbs
		pnlReturns = append(pnlReturns, pnlAbs/simStartingEquity)
		if pnlAbs >= 0 {
			tradesWon++
			grossProfit += pnlAbs
		} else {
			tradesLost++
			grossLoss += -pnlAbs
		}

		if currentEquity > peakEquity {
			peakEquity = currentEquity
		}
		dd := peakEquity - currentEquity
		var ddPct float32
		if peakEquity > 0 {
			ddPct = float32(dd / peakEquity * 100.0)
		}
		if dd > maxDD {
			maxDD = dd
			maxDDPct = ddPct
		}

		equity = append(equity, equityPoint{
			RunID:       runID,
			TS:          exitTime,
			Equity:      currentEquity,
			DrawdownAbs: dd,
			DrawdownPct: ddPct,
		})
	}

	periodTo := equity[len(equity)-1].TS

	metrics := runMetrics{
		RunID:             runID,
		StrategyVersionID: strategyVersionID,
		Symbol:            symbol,
		PeriodFrom:        periodFrom,
		PeriodTo:          periodTo,
		PnLAbs:            currentEquity - simStartingEquity,
		PnLPct:            float32((currentEquity - simStartingEquity) / simStartingEquity * 100.0),
		SharpeRatio:       float32(sharpe(pnlReturns)),
		SortinoRatio:      float32(sortino(pnlReturns)),
		MaxDrawdownAbs:    maxDD,
		MaxDrawdownPct:    maxDDPct,
		TradesTotal:       uint32(len(trades)),
		TradesWon:         tradesWon,
		TradesLost:        tradesLost,
		ProfitFactor:      profitFactor(grossProfit, grossLoss),
		Expectancy:        expectancy(currentEquity-simStartingEquity, uint32(len(trades))),
	}

	return simulationResult{Trades: trades, Equity: equity, Metrics: metrics}
}

func seedFromRunID(runID string) uint64 {
	sum := sha256.Sum256([]byte(runID))
	return binary.BigEndian.Uint64(sum[:8])
}

// regimeCodeFor returns a low-cardinality tag so regime_breakdown queries in
// results-api have something to group by. Deterministic via rng.
func regimeCodeFor(_ int, rng *rand.Rand) string {
	buckets := []string{"trend_up", "trend_down", "range"}
	return buckets[rng.Intn(len(buckets))]
}

// sharpe computes a naive per-trade Sharpe. Real annualisation will land when
// the engine handles actual bar-level returns.
func sharpe(returns []float64) float64 {
	if len(returns) == 0 {
		return 0
	}
	m := mean(returns)
	s := stddev(returns, m)
	if s == 0 {
		return 0
	}
	return m / s * math.Sqrt(float64(len(returns)))
}

// sortino — same as sharpe but denominator is downside std only.
func sortino(returns []float64) float64 {
	if len(returns) == 0 {
		return 0
	}
	m := mean(returns)
	var sumSq float64
	var n int
	for _, r := range returns {
		if r < 0 {
			sumSq += r * r
			n++
		}
	}
	if n == 0 {
		return 0
	}
	downside := math.Sqrt(sumSq / float64(n))
	if downside == 0 {
		return 0
	}
	return m / downside * math.Sqrt(float64(len(returns)))
}

func mean(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func stddev(xs []float64, m float64) float64 {
	var sumSq float64
	for _, x := range xs {
		sumSq += (x - m) * (x - m)
	}
	return math.Sqrt(sumSq / float64(len(xs)))
}

func profitFactor(grossProfit, grossLoss float64) float32 {
	if grossLoss <= 0 {
		if grossProfit <= 0 {
			return 0
		}
		return float32(math.Inf(1))
	}
	return float32(grossProfit / grossLoss)
}

func expectancy(totalPnL float64, n uint32) float64 {
	if n == 0 {
		return 0
	}
	return totalPnL / float64(n)
}

// String (short) form of metrics for the bt.run.completed summary payload.
func (m runMetrics) summarySnippet() string {
	return fmt.Sprintf(
		"trades=%d won=%d lost=%d pnl_pct=%.2f sharpe=%.2f max_dd_pct=%.2f",
		m.TradesTotal, m.TradesWon, m.TradesLost, m.PnLPct, m.SharpeRatio, m.MaxDrawdownPct,
	)
}
