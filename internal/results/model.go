package results

import "time"

type Trade struct {
	RunID       string
	TradeIndex  uint32
	Symbol      string
	Side        string
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

type EquityPoint struct {
	RunID       string
	TS          time.Time
	Equity      float64
	DrawdownAbs float64
	DrawdownPct float32
}

type RunMetrics struct {
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
	RegimeBreakdown   map[string]RegimeStats
}

type RunResult struct {
	Trades  []Trade
	Equity  []EquityPoint
	Metrics RunMetrics
}

type RegimeStats struct {
	Trades uint32  `json:"trades"`
	PnLAbs float64 `json:"pnl_abs"`
	Wins   uint32  `json:"wins"`
	Losses uint32  `json:"losses"`
}
