package runtime

import (
	"time"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/execution"
)

const StartingEquity = 100_000.0

type RunMetadata struct {
	RunID             string
	StrategyVersionID string
	Symbol            string
}

type PositionState struct {
	Open          bool
	Side          execution.Side
	EntryBarIndex int
	EntryTime     time.Time
	EntryPrice    float64
	Quantity      float64
	EntryFeesAbs  float64
	PeakPrice     float64
	TroughPrice   float64
	RegimeCode    string
}

type RuntimeState struct {
	CurrentBarIndex int
	CurrentTime     time.Time

	Position PositionState

	// BlockedEntryUntilBar implements PR-07 ordering: after any exit fill, do not
	// allow a new entry until the next bar (no same-bar reopen, and also no
	// immediate reopen on the bar immediately following an exit).
	//
	// Invariant: while CurrentBarIndex < BlockedEntryUntilBar, entry evaluation
	// must be suppressed for flat state.
	BlockedEntryUntilBar int

	RealizedPnL float64
	FeesPaid    float64
	Equity      float64
	PeakEquity  float64

	TradesTotal uint32
	TradesWon   uint32
	TradesLost  uint32

	GrossProfit float64
	GrossLoss   float64
}

func NewState() RuntimeState {
	return RuntimeState{
		CurrentBarIndex:      -1,
		BlockedEntryUntilBar: -1,
		Equity:               StartingEquity,
		PeakEquity:           StartingEquity,
	}
}

func (s *RuntimeState) MarkEquity(equity float64) (drawdownAbs float64, drawdownPct float32) {
	s.Equity = equity
	if equity > s.PeakEquity {
		s.PeakEquity = equity
	}
	drawdownAbs = s.PeakEquity - equity
	if s.PeakEquity > 0 {
		drawdownPct = float32(drawdownAbs / s.PeakEquity * 100.0)
	}
	return drawdownAbs, drawdownPct
}

type BarView struct {
	Index        int
	Timestamp    time.Time
	TradeClose   float64
	TradeCloseOK bool
}
