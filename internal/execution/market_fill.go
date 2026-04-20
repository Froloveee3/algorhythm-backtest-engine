package execution

import (
	"fmt"
	"math"
)

type Side int

const (
	SideLong Side = 1
	SideShort Side = -1
)

func (s Side) String() string {
	if s == SideShort {
		return "SHORT"
	}
	return "LONG"
}

type FillConfig struct {
	FeeBps      int
	SlippageBps int
}

type FillResult struct {
	Price       float64
	Quantity    float64
	Notional    float64
	FeesAbs     float64
	SlippageBps int32
}

func ApplyMarketFill(side Side, referencePrice, quantity float64, cfg FillConfig, isEntry bool) (FillResult, error) {
	if referencePrice <= 0 {
		return FillResult{}, fmt.Errorf("execution: non-positive reference price %f", referencePrice)
	}
	if quantity <= 0 {
		return FillResult{}, fmt.Errorf("execution: non-positive quantity %f", quantity)
	}
	price := referencePrice
	slip := float64(cfg.SlippageBps) / 10_000.0
	switch {
	case side == SideLong && isEntry:
		price = referencePrice * (1 + slip)
	case side == SideLong && !isEntry:
		price = referencePrice * (1 - slip)
	case side == SideShort && isEntry:
		price = referencePrice * (1 - slip)
	case side == SideShort && !isEntry:
		price = referencePrice * (1 + slip)
	}
	notional := price * quantity
	fees := notional * float64(cfg.FeeBps) / 10_000.0
	return FillResult{
		Price:       price,
		Quantity:    quantity,
		Notional:    notional,
		FeesAbs:     math.Max(fees, 0),
		SlippageBps: int32(cfg.SlippageBps),
	}, nil
}
