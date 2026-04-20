package results

import "math"

func ProfitFactor(grossProfit, grossLoss float64) float32 {
	if grossLoss <= 0 {
		if grossProfit <= 0 {
			return 0
		}
		return float32(math.Inf(1))
	}
	return float32(grossProfit / grossLoss)
}

func Expectancy(totalPnL float64, n uint32) float64 {
	if n == 0 {
		return 0
	}
	return totalPnL / float64(n)
}

func Sharpe(returns []float64) float64 {
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

func Sortino(returns []float64) float64 {
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
