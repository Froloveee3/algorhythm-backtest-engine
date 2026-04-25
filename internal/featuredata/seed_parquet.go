package featuredata

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// V1SeedMinuteBars builds a deterministic v1 feature-parquet blob suitable for
// MinIO seeding / Stage 6.1 E2E. Rows are contiguous 1-minute bars starting at
// startUnixMs; indicator columns required by resolve / runtime are populated so
// simple DSL predicates (ema_20_gt_ema_50, tp_sl exits) have material to work with.
//
// Mirrors the producer contract exercised in reader_test.go (same featureRow wire shape).
func V1SeedMinuteBars(symbol string, startUnixMs int64, n int) ([]byte, error) {
	if symbol == "" {
		return nil, fmt.Errorf("symbol required")
	}
	if n <= 0 {
		return nil, fmt.Errorf("n must be positive")
	}
	pf := func(v float64) *float64 { return &v }
	pb := func(v bool) *bool { return &v }

	rows := make([]featureRow, n)
	for i := range rows {
		rows[i] = featureRow{
			Symbol:        symbol,
			TimestampUTC:  startUnixMs + int64(i)*minuteMillis,
			CloseTradeI64: 5_000_000_000_000, // 50_000 @ scale=8
			MarkCloseI64:  5_000_000_000_000,
		}
		// Warm-ish path: populate EMAs after row 55 so predicates can evaluate mid-series.
		if i >= 55 {
			e20 := 49500.0 + float64(i)*0.05
			e50 := 49480.0 + float64(i)*0.045
			rows[i].Ema20 = pf(e20)
			rows[i].Ema50 = pf(e50)
			if i >= 100 && e20 > e50 {
				rows[i].TrendUp = pb(true)
				rows[i].TrendDown = pb(false)
				rows[i].Flat = pb(false)
			}
		}
		if i >= n-50 {
			rows[i].Rsi14 = pf(35.0 + float64(i%10))
		}
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[featureRow](&buf)
	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("parquet write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("parquet close: %w", err)
	}
	return buf.Bytes(), nil
}
