package featuredata

import (
	"context"
	"testing"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/storage"
)

func TestV1SeedMinuteBars_ReadFeatureFrame(t *testing.T) {
	const start = int64(1_698_796_800_000) // 2023-11-01T00:00:00Z
	blob, err := V1SeedMinuteBars("BTCUSDT", start, 200)
	if err != nil {
		t.Fatal(err)
	}
	key := "features/feature_set=btcusdt_futures_mvp/exchange=binance/symbol=BTCUSDT/interval=1m/year=2023/month=11/data.parquet"
	store := storage.NewMemoryReader()
	store.Put(key, blob)

	req := ReadRequest{
		FeatureSet:     FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1},
		ExpectedSymbol: "BTCUSDT",
		Partitions: []Partition{
			{Year: 2023, Month: 11, ObjectKey: key},
		},
		RequiredColumns: []ColumnName{ColCloseTradeI64, ColEMA20},
	}
	frame, err := ReadFeatureFrame(context.Background(), store, req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if frame.RowCount != 200 {
		t.Fatalf("RowCount: want 200, got %d", frame.RowCount)
	}
	if frame.PriceScale != 8 {
		t.Fatalf("PriceScale: want 8, got %d", frame.PriceScale)
	}
	if len(blob) < 24 || string(blob[:4]) != "PAR1" {
		t.Fatal("unexpected parquet magic")
	}
}
