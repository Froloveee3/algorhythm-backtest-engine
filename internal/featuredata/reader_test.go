package featuredata

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/algorhythm/backtest-engine/internal/storage"
	"github.com/parquet-go/parquet-go"
)

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

func pf(v float64) *float64 { return &v }
func pb(v bool) *bool       { return &v }

// writeRows serialises rows into a v1-compatible parquet blob. Uses the same
// struct the reader mirrors, so producer/consumer tag drift would surface here.
func writeRows(t *testing.T, rows []featureRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[featureRow](&buf)
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			t.Fatalf("parquet write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet close: %v", err)
	}
	return buf.Bytes()
}

// buildMonth produces N contiguous minute rows starting at startMs with a given
// symbol. Price fields are set; regime flags are kept null (warmup).
func buildMonth(symbol string, startMs int64, n int) []featureRow {
	out := make([]featureRow, n)
	for i := range n {
		out[i] = featureRow{
			Symbol:        symbol,
			TimestampUTC:  startMs + int64(i)*60_000,
			CloseTradeI64: 5_000_000_000_000, // 50_000 @ scale=8
			MarkCloseI64:  5_000_000_000_000,
		}
	}
	return out
}

func newStore(t *testing.T, blobs map[string][]byte) storage.ObjectReader {
	t.Helper()
	s := storage.NewMemoryReader()
	for k, v := range blobs {
		s.Put(k, v)
	}
	return s
}

func defaultKey() FeatureSetKey { return FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 1} }

// ---------------------------------------------------------------------------
// Happy-path coverage
// ---------------------------------------------------------------------------

func TestReadFeatureFrame_SinglePartitionHappyPath(t *testing.T) {
	rows := buildMonth("BTCUSDT", 1_700_000_000_000, 3)
	rows[2].Ema20 = pf(49_500.0) // one real indicator value at the last row
	rows[2].TrendUp = pb(true)
	rows[2].TrendDown = pb(false)
	rows[2].Flat = pb(false)

	store := newStore(t, map[string][]byte{
		"features/feature_set=btcusdt_futures_mvp/exchange=binance/symbol=BTCUSDT/interval=1m/year=2023/month=11/data.parquet": writeRows(t, rows),
	})

	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions: []Partition{
			{Year: 2023, Month: 11, ObjectKey: "features/feature_set=btcusdt_futures_mvp/exchange=binance/symbol=BTCUSDT/interval=1m/year=2023/month=11/data.parquet"},
		},
		RequiredColumns: []ColumnName{ColCloseTradeI64, ColEMA20, ColTrendUp},
	}

	frame, err := ReadFeatureFrame(context.Background(), store, req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if frame.RowCount != 3 {
		t.Fatalf("RowCount: want 3, got %d", frame.RowCount)
	}
	if frame.PriceScale != 8 {
		t.Fatalf("PriceScale: want 8, got %d", frame.PriceScale)
	}
	if frame.Symbol != "BTCUSDT" {
		t.Fatalf("Symbol: want BTCUSDT, got %q", frame.Symbol)
	}
	// Timestamps monotone + minute aligned.
	for i := 1; i < frame.RowCount; i++ {
		if frame.Timestamps[i]-frame.Timestamps[i-1] != 60_000 {
			t.Fatalf("timestamps step: row %d diff %d", i, frame.Timestamps[i]-frame.Timestamps[i-1])
		}
	}
	// close_trade_i64 always valid.
	close := frame.Int64s[ColCloseTradeI64]
	if close == nil || len(close.Values) != 3 {
		t.Fatalf("ColCloseTradeI64 missing or wrong length")
	}
	// ema_20 null for first two rows, valid for last.
	ema := frame.Floats[ColEMA20]
	if ema == nil {
		t.Fatalf("ColEMA20 missing")
	}
	if ema.IsValid(0) || ema.IsValid(1) {
		t.Fatalf("ema rows 0/1 should be null")
	}
	if !ema.IsValid(2) {
		t.Fatalf("ema row 2 should be valid")
	}
	if ema.Values[2] != 49_500.0 {
		t.Fatalf("ema row 2 value: want 49500, got %v", ema.Values[2])
	}
	// trend_up valid at row 2.
	trend := frame.Bools[ColTrendUp]
	if trend == nil || !trend.IsValid(2) || trend.Values[2] != 1 {
		t.Fatalf("trend_up row 2 expected valid=true")
	}
	// Column not requested should not materialise.
	if _, present := frame.Floats[ColRSI14]; present {
		t.Fatalf("RSI14 should not be materialised when not requested")
	}
}

func TestReadFeatureFrame_CrossMonthBoundaryKeepFirst(t *testing.T) {
	// Month 11 last minute:
	startNov := int64(1_700_000_000_000)
	rowsNov := buildMonth("BTCUSDT", startNov, 3) // 3 minutes
	tailTs := rowsNov[2].TimestampUTC

	// Month 12 first bar duplicates the Nov tail, then advances normally.
	rowsDec := []featureRow{
		{ // duplicate of Nov tail
			Symbol:        "BTCUSDT",
			TimestampUTC:  tailTs,
			CloseTradeI64: 7_777_777_777_777, // different value than Nov's keep-first
			MarkCloseI64:  7_777_777_777_777,
		},
		{
			Symbol:        "BTCUSDT",
			TimestampUTC:  tailTs + 60_000,
			CloseTradeI64: 5_000_000_000_000,
			MarkCloseI64:  5_000_000_000_000,
		},
		{
			Symbol:        "BTCUSDT",
			TimestampUTC:  tailTs + 120_000,
			CloseTradeI64: 5_000_000_000_000,
			MarkCloseI64:  5_000_000_000_000,
		},
	}

	keyNov := "features/month=11/data.parquet"
	keyDec := "features/month=12/data.parquet"
	store := newStore(t, map[string][]byte{
		keyNov: writeRows(t, rowsNov),
		keyDec: writeRows(t, rowsDec),
	})

	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions: []Partition{
			{Year: 2023, Month: 11, ObjectKey: keyNov},
			{Year: 2023, Month: 12, ObjectKey: keyDec},
		},
		RequiredColumns: []ColumnName{ColCloseTradeI64},
	}

	frame, err := ReadFeatureFrame(context.Background(), store, req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 3 (Nov) + 2 (Dec after drop of dup head) = 5
	if frame.RowCount != 5 {
		t.Fatalf("RowCount: want 5, got %d", frame.RowCount)
	}
	// The duplicate timestamp must have kept Nov's value, not Dec's.
	close := frame.Int64s[ColCloseTradeI64]
	if close.Values[2] != 5_000_000_000_000 {
		t.Fatalf("keep-first violated: row 2 value %d (want Nov's 5_000_000_000_000)", close.Values[2])
	}
	// Strictly increasing timestamps.
	for i := 1; i < frame.RowCount; i++ {
		if frame.Timestamps[i]-frame.Timestamps[i-1] != 60_000 {
			t.Fatalf("gap at %d: %d ms", i, frame.Timestamps[i]-frame.Timestamps[i-1])
		}
	}
}

// ---------------------------------------------------------------------------
// Negative coverage - one test per Reason
// ---------------------------------------------------------------------------

func assertReason(t *testing.T, err error, want Reason) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error with reason=%s, got nil", want)
	}
	re, ok := AsReaderError(err)
	if !ok {
		t.Fatalf("want *ReaderError (reason=%s), got %T: %v", want, err, err)
	}
	if re.Reason != want {
		t.Fatalf("want reason=%s, got %s (msg=%s)", want, re.Reason, re.Message)
	}
}

func TestReadFeatureFrame_UnknownFeatureSet(t *testing.T) {
	store := newStore(t, nil)
	req := ReadRequest{
		FeatureSet:     FeatureSetKey{Code: "ethusdt_futures_mvp", Version: 1},
		ExpectedSymbol: "ETHUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureSetUnsupported)
}

// Same code different version: registry must miss and fail unsupported.
// Pins the (code, version) keying rule from contract §4.
func TestReadFeatureFrame_UnknownFeatureSetVersion(t *testing.T) {
	store := newStore(t, nil)
	req := ReadRequest{
		FeatureSet:     FeatureSetKey{Code: "btcusdt_futures_mvp", Version: 2},
		ExpectedSymbol: "BTCUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureSetUnsupported)
}

func TestReadFeatureFrame_PartitionMissing(t *testing.T) {
	store := newStore(t, nil) // nothing uploaded
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "not-there"}},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeaturePartitionMissing)
}

func TestReadFeatureFrame_SchemaMismatch(t *testing.T) {
	// Write a totally different parquet schema to the key.
	type junk struct {
		X int64 `parquet:"x"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[junk](&buf)
	_, _ = w.Write([]junk{{X: 1}})
	_ = w.Close()

	store := newStore(t, map[string][]byte{"k": buf.Bytes()})
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureSchemaMismatch)
}

func TestReadFeatureFrame_RequestedColumnNotInContract(t *testing.T) {
	store := newStore(t, map[string][]byte{
		"k": writeRows(t, buildMonth("BTCUSDT", 1_700_000_000_000, 1)),
	})
	req := ReadRequest{
		FeatureSet:      defaultKey(),
		ExpectedSymbol:  "BTCUSDT",
		Partitions:      []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
		RequiredColumns: []ColumnName{"bollinger_bands_20"}, // not in v1
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureSchemaMismatch)
}

func TestReadFeatureFrame_RowInvariantViolated_TimestampStep(t *testing.T) {
	rows := buildMonth("BTCUSDT", 1_700_000_000_000, 3)
	rows[2].TimestampUTC = rows[1].TimestampUTC + 120_000 // 2-minute gap
	store := newStore(t, map[string][]byte{"k": writeRows(t, rows)})
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureRowInvariantViolated)
}

func TestReadFeatureFrame_RowInvariantViolated_SymbolMismatch(t *testing.T) {
	rows := buildMonth("BTCUSDT", 1_700_000_000_000, 3)
	rows[1].Symbol = "ETHUSDT" // violates single-symbol rule
	store := newStore(t, map[string][]byte{"k": writeRows(t, rows)})
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureRowInvariantViolated)
}

func TestReadFeatureFrame_RowInvariantViolated_RegimeFlagsNotExclusive(t *testing.T) {
	rows := buildMonth("BTCUSDT", 1_700_000_000_000, 2)
	// Warm state: two trues across trend flags.
	rows[1].TrendUp = pb(true)
	rows[1].TrendDown = pb(true)
	rows[1].Flat = pb(false)
	store := newStore(t, map[string][]byte{"k": writeRows(t, rows)})
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureRowInvariantViolated)
}

func TestReadFeatureFrame_RowInvariantViolated_NonAdjacentDuplicate(t *testing.T) {
	// A duplicate that is NOT on an adjacent month boundary must be rejected
	// even if the specific collision pattern is "last of M" vs "first of M+2".
	// Here we construct a simpler case: duplicate timestamp inside a single
	// partition is already not possible (sort step catches it first), so test
	// a three-way collision across two files which cannot be keep-first.
	rows1 := buildMonth("BTCUSDT", 1_700_000_000_000, 2)
	// Partition 2 shares both of partition 1's timestamps - not a boundary
	// overlap (too wide), must fail.
	rows2 := []featureRow{
		rows1[0], // collides with row 0 of p1 -- not a boundary pattern
		rows1[1],
	}
	store := newStore(t, map[string][]byte{
		"p1": writeRows(t, rows1),
		"p2": writeRows(t, rows2),
	})
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions: []Partition{
			{Year: 2023, Month: 11, ObjectKey: "p1"},
			{Year: 2023, Month: 12, ObjectKey: "p2"},
		},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureRowInvariantViolated)
}

func TestReadFeatureFrame_DatasetGap_BoundaryMissing(t *testing.T) {
	// Month 11 tail followed by Month 12 head with a 5-minute gap.
	rowsNov := buildMonth("BTCUSDT", 1_700_000_000_000, 3)
	tailTs := rowsNov[2].TimestampUTC
	rowsDec := []featureRow{
		{
			Symbol:        "BTCUSDT",
			TimestampUTC:  tailTs + 5*60_000, // 5-minute gap
			CloseTradeI64: 5_000_000_000_000,
			MarkCloseI64:  5_000_000_000_000,
		},
	}
	store := newStore(t, map[string][]byte{
		"nov": writeRows(t, rowsNov),
		"dec": writeRows(t, rowsDec),
	})
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions: []Partition{
			{Year: 2023, Month: 11, ObjectKey: "nov"},
			{Year: 2023, Month: 12, ObjectKey: "dec"},
		},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureDatasetGap)
}

func TestReadFeatureFrame_DatasetGap_SkipMonth(t *testing.T) {
	rowsNov := buildMonth("BTCUSDT", 1_700_000_000_000, 3)
	// Skip December entirely: 11 -> 01/next-year would be non-adjacent.
	rowsJan := buildMonth("BTCUSDT", 1_710_000_000_000, 1)
	store := newStore(t, map[string][]byte{
		"nov": writeRows(t, rowsNov),
		"jan": writeRows(t, rowsJan),
	})
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions: []Partition{
			{Year: 2023, Month: 11, ObjectKey: "nov"},
			{Year: 2024, Month: 1, ObjectKey: "jan"},
		},
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureDatasetGap)
}

func TestReadFeatureFrame_DatasetEmpty(t *testing.T) {
	// Empty partition list.
	store := newStore(t, nil)
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions:     nil,
	}
	_, err := ReadFeatureFrame(context.Background(), store, req, nil)
	assertReason(t, err, ReasonFeatureDatasetEmpty)
}

// ---------------------------------------------------------------------------
// Sanity: ContextCanceled is surfaced as a plain error, not a ReaderError.
// The worker can distinguish IO interruption from a data-contract failure.
// ---------------------------------------------------------------------------

func TestReadFeatureFrame_ContextCanceled(t *testing.T) {
	rows := buildMonth("BTCUSDT", 1_700_000_000_000, 2)
	store := newStore(t, map[string][]byte{"k": writeRows(t, rows)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := ReadRequest{
		FeatureSet:     defaultKey(),
		ExpectedSymbol: "BTCUSDT",
		Partitions:     []Partition{{Year: 2023, Month: 11, ObjectKey: "k"}},
	}
	_, err := ReadFeatureFrame(ctx, store, req, nil)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if _, isReader := AsReaderError(err); isReader {
		t.Fatalf("context cancel leaked through ReaderError layer: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
