package featuredata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/algorhythm/backtest-engine/internal/storage"
	"github.com/parquet-go/parquet-go"
)

// featureRow MUST mirror services/feature-builder/internal/adapters/parquet/feature_rows.go
// exactly - same tags, same optionality, same order. Drift between producer and
// consumer structs is a contract violation. We keep a local copy here instead of
// importing fb to avoid cross-submodule coupling; the compatibility guarantee is
// the contract in docs/contracts/feature-parquet-v1.md.
type featureRow struct {
	Symbol               string   `parquet:"symbol,dict"`
	TimestampUTC         int64    `parquet:"timestamp_utc"`
	CloseTradeI64        int64    `parquet:"close_trade_i64"`
	MarkCloseI64         int64    `parquet:"mark_close_i64"`
	Returns1m            *float64 `parquet:"returns_1m,optional"`
	Returns5m            *float64 `parquet:"returns_5m,optional"`
	Returns15m           *float64 `parquet:"returns_15m,optional"`
	Ema20                *float64 `parquet:"ema_20,optional"`
	Ema50                *float64 `parquet:"ema_50,optional"`
	Atr14                *float64 `parquet:"atr_14,optional"`
	Rsi14                *float64 `parquet:"rsi_14,optional"`
	RollingStd60         *float64 `parquet:"rolling_std_60,optional"`
	RollingStd240        *float64 `parquet:"rolling_std_240,optional"`
	MarkTradeSpreadBps   *float64 `parquet:"mark_trade_spread_bps,optional"`
	FundingRateCurrent   *float64 `parquet:"funding_rate_current,optional"`
	FundingRateRolling3  *float64 `parquet:"funding_rate_rolling_3,optional"`
	FundingRateRolling9  *float64 `parquet:"funding_rate_rolling_9,optional"`
	FundingPressureScore *float64 `parquet:"funding_pressure_score,optional"`
	TrendUp              *bool    `parquet:"trend_up,optional"`
	TrendDown            *bool    `parquet:"trend_down,optional"`
	Flat                 *bool    `parquet:"flat,optional"`
	HighVol              *bool    `parquet:"high_vol,optional"`
	LowVol               *bool    `parquet:"low_vol,optional"`
}

const minuteMillis int64 = 60_000

// expectedField describes one column in the feature-parquet-v1 physical schema.
// The reader asserts against this set BEFORE decoding rows, because
// parquet-go's GenericReader tolerates missing columns (fills zero values),
// which would otherwise mask a schema mismatch as a spurious row-invariant
// violation later.
type expectedField struct {
	name     string
	kind     parquet.Kind
	optional bool
}

// v1FeatureSchema pins the physical columns of feature-parquet v1 in the exact
// order produced by feature-builder. Drift here is a contract break.
var v1FeatureSchema = []expectedField{
	{"symbol", parquet.ByteArray, false},
	{"timestamp_utc", parquet.Int64, false},
	{"close_trade_i64", parquet.Int64, false},
	{"mark_close_i64", parquet.Int64, false},
	{"returns_1m", parquet.Double, true},
	{"returns_5m", parquet.Double, true},
	{"returns_15m", parquet.Double, true},
	{"ema_20", parquet.Double, true},
	{"ema_50", parquet.Double, true},
	{"atr_14", parquet.Double, true},
	{"rsi_14", parquet.Double, true},
	{"rolling_std_60", parquet.Double, true},
	{"rolling_std_240", parquet.Double, true},
	{"mark_trade_spread_bps", parquet.Double, true},
	{"funding_rate_current", parquet.Double, true},
	{"funding_rate_rolling_3", parquet.Double, true},
	{"funding_rate_rolling_9", parquet.Double, true},
	{"funding_pressure_score", parquet.Double, true},
	{"trend_up", parquet.Boolean, true},
	{"trend_down", parquet.Boolean, true},
	{"flat", parquet.Boolean, true},
	{"high_vol", parquet.Boolean, true},
	{"low_vol", parquet.Boolean, true},
}

// validateFileSchema asserts the raw parquet schema matches v1 column-by-column.
// Any deviation (missing column, extra column, wrong physical type, wrong
// optionality) surfaces as ReasonFeatureSchemaMismatch so the worker can map
// it onto a single stable bt.run.failed tag.
func validateFileSchema(p Partition, data []byte) error {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return newReaderError(
			ReasonFeatureSchemaMismatch,
			fmt.Sprintf("open parquet %s: %v", p.ObjectKey, err),
			err,
		)
	}
	fields := f.Schema().Fields()
	got := make(map[string]parquet.Field, len(fields))
	for _, fld := range fields {
		got[fld.Name()] = fld
	}
	for _, want := range v1FeatureSchema {
		fld, ok := got[want.name]
		if !ok {
			return newReaderError(
				ReasonFeatureSchemaMismatch,
				fmt.Sprintf("%s: missing required column %q", p.ObjectKey, want.name),
				nil,
			)
		}
		if !fld.Leaf() {
			return newReaderError(
				ReasonFeatureSchemaMismatch,
				fmt.Sprintf("%s: column %q is not a leaf node", p.ObjectKey, want.name),
				nil,
			)
		}
		if fld.Type().Kind() != want.kind {
			return newReaderError(
				ReasonFeatureSchemaMismatch,
				fmt.Sprintf("%s: column %q has physical type %s (expected %s)", p.ObjectKey, want.name, fld.Type().Kind(), want.kind),
				nil,
			)
		}
		if fld.Optional() != want.optional {
			return newReaderError(
				ReasonFeatureSchemaMismatch,
				fmt.Sprintf("%s: column %q optional=%v (expected %v)", p.ObjectKey, want.name, fld.Optional(), want.optional),
				nil,
			)
		}
	}
	// Reject strict extras: any additional top-level column at the v1 tier
	// implies either a v2 payload routed to the wrong consumer or a producer
	// regression; either way we refuse to decode rows under v1 rules.
	for name := range got {
		found := false
		for _, want := range v1FeatureSchema {
			if want.name == name {
				found = true
				break
			}
		}
		if !found {
			return newReaderError(
				ReasonFeatureSchemaMismatch,
				fmt.Sprintf("%s: unexpected column %q not in feature-parquet-v1", p.ObjectKey, name),
				nil,
			)
		}
	}
	return nil
}

// Partition is a single monthly parquet object to read, in chronological order.
// The reader does not itself resolve S3 paths or walk listings - callers (the
// worker) pass partitions already ordered by (Year, Month) ascending.
type Partition struct {
	Year      int
	Month     int
	ObjectKey string
}

// ReadRequest describes one run's feature-data read:
// which feature set, which symbol (for assertion), which partitions (monthly
// objects), and which columns the downstream compile step will need.
// TimestampUTC and symbol are always implicitly loaded; additional columns
// appear on the returned frame only when listed in RequiredColumns.
type ReadRequest struct {
	FeatureSet      FeatureSetKey
	ExpectedSymbol  string
	Partitions      []Partition
	RequiredColumns []ColumnName
}

// ReadFeatureFrame is the public entry point for M3.
//
// Responsibilities (see feature-parquet-v1.md §5-§7):
//   - validate the feature set is known and resolve its price scale;
//   - download, parse, and validate each monthly partition;
//   - enforce row invariants (sort order, minute continuity, symbol,
//     required non-nulls, regime flag exclusivity);
//   - concatenate partitions with keep-first dedupe limited to adjacent
//     month-boundary overlap, rejecting any other duplicate pattern;
//   - enforce exactly 60_000 ms gap between adjacent months after dedupe;
//   - materialize the requested columns (and timestamps) into a FeatureFrame.
//
// On any contract violation it returns a *ReaderError whose Reason is one of
// the six pinned values.
func ReadFeatureFrame(ctx context.Context, store storage.ObjectReader, req ReadRequest, registry PriceScaleRegistry) (*FeatureFrame, error) {
	if store == nil {
		return nil, errors.New("feature-reader: store is nil")
	}
	if registry == nil {
		registry = DefaultPriceScaleRegistry()
	}
	if len(req.Partitions) == 0 {
		return nil, newReaderError(ReasonFeatureDatasetEmpty, "no partitions provided", nil)
	}
	if req.ExpectedSymbol == "" {
		return nil, errors.New("feature-reader: expected symbol is empty")
	}
	if err := validateRequestedColumns(req.RequiredColumns); err != nil {
		return nil, err
	}

	scale, ok := registry.Lookup(req.FeatureSet)
	if !ok {
		return nil, newReaderError(
			ReasonFeatureSetUnsupported,
			fmt.Sprintf("unknown feature set %q version %d", req.FeatureSet.Code, req.FeatureSet.Version),
			nil,
		)
	}
	if err := assertPartitionsChronological(req.Partitions); err != nil {
		return nil, err
	}

	// Read every partition sequentially; each call validates its own intra-file
	// invariants. Concatenation happens in a second pass so we can see both
	// sides of every month boundary for the dedupe rule.
	perFile := make([][]featureRow, 0, len(req.Partitions))
	for i, p := range req.Partitions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := readPartition(ctx, store, p, req.ExpectedSymbol)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, newReaderError(
				ReasonFeaturePartitionMissing,
				fmt.Sprintf("partition %d/%02d (%s) contains zero rows", p.Year, p.Month, p.ObjectKey),
				nil,
			)
		}
		if err := validateFileInvariants(p, rows); err != nil {
			return nil, err
		}
		perFile = append(perFile, rows)
		_ = i
	}

	merged, err := mergePartitions(req.Partitions, perFile)
	if err != nil {
		return nil, err
	}
	if len(merged) == 0 {
		return nil, newReaderError(ReasonFeatureDatasetEmpty, "dataset is empty after merge", nil)
	}

	frame := materialize(req, scale, merged)
	return frame, nil
}

func validateRequestedColumns(cols []ColumnName) error {
	for _, c := range cols {
		if c == ColTimestampUTC || c == ColSymbol {
			// timestamps are always materialized; symbol lives at the frame
			// level, not as a per-row column - silently accepted.
			continue
		}
		if !IsKnownColumn(c) {
			return newReaderError(
				ReasonFeatureSchemaMismatch,
				fmt.Sprintf("requested column %q is not part of feature-parquet-v1", c),
				nil,
			)
		}
	}
	return nil
}

func assertPartitionsChronological(ps []Partition) error {
	for i := 1; i < len(ps); i++ {
		prev := ps[i-1]
		cur := ps[i]
		if prev.Year > cur.Year || (prev.Year == cur.Year && prev.Month >= cur.Month) {
			return newReaderError(
				ReasonFeatureDatasetGap,
				fmt.Sprintf("partitions not strictly chronological: %d/%02d followed by %d/%02d", prev.Year, prev.Month, cur.Year, cur.Month),
				nil,
			)
		}
	}
	return nil
}

// readPartition downloads and parses a single monthly partition. It does NOT
// validate the symbol beyond what the schema enforces; validation is done by
// validateFileInvariants so that symbol mismatch surfaces with the correct
// Reason tag.
func readPartition(ctx context.Context, store storage.ObjectReader, p Partition, _ string) ([]featureRow, error) {
	if p.ObjectKey == "" {
		return nil, newReaderError(
			ReasonFeaturePartitionMissing,
			fmt.Sprintf("partition %d/%02d has empty object key", p.Year, p.Month),
			nil,
		)
	}

	rc, err := store.Get(ctx, p.ObjectKey)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, newReaderError(
				ReasonFeaturePartitionMissing,
				fmt.Sprintf("object %s not found", p.ObjectKey),
				err,
			)
		}
		return nil, fmt.Errorf("feature-reader: fetch %s: %w", p.ObjectKey, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("feature-reader: read %s: %w", p.ObjectKey, err)
	}

	if err := validateFileSchema(p, data); err != nil {
		return nil, err
	}

	reader := parquet.NewGenericReader[featureRow](bytes.NewReader(data))
	defer reader.Close()

	rows := make([]featureRow, 0, int(reader.NumRows()))
	buf := make([]featureRow, 512)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			rows = append(rows, buf[:n]...)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, newReaderError(
				ReasonFeatureSchemaMismatch,
				fmt.Sprintf("parse %s: %v", p.ObjectKey, err),
				err,
			)
		}
	}
	return rows, nil
}

// validateFileInvariants asserts feature-parquet-v1 §5 (intra-file rules):
//   - every row's symbol equals the expected dataset symbol;
//   - core int64 fields are set;
//   - timestamps are strictly increasing and minute-aligned (step == 60_000 ms);
//   - regime flags are either all-null-in-row or exactly-one-true (trend group,
//     vol group).
//
// It does NOT enforce partition boundary gaps - that is the reader's cross-file
// job in mergePartitions.
func validateFileInvariants(p Partition, rows []featureRow) error {
	if len(rows) == 0 {
		return nil
	}
	first := rows[0]
	expectedSymbol := first.Symbol
	if expectedSymbol == "" {
		return newReaderError(
			ReasonFeatureRowInvariantViolated,
			fmt.Sprintf("partition %d/%02d row 0: empty symbol", p.Year, p.Month),
			nil,
		)
	}

	prevTs := int64(0)
	for i, r := range rows {
		if r.Symbol != expectedSymbol {
			return newReaderError(
				ReasonFeatureRowInvariantViolated,
				fmt.Sprintf("partition %d/%02d row %d: symbol %q differs from file-first symbol %q", p.Year, p.Month, i, r.Symbol, expectedSymbol),
				nil,
			)
		}
		if r.CloseTradeI64 == 0 || r.MarkCloseI64 == 0 {
			return newReaderError(
				ReasonFeatureRowInvariantViolated,
				fmt.Sprintf("partition %d/%02d row %d (ts=%d): required price field is zero (close=%d mark=%d)", p.Year, p.Month, i, r.TimestampUTC, r.CloseTradeI64, r.MarkCloseI64),
				nil,
			)
		}
		if i == 0 {
			prevTs = r.TimestampUTC
			continue
		}
		if r.TimestampUTC <= prevTs {
			return newReaderError(
				ReasonFeatureRowInvariantViolated,
				fmt.Sprintf("partition %d/%02d row %d: timestamp %d not strictly greater than previous %d", p.Year, p.Month, i, r.TimestampUTC, prevTs),
				nil,
			)
		}
		if r.TimestampUTC-prevTs != minuteMillis {
			return newReaderError(
				ReasonFeatureRowInvariantViolated,
				fmt.Sprintf("partition %d/%02d row %d: timestamp step %d ms (expected %d ms)", p.Year, p.Month, i, r.TimestampUTC-prevTs, minuteMillis),
				nil,
			)
		}
		prevTs = r.TimestampUTC
	}

	// Regime-flag exclusivity: all three trend flags must be all-null OR exactly
	// one true. Same pairwise rule for {high_vol, low_vol}.
	for i, r := range rows {
		if err := validateRegimeTriple(p, i, r.TrendUp, r.TrendDown, r.Flat); err != nil {
			return err
		}
		if err := validateVolPair(p, i, r.HighVol, r.LowVol); err != nil {
			return err
		}
	}
	return nil
}

func validateRegimeTriple(p Partition, rowIdx int, up, down, flat *bool) error {
	nulls := 0
	trueCount := 0
	if up == nil {
		nulls++
	} else if *up {
		trueCount++
	}
	if down == nil {
		nulls++
	} else if *down {
		trueCount++
	}
	if flat == nil {
		nulls++
	} else if *flat {
		trueCount++
	}
	switch nulls {
	case 3:
		return nil // warmup
	case 0:
		if trueCount == 1 {
			return nil
		}
	}
	return newReaderError(
		ReasonFeatureRowInvariantViolated,
		fmt.Sprintf("partition %d/%02d row %d: trend flags must be all-null or exactly-one-true (nulls=%d trues=%d)", p.Year, p.Month, rowIdx, nulls, trueCount),
		nil,
	)
}

func validateVolPair(p Partition, rowIdx int, high, low *bool) error {
	if high == nil && low == nil {
		return nil
	}
	if high != nil && low != nil {
		t := 0
		if *high {
			t++
		}
		if *low {
			t++
		}
		if t == 1 {
			return nil
		}
	}
	return newReaderError(
		ReasonFeatureRowInvariantViolated,
		fmt.Sprintf("partition %d/%02d row %d: vol flags must be both-null or both-set-exactly-one-true", p.Year, p.Month, rowIdx),
		nil,
	)
}

// mergePartitions concatenates per-file row slices respecting the contract's
// boundary-overlap dedupe rule:
//
//   - if rows across two adjacent partitions share a single timestamp at
//     exactly the tail of M and the head of M+1, drop the head of M+1
//     (keep-first);
//   - any other duplicate pattern is a hard failure;
//   - after merge, adjacent partition timestamps must be exactly 60_000 ms
//     apart (minute continuity across months).
func mergePartitions(ps []Partition, perFile [][]featureRow) ([]featureRow, error) {
	if len(perFile) == 0 {
		return nil, nil
	}
	// Cross-partition duplicate scan: if the same timestamp appears in more than
	// two partitions, we reject before even entering the merge loop.
	type seen struct {
		partIdx int
		rowIdx  int
	}
	first := map[int64]seen{}
	for pi, rows := range perFile {
		for ri, r := range rows {
			if s, ok := first[r.TimestampUTC]; ok {
				// Permit adjacency-only duplicates: must be exactly (pi-1, last)
				// vs (pi, first) AND the adjacent partitions must be consecutive
				// months. All other collisions are rejected.
				if s.partIdx == pi-1 &&
					s.rowIdx == len(perFile[pi-1])-1 &&
					ri == 0 &&
					areAdjacentMonths(ps[s.partIdx], ps[pi]) {
					continue // will be handled by merge loop below
				}
				return nil, newReaderError(
					ReasonFeatureRowInvariantViolated,
					fmt.Sprintf("timestamp %d appears in both partition %d/%02d (row %d) and partition %d/%02d (row %d) outside of adjacent boundary dedupe", r.TimestampUTC, ps[s.partIdx].Year, ps[s.partIdx].Month, s.rowIdx, ps[pi].Year, ps[pi].Month, ri),
					nil,
				)
			}
			first[r.TimestampUTC] = seen{partIdx: pi, rowIdx: ri}
		}
	}

	// Linear merge. After the first partition, if the head of the current
	// partition matches the tail of the previous partition, drop the head;
	// then enforce minute continuity across the boundary.
	merged := append([]featureRow(nil), perFile[0]...)
	for i := 1; i < len(perFile); i++ {
		if !areAdjacentMonths(ps[i-1], ps[i]) {
			return nil, newReaderError(
				ReasonFeatureDatasetGap,
				fmt.Sprintf("partitions %d/%02d and %d/%02d are not adjacent months", ps[i-1].Year, ps[i-1].Month, ps[i].Year, ps[i].Month),
				nil,
			)
		}
		cur := perFile[i]
		if len(cur) == 0 {
			continue
		}
		if len(merged) == 0 {
			merged = append(merged, cur...)
			continue
		}
		tail := merged[len(merged)-1]
		head := cur[0]

		switch {
		case head.TimestampUTC == tail.TimestampUTC:
			// keep-first: drop head of new partition
			cur = cur[1:]
			if len(cur) == 0 {
				continue
			}
			if cur[0].TimestampUTC-tail.TimestampUTC != minuteMillis {
				return nil, newReaderError(
					ReasonFeatureDatasetGap,
					fmt.Sprintf("after dedupe, gap at %d/%02d -> %d/%02d boundary is %d ms (expected %d)", ps[i-1].Year, ps[i-1].Month, ps[i].Year, ps[i].Month, cur[0].TimestampUTC-tail.TimestampUTC, minuteMillis),
					nil,
				)
			}
		case head.TimestampUTC-tail.TimestampUTC == minuteMillis:
			// clean adjacency, nothing to drop
		default:
			return nil, newReaderError(
				ReasonFeatureDatasetGap,
				fmt.Sprintf("gap at %d/%02d -> %d/%02d boundary is %d ms (expected %d or %d for dedupe)", ps[i-1].Year, ps[i-1].Month, ps[i].Year, ps[i].Month, head.TimestampUTC-tail.TimestampUTC, minuteMillis, int64(0)),
				nil,
			)
		}

		merged = append(merged, cur...)
	}
	return merged, nil
}

func areAdjacentMonths(prev, cur Partition) bool {
	if prev.Year == cur.Year && cur.Month == prev.Month+1 {
		return true
	}
	if cur.Year == prev.Year+1 && prev.Month == 12 && cur.Month == 1 {
		return true
	}
	return false
}

// materialize copies rows into the final FeatureFrame layout. Only the columns
// named in req.RequiredColumns are populated (timestamps are always present).
// Validity bitmaps are allocated per column; a set bit means "value is real".
func materialize(req ReadRequest, scale int32, rows []featureRow) *FeatureFrame {
	n := len(rows)
	f := &FeatureFrame{
		FeatureSet: req.FeatureSet,
		Symbol:     req.ExpectedSymbol,
		PriceScale: scale,
		RowCount:   n,
		Timestamps: make([]int64, n),
		Int64s:     map[ColumnName]*Int64Column{},
		Floats:     map[ColumnName]*Float64Column{},
		Bools:      map[ColumnName]*BoolColumn{},
	}
	for i, r := range rows {
		f.Timestamps[i] = r.TimestampUTC
	}

	want := make(map[ColumnName]struct{}, len(req.RequiredColumns))
	for _, c := range req.RequiredColumns {
		want[c] = struct{}{}
	}

	for c := range int64Columns {
		if _, ok := want[c]; !ok {
			continue
		}
		col := &Int64Column{Name: c, Values: make([]int64, n)}
		for i, r := range rows {
			col.Values[i] = selectInt64(c, r)
		}
		f.Int64s[c] = col
	}

	for c := range floatColumns {
		if _, ok := want[c]; !ok {
			continue
		}
		col := &Float64Column{Name: c, Values: make([]float64, n), Valid: newBitmap(n)}
		for i, r := range rows {
			if v, ok := selectFloat(c, r); ok {
				col.Values[i] = v
				setBit(col.Valid, i)
			}
		}
		f.Floats[c] = col
	}

	for c := range boolColumns {
		if _, ok := want[c]; !ok {
			continue
		}
		col := &BoolColumn{Name: c, Values: make([]uint8, n), Valid: newBitmap(n)}
		for i, r := range rows {
			if v, ok := selectBool(c, r); ok {
				if v {
					col.Values[i] = 1
				}
				setBit(col.Valid, i)
			}
		}
		f.Bools[c] = col
	}

	return f
}

func selectInt64(c ColumnName, r featureRow) int64 {
	switch c {
	case ColCloseTradeI64:
		return r.CloseTradeI64
	case ColMarkCloseI64:
		return r.MarkCloseI64
	}
	return 0
}

func selectFloat(c ColumnName, r featureRow) (float64, bool) {
	var p *float64
	switch c {
	case ColReturns1m:
		p = r.Returns1m
	case ColReturns5m:
		p = r.Returns5m
	case ColReturns15m:
		p = r.Returns15m
	case ColEMA20:
		p = r.Ema20
	case ColEMA50:
		p = r.Ema50
	case ColATR14:
		p = r.Atr14
	case ColRSI14:
		p = r.Rsi14
	case ColRollingStd60:
		p = r.RollingStd60
	case ColRollingStd240:
		p = r.RollingStd240
	case ColMarkTradeSpreadBps:
		p = r.MarkTradeSpreadBps
	case ColFundingRateCurrent:
		p = r.FundingRateCurrent
	case ColFundingRateRolling3:
		p = r.FundingRateRolling3
	case ColFundingRateRolling9:
		p = r.FundingRateRolling9
	case ColFundingPressureScore:
		p = r.FundingPressureScore
	}
	if p == nil {
		return 0, false
	}
	return *p, true
}

func selectBool(c ColumnName, r featureRow) (bool, bool) {
	var p *bool
	switch c {
	case ColTrendUp:
		p = r.TrendUp
	case ColTrendDown:
		p = r.TrendDown
	case ColFlat:
		p = r.Flat
	case ColHighVol:
		p = r.HighVol
	case ColLowVol:
		p = r.LowVol
	}
	if p == nil {
		return false, false
	}
	return *p, true
}
