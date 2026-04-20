package featuredata

// FeatureFrame is the materialized, cross-partition view of a feature dataset
// for a single run. It is the cold-path handoff point between the reader (M3)
// and the compile step (M4); the hot bar loop should bind directly to the
// Values slices by index and never go through maps per bar.
//
// Invariants upheld on construction:
//   - Timestamps is strictly increasing, minute-aligned (every step 60_000 ms).
//   - Len(Timestamps) == RowCount.
//   - For every materialized column: len(col.Values) == RowCount.
//   - Int64Columns never carry nulls (required by the contract).
//   - Floats/Bools may carry nulls; callers MUST consult the validity bitmap.
//   - Symbol is a single constant across all rows (v1 is single-symbol per dataset).
type FeatureFrame struct {
	FeatureSet FeatureSetKey
	Symbol     string
	// PriceScale is the power-of-ten divisor for close_trade_i64 / mark_close_i64.
	// For contract v1 it is pinned per (code, version) via PriceScaleRegistry.
	PriceScale int32

	RowCount int

	Timestamps []int64

	Int64s map[ColumnName]*Int64Column
	Floats map[ColumnName]*Float64Column
	Bools  map[ColumnName]*BoolColumn
}

// Int64Column holds a required, non-null int64 column (e.g. close_trade_i64).
// Valid is always nil for contract-required columns; the field exists so
// downstream code can treat it uniformly.
type Int64Column struct {
	Name   ColumnName
	Values []int64
	Valid  []uint64 // nil for required columns, else bitmap
}

// Float64Column holds a nullable float64 column (indicators, funding rates, etc.).
// Values[i] is meaningful only when IsValid(i) is true; during warmup it will
// typically be 0.0 but callers MUST NOT interpret it.
type Float64Column struct {
	Name   ColumnName
	Values []float64
	Valid  []uint64 // bitmap, len == (RowCount+63)/64
}

// BoolColumn holds a nullable boolean column (regime / volatility flags).
// Values[i] is 0 or 1; interpret only when IsValid(i) is true.
type BoolColumn struct {
	Name   ColumnName
	Values []uint8
	Valid  []uint64
}

// Bitmap helpers (internal).

func newBitmap(n int) []uint64 {
	if n <= 0 {
		return nil
	}
	return make([]uint64, (n+63)/64)
}

func setBit(bm []uint64, i int) {
	bm[i>>6] |= 1 << uint(i&63)
}

func testBit(bm []uint64, i int) bool {
	if bm == nil {
		return true
	}
	return bm[i>>6]&(1<<uint(i&63)) != 0
}

// IsValid reports whether index i holds a real value or is null.
// Required columns (Valid == nil) always report true.
func (c *Int64Column) IsValid(i int) bool { return testBit(c.Valid, i) }

// IsValid reports whether index i holds a real value or is null.
func (c *Float64Column) IsValid(i int) bool { return testBit(c.Valid, i) }

// IsValid reports whether index i holds a real value or is null.
func (c *BoolColumn) IsValid(i int) bool { return testBit(c.Valid, i) }
