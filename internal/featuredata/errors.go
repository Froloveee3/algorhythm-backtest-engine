package featuredata

import (
	"errors"
	"fmt"
)

// Reason is the stable string tag that flows into bt.run.failed.payload.reason.
// Values are pinned by feature-parquet-v1.md §8; renaming them is a client-visible
// contract change.
type Reason string

const (
	// ReasonFeatureSetUnsupported - (feature_set_code, feature_set_version) is not
	// in the consumer's PriceScaleRegistry / column-set whitelist.
	ReasonFeatureSetUnsupported Reason = "feature_set_unsupported"
	// ReasonFeaturePartitionMissing - an expected monthly partition object is absent.
	ReasonFeaturePartitionMissing Reason = "feature_partition_missing"
	// ReasonFeatureSchemaMismatch - parquet schema does not match v1 column set / types.
	ReasonFeatureSchemaMismatch Reason = "feature_schema_mismatch"
	// ReasonFeatureRowInvariantViolated - sort order, minute continuity, nulls in required
	// fields, regime-flag exclusivity, duplicate timestamps inside a file, etc.
	ReasonFeatureRowInvariantViolated Reason = "feature_row_invariant_violated"
	// ReasonFeatureDatasetGap - gap between adjacent monthly partitions is not
	// exactly 60_000 ms after dedupe, or duplicates straddle a non-adjacent boundary.
	ReasonFeatureDatasetGap Reason = "feature_dataset_gap"
	// ReasonFeatureDatasetEmpty - no rows survived across all partitions.
	ReasonFeatureDatasetEmpty Reason = "feature_dataset_empty"
)

// ReaderError carries a stable machine-readable Reason alongside a human-readable
// message. Reason is what the worker publishes in bt.run.failed; Message is for
// logs and detail fields. Use errors.As to recover the ReaderError from callers.
type ReaderError struct {
	Reason  Reason
	Message string
	// Cause is optional; present when wrapping an underlying IO / parquet error.
	Cause error
}

func (e *ReaderError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("feature-reader: %s: %s: %v", e.Reason, e.Message, e.Cause)
	}
	return fmt.Sprintf("feature-reader: %s: %s", e.Reason, e.Message)
}

func (e *ReaderError) Unwrap() error { return e.Cause }

func newReaderError(reason Reason, msg string, cause error) *ReaderError {
	return &ReaderError{Reason: reason, Message: msg, Cause: cause}
}

// AsReaderError extracts the typed reader error from an error chain; useful
// for the worker to map onto bt.run.failed payloads.
func AsReaderError(err error) (*ReaderError, bool) {
	var re *ReaderError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}
