// Package runresolve is the M5 dataset-resolution boundary of the backtest
// engine. Given an experiment_run id it walks control-plane's relational
// chain to the feature-parquet dataset the engine should read:
//
//	experiment_run
//	  → experiment_batch.feature_set_version_id
//	  → feature_set_version (version, feature_set_id)
//	  → feature_set (code)
//	  ⇒ featuredata.FeatureSetKey{Code, Version}
//
// It then discovers the monthly feature parquet dataset keyed by (symbol,
// dataset_type="feature_<code>_<interval>") and loads its partitions.
//
// Like cpclient, this package owns only HTTP/read concerns — it does NOT
// open parquet files, touch MinIO, or verify column availability. Those are
// featuredata / featurecompat responsibilities.
//
// The resolver is deliberately read-only: ADR-004 v2 puts all terminal run
// state on control-plane's worker. The engine never PATCHes datasets or
// feature_set_versions back.
package runresolve

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/cpclient"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
)

// Default dataset listing limit. Feature-builder registers one dataset per
// (symbol, dataset_type) build, so in practice we only need the most recent
// entry; we still fetch a small page so tests can exercise tie-breaking.
const defaultDatasetListLimit = 50

// ErrDatasetNotFound is returned when no feature-type dataset matches the
// resolved (symbol, code, interval) triple. Callers map this onto a
// bt.run.failed event with a human-readable reason.
var ErrDatasetNotFound = errors.New("runresolve: feature dataset not found")

// ErrAmbiguousDataset is returned when more than one dataset with the exact
// same (symbol, dataset_type, period) is listed by control-plane. CP's
// current feature-builder flow never creates such collisions, but we refuse
// to silently pick one if it happens.
var ErrAmbiguousDataset = errors.New("runresolve: multiple feature datasets with identical signature")

// Resolved captures every CP-side fact the engine needs to start reading
// feature parquet for a single run. It does NOT decide the executor layout —
// that belongs to the downstream DSL-driven executor.
//
// Partitions are sorted by (Year, Month) ascending; callers may pass them
// straight into featuredata.ReadFeatureFrame.
type Resolved struct {
	Run       cpclient.ExperimentRun
	Batch     cpclient.ExperimentBatch
	FeatureSet        cpclient.FeatureSet
	FeatureSetVersion cpclient.FeatureSetVersion

	Key featuredata.FeatureSetKey

	Dataset    cpclient.Dataset
	Partitions []featuredata.Partition

	Symbol   string
	Interval string
}

// Resolve walks the relational chain from an experiment_run to the bound
// feature-parquet dataset and returns a fully typed Resolved struct.
//
//   - interval selects which monthly feature dataset to bind. For MVP all
//     runs are "1m"; the argument is explicit so v2 plans carrying a
//     non-"1m" interval do not silently fall back to the wrong dataset.
//
// Any HTTP error from cpclient is returned wrapped; cpclient.ErrNotFound
// matches for 404s at any hop and callers can branch on it.
func Resolve(ctx context.Context, cp *cpclient.Client, runID, interval string) (*Resolved, error) {
	if cp == nil {
		return nil, errors.New("runresolve: cp client is nil")
	}
	if runID == "" {
		return nil, errors.New("runresolve: empty run id")
	}
	if interval == "" {
		return nil, errors.New("runresolve: empty interval")
	}

	run, err := cp.GetExperimentRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("get experiment_run %s: %w", runID, err)
	}
	if run.ExperimentBatchID == "" {
		return nil, fmt.Errorf("experiment_run %s: empty experiment_batch_id", runID)
	}
	if run.Symbol == "" {
		return nil, fmt.Errorf("experiment_run %s: empty symbol", runID)
	}

	batch, err := cp.GetExperimentBatch(ctx, run.ExperimentBatchID)
	if err != nil {
		return nil, fmt.Errorf("get experiment_batch %s: %w", run.ExperimentBatchID, err)
	}
	if batch.FeatureSetVersionID == "" {
		return nil, fmt.Errorf("experiment_batch %s: empty feature_set_version_id", batch.ID)
	}

	fsv, err := cp.GetFeatureSetVersion(ctx, batch.FeatureSetVersionID)
	if err != nil {
		return nil, fmt.Errorf("get feature_set_version %s: %w", batch.FeatureSetVersionID, err)
	}
	if fsv.FeatureSetID == "" {
		return nil, fmt.Errorf("feature_set_version %s: empty feature_set_id", fsv.ID)
	}
	if fsv.Version <= 0 {
		return nil, fmt.Errorf("feature_set_version %s: non-positive Version=%d", fsv.ID, fsv.Version)
	}

	fs, err := cp.GetFeatureSet(ctx, fsv.FeatureSetID)
	if err != nil {
		return nil, fmt.Errorf("get feature_set %s: %w", fsv.FeatureSetID, err)
	}
	if fs.Code == "" {
		return nil, fmt.Errorf("feature_set %s: empty Code", fs.ID)
	}

	key := featuredata.FeatureSetKey{Code: fs.Code, Version: fsv.Version}

	// Feature-builder registers feature datasets with dataset_type encoded as
	// "feature_<code>_<interval>" (see services/feature-builder §DatasetType).
	// We discover the matching dataset by filtering ListDatasets on
	// (symbol, dataset_type); the CP does not yet carry a foreign key from
	// datasets to feature_set_versions, so this encoding is the link.
	datasetType := "feature_" + fs.Code + "_" + interval
	list, err := cp.ListDatasets(ctx, run.Symbol, datasetType, defaultDatasetListLimit)
	if err != nil {
		return nil, fmt.Errorf("list datasets symbol=%s type=%s: %w", run.Symbol, datasetType, err)
	}
	ds, err := pickFeatureDataset(list, key)
	if err != nil {
		return nil, fmt.Errorf("resolve dataset symbol=%s type=%s: %w", run.Symbol, datasetType, err)
	}

	raw, err := cp.ListDatasetPartitions(ctx, ds.ID)
	if err != nil {
		return nil, fmt.Errorf("list dataset_partitions %s: %w", ds.ID, err)
	}
	parts := toFeaturePartitions(raw)

	return &Resolved{
		Run:               *run,
		Batch:             *batch,
		FeatureSet:        *fs,
		FeatureSetVersion: *fsv,
		Key:               key,
		Dataset:           ds,
		Partitions:        parts,
		Symbol:            run.Symbol,
		Interval:          interval,
	}, nil
}

// pickFeatureDataset selects a single Dataset from a cpclient listing. We
// reject empty lists (no build for the (symbol, fs) yet) and refuse to pick
// one out of several identical candidates — CP's schema permits multiple
// feature-builder runs per (symbol, code, interval) but we require the
// operator to clean ambiguity before the engine binds.
func pickFeatureDataset(list []cpclient.Dataset, key featuredata.FeatureSetKey) (cpclient.Dataset, error) {
	if len(list) == 0 {
		return cpclient.Dataset{}, ErrDatasetNotFound
	}
	// The list endpoint returns datasets recency-first (CP orders by created_at DESC).
	// We keep that convention but verify there isn't an exact-signature tie at the head.
	head := list[0]
	if len(list) > 1 {
		next := list[1]
		if head.Symbol == next.Symbol &&
			head.DatasetType == next.DatasetType &&
			head.Interval == next.Interval &&
			head.PeriodFrom.Equal(next.PeriodFrom) &&
			head.PeriodTo.Equal(next.PeriodTo) {
			return cpclient.Dataset{}, fmt.Errorf("%w: key=%+v at %s/%s", ErrAmbiguousDataset, key, head.DatasetType, head.Symbol)
		}
	}
	return head, nil
}

// featureParquetFileName is the fixed leaf filename feature-builder writes for
// every monthly feature dataset. The producer stores DatasetPartition.S3Path
// as the *directory* prefix (…/year=YYYY/month=MM) and materialises the parquet
// at "<S3Path>/data.parquet"; the reader consumes full object keys, so the
// resolver appends this constant once. See
// services/feature-builder/internal/app/build_features.go (datasetPartitionKeys).
const featureParquetFileName = "data.parquet"

// toFeaturePartitions converts cpclient-typed partitions into the narrower
// featuredata.Partition the reader consumes, sorting by (Year, Month) ascending
// so downstream chronological invariants hold without another pass.
//
// Each partition's S3Path is treated as a month-level directory prefix per
// feature-builder's convention; the fixed parquet file name is appended so the
// output ObjectKey is a complete storage key. A path already ending in a
// concrete .parquet object is passed through unchanged so the resolver stays
// forward-compatible with a producer that registers full object keys.
func toFeaturePartitions(raw []cpclient.DatasetPartition) []featuredata.Partition {
	out := make([]featuredata.Partition, 0, len(raw))
	for _, p := range raw {
		out = append(out, featuredata.Partition{
			Year:      p.Year,
			Month:     p.Month,
			ObjectKey: objectKeyForPartition(p.S3Path),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Year != out[j].Year {
			return out[i].Year < out[j].Year
		}
		return out[i].Month < out[j].Month
	})
	return out
}

// objectKeyForPartition normalises a dataset_partitions.s3_path row into the
// full parquet object key the reader will request from storage. Leading
// slashes are stripped (MinIO / storage.ObjectReader expect bare keys) and
// the fixed feature-parquet leaf filename is appended when s3_path points at
// a directory prefix.
func objectKeyForPartition(s3Path string) string {
	s := trimLeadingSlash(s3Path)
	if s == "" {
		return ""
	}
	if endsWith(s, ".parquet") {
		return s
	}
	return s + "/" + featureParquetFileName
}

func trimLeadingSlash(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}

func endsWith(s, suffix string) bool {
	if len(suffix) > len(s) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}
