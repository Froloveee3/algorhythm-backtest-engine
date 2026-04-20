// Package storage is the backtest-engine's read-only view of the object
// store that holds raw / feature parquet. All production code accesses S3
// via the ObjectReader interface; the MinIO implementation is just the
// default wiring used by cmd/worker.
//
// Kept read-only on purpose: no code path in the engine should ever be
// *writing* to the object store during a backtest run. That avoids a
// whole class of accidents (engine overwriting a feature month, engine
// uploading rogue artefacts) without having to relitigate who owns what.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrObjectNotFound is returned by ObjectReader implementations when the
// requested key is absent. Callers can branch on this via errors.Is so
// the engine can turn "feature parquet missing" into a typed
// `bt.run.failed` reason instead of a generic 500.
var ErrObjectNotFound = errors.New("storage: object not found")

// ObjectStat is the metadata the parquet reader actually needs up-front:
// the object length, so it can plan how much to buffer / stream. Other
// fields can be added later (ETag for deterministic rerun checks).
type ObjectStat struct {
	Key  string
	Size int64
}

// ObjectReader is the read-only object-store interface the engine depends on.
// A single instance is safe for concurrent use.
type ObjectReader interface {
	// Stat returns the size of an object. Returns ErrObjectNotFound if the
	// object doesn't exist under the configured bucket.
	Stat(ctx context.Context, key string) (ObjectStat, error)

	// Get opens an object for streaming reads. Caller MUST close the returned
	// ReadCloser. Returns ErrObjectNotFound when missing.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// List lists all keys under the given prefix. The underlying SDK already
	// paginates; this method buffers the full result for convenience because
	// the engine only ever lists a single dataset at a time (bounded by
	// month partitions, not millions of objects).
	List(ctx context.Context, prefix string) ([]string, error)
}

// ---------------------------------------------------------------------------
// MinIO implementation
// ---------------------------------------------------------------------------

// Config holds the MinIO connection knobs the engine reads from env vars
// (BT_MINIO_*). Endpoint may come with or without a scheme; we strip it so
// that the SDK sees a plain host:port. Bucket is required.
type Config struct {
	Endpoint  string // host:port or http(s)://host:port
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	Region    string // optional, leave empty to use SDK default
}

// Validate surfaces config mistakes at startup rather than on the first read.
func (c Config) Validate() error {
	if strings.TrimSpace(c.Endpoint) == "" {
		return errors.New("storage: MinIO endpoint is required")
	}
	if strings.TrimSpace(c.Bucket) == "" {
		return errors.New("storage: MinIO bucket is required")
	}
	return nil
}

// MinIOReader implements ObjectReader on top of the minio-go/v7 SDK.
type MinIOReader struct {
	client *minio.Client
	bucket string
}

var _ ObjectReader = (*MinIOReader)(nil)

// ConfigFromEnv reads the engine's standard MinIO env convention and returns
// a Config. Empty required fields (endpoint, bucket) will be caught by
// Validate() — this function itself returns a Config without validating so
// tests can overlay values without going through the environment.
//
// Env contract:
//
//	BT_MINIO_ENDPOINT        (required, e.g. "localhost:9100" or "https://...")
//	BT_MINIO_ACCESS_KEY      (required for non-anon MinIO)
//	BT_MINIO_SECRET_KEY      (required for non-anon MinIO)
//	BT_MINIO_BUCKET          (required, e.g. "algorhythm-datasets")
//	BT_MINIO_USE_SSL         ("true" enables TLS; anything else = plain)
//	BT_MINIO_REGION          (optional)
func ConfigFromEnv() Config {
	return Config{
		Endpoint:  os.Getenv("BT_MINIO_ENDPOINT"),
		AccessKey: os.Getenv("BT_MINIO_ACCESS_KEY"),
		SecretKey: os.Getenv("BT_MINIO_SECRET_KEY"),
		Bucket:    os.Getenv("BT_MINIO_BUCKET"),
		UseSSL:    strings.EqualFold(os.Getenv("BT_MINIO_USE_SSL"), "true"),
		Region:    os.Getenv("BT_MINIO_REGION"),
	}
}

// NewMinIOReader builds a read-only MinIO-backed ObjectReader. It does NOT
// ensure the bucket exists: the engine has no authority to create buckets,
// and if the bucket is missing that's a deployment bug we want to surface
// loud.
func NewMinIOReader(cfg Config) (*MinIOReader, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	endpoint := stripScheme(cfg.Endpoint)
	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	}
	if cfg.Region != "" {
		opts.Region = cfg.Region
	}
	client, err := minio.New(endpoint, opts)
	if err != nil {
		return nil, fmt.Errorf("storage: minio.New: %w", err)
	}
	return &MinIOReader{client: client, bucket: cfg.Bucket}, nil
}

func (r *MinIOReader) Stat(ctx context.Context, key string) (ObjectStat, error) {
	info, err := r.client.StatObject(ctx, r.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if isNoSuchKey(err) {
			return ObjectStat{}, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
		}
		return ObjectStat{}, fmt.Errorf("storage: stat %s: %w", key, err)
	}
	return ObjectStat{Key: info.Key, Size: info.Size}, nil
}

func (r *MinIOReader) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := r.client.GetObject(ctx, r.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: get %s: %w", key, err)
	}
	// GetObject is lazy — the SDK only errors on first read. Probe via Stat
	// so the caller sees ErrObjectNotFound immediately and doesn't have to
	// peel an SDK-specific error off a mid-stream read.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		if isNoSuchKey(err) {
			return nil, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
		}
		return nil, fmt.Errorf("storage: get %s: %w", key, err)
	}
	return obj, nil
}

func (r *MinIOReader) List(ctx context.Context, prefix string) ([]string, error) {
	ch := r.client.ListObjects(ctx, r.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	var keys []string
	for obj := range ch {
		if obj.Err != nil {
			return nil, fmt.Errorf("storage: list %s: %w", prefix, obj.Err)
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func stripScheme(ep string) string {
	ep = strings.TrimPrefix(ep, "https://")
	ep = strings.TrimPrefix(ep, "http://")
	return strings.TrimSuffix(ep, "/")
}

func isNoSuchKey(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

// ---------------------------------------------------------------------------
// In-memory fake for tests
// ---------------------------------------------------------------------------

// MemoryReader is a deterministic in-memory ObjectReader for tests. Safe for
// concurrent use: downstream engine code reads the same month parquet from
// many goroutines during integration checks.
type MemoryReader struct {
	mu   sync.RWMutex
	objs map[string][]byte
}

var _ ObjectReader = (*MemoryReader)(nil)

// NewMemoryReader builds an empty fake reader.
func NewMemoryReader() *MemoryReader { return &MemoryReader{objs: map[string][]byte{}} }

// Put seeds the fake with a key/payload. Only meant for test setup.
func (m *MemoryReader) Put(key string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = append([]byte(nil), body...)
}

func (m *MemoryReader) Stat(_ context.Context, key string) (ObjectStat, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.objs[key]
	if !ok {
		return ObjectStat{}, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	return ObjectStat{Key: key, Size: int64(len(b))}, nil
}

func (m *MemoryReader) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *MemoryReader) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.objs))
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	// Sort so tests are deterministic — matches the implicit sort order the
	// MinIO SDK uses for the same lexical prefix.
	sortStrings(keys)
	return keys, nil
}

// sortStrings is insertion-sort for tiny inputs; avoids pulling "sort" just
// to order at most a few dozen month partitions.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
