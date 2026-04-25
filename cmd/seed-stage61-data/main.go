// Seed Stage 6.1 E2E: uploads v1 feature parquet to MinIO and registers the
// matching dataset + month partition in control-plane so runresolve.Resolve
// finds feature_<code>_<interval> for BTCUSDT.
//
// Usage:
//
//	BT_FEATURE_READ_FRAME=true and BT_MINIO_* must match these values when running backtest-engine.
//
//	go run ./cmd/seed-stage61-data \
//	  -cp http://localhost:8080 \
//	  -minio-endpoint localhost:9000 \
//	  -minio-access minioadmin \
//	  -minio-secret minioadmin \
//	  -bucket algorhythm-datasets
//
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/cpclient"
	"github.com/algorhythm-llc/algorhythm-backtest-engine/internal/featuredata"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	cpURL := flag.String("cp", "http://localhost:8080", "control-plane base URL")
	minioEp := flag.String("minio-endpoint", "localhost:9000", "MinIO API endpoint host:port")
	minioAK := flag.String("minio-access", "minioadmin", "MinIO access key")
	minioSK := flag.String("minio-secret", "minioadmin", "MinIO secret key")
	bucket := flag.String("bucket", "algorhythm-datasets", "bucket name")
	useSSL := flag.Bool("minio-use-ssl", false, "use HTTPS for MinIO")
	region := flag.String("minio-region", "", "optional region")
	exchange := flag.String("exchange", "binance", "exchange segment in object key")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	fsCode := flag.String("feature-set-code", "btcusdt_futures_mvp", "feature_set code (matches registry)")
	interval := flag.String("interval", "1m", "cadence suffix in dataset_type and path")
	year := flag.Int("year", 2023, "partition year")
	month := flag.Int("month", 11, "partition month")
	rows := flag.Int("rows", 2500, "minute bars (covers indicators + exits)")
	skipMinio := flag.Bool("skip-minio", false, "only register dataset/partition rows in CP (upload skipped)")
	skipCP := flag.Bool("skip-cp", false, "only upload parquet (no CP API calls)")
	flag.Parse()

	ctx := context.Background()

	start := time.Date(*year, time.Month(*month), 1, 0, 0, 0, 0, time.UTC)
	startMs := start.UnixMilli()

	blob, err := featuredata.V1SeedMinuteBars(*symbol, startMs, *rows)
	if err != nil {
		log.Fatal(err)
	}

	prefix := fmt.Sprintf(
		"features/feature_set=%s/exchange=%s/symbol=%s/interval=%s/year=%d/month=%02d",
		*fsCode, *exchange, *symbol, *interval, *year, *month,
	)
	objectKey := prefix + "/data.parquet"
	datasetType := "feature_" + *fsCode + "_" + *interval

	if !*skipMinio {
		cli, err := minio.New(stripScheme(*minioEp), &minio.Options{
			Creds:  credentials.NewStaticV4(*minioAK, *minioSK, ""),
			Secure: *useSSL,
			Region: *region,
		})
		if err != nil {
			log.Fatal(err)
		}
		exists, err := cli.BucketExists(ctx, *bucket)
		if err != nil {
			log.Fatal(err)
		}
		if !exists {
			opts := minio.MakeBucketOptions{}
			if *region != "" {
				opts.Region = *region
			}
			if err := cli.MakeBucket(ctx, *bucket, opts); err != nil {
				log.Fatal(err)
			}
			log.Printf("created bucket %s", *bucket)
		}
		_, err = cli.PutObject(ctx, *bucket, objectKey, bytes.NewReader(blob), int64(len(blob)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("uploaded s3://%s/%s (%d bytes)", *bucket, objectKey, len(blob))
	}

	if *skipCP {
		log.Printf("skip-cp: dataset_type=%s partition_prefix=%s", datasetType, prefix)
		return
	}

	client := cpclient.New(*cpURL)
	list, err := client.ListDatasets(ctx, *symbol, datasetType, 10)
	if err != nil {
		log.Fatal(err)
	}

	var dsID string
	if len(list) > 0 {
		dsID = list[0].ID
		log.Printf("reuse dataset id=%s", dsID)
	} else {
		tEnd := start.AddDate(0, 1, 0).Add(-time.Second)
		rootPrefix := fmt.Sprintf(
			"features/feature_set=%s/exchange=%s/symbol=%s/interval=%s",
			*fsCode, *exchange, *symbol, *interval,
		)
		body := map[string]any{
			"exchange":      *exchange,
			"symbol":        *symbol,
			"dataset_type":  datasetType,
			"interval":      *interval,
			"period_from":   start.UTC().Format(time.RFC3339),
			"period_to":     tEnd.UTC().Format(time.RFC3339),
			"s3_prefix":     rootPrefix,
			"status":        "ready",
			"source_type":   "seed-stage61-data",
			"row_count":     int64(*rows),
			"min_ts":        start.UTC().Format(time.RFC3339),
			"max_ts":        time.UnixMilli(startMs + int64(*rows-1)*60_000).UTC().Format(time.RFC3339),
		}
		ds, err := postDataset(ctx, *cpURL, body)
		if err != nil {
			log.Fatal(err)
		}
		dsID = ds.ID
		log.Printf("created dataset id=%s", dsID)
	}

	parts, err := client.ListDatasetPartitions(ctx, dsID)
	if err != nil {
		log.Fatal(err)
	}
	found := false
	for _, p := range parts {
		if p.Year == *year && p.Month == *month {
			found = true
			break
		}
	}
	if !found {
		partBody := map[string]any{
			"dataset_id": dsID,
			"year":       *year,
			"month":      *month,
			"s3_path":    prefix,
		}
		if err := postJSON(ctx, *cpURL, http.MethodPost, "/api/v1/dataset-partitions", partBody, nil); err != nil {
			log.Fatal(err)
		}
		log.Printf("created partition year=%d month=%02d path=%s", *year, *month, prefix)
	} else {
		log.Printf("partition year=%d month=%02d already exists", *year, *month)
	}

	log.Printf("done. Point backtest-engine at bucket=%q endpoint=%q and set BT_FEATURE_READ_FRAME=true", *bucket, *minioEp)
}

func stripScheme(ep string) string {
	ep = strings.TrimPrefix(ep, "https://")
	ep = strings.TrimPrefix(ep, "http://")
	return strings.TrimSuffix(ep, "/")
}

func postDataset(ctx context.Context, base string, body map[string]any) (*cpclient.Dataset, error) {
	var out cpclient.Dataset
	if err := postJSON(ctx, base, http.MethodPost, "/api/v1/datasets", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func postJSON(ctx context.Context, base, method, path string, body any, out any) error {
	base = strings.TrimSuffix(strings.TrimSpace(base), "/")
	u := base + path
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s -> %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil && len(b) > 0 {
		if err := json.Unmarshal(b, out); err != nil {
			return fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return nil
}
