package jobs

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	AddedRowCount atomic.Int64
)

func TimeBucketsJobs(endTime time.Time, generateCount, batchSize int64, deltaRecord time.Duration) chan pgx.CopyFromSource {
	ch := make(chan pgx.CopyFromSource)
	go func() {
		defer close(ch)

		location, err := time.LoadLocation("UTC")
		if err != nil {
			log.Fatalf("Error loading time zone: %v", err)
		}

		jsonDir := "./pkg/jobs"
		validFileNamePattern := regexp.MustCompile(`^\d+\.json$`)

		var wg sync.WaitGroup

		for i := int64(0); i < generateCount; i++ {
			wg.Add(1)
			go func(endTime time.Time, uniqueKeySuffix int) {
				defer wg.Done()

				interval := time.Duration(batchSize) * deltaRecord
				startTime := endTime.Add(-interval).In(location)

				log.Printf("[Jobs table] Generating bucket from %v to %v with %d rows\n", startTime.Format("2006-01-02 15:04:05.000"), endTime.Format("2006-01-02 15:04:05.000"), batchSize)

				files, err := os.ReadDir(jsonDir)
				if err != nil {
					log.Fatalf("Error reading directory: %v", err)
				}

				for _, file := range files {
					if file.IsDir() || !validFileNamePattern.MatchString(file.Name()) {
						continue
					}

					data, err := os.ReadFile(filepath.Join(jsonDir, file.Name()))
					if err != nil {
						log.Printf("Error reading file %s: %v", file.Name(), err)
						continue
					}

					wfOutput := string(data)
					log.Printf("Processing file %s", file.Name())
					source := copyFromRowsJobs(endTime, deltaRecord, &wfOutput)
					ch <- source
				}
			}(endTime, int(i))

			endTime = endTime.Add(-time.Duration(batchSize) * deltaRecord)
		}

		wg.Wait()
	}()
	return ch
}

func ProcessCopyJobs(sourceCh chan pgx.CopyFromSource, pool *pgxpool.Pool) {
	var wg sync.WaitGroup

	for source := range sourceCh {
		wg.Add(1)
		go func(source pgx.CopyFromSource) {
			defer wg.Done()
			conn, err := pool.Acquire(context.Background())
			if err != nil {
				log.Panicf("Error acquiring connection from pool: %v", err)
			}
			defer conn.Release()

			start := time.Now()
			copyCount, err := conn.CopyFrom(
				context.Background(),
				pgx.Identifier{"workflows_jobs"},
				[]string{"ts", "key", "workflow_key", "output", "status"},
				source,
			)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
					log.Printf("Duplicate key error: %v", err)
					return
				}
				log.Panicf("Error copying data to database: %v", err)
			}
			end := time.Now()
			duration := end.Sub(start)

			log.Printf("[Jobs table] Copied %d rows in %v (%f rows/sec)\n", copyCount, duration, float64(copyCount)/duration.Seconds())
		}(source)
	}

	wg.Wait()
}

type copyFromRowsJob struct {
	ts        time.Time
	wfOutput  *string
	status    int16
	delta     time.Duration
	keySuffix int
}

func copyFromRowsJobs(endTime time.Time, delta time.Duration, wfOutput *string) pgx.CopyFromSource {
	return &copyFromRowsJob{
		ts:       endTime,
		wfOutput: wfOutput,
		status:   10,
		delta:    delta,
	}
}

func (ctr *copyFromRowsJob) Next() bool {
	return ctr.ts.After(time.Now().Add(-ctr.delta))
}

func (ctr *copyFromRowsJob) Values() ([]interface{}, error) {
	AddedRowCount.Add(1)
	key := ctr.ts.UnixNano() + int64(ctr.keySuffix)
	ctr.keySuffix++
	values := []interface{}{ctr.ts, key, 2251799813831553, ctr.wfOutput, ctr.status}
	return values, nil
}

func (ctr *copyFromRowsJob) Err() error {
	return nil
}
