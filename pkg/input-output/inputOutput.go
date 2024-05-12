package input_output

import (
	"context"
	_ "embed"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	AddedRowCount atomic.Int64
)

func TimeBucketsIO(endTime time.Time, generateCount, batchSize int64, deltaRecord time.Duration, wfImput, wfOutput *string) chan pgx.CopyFromSource {
	ch := make(chan pgx.CopyFromSource)
	go func() {
		defer close(ch)
		for AddedRowCount.Load() < generateCount {
			interval := time.Duration(batchSize) * deltaRecord
			startTime := endTime.Add(-interval)

			log.Printf("[IO table] Generating bucket from %v to %v with %d rows\n", startTime.Format("2006-01-02 15:04:05.000"), endTime.Format("2006-01-02 15:04:05.000"), batchSize)
			ch <- сopyFromRowsIO(startTime, endTime, deltaRecord, wfImput, wfOutput)
			endTime = startTime
		}
		log.Println("Done generating buckets")
	}()
	return ch
}

func ProcessCopyIO(sourceCh chan pgx.CopyFromSource, pool *pgxpool.Pool) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Panicf("Error acquiring connection from pool: %v", err)
	}
	defer conn.Release()
	for source := range sourceCh {
		start := time.Now()

		copyCount, err := conn.CopyFrom(
			context.Background(),
			pgx.Identifier{"workflows_input_output"},
			[]string{"ts", "key", "input", "output"},
			source,
		)
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
				log.Printf("Skipping insertion of duplicate key: %v", pgErr.Message)
				continue
			}
			log.Panicf("Error copying data to database: %v", err)
		}
		end := time.Now()
		duration := end.Sub(start)

		log.Printf("[IO table] Copied %d rows in %v (%f rows/sec)\n", copyCount, duration, float64(copyCount)/duration.Seconds())
	}
}

func сopyFromRowsIO(startTime, endTime time.Time, delta time.Duration, wfInput, wfOutput *string) pgx.CopyFromSource {
	return &copyFromRowsIO{
		startTs:  startTime,
		endTime:  endTime,
		delta:    delta,
		wfInput:  wfInput,
		wfOutput: wfOutput,
	}
}

type copyFromRowsIO struct {
	startTs  time.Time
	endTime  time.Time
	delta    time.Duration
	wfInput  *string
	wfOutput *string
}

func (ctr *copyFromRowsIO) Next() bool {
	return ctr.startTs.Before(ctr.endTime)
}

func (ctr *copyFromRowsIO) Values() ([]interface{}, error) {
	ctr.startTs = ctr.startTs.Add(ctr.delta)
	AddedRowCount.Add(1)
	return []interface{}{ctr.startTs, ctr.startTs.UnixNano(), *ctr.wfInput, *ctr.wfOutput}, nil
}

func (ctr *copyFromRowsIO) Err() error {
	return nil
}
