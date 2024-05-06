package instance

import (
	"context"
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"net/http"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/urfave/cli/v2"

	"db_generator/pkg/db"
)

var (
	AddedRowCount atomic.Int64
)

func currentRowCount() int64 {
	log.Println("Asking current row count...")
	conn, err := db.GetConnection()
	if err != nil {
		log.Panicf("Error getting connection: %v", err)
	}
	defer conn.Close()

	// Получаем последнее время
	latestTimestamp := latestTimestamp()

	var current int64
	if err := conn.QueryRow("select count(*) from workflow_instances where ts < $1", latestTimestamp).Scan(&current); err != nil {
		log.Panicf("Error getting current row count: %v", err)
	}

	log.Println("Current row count is", current)
	return current
}

func latestTimestamp() time.Time {
	log.Println("Latest row in database...")

	conn, err := db.GetConnection()
	if err != nil {
		log.Panicf("Error getting connection: %v", err)
	}
	defer conn.Close()
	var latest time.Time
	if err := conn.QueryRow("select ts from workflow_instances order by ts limit 1").Scan(&latest); err != nil {
		log.Printf("Error getting current row count: %v", err)
		latest = time.Now()
	}

	log.Println("Latest row is", latest)
	return latest
}

func timeBuckets(endTime time.Time, generateCount, batchSize int64, deltaRecord time.Duration, wfID int64) chan pgx.CopyFromSource {
	ch := make(chan pgx.CopyFromSource)

	go func() {
		defer close(ch)
		for AddedRowCount.Load() < generateCount {
			interval := time.Duration(batchSize) * deltaRecord
			startTime := endTime.Add(-interval)

			log.Printf("Generating bucket from %v to %v with %d rows\n", startTime.Format("2006-01-02 15:04:05.000"), endTime.Format("2006-01-02 15:04:05.000"), batchSize)
			ch <- сopyFromRows(startTime, endTime, deltaRecord, wfID)

			endTime = startTime
		}

		log.Println("Done generating buckets")
	}()

	return ch
}

func processCopy(sourceCh chan pgx.CopyFromSource, pool *pgxpool.Pool) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Panicf("Error acquiring connection from pool: %v", err)
	}
	defer conn.Release()

	for source := range sourceCh {
		start := time.Now()

		copyCount, err := conn.CopyFrom(
			context.Background(),
			pgx.Identifier{"workflow_instances"},
			[]string{"ts", "startts", "endts", "key", "workflowkey", "alternateid1", "alternateid2", "action", "callbackurl", "operationstatus", "completionstatus", "callbackperformed", "category", "msisdn", "imsi", "errorcode"},
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

		log.Printf("Copied %d rows in %v (%f rows/sec)\n", copyCount, duration, float64(copyCount)/duration.Seconds())
	}
}

func generator(instancesTotal int64, workersNum int, wfID int64, batchSize int64, deltaRecord time.Duration) error {
	pool, err := pgxpool.Connect(context.Background(), "postgres://postgres:sQHiQuMQHOSwikBfFMnpD3i4k9Bq1KMn4kIiL7yjX8BGGJujSt2OOqJbm74qjSbY@172.16.161.12:5432/activation?sslmode=disable&timezone=Europe%2FMoscow&search_path=zeebe")
	if err != nil {
		log.Fatalf("Error getting connection pool: %v", err)
	}
	defer pool.Close()

	latest := latestTimestamp()
	generateCount := instancesTotal - currentRowCount()
	sourcesCh := timeBuckets(latest, generateCount, batchSize, deltaRecord, wfID)

	var wg sync.WaitGroup
	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processCopy(sourcesCh, pool)
		}()
	}

	start := time.Now()
	wg.Wait()

	took := time.Since(start)
	log.Printf("Data generation completed successfully in %v", took)

	return nil
}

func Cmd() *cli.Command {
	return &cli.Command{
		Name: "instances",
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "instance_total",
				Value: 1_000_000_000,
				Usage: "Total workflow that we assume after generation",
			},
			&cli.IntFlag{
				Name:  "workers",
				Value: runtime.NumCPU() / 2,
				Usage: "Number of parallel requests to make",
			},
			&cli.Int64Flag{
				Name:  "wfID",
				Value: 2251799813831553,
				Usage: "ID of the workflow to generate",
			},
			&cli.Int64Flag{
				Name:  "batch_size",
				Value: 100_000,
				Usage: "Batch size for DB insert",
			},
			&cli.DurationFlag{
				Name:  "delta_record",
				Value: 5 * time.Millisecond,
				Usage: "Delta between records in batch",
			},
		},
		Action: func(cCtx *cli.Context) error {
			instancesTotal := cCtx.Int64("instance_total")
			workersNum := cCtx.Int("workers")
			wfID := cCtx.Int64("wfID")
			batchSize := cCtx.Int64("batch_size")
			deltaRecord := cCtx.Duration("delta_record")

			return generator(instancesTotal, workersNum, wfID, batchSize, deltaRecord)
		},
	}
}

func сopyFromRows(startTime, endTime time.Time, delta time.Duration, wfID int64) pgx.CopyFromSource {
	return &copyFromRows{
		startTs: startTime,
		endTime: endTime,
		delta:   delta,
		wfID:    wfID,
	}
}

type copyFromRows struct {
	startTs time.Time
	endTime time.Time
	delta   time.Duration
	wfID    int64
}

func (ctr *copyFromRows) Next() bool {
	return ctr.startTs.Before(ctr.endTime)
}

func (ctr *copyFromRows) Values() ([]interface{}, error) {
	ctr.startTs = ctr.startTs.Add(ctr.delta)
	AddedRowCount.Add(1)

	endTs := ctr.startTs.Add(10 * time.Second)
	key := ctr.startTs.UnixNano()
	errorCode := http.StatusNotFound
	errorCodeStr := strconv.Itoa(errorCode)

	return []interface{}{ctr.startTs, ctr.startTs, endTs, key, ctr.wfID, "some_id", "some_id", int32(0), "", int32(1), int32(1), true, "80", "01_generated", "401015699499878", errorCodeStr}, nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}
