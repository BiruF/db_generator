package instance

import (
	"context"
	_ "embed"
	"log"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	uuid "github.com/satori/go.uuid"
)

var (
	AddedRowCount atomic.Int64
)

func TimeBucketsInst(endTime time.Time, generateCount, batchSize int64, deltaRecord time.Duration, wfID int64) chan pgx.CopyFromSource {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatalf("Ошибка загрузки часового пояса: %v", err)
	}

	ch := make(chan pgx.CopyFromSource)

	go func() {
		defer close(ch)
		for AddedRowCount.Load() < generateCount {
			interval := time.Duration(batchSize) * deltaRecord
			startTime := endTime.Add(-interval).In(location)

			log.Printf("[Instance table] Generating bucket from %v to %v with %d rows\n", startTime.Format("2006-01-02 15:04:05.000"), endTime.Format("2006-01-02 15:04:05.000"), batchSize)
			ch <- сopyFromRows(startTime, endTime, deltaRecord, wfID)
			endTime = startTime
		}

		log.Println("Done generating buckets")
	}()

	return ch
}

func ProcessCopyInstance(sourceCh chan pgx.CopyFromSource, pool *pgxpool.Pool) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Panicf("Error acquiring connection from pool: %v", err)
	}
	defer conn.Release()

	for source := range sourceCh {
		start := time.Now()
		rand.Seed(time.Now().UnixNano())

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

		log.Printf("[Instances table] Copied %d rows in %v (%f rows/sec)\n", copyCount, duration, float64(copyCount)/duration.Seconds())
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
	errorCode := getErrorCode()
	//errorCodeStr := strconv.Itoa(errorCode)
	idInstance := uuid.NewV4()
	id := rand.Intn(900000) + 100001 // Генерация случайного числа от 100001 до 999999
	idStr := strconv.Itoa(id)
	return []interface{}{ctr.startTs, ctr.startTs, endTs, ctr.startTs.UnixNano(), ctr.wfID, idInstance, idStr, int32(0), "", int32(1), int32(1), true, "80", "01_generated", "401015699499878", errorCode}, nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}
