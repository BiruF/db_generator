package generator

import (
	"context"
	_ "embed"
	"log"
	"os"

	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	input_output "db_generator/pkg/input-output"
	"db_generator/pkg/instance"

	jobs "db_generator/pkg/jobs"

	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	AddedRowCount atomic.Int64
)

const (
	PathInput  = "./io-data/wf_input.json"
	PathOutput = "./io-data/wf_output.json"
)

func currentRowCount(pool *pgxpool.Pool) int64 {
	log.Println("Asking current row count...")
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Panicf("Error acquiring connection from pool: %v", err)
	}
	defer conn.Release()

	latestTimestamp := latestTimestamp(pool)

	var current int64
	if err := conn.QueryRow(context.Background(), "select count(*) from workflow_instances where ts < $1", latestTimestamp).Scan(&current); err != nil {
		log.Panicf("Error getting current row count: %v", err)
	}

	log.Println("Current row count is", current)
	return current
}

func latestTimestamp(pool *pgxpool.Pool) time.Time {
	log.Println("Latest row in database...")

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Panicf("Error acquiring connection from pool: %v", err)
	}
	defer conn.Release()

	var latest time.Time
	if err := conn.QueryRow(context.Background(), "select ts from workflow_instances order by ts limit 1").Scan(&latest); err != nil {
		log.Printf("Error getting current row count: %v", err)
		latest = time.Now()
	}

	log.Println("Latest row is", latest)
	return latest
}

func Generate(instancesTotal int64, workersNum int, wfID int64, batchSize int64, deltaRecord time.Duration) error {

	absInputPath, _ := filepath.Abs(PathInput)
	absOutputPath, _ := filepath.Abs(PathOutput)
	wfInput, err := getFileAsStr(absInputPath)
	if err != nil {
		log.Fatalf("Error path: %v", err)
	}
	wfOutput, err := getFileAsStr(absOutputPath)
	if err != nil {
		log.Fatalf("Error path: %v", err)
	}

	pool, err := pgxpool.Connect(context.Background(), "postgres://postgres:sQHiQuMQHOSwikBfFMnpD3i4k9Bq1KMn4kIiL7yjX8BGGJujSt2OOqJbm74qjSbY@172.16.161.12:5432/activation?sslmode=disable&timezone=Europe%2FMoscow&search_path=zeebe")
	if err != nil {
		log.Fatalf("Error getting connection pool: %v", err)
	}
	defer pool.Close()

	latest := latestTimestamp(pool)
	generateCount := instancesTotal - currentRowCount(pool)
	sourcesChInst := instance.TimeBucketsInst(latest, generateCount, batchSize, deltaRecord, wfID)
	sourcesChIO := input_output.TimeBucketsIO(latest, generateCount, batchSize, deltaRecord, &wfInput, &wfOutput)
	sourcesChJobs := jobs.TimeBucketsJobs(latest, generateCount, batchSize, deltaRecord)

	var wg sync.WaitGroup
	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance.ProcessCopyInstance(sourcesChInst, pool)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			input_output.ProcessCopyIO(sourcesChIO, pool)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			jobs.ProcessCopyJobs(sourcesChJobs, pool)
		}()

	}

	start := time.Now()
	wg.Wait()

	took := time.Since(start)
	log.Printf("Data generation completed successfully in %v", took)

	return nil
}

func getFileAsStr(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
