package input_output

import (
	"context"
	_ "embed"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/urfave/cli/v2"
)

var (
	AddedRowCount atomic.Int64

	//go:embed wf_input.json
	wf_input []byte
	//go:embed wf_output.json
	wf_output []byte
)

func jsonStream(workersNum int, batchSize int64) chan pgx.CopyFromSource {
	ch := make(chan pgx.CopyFromSource)

	go func() {
		defer close(ch)

		for {
			ch <- сopyFromRows()
		}
	}()

	return ch
}

func generator(workersNum int, batchSize int64) error {
	pool, err := pgxpool.Connect(context.Background(), "postgres://postgres:sQHiQuMQHOSwikBfFMnpD3i4k9Bq1KMn4kIiL7yjX8BGGJujSt2OOqJbm74qjSbY@172.16.161.12:5432/activation?sslmode=disable&timezone=Europe%2FMoscow&search_path=zeebe")
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer pool.Close()

	sourcesCh := jsonStream(workersNum, batchSize)

	var wg sync.WaitGroup
	for i := 0; i < workersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processCopy(sourcesCh, pool)
		}()
	}

	start := time.Now()
	go Report(&AddedRowCount)
	wg.Wait()

	took := time.Since(start)
	rowsRead := AddedRowCount.Load()
	rowRate := float64(rowsRead) / took.Seconds()
	log.Printf("Generated %d rows, took %v with %d worker(s) (mean rate %f/sec)", rowsRead, took, workersNum, rowRate)

	return nil
}

func processCopy(sourceCh chan pgx.CopyFromSource, pool *pgxpool.Pool) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatalf("Error acquiring connection from pool: %v", err)
	}
	defer conn.Release()

	for source := range sourceCh {
		log.Println("Copying data to database...")
		copyCount, queryErr := conn.Conn().CopyFrom(context.Background(),
			pgx.Identifier{"workflows_input_output"},
			[]string{"ts", "key", "input", "output"},
			source,
		)
		if queryErr != nil {
			log.Panicf("Error copying data to database: %v", queryErr)
		}

		log.Printf("Copied %d rows\n", copyCount)
	}
}

func Cmd() *cli.Command {
	return &cli.Command{
		Name: "input_output",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:  "workers",
				Value: runtime.NumCPU() / 2,
				Usage: "Number of parallel requests to make",
			},
			&cli.Int64Flag{
				Name:  "batch_size",
				Value: 100_000,
				Usage: "Batch size for DB insert",
			},
		},
		Action: func(cCtx *cli.Context) error {
			workersNum := cCtx.Int("workers")
			batchSize := cCtx.Int64("batch_size")

			err := generator(workersNum, batchSize)
			if err != nil {
				log.Printf("Error generating input-output data: %v", err)
				return err
			}

			return nil
		},
	}
}

func сopyFromRows() pgx.CopyFromSource {
	return &copyFromRows{}
}

type copyFromRows struct{}

func (ctr *copyFromRows) Next() bool {
	return true
}

func (ctr *copyFromRows) Values() ([]interface{}, error) {
	AddedRowCount.Add(1)

	return []interface{}{time.Now(), int64(1), string(wf_input), string(wf_output)}, nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}

func Report(count *atomic.Int64) {
	for {
		time.Sleep(10 * time.Second)
		log.Printf("Added %d rows", count.Load())
	}
}
