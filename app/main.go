package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"

	"db_generator/workflow"

	_ "github.com/lib/pq"
)

func main() {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s TimeZone=%s",
		os.Getenv("DATABASE_USER"), os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"),
		os.Getenv("DATABASE_HOST"), os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_SSL"),
		os.Getenv("DATABASE_TIMEZONE"))

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := workflow.CreateTables(db); err != nil {
		log.Fatal(err)
	}

	if err := workflow.ClearDatabase(db); err != nil {
		log.Fatal(err)
	}

	errCh := make(chan error)
	doneCh := make(chan struct{})

	var countRecords int = 100000

	var wg sync.WaitGroup

	var bars []*progressbar.ProgressBar
	for i := 0; i < 3; i++ {
		bar := progressbar.NewOptions(countRecords,
			progressbar.OptionSetDescription(fmt.Sprintf("Inserting Records")),
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionThrottle(65*time.Millisecond),
		)
		bars = append(bars, bar)
	}

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				instanceCount, err := workflow.GetRecordCount(db, "workflow_instances")
				if err != nil {
					log.Println(err)
				}
				ioCount, err := workflow.GetRecordCount(db, "workflows_input_output")
				if err != nil {
					log.Println(err)
				}
				jobCount, err := workflow.GetRecordCount(db, "workflows_jobs")
				if err != nil {
					log.Println(err)
				}
				log.Printf("Записи в БД: instances: %d, input/output: %d, jobs: %d\n", instanceCount, ioCount, jobCount)
			case <-doneCh:
				return
			}
		}
	}()

	start := time.Now()

	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(index int) {
			defer wg.Done()
			bar := bars[index]
			for j := 1; j <= int(countRecords); j++ {
				key := int64(j)

				switch index {
				case 0:
					instance := workflow.Instance{
						Timestamp:         time.Now(),
						StartTimestamp:    time.Now(),
						EndTimestamp:      sql.NullTime{},
						Key:               key,
						WorkflowKey:       key,
						CallbackPerformed: false,
					}
					if err := workflow.InsertInstance(db, instance); err != nil {
						errCh <- err
						return
					}
				case 1:
					io := workflow.InputOutput{
						Timestamp: time.Now(),
						Key:       key,
						Input:     "some_input",
						Output:    "some_output",
					}
					if err := workflow.InsertInputOutput(db, io); err != nil {
						errCh <- err
						return
					}
				case 2:
					job := workflow.Job{
						Timestamp:   time.Now(),
						Key:         key,
						WorkflowKey: key,
						Output:      "some_output",
						Status:      1,
						StartTS:     sql.NullTime{Time: time.Now(), Valid: true},
						EndTS:       sql.NullTime{},
					}
					if err := workflow.InsertJob(db, job); err != nil {
						errCh <- err
						return
					}
				}
				bar.Add(1)
			}
		}(i)
	}

	wg.Wait()

	close(errCh)

	for err := range errCh {
		log.Println("Ошибка:", err)
	}

	log.Println("Все записи успешно вставлены.")

	elapsed := time.Since(start)
	log.Printf("Время выполнения вставки записей: %s\n", elapsed)

	dbSize, err := workflow.GetDatabaseSize(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Размер базы данных: %s\n", dbSize)

	instanceTableSize, err := workflow.GetTableSize(db, "workflow_instances")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Размер таблицы 'workflow_instances': %s\n", instanceTableSize)

	ioTableSize, err := workflow.GetTableSize(db, "workflows_input_output")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Размер таблицы 'workflows_input_output': %s\n", ioTableSize)

	jobTableSize, err := workflow.GetTableSize(db, "workflows_jobs")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Размер таблицы 'workflows_jobs': %s\n", jobTableSize)

	log.Println("Все записи успешно вставлены.")
}
