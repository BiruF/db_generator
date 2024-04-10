package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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

	// Создаем таблицы, если их нет
	if err := workflow.CreateTables(db); err != nil {
		log.Fatal(err)
	}

	errCh := make(chan error)

	var wg sync.WaitGroup

	start := time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 1; j <= 100000; j++ {
			key := int64(j)

			instance := workflow.Instance{
				Timestamp:         time.Now(),
				StartTimestamp:    time.Now(),
				EndTimestamp:      sql.NullTime{},
				Key:               key,
				WorkflowKey:       key,
				CallbackPerformed: false,
			}

			err := workflow.InsertInstance(db, instance)
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 1; j <= 100000; j++ {
			key := int64(j)

			io := workflow.InputOutput{
				Timestamp: time.Now(),
				Key:       key,
				Input:     "some_input",
				Output:    "some_output",
			}

			err := workflow.InsertInputOutput(db, io)
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 1; j <= 100000; j++ {
			key := int64(j)

			job := workflow.Job{
				Timestamp:   time.Now(),
				Key:         key,
				WorkflowKey: key,
				Output:      "some_output",
				Status:      1,
				StartTS:     sql.NullTime{Time: time.Now(), Valid: true},
				EndTS:       sql.NullTime{},
			}

			err := workflow.InsertJob(db, job)
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		log.Fatal(err)
	}

	elapsed := time.Since(start)

	log.Printf("Время выполнения цикла вставки записей: %s\n", elapsed)
	log.Println("Все записи успешно вставлены.")
}
