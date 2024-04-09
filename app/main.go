package main

import (
	"database/sql"
	"db_generator/workflow"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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

	numThreads := 3

	errCh := make(chan error)

	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for j := 1 + threadID; j <= 5000000; j += numThreads {
				key := fmt.Sprintf("example_key_%d", j)
				err := workflow.InsertRecord(db, key)
				if err != nil {
					errCh <- err
					return
				}
			}
		}(i)
	}

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
