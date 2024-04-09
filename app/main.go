package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/lib/pq"
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

	rows, err := db.Query("SELECT * FROM records")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var (
		id   int
		time pq.NullTime
		key  string
	)

	for rows.Next() {
		err := rows.Scan(&id, &time, &key)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("ID: %d, Time: %v, Key: %s\n", id, time.Time, key)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("Запрос успешно выполнен.")
}
