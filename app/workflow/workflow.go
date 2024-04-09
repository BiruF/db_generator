package workflow

import (
	"database/sql"
	"log"
	"time"
)

type Record struct {
	ID   int
	Time time.Time
	Key  string
}

func InsertRecord(db *sql.DB, key string) error {
	_, err := db.Exec("INSERT INTO records (key) VALUES ($1)", key)
	return err
}

func GetAllRecords(db *sql.DB) ([]Record, error) {
	rows, err := db.Query("SELECT id, time, key FROM records")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		err := rows.Scan(&r.ID, &r.Time, &r.Key)
		if err != nil {
			log.Fatal(err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
