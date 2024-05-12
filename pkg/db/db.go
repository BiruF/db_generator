package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

type DB struct {
	*sql.DB
	ConnectionString string
}

func GetConnection() (*sql.DB, error) {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s TimeZone=%s",
		"postgres", "sQHiQuMQHOSwikBfFMnpD3i4k9Bq1KMn4kIiL7yjX8BGGJujSt2OOqJbm74qjSbY", "activation", "172.16.161.12", "5432", "disable", "Europe/Moscow")

	dbConn, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return dbConn, nil
}

func (d *DB) ClearDatabase() error {
	queries := []string{
		"DELETE FROM workflow_instances",
		"DELETE FROM workflows_input_output",
		"DELETE FROM workflows_jobs",
	}

	for _, query := range queries {
		_, err := d.Exec(query)
		if err != nil {
			return err
		}
	}

	return nil
}

var pool *pgxpool.Pool

var (
	ContextProgram, _ = signal.NotifyContext(context.Background(), os.Interrupt)
)

func InitializePool() error {
	// Initialize and configure the connection pool
	var err error
	config, err := pgxpool.ParseConfig("connection string")
	if err != nil {
		return err
	}
	pool, err = pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return err
	}
	return nil
}

func GetPool() (*pgxpool.Pool, error) {
	if pool == nil {
		if err := InitializePool(); err != nil {
			return nil, err
		}
	}
	return pool, nil
}
