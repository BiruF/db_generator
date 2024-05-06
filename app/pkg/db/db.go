package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

type Instance struct {
	Timestamp         time.Time
	StartTimestamp    time.Time
	EndTimestamp      sql.NullTime
	Key               int64
	WorkflowKey       int64
	AlternateID1      sql.NullString
	AlternateID2      sql.NullString
	Action            sql.NullInt32
	CallbackURL       sql.NullString
	OperationStatus   sql.NullInt32
	CompletionStatus  sql.NullInt32
	CallbackPerformed bool
	Category          sql.NullString
	MSISDN            sql.NullString
	IMSI              sql.NullString
	ErrorCode         sql.NullString
}

type InputOutput struct {
	Timestamp time.Time
	Key       int64
	Input     string
	Output    string
}

type Job struct {
	Timestamp   time.Time
	Key         int64
	WorkflowKey int64
	Output      string
	Status      int16
	StartTS     sql.NullTime
	EndTS       sql.NullTime
}

type DB struct {
	*sql.DB
	ConnectionString string
}

const (
	reportInterval = 30 * time.Second
)

func New(db *sql.DB, connStr string) *DB {
	return &DB{DB: db, ConnectionString: connStr}
}

func (d *DB) InsertInstances(instances []Instance) error {
	conn, err := pgx.Connect(context.Background(), d.ConnectionString)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	stmt := `INSERT INTO workflow_instances 
             (ts, startts, endts, key, workflowkey, alternateid1, alternateid2, action, callbackurl, operationstatus, completionstatus, callbackperformed, category, msisdn, imsi, errorcode) 
             VALUES `
	var args []interface{}
	for _, instance := range instances {
		stmt += "($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16),"
		args = append(args, instance.Timestamp, instance.StartTimestamp, instance.EndTimestamp, instance.Key, instance.WorkflowKey,
			instance.AlternateID1, instance.AlternateID2, instance.Action, instance.CallbackURL, instance.OperationStatus,
			instance.CompletionStatus, instance.CallbackPerformed, instance.Category, instance.MSISDN, instance.IMSI,
			instance.ErrorCode)
	}
	stmt = stmt[:len(stmt)-1]

	_, err = conn.Exec(context.Background(), stmt, args...)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) InsertInputOutput(io InputOutput) error {
	_, err := d.Exec(`
        INSERT INTO workflows_input_output 
        (ts, key, input, output) 
        VALUES ($1, $2, $3, $4)`,
		io.Timestamp, io.Key, io.Input, io.Output)
	return err
}

func (d *DB) InsertJob(job Job) error {
	conn, err := pgx.Connect(context.Background(), d.ConnectionString)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), `
        INSERT INTO workflows_jobs 
        (ts, key, workflow_key, output, status, startts, endts) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		job.Timestamp, job.Key, job.WorkflowKey, job.Output, job.Status, job.StartTS, job.EndTS)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) CreateTables() error {
	fmt.Println("db1")

	queries := []string{
		`CREATE TABLE IF NOT EXISTS workflow_instances (
            ts                TIMESTAMPTZ NOT NULL,
            startts           TIMESTAMPTZ NOT NULL,
            endts             TIMESTAMPTZ,
            key               BIGINT NOT NULL,
            workflowkey       BIGINT NOT NULL,
            alternateid1      TEXT,
            alternateid2      TEXT,
            action            SMALLINT,
            callbackurl       TEXT,
            operationstatus   SMALLINT,
            completionstatus  SMALLINT,
            callbackperformed BOOLEAN DEFAULT FALSE,
            category          TEXT,
            msisdn            TEXT,
            imsi              TEXT,
            errorcode         TEXT,
            PRIMARY KEY (ts, key) -- Добавление первичного ключа
        )`,

		`CREATE UNIQUE INDEX IF NOT EXISTS unique_constraint_ts_key 
		ON workflow_instances (ts, key)`,

		`CREATE TABLE IF NOT EXISTS workflows_input_output (
            ts     TIMESTAMPTZ NOT NULL,
            key    BIGINT NOT NULL,
            input  TEXT NOT NULL,
            output TEXT NOT NULL,
            PRIMARY KEY (ts, key),
            FOREIGN KEY (ts, key) REFERENCES workflow_instances (ts, key) ON DELETE CASCADE
        )`,

		`CREATE TABLE IF NOT EXISTS workflows_jobs (
            ts           TIMESTAMPTZ NOT NULL,
            key          BIGINT NOT NULL,
            workflow_key BIGINT NOT NULL,
            output       TEXT NOT NULL,
            status       SMALLINT NOT NULL,
            startts      TIMESTAMPTZ,
            endts        TIMESTAMPTZ,
            FOREIGN KEY (ts, key) REFERENCES workflow_instances (ts, key) ON DELETE CASCADE
        )`,
	}
	fmt.Println("db2")

	for _, query := range queries {
		_, err := d.Exec(query)
		if err != nil {
			return err
		}
	}
	fmt.Println("db3")

	return nil
}

func (d *DB) GetDatabaseSize() (string, error) {
	var dbSize string
	err := d.QueryRow("SELECT pg_size_pretty(pg_database_size(current_database()))").Scan(&dbSize)
	if err != nil {
		return "", err
	}
	return dbSize, nil
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return d.DB.QueryRow(query, args...)
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

func (d *DB) GetLatestTimestamp() (time.Time, error) {
	var latest time.Time
	err := d.QueryRow("SELECT MAX(ts) FROM workflow_instances").Scan(&latest)
	if err != nil {
		return time.Time{}, err
	}
	return latest, nil
}

func (d *DB) GetTableSize(tableName string) (string, error) {
	var tableSize string
	query := fmt.Sprintf("SELECT pg_size_pretty(pg_total_relation_size('%s'))", tableName)
	err := d.QueryRow(query).Scan(&tableSize)
	if err != nil {
		return "", err
	}
	return tableSize, nil
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

func (d *DB) GetRecordCount(tableName string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := d.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (d *DB) CreateUniqueConstraint() error {
	_, err := d.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS unique_constraint_ts_key 
		ON workflow_instances (ts, key)
	`)
	return err
}

func (d *DB) CreateWorkflowInputOutputTable() error {
	_, err := d.Exec(`
        CREATE TABLE IF NOT EXISTS workflows_input_output (
            ts     TIMESTAMPTZ NOT NULL,
            key    BIGINT NOT NULL,
            input  TEXT NOT NULL,
            output TEXT NOT NULL,
            PRIMARY KEY (ts, key),
            FOREIGN KEY (ts, key) REFERENCES workflow_instances (ts, key) ON DELETE CASCADE
        )
    `)
	return err
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
		// Initialize the pool if it's nil
		if err := InitializePool(); err != nil {
			return nil, err
		}
	}
	return pool, nil
}

func Report(counter *atomic.Int64) {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)

	for now := range time.NewTicker(reportInterval).C {
		rCount := counter.Load()

		took := now.Sub(prevTime)
		rowRate := float64(rCount-prevRowCount) / took.Seconds()
		overallRowRate := float64(rCount) / now.Sub(start).Seconds()
		totalTook := now.Sub(start)

		log.Printf("at %v, row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %d total rows\n", totalTook-(totalTook%time.Second), rowRate, overallRowRate, rCount)

		prevRowCount = rCount
		prevTime = now
	}
}
