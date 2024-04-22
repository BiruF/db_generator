package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

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
}

func New(db *sql.DB) *DB {
	return &DB{DB: db}
}

func (d *DB) InsertInstance(instance Instance) error {
	_, err := d.Exec(`
        INSERT INTO workflow_instances 
        (ts, startts, endts, key, workflowkey, alternateid1, alternateid2, action, callbackurl, operationstatus, completionstatus, callbackperformed, category, msisdn, imsi, errorcode) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT (ts, key) DO NOTHING`,
		instance.Timestamp, instance.StartTimestamp, instance.EndTimestamp, instance.Key, instance.WorkflowKey,
		instance.AlternateID1, instance.AlternateID2, instance.Action, instance.CallbackURL, instance.OperationStatus,
		instance.CompletionStatus, instance.CallbackPerformed, instance.Category, instance.MSISDN, instance.IMSI,
		instance.ErrorCode)
	return err
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
	_, err := d.Exec(`
        INSERT INTO workflows_jobs 
        (ts, key, workflow_key, output, status, startts, endts) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		job.Timestamp, job.Key, job.WorkflowKey, job.Output, job.Status, job.StartTS, job.EndTS)
	return err
}

func (d *DB) CreateTables() error {
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

	for _, query := range queries {
		_, err := d.Exec(query)
		if err != nil {
			return err
		}
	}

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

func SetupDBConnection() (*sql.DB, error) {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s TimeZone=%s",
		os.Getenv("DATABASE_USER"), os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"),
		os.Getenv("DATABASE_HOST"), os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_SSL"),
		os.Getenv("DATABASE_TIMEZONE"))

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
