package workflow

import (
	"database/sql"
	"time"
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

// Функция для вставки записей в таблицу workflow_instances
func InsertInstance(db *sql.DB, instance Instance) error {
	_, err := db.Exec(`
        INSERT INTO workflow_instances 
        (ts, startts, endts, key, workflowkey, alternateid1, alternateid2, action, callbackurl, operationstatus, completionstatus, callbackperformed, category, msisdn, imsi, errorcode) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		instance.Timestamp, instance.StartTimestamp, instance.EndTimestamp, instance.Key, instance.WorkflowKey,
		instance.AlternateID1, instance.AlternateID2, instance.Action, instance.CallbackURL, instance.OperationStatus,
		instance.CompletionStatus, instance.CallbackPerformed, instance.Category, instance.MSISDN, instance.IMSI,
		instance.ErrorCode)
	return err
}

// Функция для вставки записей в таблицу workflows_input_output
func InsertInputOutput(db *sql.DB, io InputOutput) error {
	_, err := db.Exec(`
        INSERT INTO workflows_input_output 
        (ts, key, input, output) 
        VALUES ($1, $2, $3, $4)`,
		io.Timestamp, io.Key, io.Input, io.Output)
	return err
}

// Функция для вставки записей в таблицу workflows_jobs
func InsertJob(db *sql.DB, job Job) error {
	_, err := db.Exec(`
        INSERT INTO workflows_jobs 
        (ts, key, workflow_key, output, status, startts, endts) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		job.Timestamp, job.Key, job.WorkflowKey, job.Output, job.Status, job.StartTS, job.EndTS)
	return err
}

func CreateTables(db *sql.DB) error {
	// SQL запросы для создания таблиц
	queries := []string{
		`CREATE TABLE IF NOT EXISTS workflow_instances (
			ts                TIMESTAMP NOT NULL,
			startts           TIMESTAMP NOT NULL,
			endts             TIMESTAMP,
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
			errorcode         TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS workflows_input_output (
			ts     TIMESTAMP NOT NULL,
			key    BIGINT NOT NULL,
			input  TEXT NOT NULL,
			output TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS workflows_jobs (
			ts           TIMESTAMP NOT NULL,
			key          BIGINT NOT NULL,
			workflow_key BIGINT NOT NULL,
			output       TEXT NOT NULL,
			status       SMALLINT NOT NULL,
			startts      TIMESTAMP,
			endts        TIMESTAMP
		)`,
	}

	// Выполнение каждого SQL запроса
	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
	}

	return nil
}
