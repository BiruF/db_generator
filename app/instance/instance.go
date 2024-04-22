package instance

import (
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"

	db "db_generator/pkg/db"
)

func CurrentRowCount(d *db.DB) int64 {
	count, err := d.GetRecordCount("workflow_instances")
	if err != nil {
		log.Panicf("Error retrieving current row count: %v", err)
	}
	log.Println("Current row count:", count)
	return int64(count)
}

func LatestTimestamp(d *db.DB) (time.Time, error) {
	var latest time.Time
	err := d.QueryRow("SELECT MAX(ts) FROM workflow_instances").Scan(&latest)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Println("No rows in the table")
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	log.Println("Latest row is", latest)
	return latest, nil
}

func timeBuckets(endTime time.Time, generateCount, batchSize int64, deltaRecord time.Duration, wfID int64) chan db.Instance {
	ch := make(chan db.Instance)

	go func() {
		defer close(ch)
		for i := int64(0); i < generateCount; i++ {
			startTime := endTime.Add(-deltaRecord * time.Duration(i))

			ch <- CreateInstance(startTime, endTime, wfID)

			if (i+1)%batchSize == 0 {
				endTime = startTime
			}
		}

		log.Println("Done generating buckets")
	}()

	return ch
}

func createJob(instance db.Instance) db.Job {
	job := db.Job{
		Timestamp:   instance.Timestamp,
		Key:         instance.Key,
		WorkflowKey: instance.WorkflowKey,
		Output:      "some_output",
		Status:      1,
		StartTS:     sql.NullTime{Time: instance.StartTimestamp},
		EndTS:       instance.EndTimestamp,
	}

	return job
}

func CreateInstance(startTime, endTime time.Time, wfID int64) db.Instance {
	key := startTime.Unix()

	endTimestamp := sql.NullTime{}
	if !endTime.IsZero() {
		endTimestamp.Time = endTime
		endTimestamp.Valid = true
	}

	return db.Instance{
		Timestamp:         startTime,
		StartTimestamp:    startTime,
		EndTimestamp:      endTimestamp,
		Key:               key,
		WorkflowKey:       key,
		AlternateID1:      sql.NullString{String: "some_id", Valid: true},
		AlternateID2:      sql.NullString{String: "some_id", Valid: true},
		Action:            sql.NullInt32{Int32: 0, Valid: true},
		CallbackURL:       sql.NullString{String: "some_url", Valid: true},
		OperationStatus:   sql.NullInt32{Int32: 1, Valid: true},
		CompletionStatus:  sql.NullInt32{Int32: 1, Valid: true},
		CallbackPerformed: true,
		Category:          sql.NullString{String: "some_category", Valid: true},
		MSISDN:            sql.NullString{String: "some_msisdn", Valid: true},
		IMSI:              sql.NullString{String: "some_imsi", Valid: true},
		ErrorCode:         sql.NullString{String: "some_error", Valid: true},
	}
}

func Generator(d *db.DB, instancesTotal, wfID int64, bar *progressbar.ProgressBar, batchSize int64, deltaRecord time.Duration, jobsPerInstance int) error {
	currentRowCount := CurrentRowCount(d)

	if currentRowCount >= instancesTotal {
		log.Printf("Nothing to generate, current row count is %d\n", currentRowCount)
		return nil
	}
	log.Printf("Generating %d rows\n", instancesTotal-currentRowCount)

	startTime := time.Now()

	updateProgressBar := func(processedCount int64) string {
		elapsedTime := time.Since(startTime).Seconds()
		rate := float64(processedCount) / elapsedTime
		bar.Set(int(processedCount))
		bar.Describe(fmt.Sprintf("%.2f records/sec", rate))
		bar.RenderBlank()

		return fmt.Sprintf("Processed %d records", processedCount)
	}
	var instanceCount, jobCount, inputOutputCount int64

	instancesCh := make(chan db.Instance)

	go func() {
		defer close(instancesCh)
		for i := int64(0); i < instancesTotal-currentRowCount; i++ {
			startTime := time.Now().Add(-deltaRecord * time.Duration(i))
			instance := CreateInstance(startTime, startTime, wfID)
			instancesCh <- instance
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for instance := range instancesCh {
				if err := d.InsertInstance(instance); err != nil {
					log.Printf("Error inserting instance: %v", err)
					continue
				}
				instanceCount++
				inputOutputCount++
				if instanceCount%1000 == 0 {
					log.Println(updateProgressBar(instanceCount))
				}

				for j := 0; j < jobsPerInstance; j++ {
					job := createJob(instance)
					if err := d.InsertJob(job); err != nil {
						log.Printf("Error inserting job: %v", err)
						continue
					}
					jobCount++
				}
			}
		}()
	}

	progressTimer := time.NewTicker(5 * time.Second)
	defer progressTimer.Stop()

	go func() {
		for range progressTimer.C {
			log.Println(updateProgressBar(instanceCount))
		}
	}()

	wg.Wait()

	fmt.Println()

	elapsedTime := time.Since(startTime)
	log.Printf("Time taken to populate the database: %v", elapsedTime)

	log.Printf("Inserted %d records into workflow_instances table\n", instanceCount)
	log.Printf("Inserted %d records into jobs table\n", jobCount)
	log.Printf("Inserted %d records into input_output table\n", inputOutputCount)

	instanceSize, err := d.GetTableSize("workflow_instances")
	if err != nil {
		log.Printf("Error retrieving size of instance table: %v\n", err)
	} else {
		log.Printf("Size of instance table: %s\n", instanceSize)
	}

	jobSize, err := d.GetTableSize("workflows_jobs")
	if err != nil {
		log.Printf("Error retrieving size of job table: %v\n", err)
	} else {
		log.Printf("Size of job table: %s\n", jobSize)
	}

	inputOutputSize, err := d.GetTableSize("workflows_input_output")
	if err != nil {
		log.Printf("Error retrieving size of input_output table: %v\n", err)
	} else {
		log.Printf("Size of input_output table: %s\n", inputOutputSize)
	}

	return nil
}
