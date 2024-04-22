package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"

	"db_generator/instance"
	"db_generator/pkg/db"
)

func main() {
	fs := flag.NewFlagSet("instances", flag.ExitOnError)
	instancesTotal := fs.Int64("instance_total", 1_000_000, "Total workflow instances to generate")
	wfID := fs.Int64("wfID", 2251799818569927, "ID of the workflow to generate")
	batchSize := fs.Int64("batch_size", 100_000, "Batch size for DB insert")
	deltaRecord := fs.Duration("delta_record", 5*time.Millisecond, "Delta between records in batch")
	fs.Parse(os.Args[1:])

	dbConn, err := db.SetupDBConnection()
	if err != nil {
		log.Fatalf("Error setting up database connection: %v", err)
	}
	defer dbConn.Close()

	d := db.New(dbConn)

	if err := d.CreateTables(); err != nil {
		log.Fatalf("Error creating tables: %v", err)
	}

	if err := d.CreateUniqueConstraint(); err != nil {
		log.Fatalf("Error creating unique constraint: %v", err)
	}

	if err := d.CreateWorkflowInputOutputTable(); err != nil {
		log.Fatalf("Error creating workflows_input_output table: %v", err)
	}

	if err := d.ClearDatabase(); err != nil {
		log.Fatalf("Error clearing database: %v", err)
	}

	bar := progressbar.Default(int64(*instancesTotal))
	if err := instance.Generator(d, *instancesTotal, *wfID, bar, *batchSize, *deltaRecord, 10); err != nil {
		log.Fatalf("Error generating data: %v", err)
	}

	fmt.Println("Data generation completed successfully!")
}
