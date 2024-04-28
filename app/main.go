package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"db_generator/input_output"
	"db_generator/instance"
	"db_generator/pkg/db"

	"github.com/urfave/cli/v2"
)

func main() {
	fs := flag.NewFlagSet("instances", flag.ExitOnError)

	connStr := os.Getenv("DATABASE_URL")
	fs.Parse(os.Args[1:])

	dbConn, err := db.SetupDBConnection()
	if err != nil {
		log.Fatalf("Error setting up database connection: %v", err)
	}
	defer dbConn.Close()

	d := db.New(dbConn, connStr)

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

	app := &cli.App{
		Commands: []*cli.Command{
			instance.Cmd(d),
			input_output.Cmd(),
		},
	}

	err = app.Run(os.Args)
	if err != nil {
		log.Fatalf("Error running the application: %v", err)
	}

	fmt.Println("Data generation completed successfully!")
}
