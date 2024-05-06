package main

import (
	"log"
	"os"

	"db_generator/input_output"
	"db_generator/instance"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			instance.Cmd(),
			input_output.Cmd(),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Error running the application: %v", err)
	}
}
