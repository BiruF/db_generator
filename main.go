package main

import (
	"log"
	"os"

	"db_generator/cmd"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			cmd.Cmd(),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Error running the application: %v", err)
	}
}
