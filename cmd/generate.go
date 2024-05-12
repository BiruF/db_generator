package cmd

import (
	"runtime"
	"time"

	"db_generator/pkg/generator"

	"github.com/urfave/cli/v2"
)

func Cmd() *cli.Command {
	return &cli.Command{
		Name: "Generator",
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "instance_total",
				Value: 1_000_00,
				Usage: "Total workflow that we assume after generation",
			},
			&cli.IntFlag{
				Name:  "workers",
				Value: runtime.NumCPU() / 2,
				Usage: "Number of parallel requests to make",
			},
			&cli.Int64Flag{
				Name:  "wfID",
				Value: 2251799813831553,
				Usage: "ID of the workflow to generate",
			},
			&cli.Int64Flag{
				Name:  "batch_size",
				Value: 1000,
				Usage: "Batch size for DB insert",
			},
			&cli.DurationFlag{
				Name:  "delta_record",
				Value: 5 * time.Millisecond,
				Usage: "Delta between records in batch",
			},
		},
		Action: func(cCtx *cli.Context) error {
			instancesTotal := cCtx.Int64("instance_total")
			workersNum := cCtx.Int("workers")
			wfID := cCtx.Int64("wfID")
			batchSize := cCtx.Int64("batch_size")
			deltaRecord := cCtx.Duration("delta_record")

			return generator.Generate(instancesTotal, workersNum, wfID, batchSize, deltaRecord)
		},
	}
}
