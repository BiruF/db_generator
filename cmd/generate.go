package cmd

import (
	"runtime"
	"time"

	"db_generator/pkg/generator"

	"github.com/urfave/cli/v2"
)

func instanceTotalFlag() *cli.Int64Flag {
	return &cli.Int64Flag{
		Name:  "instance_total",
		Value: 100_000,
		Usage: "Total workflow that we assume after generation",
	}
}

func workersFlag() *cli.IntFlag {
	return &cli.IntFlag{
		Name:  "workers",
		Value: runtime.NumCPU() / 2,
		Usage: "Number of parallel requests to make",
	}
}

func wfIDFlag() *cli.Int64Flag {
	return &cli.Int64Flag{
		Name:  "wfID",
		Value: 2251799813831553,
		//2251799813686528
		Usage: "ID of the workflow to generate",
	}
}

func batchSizeFlag() *cli.Int64Flag {
	return &cli.Int64Flag{
		Name:  "batch_size",
		Value: 1000,
		Usage: "Batch size for generation",
	}
}

func deltaRecordFlag() *cli.DurationFlag {
	return &cli.DurationFlag{
		Name:  "delta_record",
		Value: time.Second,
		Usage: "Time to wait between generating records",
	}
}

func postgresURLFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "postgres_url",
		Value: "postgres://postgres:sQHiQuMQHOSwikBfFMnpD3i4k9Bq1KMn4kIiL7yjX8BGGJujSt2OOqJbm74qjSbY@172.16.161.12:5432/activation?sslmode=disable&timezone=Europe%2FMoscow&search_path=zeebe",
		Usage: "PostgreSQL connection URL",
	}
}

func Cmd() *cli.Command {
	return &cli.Command{
		Name: "Generator",
		Flags: []cli.Flag{
			instanceTotalFlag(),
			workersFlag(),
			wfIDFlag(),
			batchSizeFlag(),
			deltaRecordFlag(),
			postgresURLFlag(),
		},
		Action: func(cCtx *cli.Context) error {
			instancesTotal := cCtx.Int64("instance_total")
			workersNum := cCtx.Int("workers")
			wfID := cCtx.Int64("wfID")
			batchSize := cCtx.Int64("batch_size")
			deltaRecord := cCtx.Duration("delta_record")
			postgresURL := cCtx.String("postgres_url")

			return generator.Generate(instancesTotal, workersNum, wfID, batchSize, deltaRecord, postgresURL)
		},
	}
}
