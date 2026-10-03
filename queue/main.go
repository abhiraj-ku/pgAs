package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

type BenchmarkConfig struct {
	DBURL           string
	Producers       int
	Consumers       int
	TotalJobs       int
	BatchSize       int
	SimulateWork    time.Duration
	PayloadSizeByte int
}
type Metrics struct {
	enquedCount   uint64
	dequedCount   uint64
	latenciesLock sync.Mutex
	latencies     []time.Duration // measures latency until picked up
}

func main() {
	// parse all flags from stdin
	dbURL := flag.String("db", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", "DB connection string")
	producers := flag.Int("producers", 16, "Number of concurrent producers")
	consumers := flag.Int("consumers", 32, "Number of concurrent consumers")
	totalJobs := flag.Int("jobs", 100000, "Total number of jobs to process")
	batchSize := flag.Int("batch", 10, "Job fetch batch size for SKIP LOCKED")
	workSimMs := flag.Int("work-ms", 2, "Simulated worker execution time in ms")
	flag.Parse()

	cfg := BenchmarkConfig{
		DBURL:        *dbURL,
		Producers:    *producers,
		Consumers:    *consumers,
		TotalJobs:    *totalJobs,
		BatchSize:    *batchSize,
		SimulateWork: time.Duration(*workSimMs) * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(cfg.DBURL)
	if err != nil {
		log.Fatalf("Invalid db config: %v", err)
	}
	// pool size = producers + consumers + headroom
	poolCfg.MaxConns = int32(cfg.Producers + cfg.Consumers + 10)
	poolCfg.MinConns = int32(cfg.Producers + cfg.Consumers)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Fatalf("failed to conect: %v", err)
	}
	defer pool.Close()

	// truncate the table prior to clean benchmark run
	_, _ = pool.Exec(ctx, "truncate job_queue RESTART IDENTITY")

	fmt.Println("==========================================================")
	fmt.Printf(" [pgAs-queue] Benchmark: %d Jobs\n", cfg.TotalJobs)
	fmt.Printf(" Producers: %d | Consumers: %d | Batch Size: %d | Sim Work: %v\n",
		cfg.Producers, cfg.Consumers, cfg.BatchSize, cfg.SimulateWork)
	fmt.Println("==========================================================")

	metrics := &Metrics{
		latencies: make([]time.Duration, 0, cfg.TotalJobs),
	}

	start := time.Now()
	var eg errgroup.Group

	// run the actual producers
	jobsPerProducers := cfg.TotalJobs / cfg.Producers

	for p := 0; p < cfg.Producers; p++ {
		eg.Go(func() error {
			return runProducers(ctx, pool, jobsPerProducers, metrics)
		})
	}
}
