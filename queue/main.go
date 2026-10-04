package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
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

	// run consumer
	for c := 0; c < cfg.Consumers; c++ {
		eg.Go(func() error {
			return runConsumers(ctx, pool, cfg, metrics)
		})
	}

	// progress ticker
	ticker := time.NewTicker(2 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				enq := atomic.LoadUint64(&metrics.enquedCount)
				deq := atomic.LoadUint64(&metrics.dequedCount)
				fmt.Printf("[Status] Enqueued: %-7d | Processed: %-7d (%.1f%%)\n", enq, deq, float64(deq)/float64(cfg.TotalJobs)*100)
				if int(deq) >= cfg.TotalJobs {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// wait unitl all target jobs are dqeueed
	for {
		if int(atomic.LoadUint64(&metrics.dequedCount)) >= cfg.TotalJobs {
			cancel()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = eg.Wait()
	dur := time.Since(start)
	ticker.Stop()

	printResults(metrics, dur, cfg.TotalJobs)
}

func runProducers(ctx context.Context, pool *pgxpool.Pool, count int, m *Metrics) error {
	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			return nil
		}
		payload, _ := json.Marshal(map[string]any{
			"order_id":   rand.Intn(1000000),
			"user_id":    rand.Intn(50000),
			"amount_usd": rand.Float64() * 500,
			"timestamp":  time.Now().UnixNano(),
		})
		priority := rand.Intn(100)
		query := `
				insert into jobs_queue(queue_name,priority,payload,scheduled_at) 
				values('order',$1,$2,now())

				`
		_, err := pool.Exec(ctx, query, priority, payload)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		atomic.AddUint64(&m.enquedCount, 1)
	}
	return nil
}

func runConsumers(ctx context.Context, pool *pgxpool.Pool, cfg BenchmarkConfig, m *Metrics) error {}
