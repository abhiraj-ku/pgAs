package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
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
	if _, err := pool.Exec(ctx, "truncate job_queue RESTART IDENTITY"); err != nil {
		log.Fatalf("failed to truncate job_queue: %v", err)
	}

	fmt.Println("==========================================================")
	fmt.Printf(" [pgAs-queue] Benchmark: %d Jobs\n", cfg.TotalJobs)
	fmt.Printf(" Producers: %d | Consumers: %d | Batch Size: %d | Sim Work: %v\n",
		cfg.Producers, cfg.Consumers, cfg.BatchSize, cfg.SimulateWork)
	fmt.Println("==========================================================")

	metrics := &Metrics{
		latencies: make([]time.Duration, 0, cfg.TotalJobs),
	}

	start := time.Now()
	eg, egCtx := errgroup.WithContext(ctx)

	// run the actual producers
	jobsPerProducers := cfg.TotalJobs / cfg.Producers

	for p := 0; p < cfg.Producers; p++ {
		eg.Go(func() error {
			return runProducers(egCtx, pool, jobsPerProducers, metrics)
		})
	}

	// run consumer
	for c := 0; c < cfg.Consumers; c++ {
		eg.Go(func() error {
			return runConsumer(egCtx, pool, cfg, metrics)
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
		select {
		case <-egCtx.Done():
			if err := eg.Wait(); err != nil {
				log.Fatalf("benchmark worker failed: %v", err)
			}
			log.Fatalf("benchmark stopped before processing all jobs: processed %d of %d",
				atomic.LoadUint64(&metrics.dequedCount), cfg.TotalJobs)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if err := eg.Wait(); err != nil {
		log.Fatalf("benchmark worker failed: %v", err)
	}
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
				insert into job_queue(queue_name,priority,payload,scheduled_at) 
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

func runConsumer(ctx context.Context, pool *pgxpool.Pool, cfg BenchmarkConfig, m *Metrics) error {
	fetchSQL := fmt.Sprintf(`
		WITH claimed AS (
			SELECT id
			FROM job_queue
			WHERE status = 'pending'
			  AND scheduled_at <= NOW()
			ORDER BY priority DESC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT %d
		)
		UPDATE job_queue
		SET status = 'completed',
		    locked_at = NOW()
		FROM claimed
		WHERE job_queue.id = claimed.id
		RETURNING job_queue.id, job_queue.created_at, job_queue.locked_at;
	`, cfg.BatchSize)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		rows, err := pool.Query(ctx, fetchSQL)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}

		var count int
		var latencies []time.Duration

		for rows.Next() {
			var id int64
			var createdAt, lockedAt time.Time
			if err := rows.Scan(&id, &createdAt, &lockedAt); err == nil {
				count++
				latencies = append(latencies, lockedAt.Sub(createdAt))
			}
		}
		rows.Close()

		if count == 0 {
			// No jobs ready, brief backoff to prevent CPU spin
			time.Sleep(2 * time.Millisecond)
			continue
		}

		// Simulate business logic work execution
		if cfg.SimulateWork > 0 {
			time.Sleep(cfg.SimulateWork)
		}

		m.latenciesLock.Lock()
		m.latencies = append(m.latencies, latencies...)
		m.latenciesLock.Unlock()

		atomic.AddUint64(&m.dequedCount, uint64(count))
	}
}

func printResults(m *Metrics, d time.Duration, totalJobs int) {
	fmt.Println("\n================ FINAL BENCHMARK REPORT ================")
	fmt.Printf("Total Elapsed Time : %v\n", d)
	fmt.Printf("Throughput (Ops/sec): %.2f jobs/sec\n", float64(totalJobs)/d.Seconds())

	m.latenciesLock.Lock()
	lats := m.latencies
	m.latenciesLock.Unlock()

	if len(lats) == 0 {
		fmt.Println("No latencies recorded.")
		return
	}

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	p50 := lats[int(float64(len(lats))*0.50)]
	p90 := lats[int(float64(len(lats))*0.90)]
	p95 := lats[int(float64(len(lats))*0.95)]
	p99 := lats[int(float64(len(lats))*0.99)]
	p999 := lats[int(float64(len(lats))*0.999)]

	fmt.Println("\n--- Queue Dwell Time (Latency between Enqueue -> Dequeue Claim) ---")
	fmt.Printf("p50  : %v\n", p50)
	fmt.Printf("p90  : %v\n", p90)
	fmt.Printf("p95  : %v\n", p95)
	fmt.Printf("p99  : %v\n", p99)
	fmt.Printf("p99.9: %v\n", p999)
	fmt.Println("========================================================")
}
