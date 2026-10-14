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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

type Metrics struct {
	setCount      uint64
	getCount      uint64
	hitCount      uint64
	missCount     uint64
	setErrorCount uint64
	getErrorCount uint64
	reaperDeleted uint64
	setLatencies  []time.Duration
	getLatencies  []time.Duration
	mu            sync.Mutex
}

func setupDB(pool *pgxpool.Pool) {
	fmt.Println("Initializing pg_cache schema...")
	schemaSQL := `
		CREATE UNLOGGED TABLE IF NOT EXISTS pg_cache (
			key VARCHAR(255) PRIMARY KEY,
			value JSONB NOT NULL,
			expires_at TIMESTAMPTZ
		);

		-- Ensure column is named expires_at if table was previously created with expired_at
		DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name = 'pg_cache' AND column_name = 'expired_at'
			) THEN
				ALTER TABLE pg_cache RENAME COLUMN expired_at TO expires_at;
			END IF;
		END $$;

		CREATE INDEX IF NOT EXISTS idx_pg_cache_expires_at ON pg_cache (expires_at);

		ALTER TABLE pg_cache SET (
			autovacuum_vacuum_scale_factor = 0.02,
			autovacuum_vacuum_cost_limit = 2000
		);

		TRUNCATE pg_cache;
	`
	_, err := pool.Exec(context.Background(), schemaSQL)
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize database schema: %v", err)
	}
}

func main() {
	dbURL := flag.String("db", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", "DB connection string")
	setters := flag.Int("setters", 16, "Number of concurrent SET workers")
	getters := flag.Int("getters", 64, "Number of concurrent GET workers")
	durationSec := flag.Int("duration", 30, "Benchmark duration in seconds")
	ttlMin := flag.Int("ttl-min", 15, "Key TTL in minutes (production standard)")
	keySpace := flag.Int("keyspace", 50000, "Distinct key cardinality to simulate working set")
	sweepIntervalSec := flag.Int("sweep-sec", 5, "Reaper run interval in seconds")
	flag.Parse()

	// pool config
	poolcfg, err := pgxpool.ParseConfig(*dbURL)
	if err != nil {
		log.Fatalf("invalid db config: %v", err)
	}
	poolcfg.MaxConns = int32(*setters + *getters + 10)
	poolcfg.MinConns = int32(*setters + *getters)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolcfg)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer pool.Close()
	setupDB(pool)

	fmt.Println("==========================================================")
	fmt.Printf(" [pgAs-cache] Workload Benchmark\n")
	fmt.Printf(" Duration  : %ds | TTL: %dm | Key Pool: %d keys\n", *durationSec, *ttlMin, *keySpace)
	fmt.Printf(" Setters   : %d  | Getters: %d | Reaper Sweep: every %ds\n", *setters, *getters, *sweepIntervalSec)
	fmt.Println("==========================================================")

	metrics := &Metrics{
		setLatencies: make([]time.Duration, 0, 200000),
		getLatencies: make([]time.Duration, 0, 1000000),
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*durationSec)*time.Second)
	defer cancel()

	var eg errgroup.Group

	// bg reaper worker
	eg.Go(func() error {
		runReaper(ctx, pool, time.Duration(*sweepIntervalSec)*time.Second, metrics)
		return nil
	})

	// run concurrent setters
	for i := 0; i < *setters; i++ {
		eg.Go(func() error {
			return runSetter(ctx, pool, *keySpace, *ttlMin, metrics)
		})
	}

	// concurrent getters
	for i := 0; i < *getters; i++ {
		eg.Go(func() error {
			return runGetter(ctx, pool, *keySpace, metrics)
		})
	}

	// live cli report
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				sets := atomic.LoadUint64(&metrics.setCount)
				gets := atomic.LoadUint64(&metrics.getCount)
				hits := atomic.LoadUint64(&metrics.hitCount)
				swept := atomic.LoadUint64(&metrics.reaperDeleted)
				setErrs := atomic.LoadUint64(&metrics.setErrorCount)
				getErrs := atomic.LoadUint64(&metrics.getErrorCount)

				hitRate := 0.0
				if gets > 0 {
					hitRate = float64(hits) / float64(gets) * 100
				}
				if setErrs > 0 || getErrs > 0 {
					fmt.Printf("[Status] SETs: %-8d | GETs: %-8d | Hit Rate: %5.1f%% | Reaper Swept: %-6d | Errors (SET: %d, GET: %d)\n",
						sets, gets, hitRate, swept, setErrs, getErrs)
				} else {
					fmt.Printf("[Status] SETs: %-8d | GETs: %-8d | Hit Rate: %5.1f%% | Reaper Swept: %-6d\n",
						sets, gets, hitRate, swept)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	_ = eg.Wait()
	ticker.Stop()
	elapsed := time.Since(start)
	printResult(metrics, elapsed)
}

func runReaper(ctx context.Context, pool *pgxpool.Pool, sweepInterval time.Duration, m *Metrics) {
	// using cte we find the expired keys without disturbing other ops and in batch
	deleteSql := `
		with expired as (
			select key from pg_cache 
			where expires_at <= now()
			limit 5000
			for update skip locked
		)
		delete from pg_cache where key in (select key from expired);
	`
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				if ctx.Err() != nil {
					return
				}
				cmdTag, err := pool.Exec(ctx, deleteSql)
				if err != nil {
					if ctx.Err() == nil {
						log.Printf("[Reaper warning] %v", err)
					}
					break
				}

				deleted := cmdTag.RowsAffected()
				if deleted > 0 {
					atomic.AddUint64(&m.reaperDeleted, uint64(deleted))
				}

				// if fewer than batch limit, the queue of expired key is drained
				if deleted < 5000 {
					break
				}

				// yield to let concurrent oltp queries breathe
				select {
				case <-time.After(50 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func runSetter(ctx context.Context, pool *pgxpool.Pool, keySpace int, ttlMin int, m *Metrics) error {
	insertSql := fmt.Sprintf(`
	insert INTO pg_cache (key, value, expires_at)
		values ($1, $2, now() + interval '%d minutes')
		on conflict (key) DO UPDATE 
		set value = EXCLUDED.value, expires_at = EXCLUDED.expires_at;
	`, ttlMin)
	for {
		if ctx.Err() != nil {
			return nil
		}

		key := fmt.Sprintf("session:%d", rand.Intn(keySpace))
		payload, _ := json.Marshal(map[string]any{
			"user_id":    rand.Intn(100000),
			"role":       "admin",
			"auth_token": "bearer-7c9b8a2e1",
			"updated_at": time.Now().UnixNano(),
		})

		start := time.Now()
		_, err := pool.Exec(ctx, insertSql, key, payload)
		dur := time.Since(start)

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			errCount := atomic.AddUint64(&m.setErrorCount, 1)
			if errCount <= 5 {
				log.Printf("[SET error #%d] %v", errCount, err)
			}
			continue
		}

		atomic.AddUint64(&m.setCount, 1)
		m.mu.Lock()
		m.setLatencies = append(m.setLatencies, dur)
		m.mu.Unlock()
	}
}

func runGetter(ctx context.Context, pool *pgxpool.Pool, keySpace int, m *Metrics) error {
	getSQL := `
		SELECT value FROM pg_cache 
		WHERE key = $1 AND expires_at > NOW();
	`

	for {
		if ctx.Err() != nil {
			return nil
		}

		key := fmt.Sprintf("session:%d", rand.Intn(keySpace))

		start := time.Now()
		var val string
		err := pool.QueryRow(ctx, getSQL, key).Scan(&val)
		dur := time.Since(start)

		if err != nil && err != pgx.ErrNoRows {
			if ctx.Err() != nil {
				return nil
			}
			errCount := atomic.AddUint64(&m.getErrorCount, 1)
			if errCount <= 5 {
				log.Printf("[GET error #%d] %v", errCount, err)
			}
			continue
		}

		if err == pgx.ErrNoRows {
			atomic.AddUint64(&m.missCount, 1)
		} else {
			atomic.AddUint64(&m.hitCount, 1)
		}

		atomic.AddUint64(&m.getCount, 1)
		m.mu.Lock()
		m.getLatencies = append(m.getLatencies, dur)
		m.mu.Unlock()
	}
}

func printResult(m *Metrics, d time.Duration) {
	fmt.Println("\n================ FINAL BENCHMARK REPORT ================")
	fmt.Printf("Total Elapsed Time : %v\n", d.Round(time.Millisecond))

	totalSets := atomic.LoadUint64(&m.setCount)
	totalGets := atomic.LoadUint64(&m.getCount)
	hits := atomic.LoadUint64(&m.hitCount)
	misses := atomic.LoadUint64(&m.missCount)
	swept := atomic.LoadUint64(&m.reaperDeleted)
	setErrs := atomic.LoadUint64(&m.setErrorCount)
	getErrs := atomic.LoadUint64(&m.getErrorCount)

	sec := d.Seconds()
	if sec <= 0 {
		sec = 1
	}

	fmt.Printf("SET Throughput : %.2f ops/sec\n", float64(totalSets)/sec)
	fmt.Printf("GET Throughput : %.2f ops/sec\n", float64(totalGets)/sec)
	if totalGets > 0 {
		fmt.Printf("Hit Rate       : %.2f%% (%d hits / %d misses)\n",
			float64(hits)/float64(totalGets)*100, hits, misses)
	}
	fmt.Printf("Reaper Cleared : %d dead rows (Reaper did not block GET/SET traffic)\n", swept)
	if setErrs > 0 || getErrs > 0 {
		fmt.Printf("Total Errors   : %d (SET: %d, GET: %d)\n", setErrs+getErrs, setErrs, getErrs)
	}

	printLatencies("SET (Upsert) Latencies", m.setLatencies)
	printLatencies("GET Latencies", m.getLatencies)
	fmt.Println("========================================================")
}

func printLatencies(name string, lats []time.Duration) {
	if len(lats) == 0 {
		return
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p50 := lats[int(float64(len(lats))*0.50)]
	p90 := lats[int(float64(len(lats))*0.90)]
	p95 := lats[int(float64(len(lats))*0.95)]
	p99 := lats[int(float64(len(lats))*0.99)]

	fmt.Printf("\n--- %s ---\n", name)
	fmt.Printf("p50 : %v\n", p50)
	fmt.Printf("p90 : %v\n", p90)
	fmt.Printf("p95 : %v\n", p95)
	fmt.Printf("p99 : %v\n", p99)
}
