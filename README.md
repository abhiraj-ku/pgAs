# pgAs

my attempt to use postgres as more than a database.

thinking in terms of postgres as:
- cache
- queue
- task runner
- other simple infra patterns

this is a one-project-at-a-time repo. as i build each one, i'll benchmark it and write the results here.

## Phase 1: pgAs-queue

Phase 1 starts with pgAs-queue: replacing dedicated message brokers (RabbitMQ, SQS, Celery) with Postgres using transactional job enqueuing and FOR UPDATE SKIP LOCKED.

To benchmark this properly by industry standards, we don't just measure raw SELECT 1 queries. Real queue benchmarks measure:

1. Producer-consumer under contention: N concurrent producers inserting mixed payloads while M concurrent workers compete for jobs.
2. Workload simulation: simulated worker processing time (for example, 2-5ms realistic micro-task duration) rather than zero-work spinning.
3. Queue delay / dwell time: latency from enqueued_at to locked_at (p50, p95, p99, p99.9).
4. Table bloat and lock saturation: monitoring dead tuples (n_dead_tup) and WAL write rates.

## run

```bash
go run .
```

for now it's just a starting point.