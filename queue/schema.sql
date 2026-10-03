create unlogged table if not exists job_queue(
    id BIGSERIAL PRIMARY KEY,
    queue_name VARCHAR(64) NOT NULL DEFAULT 'default',
    priority INT NOT NULL DEFAULT 100,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, processing, completed, failed
    payload JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- crucial: partial index covering only eligible rows for pickup (avoid non used rows)
create index if not exists idx_job_queue_fetch
on job_queue(priority desc,id asc) where status ='pending';

-- autovaccum setup: for high churn queue tables
alter table job_queue set(
    autovacuum_vacuum_scale_factor = 0.05, -- vacuums after 5% rows chnages
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_limit = 2000
);