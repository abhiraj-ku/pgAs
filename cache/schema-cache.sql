-- UNLOGGED disables the WAL 
create unlogged table if not exists pg_cache(
    key varchar(255) primary key,
    value JSONB not null,
    expires_at timestamptz
);

-- index for TTL sweeps by the reaper
create index if not exists idx_pg_cache_expires_at on pg_cache(expires_at);

-- crucial: AUTOVACCUM tuning aggresively
alter table pg_cache set(
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit = 2000
);