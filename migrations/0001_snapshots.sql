-- baselines.snapshots: one gzipped forecast.Snapshot per metric hash.
--
-- A migration file must stay transactional: the engine runs each file in one
-- transaction, so CREATE INDEX CONCURRENTLY and other non-transactional
-- statements do not belong here. Never edit a file that has been applied — add
-- a new one.
CREATE SCHEMA IF NOT EXISTS baselines;
CREATE TABLE IF NOT EXISTS baselines.snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_hash TEXT NOT NULL,
  model TEXT NOT NULL,
  season TEXT NOT NULL,
  calendar TEXT NOT NULL DEFAULT '',
  lookback_ms BIGINT NOT NULL,
  trained_at TIMESTAMPTZ NOT NULL,
  snapshot BYTEA NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT snapshots_metric_hash_unique UNIQUE (metric_hash)
);
-- Adopt a table created before the surrogate key. On the CREATE above ADD COLUMN
-- IF NOT EXISTS is a no-op; on an upgraded table it adds the column for the
-- backfill below.
ALTER TABLE baselines.snapshots ADD COLUMN IF NOT EXISTS id UUID;
UPDATE baselines.snapshots SET id = gen_random_uuid() WHERE id IS NULL;
ALTER TABLE baselines.snapshots ALTER COLUMN id SET DEFAULT gen_random_uuid();
-- The natural key must stay unique: Put upserts ON CONFLICT (metric_hash). A
-- pre-uuid table carries that uniqueness as its primary key, which the swap below
-- drops.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'baselines' AND t.relname = 'snapshots' AND c.contype = 'u'
      AND array_length(c.conkey, 1) = 1
      AND c.conkey @> ARRAY[(SELECT a.attnum FROM pg_attribute a WHERE a.attrelid = t.oid AND a.attname = 'metric_hash')]
  ) THEN
    ALTER TABLE baselines.snapshots ADD CONSTRAINT snapshots_metric_hash_unique UNIQUE (metric_hash);
  END IF;
END $$;
-- uuid-key: the primary key itself must be the uuid column.
DO $$
DECLARE pk_name TEXT; pk_cols TEXT;
BEGIN
  SELECT c.conname, string_agg(a.attname, ',' ORDER BY a.attnum)
    INTO pk_name, pk_cols
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (c.conkey)
  WHERE n.nspname = 'baselines' AND t.relname = 'snapshots' AND c.contype = 'p'
  GROUP BY c.conname;
  IF pk_cols IS NULL THEN
    ALTER TABLE baselines.snapshots ADD PRIMARY KEY (id);
  ELSIF pk_cols <> 'id' THEN
    EXECUTE format('ALTER TABLE baselines.snapshots DROP CONSTRAINT %I', pk_name);
    ALTER TABLE baselines.snapshots ADD PRIMARY KEY (id);
  END IF;
END $$;
