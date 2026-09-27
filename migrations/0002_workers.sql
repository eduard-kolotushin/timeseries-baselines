-- baselines.workers: the heartbeat table membership mode `store` reads. This
-- process writes it; nothing else does.
--
-- Its natural key column is renamed id -> worker_id so the surrogate key can use
-- id, the name every other table in this database gives its uuid primary key.
-- The worker's own SQL moves with it (Heartbeat/Peers and store_test.go).
--
-- A migration file must stay transactional: the engine runs each file in one
-- transaction, so CREATE INDEX CONCURRENTLY and other non-transactional
-- statements do not belong here. Never edit a file that has been applied — add
-- a new one.
CREATE TABLE IF NOT EXISTS baselines.workers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  worker_id TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  owned INTEGER NOT NULL DEFAULT 0,
  peers INTEGER NOT NULL DEFAULT 0,
  CONSTRAINT workers_worker_id_unique UNIQUE (worker_id)
);
-- Adopt a table created before the surrogate key: rename its text id first, so
-- the ADD COLUMN below adds a uuid instead of finding the text column. The guard
-- fires only while worker_id is absent and id is present, so it is a no-op on the
-- CREATE above and on every later run.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_attribute a
    JOIN pg_class t ON t.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'baselines' AND t.relname = 'workers'
      AND a.attname = 'worker_id' AND a.attnum > 0 AND NOT a.attisdropped
  ) AND EXISTS (
    SELECT 1
    FROM pg_attribute a
    JOIN pg_class t ON t.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'baselines' AND t.relname = 'workers'
      AND a.attname = 'id' AND a.attnum > 0 AND NOT a.attisdropped
  ) THEN
    ALTER TABLE baselines.workers RENAME COLUMN id TO worker_id;
  END IF;
END $$;
-- uuid-key: the surrogate key the primary key becomes
ALTER TABLE baselines.workers ADD COLUMN IF NOT EXISTS id UUID;
UPDATE baselines.workers SET id = gen_random_uuid() WHERE id IS NULL;
ALTER TABLE baselines.workers ALTER COLUMN id SET DEFAULT gen_random_uuid();
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'baselines' AND t.relname = 'workers' AND c.contype = 'u'
      AND array_length(c.conkey, 1) = 1
      AND c.conkey @> ARRAY[(SELECT a.attnum FROM pg_attribute a WHERE a.attrelid = t.oid AND a.attname = 'worker_id')]
  ) THEN
    ALTER TABLE baselines.workers ADD CONSTRAINT workers_worker_id_unique UNIQUE (worker_id);
  END IF;
END $$;
DO $$
DECLARE pk_name TEXT; pk_cols TEXT;
BEGIN
  SELECT c.conname, string_agg(a.attname, ',' ORDER BY a.attnum)
    INTO pk_name, pk_cols
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (c.conkey)
  WHERE n.nspname = 'baselines' AND t.relname = 'workers' AND c.contype = 'p'
  GROUP BY c.conname;
  IF pk_cols IS NULL THEN
    ALTER TABLE baselines.workers ADD PRIMARY KEY (id);
  ELSIF pk_cols <> 'id' THEN
    EXECUTE format('ALTER TABLE baselines.workers DROP CONSTRAINT %I', pk_name);
    ALTER TABLE baselines.workers ADD PRIMARY KEY (id);
  END IF;
END $$;
