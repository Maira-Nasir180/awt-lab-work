-- Shared database, shared schema, tenant_id discriminator + Row Level Security
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tenants (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug       TEXT UNIQUE NOT NULL,
  name       TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  email         TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  role          TEXT NOT NULL DEFAULT 'member',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, email)
);

CREATE TABLE IF NOT EXISTS products (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  sku        TEXT NOT NULL,
  name       TEXT NOT NULL,
  price      NUMERIC(12,2) NOT NULL CHECK (price >= 0),
  stock      INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
  UNIQUE (tenant_id, sku)
);

CREATE TABLE IF NOT EXISTS orders (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id      UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  customer_name  TEXT NOT NULL,
  status         TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | PROCESSED | FAILED | PUBLISH_FAILED
  total          NUMERIC(12,2) NOT NULL DEFAULT 0,
  failure_reason TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  order_id   UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id UUID NOT NULL REFERENCES products(id),
  quantity   INTEGER NOT NULL CHECK (quantity > 0),
  unit_price NUMERIC(12,2) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);

-- Idempotency + offset log for the Kafka consumer
CREATE TABLE IF NOT EXISTS processed_events (
  event_id     UUID PRIMARY KEY,
  tenant_id    UUID NOT NULL,
  topic        TEXT NOT NULL,
  partition    INTEGER NOT NULL,
  "offset"     BIGINT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serverless invoice metrics (cold vs warm start)
CREATE TABLE IF NOT EXISTS invoice_logs (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     UUID NOT NULL,
  order_id      UUID NOT NULL,
  cold_start    BOOLEAN NOT NULL,
  duration_ms   NUMERIC(10,2) NOT NULL,
  round_trip_ms NUMERIC(10,2),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- Row Level Security (defence in depth) ----------
DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['users','products','orders','order_items','processed_events','invoice_logs'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I USING (tenant_id = NULLIF(current_setting(''app.tenant_id'', true), '''')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting(''app.tenant_id'', true), '''')::uuid)', t);
  END LOOP;
END $$;
-- The composite index idx_orders_tenant_created is created by scripts/benchmark.js
-- so EXPLAIN ANALYZE can be shown BEFORE and AFTER (it is kept at the end of the benchmark).
