# Multi-Tenant B2B SaaS: Orders (Kafka + Postgres + Serverless)

**Name:** Maira LODHI
**Reg. No:** FA25-BSE-062
**Course:** CSC337 (AWT), Lab 9

A multi-tenant B2B order system. Each company (tenant) sees only its own data. Orders are accepted immediately (`202 Accepted`) and processed asynchronously through Kafka. A serverless function generates invoices.

---

## Live Links

| What | URL |
|---|---|
| Web app (Render) | https://saas-api-k5mi.onrender.com/ |
| Invoice function (Vercel) | https://awt-lab-work-776omliac-maira20.vercel.app/api/invoice |
| GitHub repo | https://github.com/Maira-Nasir180/awt-lab-work.git |

Demo login (Tenant A, acme): `admin@acme.com` / `Password123!`

> Note: the free Render service sleeps when idle. The first load can take 30 to 60 seconds.

---

## Architecture

```mermaid
flowchart LR
    B[Browser] -->|JWT + tenant| API[Express API]
    API -->|read / write, tenant_id on every query| PG[(Postgres - Neon)]
    API -->|publish, key = tenant_id| K[[Kafka topic: tenant.orders]]
    K --> C[Consumer: inventory-service]
    C -->|update stock, status = PROCESSED| PG
    C -->|HTTP + shared secret| F[Serverless invoice function - Vercel]
```

**Flow of one order**

1. The user logs in and gets a JWT that contains the tenant.
2. `POST /orders` saves the order as `PENDING` and publishes an event to Kafka. The API replies `202 Accepted` at once.
3. The consumer reads the event, updates stock and marks the order `PROCESSED`.
4. The consumer calls the invoice function, which creates the invoice (cold or warm start is logged).

---

## Key Design Decisions

- **Tenant isolation:** the tenant is resolved from the JWT. A token from tenant A with a header for tenant B is rejected (`403`). Every query is filtered by `tenant_id`.
- **Kafka key = `tenant_id`:** all events of one tenant go to the same partition, so their order is preserved. Different tenants spread across the 3 partitions.
- **Async processing:** the API does not wait for stock or invoice work, so responses stay fast.
- **N+1 vs JOIN:** the orders list can be loaded with one JOIN + `json_agg` query instead of 1 + N queries.
- **Composite index** `idx_orders_tenant_created` on `(tenant_id, created_at)` for the "latest orders of a tenant" query.

---

## Tech Stack

Node.js, Express, PostgreSQL (Neon), Kafka (Redpanda locally), Vercel serverless function, Render (web service), JWT.

---

## Run Locally

```bash
npm install
cp .env.example .env        # Windows: copy .env.example .env
npm run migrate
npm run seed
npm run seed:bulk           # 200,000+ orders for the benchmark
npm start                   # http://localhost:3000
```

### Environment variables (`.env`)

```
PORT=3000
DATABASE_URL=postgresql://USER:PASSWORD@HOST/DB?sslmode=require
DB_SSL=true
PG_POOL_MAX=10
JWT_SECRET=a-long-random-string
KAFKA_DISABLED=true
KAFKA_BROKERS=localhost:9092
KAFKA_SSL=false
KAFKA_SASL_MECHANISM=
KAFKA_USERNAME=
KAFKA_PASSWORD=
KAFKA_TOPIC=tenant.orders
KAFKA_GROUP_ID=inventory-service
INVOICE_FUNCTION_URL=
INVOICE_SECRET=shared-secret
```

Never commit `.env`. It is listed in `.gitignore`.

### With Kafka (local Docker)

```bash
docker compose up -d        # Postgres + Redpanda
# in .env set: KAFKA_DISABLED=false and INVOICE_FUNCTION_URL=http://localhost:3001/api/invoice
npm run create-topic
npm start                   # terminal 1
npm run consumer            # terminal 2
npm run invoice:local       # terminal 3
```

Place an order in the browser. The consumer terminal prints `[offset] ... partition=... offset=...` and `[invoice] ... COLD/WARM`.

### Tests

```bash
npm run smoke               # API and tenant isolation checks
npm run test:consumer       # consumer logic
npm run benchmark           # EXPLAIN ANALYZE before / after index
```

---

## Screenshots

### Order placed (202 Accepted)
![Order placed](docs/screenshots/01-order-placed.png)

### Orders list (JOIN + json_agg, 1 query)
![Orders list](docs/screenshots/02-orders-list-join.png)

### Products and stock (Tenant A only)
![Products and stock](docs/screenshots/03-products-stock.png)

---

## Benchmark: EXPLAIN ANALYZE

Data: 200,000+ orders for tenant acme.
Query: latest 50 orders of a tenant from the last 7 days, ordered by `created_at DESC`.

| | Plan | Execution time |
|---|---|---|
| Before index | Parallel Seq Scan + Sort (82,026 rows removed by filter) | about 43.4 ms (median 42.6 ms) |
| After index `idx_orders_tenant_created` | Index Scan, no sort needed | about 0.147 ms (median 0.147 ms) |

Result: about **290x faster** after adding the composite index.

### Before index
![EXPLAIN before](docs/screenshots/04-benchmark-before-index.png)

### After index
![EXPLAIN after](docs/screenshots/05-benchmark-after-index.png)

### N+1 vs JOIN (from `npm run smoke`)

| Strategy | Queries | Time |
|---|---|---|
| N+1 | 51 | 12621 ms |
| JOIN + json_agg | 1 | 1649 ms |

Both return the same order ids. Times are higher than local because the database is on Neon (network latency).

### Smoke test: 18 passed, 0 failed ![Smoke test](docs/screenshots/06-smoke-test.png)

---

## Deployment

- **Database:** Neon (Postgres), `DATABASE_URL` with `sslmode=require`.
- **Web service:** Render, build `npm install`, start `npm start`.
- **Invoice function:** Vercel, env var `INVOICE_SECRET`.
- **Kafka:** runs locally with Docker (Redpanda). On the free cloud deployment `KAFKA_DISABLED=true`, so orders are saved but no event is published, because free managed Kafka is not available.

---


