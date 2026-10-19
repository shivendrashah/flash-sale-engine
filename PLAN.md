# Flash-Sale Booking Engine: Project Plan

A backend where limited stock goes on sale at a fixed time, thousands of users try to buy at once,
and the system never sells more than it has, stays fast, and survives failures.

**Stack:** Java 21, Spring Boot 4.1, PostgreSQL 17, Redis 7.4, Kafka, nginx, Docker Compose, Testcontainers, k6, Prometheus + Grafana

**Status:** Phase 1 done (2026-10-02). Phase 2 done (2026-10-04 to 09): the multi-item cart, the Redis stock gate,
payment safety, the per-user purchase limit, the rate limiter and the sale-page cache. Phase 3 done (2026-10-10):
order notifications through a transactional outbox, Kafka and a worker with retries and a dead-letter topic; two app
instances behind nginx with a per-IP limit; the stock reconciliation job; and the waiting room. Phase 4 step 1 done
(2026-10-10): Prometheus and a Grafana dashboard, with gauges that recount the stock. Phase 4 step 2 done
(2026-10-10): k6 load tests, 1 instance vs 2 and the locking strategies (`loadtest/`). Phase 4 steps 3 and 4 done
(2026-10-10): failure tests under load (`loadtest/chaos.sh`) and the final README. **All four phases are complete.**
270 tests passing. Docs:
[HLD.md](HLD.md) (system design) and [LLD.md](LLD.md) (code design).

---

## Final scope

| In scope (build it) | Design doc only (`HLD.md`) |
|---|---|
| Multiple products, each with its own stock | Read replicas |
| Per-user purchase limit | Sharding / partitioning |
| Multi-item cart: all-or-nothing **or** partial (user picks) | Saga for refunds |
| 10-minute hold, auto-expiry | Cache stampede protection across instances (a per-instance single flight is built) |
| Idempotent payment (fake gateway + circuit breaker) | Multi-region, Kubernetes autoscaling |
| Redis stock gate + cache + rate limiter | |
| Two app instances behind nginx, per-IP limit (built) | Autoscaling, health-based routing by a cloud load balancer |
| Kafka notifications via outbox, retry, DLQ (built) | Change data capture (Debezium) instead of a polling relay; retry topics |
| Waiting room (virtual queue, built: a Redis queue per sale, N buyers let in each second) | Fairness rules beyond arrival order (a lottery, priority for members) |
| Reconciliation job (Redis vs DB stock, built: reports drift, repairs low counters) | Lowering high counters (left to their 60 s expiry on purpose) |
| Monitoring dashboard (built: Prometheus + Grafana, 24 panels, oversell gauges) | Alerts (Alertmanager), long-term metric storage |
| Failure tests + load tests (built: `loadtest/run.sh`, `loadtest/chaos.sh`) | Tests on separate machines, with real network latency |

---

## Data model (PostgreSQL)

```text
products      (id, name, price, created_at)
sales         (id, name, starts_at, ends_at, per_user_limit, admit_per_second)   admit_per_second added in V5
sale_items    (id, sale_id, product_id, total_stock, available_stock, sale_price, version)
holds         (id, user_id, sale_id, mode, status, total_amount, expires_at, idempotency_key UNIQUE, created_at)
hold_items    (id, hold_id, sale_item_id, quantity, unit_price)
orders        (id, hold_id UNIQUE, user_id, status, created_at)
payments      (id, hold_id, amount, status, provider, provider_ref, idempotency_key UNIQUE)
outbox_events (id, event_id UNIQUE, aggregate_type, aggregate_id, event_type, topic, message_key, payload, status,
               attempts, next_attempt_at, last_error, created_at, sent_at)        built in V4
notifications (event_id PRIMARY KEY, user_id, order_id, message, created_at)       built in V4
user_sale_units (sale_id, user_id, units, max_units)   PRIMARY KEY (sale_id, user_id); built in V3
```

Hold status: `HELD -> PAYMENT_PENDING -> CONFIRMED`, `HELD -> EXPIRED / CANCELLED`, and a declined card goes
`PAYMENT_PENDING -> HELD` (the user may retry until the hold expires). Phase 1 uses `X-User-Id` / `Idempotency-Key`
headers and `/api/...` paths; see HLD.md section 7 for the exact Phase 1 API.

## Redis keys

```text
stock:{saleId}:{saleItemId}   remaining stock counter (the fast gate; built). The braces are real: a Redis Cluster
                              hash tag, so one sale's counters share a server. Expires 60 s after seeding
stock:{saleId}:request:...    "this request already took units": copies of it take none (built)
bought:{saleId}:{userId}      (not used: the per-user limit lives in PostgreSQL, user_sale_units; see below)
idem:{key}                    idempotency key -> response, 24h TTL
rl:{userId}                   token bucket for rate limiting (built). Really "rl:alice", no braces: one key per
                              script call, so no hash tag is needed. A hash {tokens, at}; expires 2 s after last use
cache:sale:{saleId}           cached sale page (built). Really "cache:sale:42": the SaleView as JSON, SET with
                              PX 1000, so Redis deletes it after 1 s. No invalidation
wr:{saleId}:queue             waiting room (built): a sorted set, member = user id, score = turn number (INCR of
                              wr:{saleId}:turn, so the order is exact across instances, with no clock)
wr:{saleId}:admit             the admission credit (built): a hash {credit, at}, shared by every instance's admitter
wr:{saleId}:pass:{userId}     the pass (built): "may buy now"; expires after 5 min. All four keys share the hash tag
```

## APIs

```text
GET  /sales/{saleId}                       sale page + items left (cached)
POST /sales/{saleId}/queue                 join the waiting room (built) -> state, position, estimated wait
GET  /sales/{saleId}/queue                 my place: WAITING (+ position), ADMITTED or NOT_QUEUED (built)
                                           every POST/DELETE below: one token from the buyer's bucket, or 429
                                           (built; GET and /admin are not counted)
POST /holds          (Idempotency-Key)     reserve cart items for 10 min
                                           body: { items: [...], mode: ALL_OR_NOTHING | PARTIAL }   (built)
                                           returns: the hold (items, totalAmount, expiresAt) + unavailable[]
POST /holds/{id}/pay (Idempotency-Key)     pay for a hold
DELETE /holds/{id}                         cancel a hold
GET  /orders/{id}                          order status
```

---

## Phases

### Phase 1: Core flow, single item (week 1): DONE
- Project skeleton, Docker Compose (Postgres, Redis), Flyway migrations
- Hold -> pay -> confirm flow with state machine; scheduled expiry job
- **Oversell test:** 50 threads buy the last item. Show the naive version oversells, then fix it
- Three locking strategies behind one interface: pessimistic, optimistic (`@Version` + retry), Redis lock
  (built: five, adding CONDITIONAL_UPDATE (atomic `UPDATE ... WHERE stock >= qty`) and the broken NAIVE baseline)
- Fake payment gateway (Strategy + Factory), idempotency keys

**Done when:** oversell test passes for all 3 strategies; expired holds return stock. (Met: see LLD.md section 14.)

### Phase 2: Multi-item cart + Redis (week 2)
- ✅ **Step 1 (done 2026-10-04):** cart holds with two reservation modes behind a `ReservationPolicy` interface
  (Strategy pattern): `AllOrNothingPolicy` and `PartialPolicy`, combined with the five `StockReserver` strategies
  (see "Multi-item cart: how it works" below)
- ✅ Deadlock test: carts with the same two items in opposite order, concurrently (`CartTest`; with the id sort
  removed, PESSIMISTIC and CONDITIONAL_UPDATE really do deadlock)
- ✅ **Step 2 (done 2026-10-04): Redis stock gate** in front of every strategy: one Lua script per cart answers
  "sold out" without touching PostgreSQL; units the database doesn't keep go back to Redis; it fails open when Redis
  is down (see "Redis stock gate: how it works" below). Two bugs found on the way: double-clicks on the last unit got
  409 (with or without the gate), and a real Redis outage gave 500s (REDIS_LOCK included). Both fixed and tested
- ✅ **Step 3 (done 2026-10-04): payment safety.** A 2 s timeout on every provider call ("no answer" = unknown, so
  the payment stays `PENDING` and the client gets 202), a circuit breaker per provider (Resilience4j: fail fast with
  503 while a provider is down), and a **payment recovery job** that asks the provider what happened to stuck
  payments and finishes them. This closes the known gap from HLD.md section 14 (see "Payment safety: how it works")
- ✅ **Step 4 (done 2026-10-05): per-user purchase limit.** A sale may set `perUserLimit`; each buyer's units
  (held or bought) are counted in a `user_sale_units` row and changed with one conditional upsert inside the hold's
  transaction, so 10 tabs of one buyer firing at once still stop at the limit (see "Per-user limit: how it works")
- ✅ **Step 5 (done 2026-10-09): rate limiter.** A token bucket per buyer in Redis (a burst of 10, then 5 a second),
  checked by a Spring MVC interceptor before every write under `/api` (not `/api/admin`). An empty bucket is 429
  `RATE_LIMITED` + `Retry-After`, before any database work; it fails open when Redis is down. In the same step,
  Spring's own errors (missing header, broken JSON, bad value, wrong method, unknown URL) got our `code` field, and an
  unexpected exception became 500 `INTERNAL_ERROR` (see "Rate limiter: how it works")
- ✅ **Step 6 (done 2026-10-09): sale-page cache.** `GET /api/sales/{id}` reads a one-second copy in Redis
  (cache-aside, key `cache:sale:<saleId>`). On a miss, only one request per sale and app instance reads PostgreSQL;
  the others wait for its answer (a single flight). "Live or not" is worked out on every read, buying never reads the
  cache, and it fails open when Redis is down (see "Sale-page cache: how it works"). **Phase 2 is complete.**

**Done when:** ALL_OR_NOTHING never leaves a half-reserved cart; PARTIAL reserves exactly what is available
and reports the rest; no deadlocks or oversells under concurrent cart tests in either mode. (Met, and re-checked with
the Redis gate, 2026-10-04: `CartTest` runs with the gate on and checks Redis equals PostgreSQL after every test;
`CartWithoutGateTest` reruns all 34 tests with the gate off.)

### Phase 3: Async + scaling (week 3)
- ✅ **Step 1 (done 2026-10-10): outbox table -> Kafka -> notification worker, retries + dead-letter topic.** A
  paid order writes an `ORDER_CONFIRMED` event row in its own transaction. `OutboxRelay` sends due rows to Kafka
  every 500 ms (`FOR UPDATE SKIP LOCKED`, back-off while Kafka is down). `NotificationListener` tells each customer
  once (idempotent by event id). A message that keeps failing is retried 3 times, then parked on
  `flashsale.orders.DLT` (see "Outbox + Kafka: how it works" below, HLD.md 9.12, LLD.md section 2)
- ✅ **Step 2 (done 2026-10-10): two app instances behind nginx, with a per-IP limit at nginx.** A `Dockerfile`
  builds the app's image; `docker compose --profile cluster` runs `app1`, `app2` and nginx on port 8080. nginx takes
  turns between the instances, retries on the other one when an instance cannot be reached, and limits each IP to 20
  requests a second after a burst of 40 (429 `IP_RATE_LIMITED`). No Java code changed: the app was already stateless
  (see "Two instances behind nginx: how it works" below, HLD.md 9.13, LLD.md section 2)
- ✅ **Step 3 (done 2026-10-10): reconciliation job.** Every 10 s, `StockReconciler` compares each live sale
  item's Redis counter with PostgreSQL and records the drift (gauge `flashsale.stock.drift`). It repairs a counter
  only when it is too low the same way on two runs, with a compare-and-set in Redis; a counter that is too high is
  harmless and left to its 60 s expiry (see "Stock reconciliation: how it works" below, HLD.md 9.14, LLD.md section 6)
- ✅ **Step 4 (done 2026-10-10): waiting room.** A sale created with `admitPerSecond` queues its buyers in a Redis
  sorted set; an admitter lets that many in each second and gives each a 5-minute pass; a hold without a pass is
  refused with 403 `NOT_ADMITTED` (see "Waiting room: how it works" below, HLD.md 9.15, LLD.md section 2)

**Done when:** correctness holds with 2 instances; notifications survive a Kafka restart. **Both are met
(2026-10-10).** Two instances: `TwoInstancesTest` (100 buyers over two real instances, no oversell for every safe
strategy, every order announced once) and the live `docker compose --profile cluster` run. Kafka restart:
`KafkaOutageTest` pauses a real Kafka, and a live `docker compose stop kafka` / `start kafka` run delivered the
waiting event 2 s after the restart. **Phase 3 is complete:** every item above is built and both conditions hold.

### Phase 4: Proof + presentation (week 4)
- [x] Prometheus + Grafana dashboard (req/s, p99, DB pool, Kafka lag, oversell count). Done 2026-10-10: see
  "Monitoring: how it works" below, and [LLD.md](LLD.md) section 6 for the oversell gauges
- [x] k6 load tests (constant arrival rate): 1 instance vs 2 instances; compare locking strategies. Done 2026-10-10:
  see "Load tests: how it works" below, and `loadtest/RESULTS.md`
- [x] Failure tests: kill Redis, kill an app instance, stop Kafka. Document what happens. Done 2026-10-10: see
  "Failure tests: how it works" below, and `loadtest/CHAOS.md`
- [x] README (problem, diagram, one-command run, results table, design decisions) + `HLD.md` + `LLD.md`. Done
  2026-10-10

**Done when:** a stranger can run it with one command and see the results table. **Met:** from a clean state (the
app image deleted, nothing running), `loadtest/run.sh quick` built the image, started the cluster and wrote a results
row in 47 s (with Docker's build cache already holding the libraries; a first build on a new machine also downloads
them, a few minutes). `loadtest/run.sh` writes the full table. **Phase 4 is complete, and so is the plan.**

What stays design only (described in HLD.md, not built): read replicas, sharding and partitioning, a saga for
refunds, stampede protection across instances, multi-region and Kubernetes autoscaling, change data capture for the
outbox, retry topics, alerts and long-term metric storage, a highly available nginx pair, and real login.

### Phase 5: A web app (after the plan)
- ✅ **Done 2026-10-11: a React web app, so a person can use the system and watch it.** React 19, TypeScript and
  Vite in `frontend/`. nginx serves the built files at `/` and still load-balances `/api/`. Three pages: the shop,
  a sale page with the full checkout (waiting room, reservation countdown, four test cards, cancel, every error code
  as a sentence), and a demo console. New API: `GET /api/sales`, and for the console
  `POST /api/admin/sales/{id}/crowd` (fake buyers, one virtual thread each) and `GET /api/admin/sales/{id}/stats`
  (see "Web app: how it works" below, HLD.md 9.16)

**Done when:** someone with only Docker opens one URL and can buy, and can watch a crowd buy. **Met:**
`docker compose --profile cluster up -d --build`, then http://localhost:8080. A script in headless Chrome clicked
through every screen (reserve, a declined card, a paid order, the waiting room, a phone-sized screen) with no
browser errors.

---

## Multi-item cart: how it works

Built 2026-10-04. Example: Rahul's cart = 1 MacBook (sale item 1, 5 left) + 1 iPhone (sale item 2, 0 left).

Steps (both modes):
1. **Validate at the edge** (`CreateHoldRequest`): 1 to 10 items, quantity 1 to 10, **each sale item at most once**
   (a repeated item is a 400, not merged: merging would guess what the client meant).
2. **Validate in the service** (`HoldService`): every item exists (404), all items are in **one sale** (422
   `MIXED_SALES`: a hold has one sale and one expiry), and the sale is live (409).
3. **The Redis gate** (built, step 2): one Lua script, atomic, decides per mode before the database is touched.
   Rahul's ALL_OR_NOTHING cart stops here with 409; his PARTIAL cart goes on with the MacBook only.
4. **One DB transaction:** the chosen `StockReserver` visits the lines **sorted by id** (`StockLine.inIdOrder`), takes
   each one its own way (row lock, version check, conditional UPDATE, Redis lock), and asks the `ReservationPolicy`
   what to do with a short line: throw (roll everything back) or skip it. Then it inserts one hold + its hold_items
   with the prices copied. Commit.
5. One payment for the hold total. Success -> confirm everything in the hold. Failure / 10-min expiry -> release
   every held item.

| | ALL_OR_NOTHING (default) | PARTIAL |
|---|---|---|
| A short item | `AllOrNothingPolicy` throws `SoldOutException`: the transaction rolls back, earlier items included | `PartialPolicy` skips it |
| Rahul's result | 409 "Sale item 2 does not have 1 unit(s) left", nothing reserved | 201: MacBook held; `"unavailable": [{"saleItemId": 2, "quantity": 1}]` |
| Hold total | n/a (no hold) | Only the MacBook's price |
| Nothing available | 409 `SOLD_OUT` | 409 `SOLD_OUT` (no empty holds: the default `onFinished` rule) |
| Use for | Bundles, combo offers, seats together | Normal shopping carts |

Code shape (as built; the original sketch had the policy doing the reservation itself, which would have copied the
locking code into every policy):

```java
interface ReservationPolicy {                     // WHAT a short item means
    ReservationMode mode();
    void onShortage(StockLine line);              // throw = abort the cart, return = skip the line
    default void onFinished(List<StockLine> taken, List<StockLine> requested) { /* no empty holds */ }
}
interface StockReserver {                         // HOW stock is taken safely (5 strategies)
    <T> T reserve(List<StockLine> lines, ReservationPolicy policy, Function<List<StockLine>, T> saveHold);
}
```

Adding a new rule later (e.g. "partial, but at least 2 items") is a new policy class that overrides `onFinished`;
no stock strategy changes (Open/Closed).

---

## Redis stock gate: how it works

Built 2026-10-04. Full story: [HLD.md 9.8](HLD.md#98-the-redis-stock-gate-phase-2-built) (why, failure cases,
measurements) and [LLD.md section 6](LLD.md#in-front-of-every-strategy-the-redis-stock-gate-stockgate) (the code).

```text
A3 seen this key?  ->  A4 validate (3 reads)  ->  A5b GATE: one Lua script  ->  A6 PostgreSQL (still checks)  ->  A6b give back
                                                  "no": 409, stock row never     only the lines Redis let         what PostgreSQL
                                                  touched                        through                          didn't keep
```

| Decision | Choice |
|---|---|
| What Redis may decide | Only "no". Every "yes" still goes through the PostgreSQL check |
| Keys | `stock:{saleId}:{saleItemId}` (hash tag = one server per sale) + a per-request marker for double-clicks |
| Seeding | The first buyer seeds a missing counter from the `available_stock` it already read (`SET NX`); no warm-up, no stampede |
| Healing | Each counter expires 60 s after seeding; the next buyer re-seeds it. Drift lasts a minute at most |
| The one rule | Every unit taken in Redis is taken in PostgreSQL or given back (after a rollback; after an expiry or cancel *commits*) |
| Redis down | Fail open (PostgreSQL alone; `REDIS_LOCK` still fails closed with 503). 1 s Redis timeout. Readiness stays UP |
| Switch | `flashsale.inventory.stock-gate-enabled`, or `X-Stock-Gate: off` per request (tests, demos, Phase 4 benchmarks) |

Phase 3's reconciliation job (built, see "Stock reconciliation: how it works") therefore mainly reports how much
drift there was (a metric), and repairs a low counter sooner than the expiry would.

---

## Payment safety: how it works

Built 2026-10-04. Full story: [HLD.md 12](HLD.md#12-payments) (the four endings, the live runs) and
[LLD.md section 7](LLD.md#7-strategy-factory-and-template-method-2-payment-gateways) (the code).

| Ending of the provider call | Money taken? | Payment | Hold | HTTP |
|---|---|---|---|---|
| Approved | yes | `SUCCEEDED` | `CONFIRMED` + order | 200 |
| Declined | no | `FAILED` | `HELD` | 200 |
| Provider down (503, or circuit open) | no (known) | deleted (attempt undone) | `HELD` | 503 + `Retry-After` |
| No answer within 2 s | maybe | `PENDING` | `PAYMENT_PENDING` | 202 + `Location` |

| Piece | Choice |
|---|---|
| Timeout | 2 s per call, on the call itself (a real adapter: its HTTP client's timeouts) |
| Circuit breaker | Resilience4j, one per provider, a Decorator around the gateway: 50% of the last 10 calls failed (min 5) opens it; 10 s later 2 trial calls |
| Recovery job | Every 30 s: `PENDING` and untouched for 1 minute = stuck; ask the provider by gateway key; then the same step 3 |
| Two finishers | Step 3 locks the hold and only changes a `PENDING` payment; a late success on a `FAILED` one is flagged `needs_refund` |
| Database | `V2` migration: a partial index for the job's query |
| Scheduler | 2 threads, so recovery and expiry never wait for each other |

Test tokens for trying it: `tok_timeout` (charged, answer lost), `tok_lost` (never arrives), `tok_unavailable`
(provider down).

---

## Per-user limit: how it works

Built 2026-10-05. Full story: [HLD.md 9.9](HLD.md#99-the-per-user-purchase-limit-phase-2-built) (the race, the
experiments) and [LLD.md](LLD.md) sections 8 to 10 (transactions, lock order, replays).

| Piece | Choice |
|---|---|
| The rule | `sales.per_user_limit` (optional): a buyer may hold or buy at most N units in the sale, all items together. `HELD`, `PAYMENT_PENDING`, `CONFIRMED` count; expired and cancelled holds don't |
| The count | One `user_sale_units` row per (sale, buyer), changed in the same transactions as the holds |
| The atomic check | `INSERT ... ON CONFLICT DO UPDATE ... WHERE units + new <= max_units` in the hold's transaction: 1 row = counted, 0 = 409 `PURCHASE_LIMIT_EXCEEDED` and ROLLBACK |
| The early check | Before Redis and the stock rows: units held + the fewest units the request can end with > limit? 409 at once |
| Backstop | `max_units` copied into each row; `CHECK (units BETWEEN 0 AND max_units)` |
| Lock order | hold row, then stock rows, then buyer rows (sorted by sale, user) |
| Database | `V3` migration: the nullable `per_user_limit` column (instant to add) and the table |

Try it: create a sale with `"perUserLimit": 2`, then hold three times as the same user (README).

---

## Rate limiter: how it works

Built 2026-10-09. Full story: [HLD.md 9.10](HLD.md#910-the-rate-limiter-phase-2-built) (why, the trade-offs, the live runs) and
[LLD.md section 2](LLD.md#before-the-controller-the-rate-limit-middleware-ratelimit) (the code).

```text
DispatcherServlet picks the controller  ->  R1 whose bucket? (X-User-Id)  ->  R2 one Lua script on rl:<userId>  ->  R3 429, or headers + on to A1
                                            GET, /api/admin, no user:         refill from the time that passed,       X-RateLimit-Limit / -Remaining
                                            not counted                       then spend one token                    on every counted response
```

| Piece | Choice |
|---|---|
| Algorithm | Token bucket: capacity 10 (the burst), refill 5 a second. No fixed window, so no double burst at a window's edge |
| Key | Per buyer (`X-User-Id`), not per IP: many buyers can share one IP. Per-IP limits belong at the edge (nginx, built in Phase 3 step 2) |
| Where | A Spring MVC `HandlerInterceptor` on `/api/**` minus `/api/admin/**`, writes only (POST, PUT, PATCH, DELETE). Not a servlet filter: its errors would skip `GlobalExceptionHandler` |
| Atomic | One Lua script reads, refills, spends and writes the bucket, and sets its expiry. A read then a write from Java let 20 of 20 requests through |
| Clock | Redis's `TIME`, so every app instance measures the refill the same way |
| Answer | 429 `RATE_LIMITED` + `Retry-After` (seconds to the next token, rounded up) in the usual Problem Details shape |
| Redis down | Fail open: requests go through uncounted (no `X-RateLimit-*` headers), like the stock gate |
| Settings | `flashsale.rate-limit.enabled`, `capacity`, `refill-per-second`; a refill of 0 stops the app at startup |
| Error codes | The same step added a `code` to Spring's own errors (`MISSING_HEADER`, `UNREADABLE_BODY`, `INVALID_VALUE`, `METHOD_NOT_ALLOWED`, `NOT_FOUND`...) and 500 `INTERNAL_ERROR` for bugs |

Measured live: 20 requests from one buyer at the same moment gave ten 201s and ten 429s. Sent one after another with
curl, 11 got through, because the bucket refilled a little between requests (as designed).

## Sale-page cache: how it works

Built 2026-10-09. Full story: [HLD.md 9.11](HLD.md) (why, what can be stale) and
[LLD.md section 2](LLD.md#the-read-side-the-sale-page-cache-salecache) (the code).

```text
F1 SaleController.getSale  ->  F2 GET cache:sale:<id>  ->  hit: F4
                                  miss: F3 one request per sale loads it (SaleService.getSale),
                                        SET ... PX 1000; the others wait for its answer
F4 view.at(now): "active" worked out again  ->  JSON
```

| Piece | Choice |
|---|---|
| Pattern | Cache-aside: look in Redis, on a miss read PostgreSQL and store the answer. `SaleCache` sits between `SaleController` and `SaleService` |
| Key and value | `cache:sale:<saleId>`, the `SaleView` as JSON (Jackson's `JsonMapper`) |
| Freshness | A 1 s TTL and no invalidation. Stock changes on every purchase, so deleting the copy on each hold would empty the cache when the crowd is biggest |
| Stampede | A single flight per app instance: `ConcurrentHashMap.putIfAbsent` + `CompletableFuture`. One request loads, the rest `join()` its answer (or its 404). Across instances: design only |
| "Active" | Never cached: `view.at(now)` works it out on every read, so the page turns live at the exact start time |
| Buying | Never reads the cache. The hold step checks the gate and PostgreSQL, so a stale page can't sell a gone unit |
| Redis down | Fail open: the page comes from PostgreSQL. Redis is tried once per page view, so a hanging Redis costs one timeout, not three |
| Settings | `flashsale.sale-cache.enabled`, `ttl` (1 s); `ttl=0s` stops the app at startup |
| Not cached | A missing sale (404). The per-IP limit at nginx (built in Phase 3 step 2) covers someone asking for random ids |
| Metric | `flashsale.sale.cache{outcome=hit\|miss\|shared\|redis_down}` |

Measured live: a miss took 34 ms, a hit 8 ms. 2,000 page views from 50 parallel clients took 3.8 s and made 4
PostgreSQL reads. With Redis stopped, every page view still answered 200 in 5 to 16 ms. In the tests, removing the
single flight turned 50 parallel misses into 50 PostgreSQL reads.

---

## Outbox + Kafka: how it works

Built 2026-10-10. Full story: [HLD.md 9.12](HLD.md) (why an outbox, at-least-once delivery)
and [LLD.md section 2](LLD.md#after-the-purchase-the-outbox-and-the-notification-worker-outbox-notification) (the code).

```text
O1 PaymentService.recordOutcome: order + INSERT outbox_events (PENDING), ONE commit
O2 OutboxRelay (every 500 ms): claim due rows, FOR UPDATE SKIP LOCKED
O3 send each to Kafka (flashsale.orders, key = user id), wait for acks: SENT, or back off 1 s, 2 s, 4 s ... 1 min
N1 NotificationListener: route on the eventType header, parse the JSON
N2 seen this event id before? skip   N3 send (fake)   N4 INSERT notifications ... ON CONFLICT DO NOTHING
N5 on an exception: 3 retries 1 s apart, then flashsale.orders.DLT (bad JSON: straight there)
```

| Piece | Choice |
|---|---|
| Why an outbox | The database and Kafka cannot share a transaction. The event is a row in the order's transaction, so it is never lost and never sent for an order that rolled back. Kafka down never slows or fails a payment |
| Table | `outbox_events` (V4): random `event_id`, topic, key, JSON payload, `PENDING`/`SENT`, `attempts`, `next_attempt_at`, `last_error`. A partial index on the `PENDING` rows |
| Relay | `@Scheduled` every 500 ms, 100 rows per transaction, `SKIP LOCKED` so two instances never send the same row at once. Sends the batch, then waits for Kafka's answers under one 5 s deadline |
| Kafka down | Rows stay `PENDING`, and the wait doubles from 1 s up to 1 min. No `FAILED` state: every event goes out once Kafka is back |
| Producer | `acks=all`, idempotence on, `max.block.ms` 2 s and `delivery.timeout.ms` 3 s (the defaults would hold the relay up to 60 s) |
| Delivery | At least once: a crash after Kafka confirmed but before the commit sends again. The consumer drops the copy |
| Worker | `@KafkaListener`, group `notifications`, 3 partitions keyed by user id. Idempotent by `event_id` (the `notifications` table) |
| Failures | `DefaultErrorHandler`: 3 retries 1 s apart, then the dead-letter topic with the reason in headers. Unreadable JSON skips the retries |
| Fake provider | `FakeNotificationSender` logs the message. A user id that starts with `bounce` always fails (for trying the DLT) |
| Metrics | `flashsale.outbox.sent`, `flashsale.outbox.send_failed`, gauge `flashsale.outbox.pending`, `flashsale.notifications{outcome=sent\|duplicate\|dead_lettered\|unreadable}` |
| Settings | `flashsale.outbox.*` (relay interval, batch size, send timeout, back-off), `flashsale.notifications.*` (retries), `spring.kafka.*` |

Measured live (docker compose, a laptop): with Kafka up, the event was in Kafka 193 ms after the order and the
customer was notified. With Kafka stopped, a purchase still answered 200 in 0.13 s, and the event waited as `PENDING`
(4 tries). After `docker compose start kafka`, it went out about 2 s later. A purchase by `bounce-carol` was retried
3 times, then parked on `flashsale.orders.DLT`. In the tests, removing `SKIP LOCKED` made four relays send 700
messages for 250 events.

---

## Two instances behind nginx: how it works

Built 2026-10-10. Full story: [HLD.md 9.13](HLD.md) (the system view) and [LLD.md section 2](LLD.md) (what each
instance keeps in memory).

```text
browser / curl -> nginx :8080 (per-IP limit, round robin)  -> app1 :8080  \
                                                           -> app2 :8080   -> PostgreSQL, Redis, Kafka (shared)
```

| Piece | Choice |
|---|---|
| App image | `Dockerfile`, two stages: the JDK and Maven build the jar; the final image has only a Java runtime and the jar, and runs as a normal user. Java versions pinned (`21.0.8_9`) |
| Compose | Profile `cluster`: `app1`, `app2` (settings as environment variables, for example `SPRING_DATASOURCE_URL`), `nginx`. A plain `docker compose up` still starts only PostgreSQL, Redis and Kafka |
| Kafka | Two listeners: `kafka:19092` for the app containers, `localhost:9092` for the app on your Mac |
| Load balancing | Round robin, with `zone` so all of nginx's worker processes share one turn counter |
| Failover | `proxy_next_upstream error timeout`: on a connection error, try the other instance. A POST is retried only if it never reached the first instance |
| Per-IP limit | `limit_req`: 20 a second per IP, burst 40, then 429 `IP_RATE_LIMITED` in the app's error shape, with `Retry-After: 1` |
| Per-buyer limit | Unchanged: one bucket per buyer in Redis, so both instances share it |
| Startup | Flyway takes a database lock, so two instances that start together never run a migration twice |
| Test | `TwoInstancesTest`: two real apps in one JVM and a real nginx container, 9 tests |

Measured live (`docker compose --profile cluster`, a laptop):

| Check | Result |
|---|---|
| Image build | 57 s; image 596 MB |
| Startup | app1 ran the 4 migrations; app2 found the schema up to date |
| Kafka partitions | app1 got 0 and 1; app2 got 2 |
| 10 page views (new connection each) | Before the `zone` fix: all 10 on one instance. After: 5 and 5 |
| 100 page views at once from one IP | 47 x 200, 53 x 429 `IP_RATE_LIMITED` |
| app1 stopped | 10 of 10 views answered 200 (the first after a 2 s connect timeout, then retried on app2); bob held and paid |
| app1 started again | Both instances served again |

---

## Stock reconciliation: how it works

Built 2026-10-10 (Phase 3 step 3). Full story: [HLD.md 9.14](HLD.md) (why, and what it can't do) and
[LLD.md section 6](LLD.md) (the code and the tests).

The Redis stock gate keeps a copy of each item's stock. The copy can drift from PostgreSQL (the source of truth):
a crash between the Redis step and the commit, or a give-back lost while Redis was unreachable. A low copy is the
harmful direction: buyers hear "sold out" while units are left.

```text
every 10 s (StockReconciliationScheduler, FLOW S1)
  S2  read the items of live sales from PostgreSQL, then their counters from Redis (one MGET per sale)
  S3  equal                     -> nothing
      no counter                -> nothing (the next buyer seeds it)
      Redis HIGH                -> report only; harmless, the 60 s expiry heals it
      Redis LOW, first time     -> report, remember the pair
      Redis LOW, same pair again -> S4: compare-and-set the counter to PostgreSQL's number
```

| Piece | Choice |
|---|---|
| Interval | 10 s (`flashsale.reconciliation.interval`), so a real difference is fixed within about 20 s, well before the counter's 60 s expiry |
| Seen twice | Most differences are a purchase or a cancel in flight (milliseconds). The same pair on two runs is very likely real |
| Direction | Only raise a LOW counter. A mistaken raise leaves a counter a little high (harmless); a mistaken lower could turn buyers away |
| The fix | A Lua compare-and-set: set the new value only if Redis still holds the value the job read, keeping the expiry (`KEEPTTL`) |
| Two instances | Both run the job; the compare-and-set makes the second fix a no-op |
| Redis down | The run is skipped and the remembered pairs are forgotten |
| Metrics | Gauge `flashsale.stock.drift` (units off in the last run, 0 when healthy); counter `flashsale.stock.reconcile{outcome}` |
| Test | `StockReconcilerTest`, 8 tests, plus a check in `RedisOutageTest` |

The first version also lowered high counters. A test that ran the job non-stop while 60 buyers held and 30 cancelled
ended with Redis at 140 and PostgreSQL at 139: a mistaken fix is real. So only the safe direction is repaired now.

Measured live (`docker compose`, the app on a laptop):

| Check | Result |
|---|---|
| Hold of 3 units, then a cancel while Redis was paused | The give-back was lost: Redis 7, PostgreSQL 10 |
| First run after Redis came back (+11 s) | Reported "is 7 but PostgreSQL has 10" |
| Next run (+21 s) | Fixed: Redis 10. Without the job, the counter's own expiry would take up to 60 s |

---

## Waiting room: how it works

Built 2026-10-10 (Phase 3 step 4). Full story: [HLD.md 9.15](HLD.md) (why, the numbers, the failure cases) and
[LLD.md section 2](LLD.md) (the code and the design choices).

When a sale opens, far more buyers click "Buy" in the first second than the checkout can serve. The waiting room
turns that spike into a steady flow: buyers wait in line, and a fixed number a second goes on to the hold step.

```text
POST /api/sales/5/queue   (W1)  JOIN script: a new buyer gets the next turn number; joining again keeps the place
GET  /api/sales/5/queue   (W1)  STATUS script: WAITING + position + estimated wait, ADMITTED, or NOT_QUEUED
every 250 ms              (W2)  for each live sale with admitPerSecond: ADMIT script -> the next buyers get a pass
POST /api/holds           (W3)  no pass -> 403 NOT_ADMITTED, before the per-buyer limit, the gate and the locks
```

| Piece | Choice |
|---|---|
| Which sales | Only those created with `admitPerSecond` (a nullable column, V5). Other sales work as before |
| The queue | A Redis sorted set per sale. The score is a turn number from one `INCR` counter, so the order is exact across instances. One place per buyer: joining again is a no-op |
| The line can form early | Joining is allowed before the start; admission starts at the start time. Joining an ended sale is 409 `SALE_NOT_ACTIVE`, an unknown sale 404 |
| The rate | One Lua script per run takes credit (up to one second's worth, on Redis's clock), pops that many buyers and writes their passes. All instances share the credit, so two admitters still let in `admitPerSecond` a second |
| The admitter | Every 250 ms on every instance (`WaitingRoomScheduler`), so buyers go in four small groups a second |
| The pass | A Redis key that expires after 5 minutes (`flashsale.waiting-room.pass-ttl`). No table and no clean-up job |
| No pass | 403 `NOT_ADMITTED`, with a message that says where to join |
| Redis down | Fails open: joining answers `ADMITTED` and holds go through. PostgreSQL still decides every sale; only the load protection is lost |
| Metrics | `flashsale.waiting_room{outcome}`: `joined`, `admitted`, `refused`, `redis_down` |
| Tests | `WaitingRoomTest` (9) and `WaitingRoomFailureTest` (1) |

The tests were checked against broken code. Without the pass check in `HoldService`, 4 tests fail. With the script's
credit never used up, four admitters at once let in 20 buyers instead of 5.

Measured live (`docker compose`, the app on a laptop, a sale with `admitPerSecond: 20`):

| Check | Result |
|---|---|
| A hold before joining | 403 `NOT_ADMITTED` |
| 100 buyers join at once | The last one is told: position 80, about 4 s to wait (the first 20 were already in) |
| The line empties | 80 left at 0.6 s, 50 at 2.0 s, 19 at 3.4 s, none at 4.9 s: about 20 a second |
| An admitted buyer holds | 201; the pass had about 5 minutes left |

---

## Monitoring: how it works

Built 2026-10-10 (Phase 4 step 1). Run it with `docker compose --profile cluster --profile monitoring up -d --build`,
then open Grafana at http://localhost:3000 and Prometheus at http://localhost:9090/targets.

```text
app1, app2  --GET /actuator/prometheus every 5 s-->  Prometheus  --PromQL-->  Grafana "Flash sale engine"
   (StockAudit recounts the stock at each scrape)       (:9090)                    (:3000, 24 panels)
```

| Piece | Choice |
|---|---|
| Metrics out of the app | `micrometer-registry-prometheus`: every Micrometer metric at `/actuator/prometheus`, tagged with `application` (app1, app2, or the app on your Mac) |
| Latency | `http.server.requests` recorded in buckets, so p99 can be worked out across both instances (an average of two p99s is not the p99) |
| Correctness gauges | `StockAudit`: `flashsale.stock.oversold` (units held or sold beyond the stock) and `flashsale.stock.mismatch` (items whose `available_stock` disagrees with their holds), recounted from the hold rows by one SQL query at each scrape |
| Prometheus | `monitoring/prometheus.yml`: scrapes `app1:8080`, `app2:8080` and `host.docker.internal:8080` (the app on your Mac) every 5 s. The one that is not running shows DOWN |
| Grafana | Provisioned from files: the data source and the dashboard JSON load at startup. Anonymous admin, for local use only |
| Dashboard rows | Is it correct? (oversold, mismatch, gate drift, outbox backlog); traffic (req/s by status and by instance, p99 and p50 per endpoint); bottlenecks (DB pool, gate, sale cache, rate limiter, reconciliation); after the purchase (Kafka lag, outbox, notifications); waiting room and payments (circuit breakers, recovery) |
| nginx | Answers 404 for `/actuator/`: the actuator is for operators, not the public entry point |
| Tests | `MonitoringTest` (3). Spring Boot switches exporters off in tests, so the test settings turn Prometheus's back on |

Measured live (`docker compose`, both profiles, a laptop):

| Check | Result |
|---|---|
| Targets | app1 and app2 UP; `host.docker.internal:8080` DOWN with 404 in cluster mode |
| Every dashboard query | Returned data, except three with nothing to count yet (failed outbox sends, the waiting room, payment recovery) |
| Paced traffic, a 50-unit sale | Sold out: 39 orders, 39 notifications, the rest in holds; "oversold" stayed 0 |
| Requests per instance | 5.1 and 5.1 a second: nginx's round robin is even |
| p99 latency | `POST /api/holds` 32 ms, `POST /api/holds/{id}/pay` 154 ms (the fake gateway's 100 ms), `GET /api/sales/{id}` 19 ms |
| The NAIVE demo (gate off, 20 buyers, 1 unit) | All 20 got the unit; the dashboard showed oversold 19, mismatch 1 |

Two findings from the live run:

- **One laptop is one IP address.** A burst of 150 buyers from one machine got 121 x 429 from nginx's per-IP limit.
  That is the limit working. To see normal traffic on the dashboard, send requests at a steady pace (under 20 a
  second). The k6 load tests (step 2) must take this into account: either test the instances directly, or raise the
  limit for the test run.
- **Each instance was counted twice at first.** In cluster mode, port 8080 is nginx, so Prometheus's
  `host.docker.internal:8080` target scraped app1 or app2 through nginx, and "requests per instance" doubled. Making
  nginx refuse `/actuator/` fixed it (and keeps the actuator off the public entry point).

---

## Load tests: how it works

Built 2026-10-10 (Phase 4 step 2). Run `loadtest/run.sh` (the whole table, about 5 minutes) or
`loadtest/run.sh quick` (one short run). It starts the cluster if needed and writes `loadtest/RESULTS.md`. The full
analysis is in [HLD.md](HLD.md) section 15.

```text
k6 (container on the Docker network)  --round robin-->  app1, app2  (1 CPU each)
   a new buyer every 1/RATE s:                           PostgreSQL, Redis, Kafka (shared, not limited)
   view the sale page -> hold 1 unit -> pay if held
after each run: run.sh recounts the stock in PostgreSQL ("oversold", must be 0)
```

| Piece | Choice |
|---|---|
| Load model | k6 `constant-arrival-rate`: new buyers start at a fixed rate whether or not earlier ones have finished, like a real crowd |
| Where k6 runs | In a container on the compose network, calling `app1` and `app2` directly, round robin. Through nginx, k6 would be one IP address and nginx's per-IP limit (20 a second) would refuse almost everything |
| Each run | Its own new sale (`setup()`), so runs never share stock. Buyer ids include the sale id |
| Recorded | Achieved rate, held, paid, sold out (409), busy (503), errors, hold p50/p95/p99, dropped buyers, and "oversold" from a PostgreSQL recount |
| CPU | Each app container gets 1 CPU (`docker-compose.yml`), so two instances means twice the app CPU, as on two machines |
| The matrix | 1 instance vs 2 at 450 buyers a second (PESSIMISTIC, gate on); the four safe strategies at 300 a second with the gate off; OPTIMISTIC at 300 a second with the gate on. Every setup 3 times, taking turns |

Four lessons from building it:

| Lesson | What happened |
|---|---|
| Do not rebuild between runs | `docker compose up --build` in the runner recreated the app containers before each run, so the first runs measured freshly started JVMs (p50 about 1 s at 50 buyers a second). Without `--build`, the warm p50 was 5 ms |
| One run is noise | Run once each, CONDITIONAL_UPDATE's p95 went 221, 19 and 5 ms. Now every setup runs 3 times and every run is a row |
| Pick a rate that separates the setups | At 250 buyers a second a warm instance mostly kept up, so one and two instances looked alike. At 450 they differ |
| Unique buyer ids per run | Repeating a run name reused buyers' Idempotency-Keys against a new sale; the app rightly answered 422. Buyer ids now include the sale id |

Results (laptop, Docker Desktop):

| Finding | Numbers |
|---|---|
| Correct under load | 0 oversold and 0 errors in every run; median hold 1 to 3 ms in every setup |
| Two instances vs one, 450 buyers a second | Two had the lower hold p95 in 5 of 6 pairs, roughly half (e.g. 241 vs 520 ms, 315 vs 691 ms). Not more, because the shared services and k6 use the same laptop CPUs |
| OPTIMISTIC | The only strategy that answered 503 STOCK_BUSY: 6 of its 12 runs that day (up to 397 buyers in one run, p95 up to 1.3 s). A retry storm; the Redis gate does not prevent it, since the winners still update one row together |
| PESSIMISTIC, CONDITIONAL_UPDATE, REDIS_LOCK | Never busy; their tails varied run to run (p95 4 to 167 ms) and were within noise of each other |

---

## Failure tests: how it works

Built 2026-10-10 (Phase 4 step 3). Run `loadtest/chaos.sh` (all three, about 5 minutes) or
`loadtest/chaos.sh redis|app|kafka`. It writes `loadtest/CHAOS.md`.

```text
warm up (20 s, not recorded)
k6: 100 new buyers a second for 60 s, THROUGH nginx (view, hold, pay; a 502/504 is retried once, same key)
  t = 15 s   break one part:  docker compose kill redis | kill app1 | stop kafka
  t = 35 s   bring it back:   docker compose start ...
every 1 s:   outbox backlog (PENDING events) and the app containers' CPU
afterwards:  wait for the outbox to drain; count this sale's orders and notifications; recount oversold
```

| Choice | Why |
|---|---|
| Through nginx, not straight to the instances | Only nginx can move requests away from a dead instance; the load tests skip it, these must not |
| nginx's per-IP limit raised for the run (exported `NGINX_IP_RATE`, put back at the end) | k6 is one IP address; at 20 a second it would measure nginx's 429s |
| One retry with the same Idempotency-Key on 502/504 | What a real client should do; it shows that a retry after a crash gets its answer and never buys twice |
| A 10-second timeline of hold latency and CPU | One summary per run hides WHEN it was slow; the timeline caught a wrong conclusion (below) |
| A warm-up before each scenario | A scenario that restarts an instance leaves it cold for the next one |

Results (`loadtest/CHAOS.md`):

| What broke | Buyers | Failed | Hold p50 / p95 | Outbox peak | Notified | Oversold |
|---|---|---|---|---|---|---|
| Redis killed | 5,760 | 0 | 310 / 2,111 ms | 52 | all, once | 0 |
| app1 killed | 5,821 | 5 payments | 2 / 4,108 ms | 57 | all, once | 0 |
| Kafka stopped | 6,001 | 0 | 3 / 23 ms | 2,930 | all, once | 0 |

- **Redis:** nobody failed; the Redis helpers failed open and PostgreSQL sold alone. Each hold waited about 2 s (the
  rate limiter's and the gate's 1 s timeouts), from the kill until about 15 s after Redis was back (the client
  reconnecting).
- **app1:** nginx failed over at once (holds about 2 ms). 6 cut-off requests were retried with the same key and
  succeeded. 5 payments were PENDING on app1; the recovery job marked them FAILED (the in-memory fake provider died
  with app1, so it had no record) and put the holds back to HELD. The p95 comes from the end of the run, when the
  restarted app1 took traffic while still cold.
- **Kafka:** no effect on buyers; the outbox held up to 2,930 events and drained after the restart; every order was
  notified exactly once.

Two lessons from building it: (1) the raised nginx limit was first set on one `docker compose` call only, and a later
call recreated nginx at 20 a second, so two scenarios measured 429s (5,987 of 6,001 "failed"); (2) the script first
warmed up once, so the Kafka scenario measured app1's cold restart and seemed to show Kafka slowing purchases. The
timeline showed the truth; Kafka alone showed holds of 2 to 3 ms.

---

## Web app: how it works

Built 2026-10-11 (Phase 5). `docker compose --profile cluster up -d --build`, then http://localhost:8080.

```text
browser ──> nginx :8080 ──┬── /, /sale/3, /demo  -> index.html (React Router picks the page in the browser)
                          ├── /assets/*          -> the built JS and CSS (hashed names, cached for a year)
                          └── /api/*             -> per-IP limit -> app1 / app2 (round robin)
```

| Choice | Why |
|---|---|
| React, TypeScript, Vite, React Router, and no UI library | The pages are small. Plain CSS keeps the bundle at about 90 KB gzipped, and every class is readable |
| Served by nginx, not by Spring Boot | Page reloads never reach the apps. Page and API share one origin, so no CORS |
| Polling (1 s) instead of WebSockets | Stateless and cheap: the sale page comes from the 1 s Redis copy |
| The buyer is a name in `localStorage`, sent as `X-User-Id` | The backend has no login yet. Two tabs can be two buyers |
| A new Idempotency-Key per click, and one retry with the same key on 502/504 | A retry after a crash can never reserve or pay twice |
| The crowd runs inside the app, on virtual threads | One button, no extra tool. Its counters are in Redis, so both instances share them |

Measured live (two instances behind nginx): 500 fake buyers over 5 s for 100 units. 95 sold, 16 cancelled (their
units went back on sale), 389 told "sold out", 0 oversold, and all 95 orders notified through Kafka.

The click-through found three bugs, all fixed:

- A sold-out sale still showed an active "Buy now".
- The shop's cards were not aligned, because a margin rule for stacked cards also applied inside the grid.
- The bottom strip wrapped onto two lines on a phone.

