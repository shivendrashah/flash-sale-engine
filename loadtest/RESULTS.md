# Load test results

Written by `loadtest/run.sh` on 2026-10-10 16:45, on one laptop (Docker Desktop). Each row is one k6 run:
a new buyer starts at a constant rate, views the sale page, holds 1 unit, and pays if the hold worked.
Hold times are for the hold request only. Each app instance has 1 CPU; PostgreSQL, Redis and Kafka are shared.
"busy" = 503 STOCK_BUSY (asked to retry). "dropped" = buyers k6 could not start on time. "oversold" is
recounted in PostgreSQL after each run and must be 0. HLD.md 15 explains what the numbers mean.

| run | instances | strategy | gate | asked | achieved | held | paid | sold out | busy (503) | errors | hold p50 ms | hold p95 ms | hold p99 ms | dropped | oversold |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1-instance-#1 | 1 | PESSIMISTIC | on | 450/s | 448/s | 1000 | 1000 | 8001 | 0 | 0 | 1 | 520 | 726 | 0 | 0 |
| 2-instances-#1 | 2 | PESSIMISTIC | on | 450/s | 447/s | 1000 | 1000 | 8001 | 0 | 0 | 1 | 241 | 441 | 0 | 0 |
| 1-instance-#2 | 1 | PESSIMISTIC | on | 450/s | 448/s | 1000 | 1000 | 8001 | 0 | 0 | 1 | 691 | 902 | 0 | 0 |
| 2-instances-#2 | 2 | PESSIMISTIC | on | 450/s | 447/s | 1000 | 1000 | 8001 | 0 | 0 | 2 | 315 | 597 | 0 | 0 |
| 1-instance-#3 | 1 | PESSIMISTIC | on | 450/s | 448/s | 1000 | 1000 | 8001 | 0 | 0 | 1 | 615 | 893 | 0 | 0 |
| 2-instances-#3 | 2 | PESSIMISTIC | on | 450/s | 448/s | 1000 | 1000 | 8001 | 0 | 0 | 2 | 695 | 1129 | 0 | 0 |
| gate-off-PESSIMISTIC-#1 | 2 | PESSIMISTIC | off | 300/s | 297/s | 500 | 500 | 2501 | 0 | 0 | 2 | 55 | 189 | 0 | 0 |
| gate-off-OPTIMISTIC-#1 | 2 | OPTIMISTIC | off | 300/s | 297/s | 500 | 500 | 2501 | 0 | 0 | 2 | 14 | 74 | 0 | 0 |
| gate-off-CONDITIONAL_UPDATE-#1 | 2 | CONDITIONAL_UPDATE | off | 300/s | 296/s | 500 | 500 | 2501 | 0 | 0 | 2 | 119 | 320 | 0 | 0 |
| gate-off-REDIS_LOCK-#1 | 2 | REDIS_LOCK | off | 300/s | 298/s | 500 | 500 | 2501 | 0 | 0 | 2 | 33 | 240 | 0 | 0 |
| gate-off-PESSIMISTIC-#2 | 2 | PESSIMISTIC | off | 300/s | 298/s | 500 | 500 | 2501 | 0 | 0 | 2 | 7 | 24 | 0 | 0 |
| gate-off-OPTIMISTIC-#2 | 2 | OPTIMISTIC | off | 300/s | 297/s | 500 | 500 | 2104 | 397 | 0 | 3 | 1306 | 1644 | 0 | 0 |
| gate-off-CONDITIONAL_UPDATE-#2 | 2 | CONDITIONAL_UPDATE | off | 300/s | 295/s | 500 | 500 | 2501 | 0 | 0 | 2 | 6 | 13 | 0 | 0 |
| gate-off-REDIS_LOCK-#2 | 2 | REDIS_LOCK | off | 300/s | 297/s | 500 | 500 | 2501 | 0 | 0 | 2 | 6 | 19 | 0 | 0 |
| gate-off-PESSIMISTIC-#3 | 2 | PESSIMISTIC | off | 300/s | 297/s | 500 | 500 | 2501 | 0 | 0 | 2 | 4 | 7 | 0 | 0 |
| gate-off-OPTIMISTIC-#3 | 2 | OPTIMISTIC | off | 300/s | 298/s | 500 | 500 | 2501 | 0 | 0 | 2 | 8 | 88 | 0 | 0 |
| gate-off-CONDITIONAL_UPDATE-#3 | 2 | CONDITIONAL_UPDATE | off | 300/s | 298/s | 500 | 500 | 2500 | 0 | 0 | 2 | 8 | 102 | 0 | 0 |
| gate-off-REDIS_LOCK-#3 | 2 | REDIS_LOCK | off | 300/s | 296/s | 500 | 500 | 2501 | 0 | 0 | 2 | 167 | 410 | 0 | 0 |
| gate-on-OPTIMISTIC-#1 | 2 | OPTIMISTIC | on | 300/s | 297/s | 500 | 500 | 2493 | 8 | 0 | 2 | 25 | 216 | 0 | 0 |
| gate-on-OPTIMISTIC-#2 | 2 | OPTIMISTIC | on | 300/s | 297/s | 500 | 500 | 2501 | 0 | 0 | 2 | 8 | 51 | 0 | 0 |
| gate-on-OPTIMISTIC-#3 | 2 | OPTIMISTIC | on | 300/s | 295/s | 500 | 500 | 2129 | 372 | 0 | 2 | 789 | 1030 | 0 | 0 |
