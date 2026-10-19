# Failure test results

Written by `loadtest/chaos.sh` on 2026-10-10 17:21, on one laptop. Each row: k6 sends 100 new buyers
a second for 60 s through nginx (view, hold, pay; a 502/504 is retried once with the same Idempotency-Key). At
15 s one part is broken, at 35 s it comes back. "failed" = buyers whose hold or payment did not succeed after
the retry. "backlog peak" = most outbox events waiting for Kafka; "drained in" = seconds after the run until
none were left. "notified" counts this sale's orders with a notification. HLD.md 14 explains each row.

| scenario | buyers | held | paid | failed | gateway errors | server errors | 429s | retried ok | hold p50 ms | hold p95 ms | hold p99 ms | hold max ms | backlog peak | drained in | orders | notified | oversold |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| redis | 5760 | 5760 | 5760 | 0 | 0 | 0 | 0 | 0 | 310 | 2111 | 2231 | 2647 | 52 | 0 s | 5760 | 5760 | 0 |
| app | 5821 | 5821 | 5816 | 5 | 0 | 0 | 0 | 6 | 2 | 4108 | 5565 | 7570 | 57 | 0 s | 5816 | 5816 | 0 |
| kafka | 6001 | 6001 | 6001 | 0 | 0 | 0 | 0 | 0 | 3 | 23 | 57 | 231 | 2930 | 0 s | 6001 | 6001 | 0 |

- redis: duplicate notifications over all sales: 0
- app: duplicate notifications over all sales: 0
- kafka: duplicate notifications over all sales: 0
