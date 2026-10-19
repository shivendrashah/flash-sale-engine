#!/usr/bin/env bash
# =====================================================================================================
#
#   loadtest/run.sh            the whole table (about 5 minutes)
#   loadtest/run.sh quick      one short run, to check that everything works
#
# Needs Docker. It starts the cluster (PostgreSQL, Redis, Kafka, app1, app2, nginx) if it isn't running, runs k6 in a
# container on the same Docker network, and leaves the cluster running (stop it with
# "docker compose --profile cluster down -v").
# Each app instance is limited to 1 CPU (docker-compose.yml), so "2 instances" means twice the CPU, as on two
# machines. PostgreSQL, Redis and Kafka are shared and not limited.
# =====================================================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p loadtest/results
NETWORK=flash-sale-engine_default
K6_IMAGE=grafana/k6:1.1.0

echo "Starting the cluster if needed (the first time it builds the app image)..."
# No --build here: it would recreate app1 and app2 whenever anything changed, and every run would then measure
# freshly started JVMs that are still compiling (the first version of this script did exactly that: p50 ~1 s at 50
# buyers a second). After changing the code, rebuild once with: docker compose --profile cluster up -d --build
docker compose --profile cluster up -d --wait > /dev/null

# PostgreSQL recount after a run: units held or sold beyond the stock, over ALL sales (StockAudit's query)
oversold() {
  docker compose exec -T postgres psql -U flashsale -d flashsale -tAc "
    SELECT COALESCE(SUM(GREATEST(taken - total_stock, 0)), 0) FROM (
      SELECT si.total_stock, COALESCE(SUM(hi.quantity) FILTER (
               WHERE h.status IN ('HELD', 'PAYMENT_PENDING', 'CONFIRMED')), 0) AS taken
        FROM sale_items si LEFT JOIN hold_items hi ON hi.sale_item_id = si.id LEFT JOIN holds h ON h.id = hi.hold_id
       GROUP BY si.id) per_item" | tr -d '[:space:]'
}

# run <name> <targets> <strategy> <gate> <rate> <duration> <stock>
run() {
  echo "== $1 ($5/s for $6, $7 units)"
  docker run --rm --network "$NETWORK" \
    -v "$PWD/loadtest:/scripts:ro" -v "$PWD/loadtest/results:/results" \
    -e RUN="$1" -e TARGETS="$2" -e STRATEGY="$3" -e GATE="$4" -e RATE="$5" -e DURATION="$6" -e STOCK="$7" \
    "$K6_IMAGE" run --quiet /scripts/sale.js > "loadtest/results/$1.log" 2>&1 || true   # thresholds may fail
  printf '| %s |\n' "$(oversold)" >> "loadtest/results/$1.row"
  cat "loadtest/results/$1.row"
}

ONE=http://app1:8080
TWO=http://app1:8080,http://app2:8080

if [[ "${1:-}" == "quick" ]]; then
  run warm-up "$TWO" PESSIMISTIC on 50 10s 200 > /dev/null
  run quick "$TWO" PESSIMISTIC on 50 5s 100
  exit 0
fi

rm -f loadtest/results/*.row
run warm-up "$TWO" PESSIMISTIC on 100 15s 500 > /dev/null   # let the JIT compiler warm up; not in the table

# A laptop is noisy (garbage collection, Kafka, the database's own housekeeping): one run per setup can mislead. The
# first version of this table ran each strategy once, and CONDITIONAL_UPDATE's p95 then went 221, 19 and 5 ms over
# three runs. So every setup runs 3 times, taking turns, and every run is a row.
SECTIONS=()
for i in 1 2 3; do
  # 1. One instance vs two: the same crowd, the default setup (PESSIMISTIC, Redis gate on). 450 buyers a second:
  #    at 250 a warm 1-CPU instance mostly kept up, so one vs two looked the same; at 450 one instance falls behind
  #    (probed 3 times: hold p95 908/482/399 ms vs 476/236/5 ms with two).
  run "1-instance-#$i"  "$ONE" PESSIMISTIC on 450 20s 1000
  run "2-instances-#$i" "$TWO" PESSIMISTIC on 450 20s 1000
  SECTIONS+=("1-instance-#$i" "2-instances-#$i")
done
for i in 1 2 3; do
  # 2. The strategies with the gate OFF, under contention: 300 buyers a second for 500 units, every one reaching
  #    PostgreSQL, so the locking itself is what is measured.
  for s in PESSIMISTIC OPTIMISTIC CONDITIONAL_UPDATE REDIS_LOCK; do
    run "gate-off-$s-#$i" "$TWO" "$s" off 300 10s 500
    SECTIONS+=("gate-off-$s-#$i")
  done
done
for i in 1 2 3; do
  # 3. The same crowd for OPTIMISTIC with the gate ON: does Redis's pre-check take the contention away?
  run "gate-on-OPTIMISTIC-#$i" "$TWO" OPTIMISTIC on 300 10s 500
  SECTIONS+=("gate-on-OPTIMISTIC-#$i")
done

{
  echo "# Load test results"
  echo
  echo "Written by \`loadtest/run.sh\` on $(date '+%Y-%m-%d %H:%M'), on one laptop (Docker Desktop). Each row is one k6 run:"
  echo "a new buyer starts at a constant rate, views the sale page, holds 1 unit, and pays if the hold worked."
  echo "Hold times are for the hold request only. Each app instance has 1 CPU; PostgreSQL, Redis and Kafka are shared."
  echo "\"busy\" = 503 STOCK_BUSY (asked to retry). \"dropped\" = buyers k6 could not start on time. \"oversold\" is"
  echo "recounted in PostgreSQL after each run and must be 0. HLD.md 15 explains what the numbers mean."
  echo
  echo "| run | instances | strategy | gate | asked | achieved | held | paid | sold out | busy (503) | errors | hold p50 ms | hold p95 ms | hold p99 ms | dropped | oversold |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
  for r in "${SECTIONS[@]}"; do
    cat "loadtest/results/$r.row"
  done
} > loadtest/RESULTS.md
echo "Wrote loadtest/RESULTS.md"
