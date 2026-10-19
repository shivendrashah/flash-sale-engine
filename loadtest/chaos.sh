#!/usr/bin/env bash
# =====================================================================================================
# (Phase 4;, HLD.md 14). Writes loadtest/CHAOS.md.
#
#   loadtest/chaos.sh                 all three scenarios (about 5 minutes)
#   loadtest/chaos.sh redis|app|kafka one of them
#
# Each scenario: k6 sends 100 new buyers a second for 60 s THROUGH NGINX (view, hold, pay; on a 502/504 a buyer
# retries once with the same Idempotency-Key). At 15 s one part is broken; at 35 s it comes back. Afterwards the
# script waits for the outbox to drain and checks the data: no oversell, every paid order notified exactly once.
#   redis   docker compose kill redis       (SIGKILL: no clean shutdown), then start
#   app     docker compose kill app1        (one of the two instances dies mid-request), then start
#   kafka   docker compose stop kafka       then start
# nginx's per-IP limit is raised for these runs only (k6 is one IP address) and put back at the end.
# =====================================================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p loadtest/results
NETWORK=flash-sale-engine_default
K6_IMAGE=grafana/k6:1.1.0
SCENARIOS=("${@:-redis app kafka}")
read -r -a SCENARIOS <<< "${SCENARIOS[*]}"

psql() { docker compose exec -T postgres psql -U flashsale -d flashsale -tAc "$1" | tr -d '[:space:]'; }

echo "Starting the cluster with nginx's per-IP limit raised for the load..."
# Exported, so EVERY docker compose call below keeps it. (The first version set it on one call only; the "bring
# everything back" call after the first scenario then recreated nginx at 20 a second, and the next two scenarios
# measured nginx's 429s instead of the failure: 5,987 of 6,001 buyers "failed".)
export NGINX_IP_RATE=100000r/s NGINX_IP_BURST=100000
docker compose --profile cluster up -d --wait > /dev/null
restore_nginx() {
  echo "Putting nginx's per-IP limit back (20r/s, burst 40)..."
  unset NGINX_IP_RATE NGINX_IP_BURST
  docker compose --profile cluster up -d --wait nginx > /dev/null
}
trap restore_nginx EXIT

# Warm up both JVMs before EVERY scenario (not recorded). A scenario that restarts an instance leaves it cold, and the
# next scenario would measure its JIT warm-up instead of its own failure: the first version warmed up once, and the
# Kafka scenario (run right after "app" had restarted app1) showed holds of 1.6 s that had nothing to do with Kafka.
warm_up() {
  docker run --rm --network "$NETWORK" -v "$PWD/loadtest:/scripts:ro" -v "$PWD/loadtest/results:/results" \
    -e RUN=chaos-warm-up -e TARGETS=http://nginx:80 -e AUDIT=http://app2:8080 -e RATE=100 -e DURATION=20s \
    -e STOCK=5000 "$K6_IMAGE" run --quiet /scripts/sale.js > /dev/null 2>&1 || true
}

# Hold latency per 10-second window (from k6's CSV: one line per measurement), and the app containers' CPU
timeline() {
  local name=$1 csv="loadtest/results/chaos-$1.csv"
  local start; start=$(awk -F, '$1 == "hold_duration" {print $2; exit}' "$csv")
  echo "   hold latency by 10 s window (p50 / p95 ms, holds started), and app CPU (avg % of its 1 CPU):"
  for w in 0 10 20 30 40 50; do
    local values; values=$(awk -F, -v s="$start" -v w="$w" \
      '$1 == "hold_duration" && $2 - s >= w && $2 - s < w + 10 {print $3}' "$csv" | sort -n)
    local count; count=$(printf '%s\n' "$values" | grep -c . || true)
    local p50; p50=$(printf '%s\n' "$values" | awk -v n="$count" 'NR == int(n * 0.5) + 1 {printf "%d", $1}')
    local p95; p95=$(printf '%s\n' "$values" | awk -v n="$count" 'NR == int(n * 0.95) + 1 {printf "%d", $1}')
    local cpu; cpu=$(awk -v w="$w" '$1 > w && $1 <= w + 10 {gsub("%", "", $3); sum += $3; n++}
                                   END {if (n) printf "%d", sum / n; else printf "-"}' "loadtest/results/chaos-$name.cpu")
    echo "     ${w}-$((w + 10)) s: ${p50:--} / ${p95:--} ms (${count} holds), app CPU ${cpu}%"
  done
}

scenario() {
  local name=$1 break_cmd=$2 fix_cmd=$3
  warm_up
  echo "== $name: k6 for 60 s; at 15 s: $break_cmd; at 35 s: $fix_cmd"
  docker run --rm --network "$NETWORK" -v "$PWD/loadtest:/scripts:ro" -v "$PWD/loadtest/results:/results" \
    -e RUN="chaos-$name" -e TARGETS=http://nginx:80 -e AUDIT=http://app2:8080 -e RETRY=on \
    -e RATE=100 -e DURATION=60s -e STOCK=20000 \
    "$K6_IMAGE" run --quiet --out "csv=/results/chaos-$name.csv" /scripts/sale.js \
    > "loadtest/results/chaos-$name.log" 2>&1 &
  local k6=$!
  # watch the outbox backlog (events not yet confirmed by Kafka) once a second
  local peak=0
  for t in $(seq 1 60); do
    [[ $t == 15 ]] && eval "$break_cmd" > /dev/null 2>&1
    [[ $t == 35 ]] && eval "$fix_cmd" > /dev/null 2>&1
    local pending; pending=$(psql "SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'" 2>/dev/null || echo 0)
    (( pending > peak )) && peak=$pending
    # CPU of the two app containers (each is limited to 1 CPU = 100%)
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' 2>/dev/null | grep -E 'app[12]' \
      | awk -v t="$t" '{print t, $1, $2}' >> "loadtest/results/chaos-$name.cpu" &
    sleep 1
  done
  wait "$k6" || true
  docker compose --profile cluster up -d --wait > /dev/null 2>&1 || true   # make sure everything is back
  # after the run: let the outbox drain, then check the data of THIS sale
  local drain=0
  until [[ $(psql "SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'") == 0 ]] || (( drain >= 180 )); do
    sleep 1; drain=$((drain + 1))
  done
  sleep 3   # let the notification worker finish the last messages
  local sale; sale=$(psql "SELECT MAX(id) FROM sales")
  local orders; orders=$(psql "SELECT COUNT(*) FROM orders o JOIN holds h ON h.id = o.hold_id WHERE h.sale_id = $sale")
  local notified; notified=$(psql "SELECT COUNT(*) FROM notifications n JOIN orders o ON o.id = n.order_id
                                   JOIN holds h ON h.id = o.hold_id WHERE h.sale_id = $sale")
  local dupes; dupes=$(psql "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM notifications")
  local oversold; oversold=$(psql "
    SELECT COALESCE(SUM(GREATEST(taken - total_stock, 0)), 0) FROM (
      SELECT si.total_stock, COALESCE(SUM(hi.quantity) FILTER (
               WHERE h.status IN ('HELD', 'PAYMENT_PENDING', 'CONFIRMED')), 0) AS taken
        FROM sale_items si LEFT JOIN hold_items hi ON hi.sale_item_id = si.id LEFT JOIN holds h ON h.id = hi.hold_id
       GROUP BY si.id) per_item")
  printf '| %s %s| %s | %s s | %s | %s | %s |\n' "$name" "$(cat "loadtest/results/chaos-$name.chaos")" \
    "$peak" "$drain" "$orders" "$notified" "$oversold" > "loadtest/results/chaos-$name.out"
  timeline "$name" >> "loadtest/results/chaos-$name.out"
  echo "   duplicate notifications over all sales: $dupes" >> "loadtest/results/chaos-$name.out"
  cat "loadtest/results/chaos-$name.out"
}

for s in "${SCENARIOS[@]}"; do
  case "$s" in
    redis) scenario redis "docker compose kill redis" "docker compose start redis" ;;
    app)   scenario app   "docker compose kill app1"  "docker compose start app1" ;;
    kafka) scenario kafka "docker compose stop kafka" "docker compose start kafka" ;;
    *) echo "unknown scenario $s"; exit 1 ;;
  esac
done

{
  echo "# Failure test results"
  echo
  echo "Written by \`loadtest/chaos.sh\` on $(date '+%Y-%m-%d %H:%M'), on one laptop. Each row: k6 sends 100 new buyers"
  echo "a second for 60 s through nginx (view, hold, pay; a 502/504 is retried once with the same Idempotency-Key). At"
  echo "15 s one part is broken, at 35 s it comes back. \"failed\" = buyers whose hold or payment did not succeed after"
  echo "the retry. \"backlog peak\" = most outbox events waiting for Kafka; \"drained in\" = seconds after the run until"
  echo "none were left. \"notified\" counts this sale's orders with a notification. HLD.md 14 explains each row."
  echo
  echo "| scenario | buyers | held | paid | failed | gateway errors | server errors | 429s | retried ok | hold p50 ms | hold p95 ms | hold p99 ms | hold max ms | backlog peak | drained in | orders | notified | oversold |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
  for s in redis app kafka; do
    [[ -f "loadtest/results/chaos-$s.out" ]] && head -1 "loadtest/results/chaos-$s.out"
  done
  echo
  for s in redis app kafka; do
    [[ -f "loadtest/results/chaos-$s.out" ]] && echo "- $s: $(tail -1 "loadtest/results/chaos-$s.out" | sed 's/^ *//')"
  done
} > loadtest/CHAOS.md
echo "Wrote loadtest/CHAOS.md"
