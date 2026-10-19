// =====================================================================================================
// cluster and runs this script for every row of the results table.
//
// What one run does:
//   setup()     creates a fresh sale with STOCK units (so runs never share stock)
//   default()   ONE buyer: view the sale page, try to hold 1 unit, pay if the hold worked
//               k6 starts RATE new buyers every second for DURATION ("constant arrival rate"), whether or not
//               earlier buyers have finished. That is how real crowds behave: they don't wait for the server.
//   teardown()  logs the app's own StockAudit gauges and the stock left (in k6's output)
//   handleSummary()  writes results/<RUN>.json (everything) and results/<RUN>.row (one row of RESULTS.md, without
//               its last column: run.sh adds "oversold", recounted in PostgreSQL after the run)
//
// Settings (environment variables, run.sh sets them):
//   RUN        a name for this row, e.g. "2-instances-PESSIMISTIC-gate-on"
//   TARGETS    comma-separated app instances, e.g. "http://app1:8080,http://app2:8080". Requests go straight to the
//              instances, round robin, NOT through nginx: k6 is one IP address, and nginx's per-IP limit (20 a
//              second) would turn almost everything away. Real crowds come from many addresses.
//   STRATEGY   X-Concurrency-Strategy: PESSIMISTIC, OPTIMISTIC, CONDITIONAL_UPDATE or REDIS_LOCK
//   GATE       on / off: the X-Stock-Gate header (Redis's pre-check in front of PostgreSQL)
//   RATE, DURATION, STOCK
//   RETRY      on / off (default off): a buyer whose request ends in a gateway error (502/504: nginx lost the instance
//              mid-request) tries ONCE more with the SAME Idempotency-Key, as a real client should. loadtest/chaos.sh
//              turns it on: it shows whether a retry after a crash buys twice (it must not) or gets its answer.
//   AUDIT      an instance to read the StockAudit gauges from in teardown (default: the first target). nginx hides
//              /actuator/, so chaos.sh points this at an instance.
// =====================================================================================================
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const TARGETS = (__ENV.TARGETS || 'http://app1:8080').split(',');
const STRATEGY = __ENV.STRATEGY || 'PESSIMISTIC';
const GATE = __ENV.GATE || 'on';
const RATE = Number(__ENV.RATE || 200);
const DURATION = __ENV.DURATION || '20s';
const STOCK = Number(__ENV.STOCK || 1000);
const RUN = __ENV.RUN || 'manual';
const RETRY = (__ENV.RETRY || 'off') === 'on';
const AUDIT = __ENV.AUDIT || TARGETS[0];

const held = new Counter('buyers_held');          // 201: got a unit
const soldOut = new Counter('buyers_sold_out');   // 409 SOLD_OUT: a correct "no"
const busy = new Counter('buyers_busy');          // 503 STOCK_BUSY: "try again" (lock wait or retries ran out)
const failed = new Counter('buyers_failed');      // anything else: a bug, or the system falling over
const gatewayErrors = new Counter('gateway_errors');   // 502/504 from nginx: an instance died mid-request
const serverErrors = new Counter('server_errors');     // 500 (a bug) or 503 other than STOCK_BUSY
const retriedOk = new Counter('retried_ok');           // a gateway error, then the retry with the same key worked
const limited = new Counter('rate_limited');           // 429: nginx's per-IP limit or the app's per-buyer limit
const payFailed = new Counter('payments_failed');
const paid = new Counter('orders_paid');
const holdTime = new Trend('hold_duration', true);

export const options = {
  scenarios: {
    flash_sale: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.min(RATE * 2, 2000),   // virtual users k6 keeps ready to start buyers on time
      maxVUs: 4000,
    },
  },
  thresholds: {
    buyers_failed: ['count==0'],
    'hold_duration': ['p(99)<2000'],
  },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

function target() {
  // round robin over the instances, like nginx would do
  return TARGETS[exec.scenario.iterationInTest % TARGETS.length];
}

export function setup() {
  const now = new Date();
  const sale = http.post(`${TARGETS[0]}/api/admin/sales`, JSON.stringify({
    name: `load test ${RUN}`,
    startsAt: new Date(now.getTime() - 60_000).toISOString(),
    endsAt: new Date(now.getTime() + 3_600_000).toISOString(),
    items: [{ productName: 'iPhone 17', regularPrice: 79999, salePrice: 59999, stock: STOCK }],
  }), { headers: { 'Content-Type': 'application/json' } });
  if (sale.status !== 201) {
    throw new Error(`could not create the sale: ${sale.status} ${sale.body}`);
  }
  const body = sale.json();
  return { saleId: body.id, saleItemId: body.items[0].saleItemId };
}

export default function (data) {
  const base = target();
  // a new buyer every iteration, unique across runs too: a repeated run name with the same buyer ids would reuse
  // their Idempotency-Keys, and the app rightly refuses a key reused for a different request (422)
  const buyer = `${RUN}-sale${data.saleId}-${exec.scenario.iterationInTest}`;
  const headers = {
    'Content-Type': 'application/json',
    'X-User-Id': buyer,
    'X-Concurrency-Strategy': STRATEGY,
    'X-Stock-Gate': GATE,
  };

  http.get(`${base}/api/sales/${data.saleId}`, { tags: { step: 'view' } });

  const holdBody = JSON.stringify({ items: [{ saleItemId: data.saleItemId, quantity: 1 }] });
  const holdParams = { headers: { ...headers, 'Idempotency-Key': `hold-${buyer}` }, tags: { step: 'hold' },
    responseCallback: http.expectedStatuses(201, 200, 409, 503) };
  let hold = sendWithRetry(() => http.post(`${base}/api/holds`, holdBody, holdParams));
  holdTime.add(hold.timings.duration);

  // 201 = a new hold; 200 = the retry got the SAME hold back (the Idempotency-Key replay)
  if (hold.status === 201 || hold.status === 200) {
    held.add(1);
    const payBody = JSON.stringify({ provider: 'STRIPE', paymentToken: 'tok_success' });
    const payParams = { headers: { ...headers, 'Idempotency-Key': `pay-${buyer}` }, tags: { step: 'pay' } };
    const pay = sendWithRetry(() => http.post(`${base}/api/holds/${hold.json('holdId')}/pay`, payBody, payParams));
    if (check(pay, { 'paid': (r) => r.status === 200 })) {
      paid.add(1);
    } else {
      payFailed.add(1);
      classify(pay);
    }
  } else if (hold.status === 409) {
    soldOut.add(1);
  } else if (hold.status === 503 && isStockBusy(hold)) {
    busy.add(1);
  } else {
    failed.add(1);
    classify(hold);
  }
}

/** Sends; on a gateway error, and if RETRY is on, sends the same request (same Idempotency-Key) once more. */
function sendWithRetry(send) {
  const first = send();
  if (!RETRY || (first.status !== 502 && first.status !== 504)) {
    return first;
  }
  const second = send();
  if (second.status < 500) {
    retriedOk.add(1);
  }
  return second;
}

function isStockBusy(r) {
  try {
    return r.json('code') === 'STOCK_BUSY';
  } catch (e) {
    return false;
  }
}

function classify(r) {
  if (r.status === 502 || r.status === 504 || r.status === 0) {
    gatewayErrors.add(1);   // 0 = no answer at all (connection reset)
  } else if (r.status >= 500) {
    serverErrors.add(1);
  } else if (r.status === 429) {
    limited.add(1);   // counted apart, so a limit set too low for a test shows up as itself
  }
}

export function teardown(data) {
  const scrape = http.get(`${AUDIT}/actuator/prometheus`).body || '';
  const gauge = (name) => {
    const line = scrape.split('\n').find((l) => l.startsWith(name + '{'));
    return line ? Number(line.split(' ').pop()) : NaN;
  };
  const sale = http.get(`${TARGETS[0]}/api/sales/${data.saleId}`).json();
  console.log(JSON.stringify({
    run: RUN,
    oversold: gauge('flashsale_stock_oversold'),
    mismatch: gauge('flashsale_stock_mismatch'),
    leftInStock: sale.items[0].availableStock,
  }));
}

function n(data, metric) {
  const m = data.metrics[metric];
  return m ? m.values.count : 0;
}

function ms(data, metric, stat) {
  const m = data.metrics[metric];
  return m ? Math.round(m.values[stat]) : NaN;
}

export function handleSummary(data) {
  const durationS = data.state.testRunDurationMs / 1000;
  const iterations = n(data, 'iterations');
  const row = `| ${RUN} | ${TARGETS.length} | ${STRATEGY} | ${GATE} | ${RATE}/s | ${Math.round(iterations / durationS)}/s `
    + `| ${n(data, 'buyers_held')} | ${n(data, 'orders_paid')} | ${n(data, 'buyers_sold_out')} `
    + `| ${n(data, 'buyers_busy')} | ${n(data, 'buyers_failed')} `
    + `| ${ms(data, 'hold_duration', 'med')} | ${ms(data, 'hold_duration', 'p(95)')} `
    + `| ${ms(data, 'hold_duration', 'p(99)')} | ${n(data, 'dropped_iterations')} `;   // run.sh adds the last column
  // The failure tests' row (loadtest/chaos.sh): what buyers saw while something was broken
  const chaos = `| ${n(data, 'iterations')} | ${n(data, 'buyers_held')} | ${n(data, 'orders_paid')} `
    + `| ${n(data, 'buyers_failed') + n(data, 'payments_failed')} | ${n(data, 'gateway_errors')} `
    + `| ${n(data, 'server_errors')} | ${n(data, 'rate_limited')} | ${n(data, 'retried_ok')} `
    + `| ${ms(data, 'hold_duration', 'med')} `
    + `| ${ms(data, 'hold_duration', 'p(95)')} | ${ms(data, 'hold_duration', 'p(99)')} `
    + `| ${ms(data, 'hold_duration', 'max')} `;
  return {
    stdout: row,
    [`/results/${RUN}.json`]: JSON.stringify(data, null, 2),
    [`/results/${RUN}.row`]: row,
    [`/results/${RUN}.chaos`]: chaos,
  };
}
