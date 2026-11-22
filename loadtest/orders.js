// k6 load test for the order service. The same script runs against both stacks.
//
// Environment variables:
//   SCENARIO  warmup | write_rate | write_max | mixed
//   STACK     label used in the result file name (classic, modern, ...)
//   BASE_URL  default http://api:8080 (the API container on the compose network)
//   RUN       repeat number, used in the result file name
//   DURATION  default 60s
//   RATE      requests/s for write_rate (default 5000)
//   VUS       virtual users for write_max and mixed (default 192)
//   READ_PCT  percent of reads in mixed (default 80)
import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://api:8080';
const RUN = __ENV.RUN || '1';
const SCENARIO = __ENV.SCENARIO || 'write_max';
const STACK = __ENV.STACK || 'unknown';
const DURATION = __ENV.DURATION || '60s';
const RATE = parseInt(__ENV.RATE || '5000', 10);
const VUS = parseInt(__ENV.VUS || '192', 10);
const READ_PCT = parseInt(__ENV.READ_PCT || '80', 10);
const CUSTOMERS = 10000;
const SEED_ORDERS = 5000;

const createLatency = new Trend('order_create_ms', true);
const readLatency = new Trend('order_read_ms', true);
const failures = new Counter('order_failures');
const creates = new Counter('order_creates');
const reads = new Counter('order_reads');

const scenarios = {
  warmup: { executor: 'constant-vus', vus: 16, duration: '15s', exec: 'write' },
  // Open model: fixed arrival rate, measures latency at the same load on both stacks.
  write_rate: {
    executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION,
    preAllocatedVUs: 200, maxVUs: 1000, exec: 'write',
  },
  // Closed model: as fast as possible, measures maximum write throughput.
  write_max: { executor: 'constant-vus', vus: VUS, duration: DURATION, exec: 'write' },
  // Closed model: READ_PCT reads / rest writes, measures mixed throughput.
  mixed: { executor: 'constant-vus', vus: VUS, duration: DURATION, exec: 'mixed' },
};

export const options = {
  scenarios: { [SCENARIO]: scenarios[SCENARIO] },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  setupTimeout: '120s',
};

const headers = { 'Content-Type': 'application/json' };
const rnd = (n) => Math.floor(Math.random() * n);

function body() {
  return JSON.stringify({ customer_id: 1 + rnd(CUSTOMERS), amount_cents: 100 + rnd(100000) });
}

export function setup() {
  if (SCENARIO !== 'mixed') return { ids: [] };
  // Seed orders so the read path has known ids.
  const ids = [];
  const chunk = 100;
  for (let i = 0; i < SEED_ORDERS; i += chunk) {
    const reqs = [];
    for (let j = 0; j < chunk; j++) reqs.push(['POST', `${BASE}/orders`, body(), { headers }]);
    for (const r of http.batch(reqs)) {
      if (r.status === 201) ids.push(r.json('id'));
    }
  }
  return { ids };
}

function createOrder() {
  const res = http.post(`${BASE}/orders`, body(), { headers, tags: { name: 'create' } });
  createLatency.add(res.timings.duration);
  creates.add(1);
  if (!check(res, { 'create 201': (r) => r.status === 201 })) failures.add(1);
}

function readOrder(ids) {
  const id = ids[rnd(ids.length)];
  const res = http.get(`${BASE}/orders/${id}`, { tags: { name: 'read' } });
  readLatency.add(res.timings.duration);
  reads.add(1);
  if (!check(res, { 'read 200': (r) => r.status === 200 })) failures.add(1);
}

export function write() {
  createOrder();
}

export function mixed(data) {
  if (rnd(100) < READ_PCT) readOrder(data.ids);
  else createOrder();
}

export function handleSummary(data) {
  const reqs = data.metrics.http_reqs ? data.metrics.http_reqs.values : { count: 0, rate: 0 };
  const dur = data.metrics.http_req_duration.values;
  const line = `[${STACK}/${SCENARIO}/r${RUN}] reqs=${reqs.count} rps=${reqs.rate.toFixed(0)} ` +
    `p50=${dur.med.toFixed(2)}ms p95=${dur['p(95)'].toFixed(2)}ms p99=${dur['p(99)'].toFixed(2)}ms\n`;
  return {
    stdout: line,
    [`/results/${STACK}-${SCENARIO}-r${RUN}.json`]: JSON.stringify(data, null, 2),
  };
}
