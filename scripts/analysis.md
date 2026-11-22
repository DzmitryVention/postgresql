## Analysis

This section is hand-written. It refers to the run on 2026-11-17 shown above. If you run the benchmark again, the
tables change, but this text does not.

### Summary

| Question | Answer |
|---|---|
| Same CPU budget (3 CPUs): which stack writes more? | Modern: **24,508 vs 17,291 writes/s (+42 %)**. |
| Same CPU budget (3 CPUs): which stack serves more mixed load? | About equal: **39,794 vs 37,819 req/s (+5 %)**. The 1-CPU API service is the limit in both stacks. |
| Can PostgreSQL on 1 CPU replace PostgreSQL + Redis + Kafka on 3 CPUs? | For up to approximately **80 %** of the load: 14,056 writes/s (81 %) and 28,764 mixed req/s (76 %). |
| Which stack has lower latency at the same load (5,000 writes/s)? | Modern: write p50 **2.0 vs 4.5 ms**, write p99 **6.3–8.3 vs 13.5 ms**. |
| Which stack delivers events faster? | Modern: event p50 **3.5–3.8 vs 9.7 ms**, event p99 **12.7–14.5 vs 26.1 ms**. |
| Which stack uses less memory? | Modern: **463–599 MiB vs 1,886 MiB** of infrastructure RAM at maximum writes. |
| Which stack costs less on AWS for the same load? | Modern: **19–57 % less**, depending on the load (see the cost estimate above). |
| Is `LISTEN/NOTIFY` usable for the event wake-up? | No, not for a write path: maximum **1,528 writes/s**, 9× less than polling. |

### Why the previous result was almost the same

The previous run showed 23,001 vs 24,023 writes/s. That run had three problems:

| Problem in the previous run | Effect | Fix in this run |
|---|---|---|
| No network latency between services | The 3 extra network calls of the classic write path cost almost nothing. | `netproxy` adds a 1 ms round trip on each hop. |
| Classic infrastructure had 5 CPUs, modern had 2 | The budgets were not equal. | 1 dedicated core per service; modern runs with 1 and with 3 cores. |
| API connection pool of 32 | With network latency, the pool limits throughput: 32 connections / 3 ms ≈ 10,700 writes/s for both stacks. | Pool of 96 connections. |
| CFS CPU quota (`cpus: 1`) | Throttling added 50–70 ms to the p99 latency. | Dedicated cores (`cpuset`), like an EC2 vCPU. |
| One run per scenario | No information about spread. | 3 runs per scenario; median and range. |

Two earlier attempts of this run were discarded. In one, the Mac went into system sleep and froze the Docker VM for
more than 25 minutes. In the other, one run took 454 s instead of 45 s, probably for the same reason. The runner now
uses `caffeinate` and repeats any run that takes much longer than planned. The run in this report needed no repeat.

### Write path

| Stack | Network calls per order (API) | Max writes/s | Infra CPU used at max | Writes/s per infra CPU |
|---|---:|---:|---:|---:|
| Classic | 3 (PostgreSQL, Kafka, Redis) | 17,291 | 139 % of 300 % | 12,441 |
| Modern 1 CPU | 1 (one statement) | 14,056 | 92 % of 100 % | 15,305 |
| Modern 3 CPU | 1 (one statement) | 24,508 | 183 % of 300 % | 13,379 |

- In the classic stack, no infrastructure component reaches 100 % CPU (PostgreSQL 84 %, Kafka 43 %, Redis 13 %).
  The limit is the latency of 3 sequential network calls. Each order waits for PostgreSQL, then for the Kafka
  acknowledgement, then for Redis.
- PostgreSQL on 1 CPU is CPU-bound at 92 %. With 3 CPUs it reaches 24,508 writes/s. The range of the runs is wider
  (22,031–27,071), because the WAL flush on commit is the next limit.
- The modern stack does more work per write in PostgreSQL (order + outbox row, then delete), but it does no work in
  Kafka and Redis. Per infrastructure CPU it writes 8–23 % more orders.

### Event latency

- At 5,000 writes/s, the modern pipeline delivers events in 3.5–3.8 ms (p50). The classic pipeline needs 9.7 ms.
  The classic event passes through 2 more network hops (API → Kafka → worker) and the Kafka fetch cycle.
- At maximum load on 1 CPU, the modern worker competes with the API for the same PostgreSQL core.
  In the `mixed` scenario, its event p50 increases to 21.2 ms (classic: 10.8 ms). With 3 CPUs, it is 5.3 ms.
- No stack built a lasting backlog. All backlogs drained in 1 second or less after each run.

### Read path and the role of Redis

- In the `mixed` scenario, the API container is the limit in all stacks: it uses 69–80 % of its single core.
  For this reason, the mixed throughput is almost the same for classic (37,819) and modern 3 CPU (39,794).
- Read latency is almost the same: p50 2.88 ms (Redis) vs 3.02 ms (PostgreSQL primary key lookup).
- Redis is cheaper per read. In `mixed`, classic needs 34 core-µs of infrastructure CPU per request
  (PostgreSQL 13.2 + Redis 7.8 + Kafka 12.9). Modern PostgreSQL needs 41 core-µs per request.
- The test reads 5,000 hot orders. All of them fit in memory. With a large working set that does not fit in
  PostgreSQL shared buffers, a Redis cache gives a bigger advantage.

### LISTEN/NOTIFY

| Metric (1 CPU) | Polling (`modern-1cpu`) | `LISTEN/NOTIFY` (`modern-notify`) |
|---|---:|---:|
| Maximum writes/s | 14,056 | 1,528 (9.2× less) |
| Mixed req/s | 28,764 | 6,307 (4.6× less) |
| Served writes/s with 5,000/s offered | 5,000 | 1,556 (466,101 requests dropped by k6) |
| Write p50 latency with 5,000/s offered | 2.05 ms | 645 ms |

Each transaction that sends `NOTIFY` takes a database-wide lock at commit. All writers then commit one at a time.
PostgreSQL uses only 59–69 % CPU, but it cannot accept more writes. Use polling with `FOR UPDATE SKIP LOCKED`.

### AWS cost conclusions

- For small and medium loads, the classic stack pays for minimum production footprints that the load does not need:
  3 MSK brokers ($447/month) and 2 ElastiCache nodes ($185/month). The modern stack saves **$660–755/month (56–57 %)**.
- At 25,000 req/s, the modern stack needs a bigger RDS instance (4 vCPU). It still saves **41 %**.
- At 100,000 req/s (20,000 orders/s), PostgreSQL must carry the full load on one 16-vCPU primary. The savings drop to
  **19 %**. This is also close to the write limit of one primary. Above this level, the classic stack scales out
  more easily (Kafka partitions, Redis shards).
- Cross-AZ traffic is a large cost at high load for both stacks. The model assumes 2.7× more bytes per order for the
  classic stack (2,400 vs 900 bytes), because each order crosses the network to PostgreSQL, Kafka, Redis and the worker.
  This value is an estimate, not a measurement.
- **Lambda instead of Fargate:** at a sustained load of thousands of requests per second, AWS Lambda is expensive.
  At 1,250 req/s average (Small profile), the request fee alone is approximately $650/month
  ($0.20 per million requests), compared with $58/month for 2 Fargate tasks. Lambda also needs RDS Proxy for
  PostgreSQL connection pooling. The Lambda cost is the same for both architectures, so it does not change the ranking.

### Recommendations

1. Use PostgreSQL 18 only for event-driven services with up to approximately 10,000–20,000 orders/s on one primary.
2. Write the event in the same statement or transaction as the state change (transactional outbox).
3. Consume with `FOR UPDATE SKIP LOCKED` in batches. Do not send `NOTIFY` for each write.
4. Give PostgreSQL at least 2 vCPUs. On 1 vCPU, the event worker competes with the API for CPU.
5. Size the connection pools for the network latency: `pool size ≥ target req/s × time per request`.
6. Add Redis when the working set does not fit in PostgreSQL memory, or when the read cost per request matters.
7. Add Kafka when many consumers need the events, when you need replay, or above the write limit of one primary.

### Limits of this test

- All containers run on one host. A benchmark core is an Apple silicon core. The cost model assumes
  1 core = 1.5 Graviton vCPU. If this factor is wrong, all instance sizes change by the same ratio.
- `netproxy` adds 1.14–1.18 ms per round trip at idle. A separate test under load measured 1.28 ms (mixed load) and
  1.60 ms (maximum writes). The extra queueing affects the classic stack more, because it makes more network calls.
- Kafka runs as 1 broker with replication factor 1, and PostgreSQL runs without a standby. In production, MSK with
  replication factor 3 and RDS Multi-AZ add commit latency to both stacks.
- Kafka does not `fsync` each message. PostgreSQL does `fsync` the WAL on each commit. This favors the classic stack.
- In `write_rate`, k6 dropped approximately 0.3 % of the planned requests in every stack (1,901–2,467 of 675,000).
  This happens while k6 starts new virtual users. It has the same effect on all stacks.
- The prices are on-demand list prices in us-east-1. Reserved instances or savings plans reduce all lines by 30–50 %.

### Price sources

| Price | Value used | Source |
|---|---|---|
| RDS PostgreSQL db.m7g/m8g.large | $0.168/h single-AZ, ×2 Multi-AZ | [cloudprice.net](https://cloudprice.net/aws/rds), [codewithsql.com](https://codewithsql.com/cloud-databases/aws/rds/pricing) |
| MSK kafka.m7g.large, storage | $0.204/h per broker, $0.10/GB-month | [cloudburn.io](https://cloudburn.io/tools/amazon-msk-pricing-calculator), [wring.co](https://wring.co/blog/aws-msk-pricing-guide) |
| ElastiCache for Valkey cache.m7g.large | $0.1264/h | [cloud-instances.info](https://cloud-instances.info/cache/), [selfhost.dev](https://selfhost.dev/blog/aws-redis-pricing/) |
| Fargate ARM | $0.03238/vCPU-h, $0.00356/GB-h | [cloudzero.com](https://www.cloudzero.com/blog/fargate-cost/), [fortem.dev](https://fortem.dev/blog/aws-fargate-pricing-real-costs/) |
| RDS gp3 storage, cross-AZ transfer, Lambda | $0.115/GB-month (×2 Multi-AZ), $0.01/GB each direction, $0.20 per million requests | AWS standard list prices; not checked again for this report |

Check the prices on the official AWS pricing pages before you make a budget decision.

### Reproduce

1. Run `scripts/run-benchmark.sh` from the repository root (approximately 45 minutes).
2. Read the raw data in `results/`. The script overwrites `RESULT.md` and keeps this section.
