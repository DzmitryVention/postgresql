#!/usr/bin/env python3
"""Builds RESULT.md from the files in results/. Standard library only.

Run from the repository root:  python3 scripts/report.py
(run-benchmark.sh runs it in a python:3.13-alpine container.)

Each scenario runs REPEATS times. The tables show the median of the runs.
Throughput also shows the range (min-max) of the runs.
"""
import csv
import json
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results"
OUT = ROOT / "RESULT.md"
ANALYSIS = ROOT / "scripts" / "analysis.md"

STACK_LABELS = {
    "classic": "Classic: PG + Redis + Kafka (3 × 1 CPU)",
    "modern-1cpu": "Modern: PG only (1 CPU)",
    "modern-3cpu": "Modern: PG only (3 CPU)",
    "modern-notify": "Modern: PG + LISTEN/NOTIFY (1 CPU)",
}
INFRA = {  # CPU and memory limits of the infrastructure containers
    "classic": "PostgreSQL 1 CPU / 2 GB, Redis 1 CPU / 1 GB, Kafka 1 CPU / 2 GB",
    "modern-1cpu": "PostgreSQL 1 CPU / 2 GB",
    "modern-3cpu": "PostgreSQL 3 CPU / 5 GB",
    "modern-notify": "PostgreSQL 1 CPU / 2 GB, NOTIFY wake-up",
}
INFRA_CPU = {"classic": 3, "modern-1cpu": 1, "modern-3cpu": 3, "modern-notify": 1}
SCENARIOS = {
    "write_rate": "Fixed write rate (open model)",
    "write_max": "Maximum write throughput (closed model)",
    "mixed": "Mixed load: 80 % reads / 20 % writes (closed model)",
}
APP_SERVICES = ("api", "worker")
NET_SERVICES = ("netproxy",)


# ---------- helpers ----------

def load_json(p):
    return json.loads(p.read_text()) if p.exists() else None


def secs(duration):
    m = re.fullmatch(r"(\d+)(ms|s|m)", duration)
    n, unit = int(m.group(1)), m.group(2)
    return n / 1000 if unit == "ms" else n * 60 if unit == "m" else n


def metric(k6, name, field):
    m = k6["metrics"].get(name)
    return m["values"].get(field) if m else None


def fmt(v, digits=1):
    if v is None:
        return "–"
    if isinstance(v, str):
        return v
    if isinstance(v, int) or v >= 100:
        return f"{v:,.0f}"
    return f"{v:,.{digits}f}"


def rng(lo, hi):
    return f"{lo:,.0f}–{hi:,.0f}"


def mem_mib(s):
    used = s.split("/")[0].strip()
    m = re.fullmatch(r"([\d.]+)\s*([KMG]i?B|B|kB)", used)
    if not m:
        return 0.0
    n, u = float(m.group(1)), m.group(2)
    return n * {"B": 1 / 2**20, "KiB": 1 / 1024, "kB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}[u]


def docker_usage(path):
    """Average CPU % and peak memory per service, from docker stats samples."""
    per = {}
    if not path.exists():
        return per
    for row in csv.reader(path.open(), delimiter=";"):
        if len(row) < 4:
            continue
        svc = re.sub(r"^eda-[\w-]+?-(\w+)-\d+$", r"\1", row[1])
        try:
            cpu = float(row[2].rstrip("%") or 0)
        except ValueError:
            continue
        d = per.setdefault(svc, {"cpu": [], "mem": 0.0})
        d["cpu"].append(cpu)
        d["mem"] = max(d["mem"], mem_mib(row[3]))
    return {k: {"cpu": sum(v["cpu"]) / len(v["cpu"]), "mem": v["mem"]} for k, v in per.items() if v["cpu"]}


# ---------- data collection ----------

def collect_run(stack, scenario, run, duration_s):
    k6 = load_json(RES / f"{stack}-{scenario}-r{run}.json")
    app = load_json(RES / f"{stack}-{scenario}-r{run}-app.json")
    if not k6 or not app:
        return None
    creates = metric(k6, "order_creates", "count") or 0
    reads = metric(k6, "order_reads", "count") or 0
    usage = docker_usage(RES / f"{stack}-{scenario}-r{run}-docker.csv")
    infra = {k: v for k, v in usage.items() if k not in APP_SERVICES + NET_SERVICES}
    app_u = {k: v for k, v in usage.items() if k in APP_SERVICES}
    drain = app["drain_seconds"]
    return {
        "rps": (creates + reads) / duration_s,
        "write_rps": creates / duration_s,
        "read_rps": reads / duration_s,
        "create": {f: metric(k6, "order_create_ms", f) or 0 for f in ("med", "p(95)", "p(99)", "max")},
        "read": {f: metric(k6, "order_read_ms", f) or 0 for f in ("med", "p(95)", "p(99)", "max")},
        "failures": metric(k6, "order_failures", "count") or 0,
        "dropped": metric(k6, "dropped_iterations", "count") or 0,
        "e2e": {k: app["worker"]["e2e_latency"][k] for k in ("count", "p50_ms", "p95_ms", "p99_ms", "max_ms")},
        "drain": float(drain) if drain != "timeout" else float("inf"),
        "publish_errors": app["api"].get("publish_errors", 0),
        "usage": usage,
        "infra_cpu": sum(v["cpu"] for v in infra.values()),
        "infra_mem": sum(v["mem"] for v in infra.values()),
        "app_cpu": sum(v["cpu"] for v in app_u.values()),
        "net_cpu": sum(v["cpu"] for k, v in usage.items() if k in NET_SERVICES),
    }


def median_of(runs):
    """Median of every numeric leaf over the runs (dicts are merged recursively)."""
    first = runs[0]
    if isinstance(first, dict):
        keys = set().union(*(r.keys() for r in runs))
        return {k: median_of([r[k] for r in runs if k in r]) for k in keys}
    return statistics.median(runs)


def collect(stack, scenario, repeats, duration_s):
    runs = [r for r in (collect_run(stack, scenario, i, duration_s) for i in range(1, repeats + 1)) if r]
    if not runs:
        return None
    agg = median_of(runs)
    agg["runs"] = len(runs)
    agg["rps_min"] = min(r["rps"] for r in runs)
    agg["rps_max"] = max(r["rps"] for r in runs)
    agg["write_rps_min"] = min(r["write_rps"] for r in runs)
    agg["write_rps_max"] = max(r["write_rps"] for r in runs)
    agg["failures_total"] = sum(r["failures"] for r in runs)
    agg["dropped_total"] = sum(r["dropped"] for r in runs)
    agg["publish_errors_total"] = sum(r["publish_errors"] for r in runs)
    agg["drain_max"] = max(r["drain"] for r in runs)
    return agg


def table(headers, rows, numeric=True):
    """numeric: True = right-align all columns except the first; False = left-align all;
    a list of booleans = per column."""
    if isinstance(numeric, list):
        align = ["---:" if n else "---" for n in numeric]
    else:
        align = ["---" if i == 0 or not numeric else "---:" for i in range(len(headers))]
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(align) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def drain_txt(v):
    return "timeout" if v == float("inf") else f"{v:.0f}"


# ---------- AWS cost model ----------
#
# Prices: us-east-1, on-demand, Linux, 730 hours/month (checked 2026-11-17, see RESULT.md for sources).
HOURS = 730
RDS_VCPU_HOUR = 0.084              # db.m7g/m8g: large (2 vCPU) = $0.168/h single-AZ; price is linear per vCPU
RDS_SIZES = [2, 4, 8, 16, 32, 48, 64]
RDS_GP3_GB_MONTH_MULTI_AZ = 0.23   # gp3 $0.115/GB-month single-AZ, x2 for Multi-AZ
CACHE_NODE_HOUR = {2: 0.1264, 4: 0.2528, 8: 0.5056}   # ElastiCache for Valkey cache.m7g.large/xlarge/2xlarge
MSK_BROKER_HOUR = {2: 0.204, 4: 0.408, 8: 0.816}      # kafka.m7g.large/xlarge/2xlarge
MSK_GB_MONTH = 0.10
FARGATE_VCPU_HOUR = 0.03238        # ARM (Graviton)
FARGATE_GB_HOUR = 0.00356
CROSS_AZ_GB = 0.02                 # $0.01/GB out + $0.01/GB in
# Assumptions
CORE_TO_VCPU = 1.5                 # 1 benchmark core (Apple silicon) ~ 1.5 Graviton vCPU
TARGET_UTIL = 0.6                  # size for 60 % CPU at peak
PEAK_TO_AVG = 2.0                  # average load = peak / 2 (for traffic and storage)
EVENT_BYTES = 300                  # stored size of one event in Kafka
KAFKA_RETENTION_DAYS = 7
DB_STORAGE_GB = 500                # same for both stacks
CROSS_AZ_SHARE = 2 / 3             # 3 AZs: 2 of 3 calls leave the AZ
BYTES_PER_WRITE = {"classic": 2400, "modern": 900}   # network bytes per order, all hops
BYTES_PER_READ = 600
PROFILES = [  # peak load, 80 % reads / 20 % writes
    ("Small", 2_500),
    ("Medium", 10_000),
    ("Large", 25_000),
    ("X-Large", 100_000),
]


def pick(sizes, need, minimum):
    for sz in sizes:
        if sz >= max(need, minimum):
            return sz
    return sizes[-1]


def aws_cost(kind, cpu_per_req, rps):
    """Monthly cost lines for one architecture at a peak load of rps requests/s."""
    vcpu = {k: rps * v * CORE_TO_VCPU / TARGET_UTIL for k, v in cpu_per_req.items()}
    writes = rps * 0.2
    lines = []
    # PostgreSQL: RDS Multi-AZ (primary + standby)
    size = pick(RDS_SIZES, vcpu.get("postgres", 0), 2)
    lines.append((f"RDS PostgreSQL Multi-AZ, {size} vCPU (needs {vcpu.get('postgres', 0):.1f})",
                  size * RDS_VCPU_HOUR * 2 * HOURS))
    lines.append((f"RDS gp3 storage {DB_STORAGE_GB} GB Multi-AZ", DB_STORAGE_GB * RDS_GP3_GB_MONTH_MULTI_AZ))
    if kind == "classic":
        r = vcpu.get("redis", 0)
        rsize = pick(sorted(CACHE_NODE_HOUR), r, 2)
        lines.append((f"ElastiCache Valkey 2 nodes × {rsize} vCPU (needs {r:.1f})", 2 * CACHE_NODE_HOUR[rsize] * HOURS))
        k = vcpu.get("kafka", 0)   # with RF=3 on 3 brokers, every broker writes every message
        ksize = pick(sorted(MSK_BROKER_HOUR), k, 2)
        lines.append((f"MSK 3 brokers × {ksize} vCPU (needs {k:.1f} per broker)", 3 * MSK_BROKER_HOUR[ksize] * HOURS))
        gb = writes / PEAK_TO_AVG * EVENT_BYTES * 86400 * KAFKA_RETENTION_DAYS * 3 / 1e9 * 1.3
        lines.append((f"MSK storage {gb:,.0f} GB (7 days, RF=3, +30 %)", gb * MSK_GB_MONTH))
    for svc in ("api", "worker"):
        need = vcpu.get(svc, 0)
        tasks = max(2, -(-need // 1))            # 1 vCPU / 2 GB tasks, at least 2 for HA
        lines.append((f"Fargate {svc}: {tasks:.0f} × (1 vCPU, 2 GB) (needs {need:.1f})",
                       tasks * (FARGATE_VCPU_HOUR + 2 * FARGATE_GB_HOUR) * HOURS))
    gb_month = (writes * BYTES_PER_WRITE[kind] + rps * 0.8 * BYTES_PER_READ) / PEAK_TO_AVG \
        * 86400 * 30 / 1e9 * CROSS_AZ_SHARE
    lines.append((f"Cross-AZ traffic {gb_month:,.0f} GB", gb_month * CROSS_AZ_GB))
    return lines


def cpu_per_request(d):
    """Core-seconds per request for each service, from a mixed-load run."""
    return {k: v["cpu"] / 100 / d["rps"] for k, v in d["usage"].items() if k not in NET_SERVICES}


def cost_section(data, stacks, w):
    classic = data.get(("classic", "mixed"))
    modern = data.get(("modern-3cpu", "mixed")) or data.get(("modern-1cpu", "mixed"))
    if not classic or not modern:
        return
    cpr = {"classic": cpu_per_request(classic), "modern": cpu_per_request(modern)}
    w("## AWS cost estimate for the same performance\n")
    w("The model sizes both architectures for the **same peak load** (80 % reads / 20 % writes) with production "
      "high availability. It uses the CPU time per request that this benchmark measured in the `mixed` scenario.\n")
    rows = []
    for kind in ("classic", "modern"):
        rows.append([kind] + [f"{cpr[kind].get(s, 0) * 1e6:.1f}" for s in ("postgres", "redis", "kafka", "api", "worker")])
    w("**Measured CPU time per request (core-µs, mixed load)**\n")
    w(table(["Architecture", "PostgreSQL", "Redis", "Kafka", "API", "Worker"], rows))
    w("")
    w(f"Assumptions: 1 benchmark core = {CORE_TO_VCPU} Graviton vCPU; sizing for {TARGET_UTIL:.0%} CPU at peak; "
      f"average load = peak / {PEAK_TO_AVG:.0f}; RDS Multi-AZ, ElastiCache primary + replica, MSK 3 brokers with "
      f"replication factor 3, at least 2 Fargate tasks per service; {DB_STORAGE_GB} GB database storage for both; "
      f"Kafka retention {KAFKA_RETENTION_DAYS} days. Prices: us-east-1, on-demand, {HOURS} hours/month.\n")
    summary = []
    for name, rps in PROFILES:
        w(f"### {name}: {rps:,} req/s peak ({rps // 5:,} orders/s)\n")
        totals = {}
        rows = []
        for kind in ("classic", "modern"):
            lines = aws_cost(kind, cpr[kind], rps)
            totals[kind] = sum(c for _, c in lines)
            rows += [[kind if i == 0 else "", item, f"${cost:,.0f}"] for i, (item, cost) in enumerate(lines)]
            rows.append(["", f"**Total {kind}**", f"**${totals[kind]:,.0f}**"])
        w(table(["Architecture", "Component", "USD / month"], rows, numeric=[False, False, True]))
        w("")
        summary.append([name, f"{rps:,}", f"${totals['classic']:,.0f}", f"${totals['modern']:,.0f}",
                        f"{(1 - totals['modern'] / totals['classic']):.0%}", f"${(totals['classic'] - totals['modern']) * 12:,.0f}"])
    w("### Cost summary\n")
    w(table(["Profile", "Peak req/s", "Classic / month", "Modern / month", "Modern saves", "Savings / year"], summary))
    w("")


# ---------- report ----------

def main():
    params = load_json(RES / "params.json")
    duration_s = secs(params["duration"])
    repeats = int(params.get("repeats", 1))
    rate = params["rate"]
    stacks = [s for s in params["stacks"].split() if (RES / f"{s}-pgversion.txt").exists()]
    scenarios = [s for s in params["scenarios"].split() if s in SCENARIOS]
    data = {(st, sc): collect(st, sc, repeats, duration_s) for st in stacks for sc in scenarios}
    label = lambda st: STACK_LABELS.get(st, st)
    md = []
    w = md.append

    w("# Event-driven architecture benchmark: PostgreSQL + Redis + Kafka vs PostgreSQL 18 only\n")
    w(f"Run date: {params['date']}. Generated by `scripts/report.py` from the files in `results/`. "
      f"Each scenario ran {repeats} times for {params['duration']}. The tables show the **median** of the runs. "
      f"Ranges show the lowest and the highest run.\n")

    # ---------- headline ----------
    w("## Key numbers\n")
    rows = []
    for st in stacks:
        wm, mx, wr = data.get((st, "write_max")), data.get((st, "mixed")), data.get((st, "write_rate"))
        rows.append([
            label(st), INFRA_CPU.get(st, "?"),
            f"**{fmt(wm['write_rps'])}** ({rng(wm['write_rps_min'], wm['write_rps_max'])})" if wm else "–",
            f"**{fmt(mx['rps'])}** ({rng(mx['rps_min'], mx['rps_max'])})" if mx else "–",
            fmt(wr and wr["create"]["p(99)"], 2),
            fmt(wr and wr["e2e"]["p50_ms"]), fmt(wr and wr["e2e"]["p99_ms"]),
        ])
    w(table(["Stack", "Infra CPUs", "Max writes/s (range)", "Mixed req/s (range)",
             f"Write p99 ms @ {rate}/s", f"Event p50 ms @ {rate}/s", f"Event p99 ms @ {rate}/s"], rows))
    w("")

    # ---------- environment ----------
    w("## Test environment\n")
    w(table(["Item", "Value"], [
        ["Host (Docker VM)", f"{params['host_cpus']} CPUs, {params['host_mem_bytes'] / 2**30:.1f} GiB RAM, Docker {params['docker']}"],
        ["Load generator", "k6 1.3.0 in Docker, pinned to cores 9–11. It calls the API directly (the client hop is the same for all stacks)."],
        ["Network emulation", f"`netproxy`: {params['net_delay_us']} µs one way = "
                              f"{2 * params['net_delay_us'] / 1000:.1f} ms round trip on every hop API/worker → PostgreSQL, Redis, Kafka. Cores 5–8, not part of the measured budget."],
        ["Duration × repeats", f"{params['duration']} × {repeats} per scenario, after a 15 s warm-up"],
        ["Fixed rate (write_rate)", f"{rate} requests/s"],
        ["Virtual users (write_max, mixed)", params["vus"]],
        ["CPU allocation", "Each service has dedicated cores (`cpuset`), no CFS quota: PostgreSQL core 0 (0–2 for 3 CPU), Redis 1, Kafka 2, api 3, worker 4"],
        ["Application containers", "api 1 CPU / 512 MB (PostgreSQL pool 96), worker 1 CPU / 512 MB (4 consumers, batch 500)"],
        ["PostgreSQL settings (all stacks)", "`shared_buffers=512MB`, `synchronous_commit=on`, `io_method=worker`, `max_wal_size=4GB`"],
    ], numeric=False))
    w("")
    rows = []
    for st in stacks:
        ver = (RES / f"{st}-pgversion.txt").read_text().split(" on ")[0].strip()
        net = {}
        p = RES / f"{st}-network.txt"
        if p.exists():
            for line in p.read_text().split():
                host, ms = line.split(";")
                net[host] = float(ms)
        rtt = f"{net.get('postgres', 0):.3f} ms → {net.get('netproxy', 0):.3f} ms" if net else "–"
        rows.append([label(st), ver, INFRA.get(st, ""), rtt])
    w(table(["Stack", "PostgreSQL", "Infrastructure limits", "Measured PG round trip: direct → through proxy"],
            rows, numeric=False))
    w("")
    w("Component versions: PostgreSQL 18.7, Redis 8.10.3, Apache Kafka 4.4.0 (KRaft, 1 broker), Go 1.25 (pgx 5, franz-go, go-redis 9).\n")

    # ---------- per scenario ----------
    for sc in scenarios:
        w(f"## Scenario `{sc}`: {SCENARIOS[sc]}\n")
        rows = []
        for st in stacks:
            d = data.get((st, sc))
            if not d:
                continue
            row = [label(st), f"{fmt(d['rps'])} ({rng(d['rps_min'], d['rps_max'])})", fmt(d["write_rps"])]
            if sc == "mixed":
                row += [fmt(d["read_rps"]), fmt(d["read"]["med"], 2), fmt(d["read"]["p(99)"], 2)]
            row += [fmt(d["create"]["med"], 2), fmt(d["create"]["p(95)"], 2), fmt(d["create"]["p(99)"], 2),
                    fmt(int(d["failures_total"]))]
            if sc == "write_rate":
                row += [fmt(int(d["dropped_total"]))]
            rows.append(row)
        hdr = ["Stack", "Req/s (range)", "Writes/s"]
        if sc == "mixed":
            hdr += ["Reads/s", "Read p50 ms", "Read p99 ms"]
        hdr += ["Write p50 ms", "Write p95 ms", "Write p99 ms", "Failed (all runs)"]
        if sc == "write_rate":
            hdr += ["Dropped (all runs)"]
        w("**HTTP API (measured by k6)**\n")
        w(table(hdr, rows))
        w("")

        rows = []
        for st in stacks:
            d = data.get((st, sc))
            if not d:
                continue
            e = d["e2e"]
            rows.append([label(st), fmt(int(e["count"])), fmt(e["p50_ms"]), fmt(e["p95_ms"]),
                         fmt(e["p99_ms"]), fmt(e["max_ms"]), drain_txt(d["drain_max"])])
        w("**Event pipeline (API request start → projection committed by the worker)**\n")
        w(table(["Stack", "Events per run", "p50 ms", "p95 ms", "p99 ms", "max ms", "Backlog drain s (worst run)"], rows))
        w("")

        rows = []
        for st in stacks:
            d = data.get((st, sc))
            if not d:
                continue
            u = d["usage"]
            parts = ", ".join(f"{k} {v['cpu']:.0f} %" for k, v in sorted(u.items()) if k not in NET_SERVICES)
            rows.append([label(st), f"{d['infra_cpu']:.0f} % of {INFRA_CPU.get(st, 0) * 100} %",
                         f"{d['infra_mem']:.0f} MiB", f"{d['app_cpu']:.0f} %", parts,
                         fmt(d["rps"] / (d["infra_cpu"] / 100), 0) if d["infra_cpu"] else "–",
                         f"{d['net_cpu']:.0f} %"])
        w("**Resource usage (docker stats, average over the run; 100 % = 1 CPU)**\n")
        w(table(["Stack", "Infra CPU used", "Infra peak RAM", "App CPU (api + worker)", "Per container",
                 "Req/s per infra CPU", "netproxy CPU"], rows))
        w("")

    # ---------- consistency ----------
    w("## Data consistency after all runs\n")
    rows = []
    for st in stacks:
        p = RES / f"{st}-consistency.txt"
        if p.exists():
            orders, projected, size = p.read_text().strip().split(";")
            pub = sum(int((data.get((st, sc)) or {}).get("publish_errors_total", 0)) for sc in scenarios)
            ok = "yes" if orders == projected else "NO"
            rows.append([label(st), f"{int(orders):,}", f"{int(projected):,}", ok, pub, size])
    w(table(["Stack", "Orders", "Events in projection", "Match", "Publish errors", "DB size"], rows))
    w("")

    cost_section(data, stacks, w)

    if ANALYSIS.exists():
        w(ANALYSIS.read_text().rstrip() + "\n")

    OUT.write_text("\n".join(md))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
