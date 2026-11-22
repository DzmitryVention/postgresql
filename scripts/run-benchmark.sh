#!/usr/bin/env bash
# Runs the full benchmark with Docker and writes RESULT.md.
#
# Usage: scripts/run-benchmark.sh
# Tunables (environment):
#   STACKS="classic modern-1cpu modern-3cpu modern-notify"
#   SCENARIOS="write_rate write_max mixed"
#   DURATION=45s  REPEATS=3  RATE=5000  VUS=192  NET_DELAY_US=500
set -euo pipefail

# macOS: prevent idle/system sleep while the benchmark runs. A sleeping host
# freezes the Docker VM and invalidates the run.
if [ "$(uname)" = "Darwin" ] && [ -z "${CAFFEINATED:-}" ] && command -v caffeinate >/dev/null; then
  export CAFFEINATED=1
  exec caffeinate -ims "$0" "$@"
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"
DURATION="${DURATION:-45s}"
REPEATS="${REPEATS:-3}"
RATE="${RATE:-5000}"
VUS="${VUS:-192}"
NET_DELAY_US="${NET_DELAY_US:-500}"
STACKS="${STACKS:-classic modern-1cpu modern-3cpu modern-notify}"
SCENARIOS="${SCENARIOS:-write_rate write_max mixed}"
K6_IMAGE="grafana/k6:1.3.0"
K6_CPUSET="${K6_CPUSET:-9-11}"   # services use cores 0-4, netproxy 5-8
API="http://localhost:8080"
WORKER="http://localhost:8081"
export NET_DELAY_US

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

rm -rf "$RESULTS" && mkdir -p "$RESULTS"
cat >"$RESULTS/params.json" <<EOF
{"duration": "$DURATION", "repeats": $REPEATS, "rate": $RATE, "vus": $VUS, "net_delay_us": $NET_DELAY_US,
 "stacks": "$STACKS", "scenarios": "$SCENARIOS",
 "host_cpus": $(docker info --format '{{.NCPU}}'), "host_mem_bytes": $(docker info --format '{{.MemTotal}}'),
 "docker": "$(docker version --format '{{.Server.Version}}')", "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"}
EOF

# stack name -> compose directory and extra environment
stack_dir() { case "$1" in classic) echo classic ;; *) echo modern ;; esac; }
stack_env() {
  case "$1" in
    modern-1cpu)   echo "PG_CPUSET=0 PG_MEM=2g" ;;
    modern-3cpu)   echo "PG_CPUSET=0-2 PG_MEM=5g" ;;               # same budget as classic PG + Redis + Kafka
    modern-notify) echo "PG_CPUSET=0 PG_MEM=2g PG_NOTIFY=true" ;;   # LISTEN/NOTIFY wake-up instead of polling
    *)             echo "" ;;
  esac
}

compose() { # compose <stack> <args...>
  local s="$1"; shift
  env $(stack_env "$s") docker compose -p "eda-$s" -f "$ROOT/docker/$(stack_dir "$s")/docker-compose.yml" "$@"
}

sample_stats() { # sample_stats <stack> <outfile> ; runs until killed
  local ids; ids=$(compose "$1" ps -q | tr '\n' ' ')
  while true; do
    docker stats --no-stream --format '{{.Name}};{{.CPUPerc}};{{.MemUsage}}' $ids 2>/dev/null \
      | sed "s/^/$(date +%s);/" >>"$2" || true
  done
}

wait_drain() { # wait until the worker processed every created order; prints drain seconds
  local start created processed
  start=$(date +%s)
  for _ in $(seq 1 1200); do
    created=$(curl -fsS "$API/stats" | sed -E 's/.*"created":([0-9]+).*/\1/')
    processed=$(curl -fsS "$WORKER/stats" | sed -E 's/.*"processed":([0-9]+).*/\1/')
    if [ "$processed" -ge "$created" ]; then echo $(( $(date +%s) - start )); return; fi
    sleep 0.5
  done
  echo "timeout"
}

run_k6() { # run_k6 <stack> <scenario> <run>
  docker run --rm --network "eda-$1_default" --cpuset-cpus "$K6_CPUSET" \
    -v "$ROOT/loadtest:/scripts:ro" -v "$RESULTS:/results" \
    -e STACK="$1" -e SCENARIO="$2" -e RUN="$3" -e DURATION="$DURATION" -e RATE="$RATE" -e VUS="$VUS" \
    "$K6_IMAGE" run --quiet --no-usage-report /scripts/orders.js
}

run_ok() { # run_ok <stack> <scenario> <run>: false if the k6 run took much longer than planned
  local ms
  ms=$(sed -nE 's/.*"testRunDurationMs": ([0-9]+).*/\1/p' "$RESULTS/$1-$2-r$3.json" | head -1)
  [ -n "$ms" ] && [ "$ms" -le $(( (${DURATION%s} + 60) * 1000 )) ]
}

net_calibration() { # measure PostgreSQL round trip directly and through the latency proxy
  local out="$RESULTS/$1-network.txt"
  for host in postgres netproxy; do
    docker run --rm --network "eda-$1_default" -e PGPASSWORD=bench postgres:18.7 sh -c \
      "echo 'SELECT 1;' > /tmp/q.sql && pgbench -n -h $host -U bench -c 1 -T 5 -f /tmp/q.sql bench 2>/dev/null" \
      | sed -nE "s/^latency average = ([0-9.]+) ms/$host;\1/p" >>"$out"
  done
  sed 's/^/    rtt /' "$out"
}

for stack in $STACKS; do
  log "[$stack] starting environment"
  compose "$stack" down -v --remove-orphans >/dev/null 2>&1 || true
  compose "$stack" up -d --build --wait --quiet-pull
  compose "$stack" exec -T postgres psql -U bench -d bench -Atc 'SELECT version()' >"$RESULTS/$stack-pgversion.txt"
  net_calibration "$stack"

  log "[$stack] warmup"
  run_k6 "$stack" warmup 0 >/dev/null
  wait_drain >/dev/null

  for run in $(seq 1 "$REPEATS"); do
    for scenario in $SCENARIOS; do
      for attempt in 1 2 3; do
        log "[$stack] scenario $scenario, run $run/$REPEATS (attempt $attempt)"
        rm -f "$RESULTS/$stack-$scenario-r$run-docker.csv"
        curl -fsS -X POST "$API/stats/reset" && curl -fsS -X POST "$WORKER/stats/reset"
        sample_stats "$stack" "$RESULTS/$stack-$scenario-r$run-docker.csv" &
        sampler=$!
        run_k6 "$stack" "$scenario" "$run"
        kill "$sampler" 2>/dev/null || true; wait "$sampler" 2>/dev/null || true
        drain=$(wait_drain)
        echo "    backlog drained in ${drain}s"
        printf '{"drain_seconds": "%s", "attempt": %s, "api": %s, "worker": %s}\n' "$drain" "$attempt" \
          "$(curl -fsS "$API/stats")" "$(curl -fsS "$WORKER/stats")" >"$RESULTS/$stack-$scenario-r$run-app.json"
        if run_ok "$stack" "$scenario" "$run"; then break; fi
        echo "    WARNING: run took much longer than $DURATION (host sleep or VM freeze?). Repeating it." | tee -a "$RESULTS/warnings.txt"
      done
    done
  done

  log "[$stack] consistency check"
  compose "$stack" exec -T postgres psql -U bench -d bench -At -F';' -c \
    "SELECT (SELECT count(*) FROM orders), (SELECT coalesce(sum(order_count),0) FROM customer_stats),
            pg_size_pretty(pg_database_size('bench'))" >"$RESULTS/$stack-consistency.txt"
  cat "$RESULTS/$stack-consistency.txt"

  log "[$stack] stopping environment"
  compose "$stack" down -v --remove-orphans >/dev/null
done

log "generating RESULT.md"
docker run --rm -v "$ROOT:/work" -w /work python:3.13-alpine python3 scripts/report.py
log "done: $ROOT/RESULT.md"
