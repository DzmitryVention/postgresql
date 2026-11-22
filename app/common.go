package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Order is the aggregate that the API writes and reads.
type Order struct {
	ID          string    `json:"id"`
	CustomerID  int64     `json:"customer_id"`
	AmountCents int64     `json:"amount_cents"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

// OrderEvent is the "OrderCreated" domain event. Both stacks carry the same payload.
type OrderEvent struct {
	Type        string    `json:"type"`
	OrderID     string    `json:"order_id"`
	CustomerID  int64     `json:"customer_id"`
	AmountCents int64     `json:"amount_cents"`
	CreatedAt   time.Time `json:"created_at"`
}

type createOrderRequest struct {
	CustomerID  int64 `json:"customer_id"`
	AmountCents int64 `json:"amount_cents"`
}

// ---------- configuration helpers ----------

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("invalid %s=%q: %v", key, v, err)
		}
		return n
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			log.Fatalf("invalid %s=%q: %v", key, v, err)
		}
		return b
	}
	return def
}

func mustPool(ctx context.Context, dsn string, maxConns int) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("parse PG_DSN: %v", err)
	}
	cfg.MaxConns = int32(maxConns)
	cfg.MinConns = int32(maxConns)
	var pool *pgxpool.Pool
	for i := 0; i < 60; i++ {
		pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool
			}
			pool.Close()
		}
		log.Printf("waiting for postgres: %v", err)
		time.Sleep(time.Second)
	}
	log.Fatalf("postgres unavailable: %v", err)
	return nil
}

// ---------- projection (identical SQL in both stacks) ----------

// upsertCustomerStatsSQL applies aggregated deltas in customer_id order.
// The fixed lock order prevents deadlocks between concurrent consumers.
const upsertCustomerStatsSQL = `
INSERT INTO customer_stats (customer_id, order_count, total_cents, updated_at)
SELECT c, n, t, now()
FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) AS d(c, n, t)
ORDER BY c
ON CONFLICT (customer_id) DO UPDATE
SET order_count = customer_stats.order_count + EXCLUDED.order_count,
    total_cents = customer_stats.total_cents + EXCLUDED.total_cents,
    updated_at  = now()`

func aggregate(events []OrderEvent) (ids, counts, totals []int64) {
	type agg struct{ n, t int64 }
	m := make(map[int64]*agg, len(events))
	for _, e := range events {
		a := m[e.CustomerID]
		if a == nil {
			a = &agg{}
			m[e.CustomerID] = a
		}
		a.n++
		a.t += e.AmountCents
	}
	ids = make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	counts = make([]int64, len(ids))
	totals = make([]int64, len(ids))
	for i, id := range ids {
		counts[i] = m[id].n
		totals[i] = m[id].t
	}
	return ids, counts, totals
}

func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40P01" || pgErr.Code == "40001" // deadlock, serialization failure
	}
	return false
}

// ---------- statistics ----------

// LatencyRecorder keeps every sample (microseconds) so percentiles are exact.
type LatencyRecorder struct {
	mu      sync.Mutex
	samples []int64
}

func (r *LatencyRecorder) Add(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d.Microseconds())
	r.mu.Unlock()
}

func (r *LatencyRecorder) Reset() {
	r.mu.Lock()
	r.samples = make([]int64, 0, 1<<20)
	r.mu.Unlock()
}

type LatencySummary struct {
	Count int64   `json:"count"`
	Avg   float64 `json:"avg_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

func (r *LatencyRecorder) Summary() LatencySummary {
	r.mu.Lock()
	s := append([]int64(nil), r.samples...)
	r.mu.Unlock()
	if len(s) == 0 {
		return LatencySummary{}
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var sum int64
	for _, v := range s {
		sum += v
	}
	pct := func(p float64) float64 {
		idx := int(p*float64(len(s))) - 1
		if idx < 0 {
			idx = 0
		}
		return float64(s[idx]) / 1000
	}
	return LatencySummary{
		Count: int64(len(s)),
		Avg:   float64(sum) / float64(len(s)) / 1000,
		P50:   pct(0.50),
		P95:   pct(0.95),
		P99:   pct(0.99),
		Max:   float64(s[len(s)-1]) / 1000,
	}
}

// Counters shared by API and worker roles.
type Counters struct {
	Created       atomic.Int64 // API: orders created (HTTP 201)
	Errors        atomic.Int64 // API/worker: failed operations
	PublishErrors atomic.Int64 // classic API: order saved, event publish failed (dual-write gap)
	Processed     atomic.Int64 // worker: events applied to the projection
	Batches       atomic.Int64 // worker: batches applied
}

func (c *Counters) Reset() {
	c.Created.Store(0)
	c.Errors.Store(0)
	c.PublishErrors.Store(0)
	c.Processed.Store(0)
	c.Batches.Store(0)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func serve(ctx context.Context, addr string, h http.Handler) {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()
	log.Printf("listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
