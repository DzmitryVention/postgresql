package main

// Modern stack: PostgreSQL 18 only.
//   - Transactional outbox: the order row and the event row commit in ONE statement.
//   - Queue consumers claim events with FOR UPDATE SKIP LOCKED.
//   - Idle consumers poll every POLL_INTERVAL_MS. Optional: a NOTIFY trigger +
//     LISTEN wakes them at once (PG_NOTIFY=true), see the warning below.
//   - Event consumption and projection update commit in ONE statement (exactly-once).
//   - Reads go straight to PostgreSQL (shared_buffers is the cache).

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type modernAPI struct {
	pool *pgxpool.Pool
	c    *Counters
}

// Order + outbox event in one atomic statement, one network round trip.
const createOrderWithEventSQL = `
WITH o AS (
  INSERT INTO orders (customer_id, amount_cents, status, created_at)
  VALUES ($1, $2, 'NEW', $3)
  RETURNING id, customer_id, amount_cents, status, created_at
), e AS (
  INSERT INTO order_events (event_type, payload, created_at)
  SELECT 'OrderCreated',
         jsonb_build_object('order_id', id, 'customer_id', customer_id, 'amount_cents', amount_cents),
         created_at
  FROM o
)
SELECT id::text, status FROM o`

func (a *modernAPI) create(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	o := Order{CustomerID: req.CustomerID, AmountCents: req.AmountCents, CreatedAt: time.Now().UTC()}
	if err := a.pool.QueryRow(r.Context(), createOrderWithEventSQL, o.CustomerID, o.AmountCents, o.CreatedAt).
		Scan(&o.ID, &o.Status); err != nil {
		a.c.Errors.Add(1)
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	a.c.Created.Add(1)
	writeJSON(w, 201, o)
}

func (a *modernAPI) get(w http.ResponseWriter, r *http.Request) {
	o, err := loadOrder(r.Context(), a.pool, r.PathValue("id"))
	if err != nil {
		writeOrderErr(w, err)
		return
	}
	writeJSON(w, 200, o)
}

// ---------- worker ----------

// Claim a batch, delete it from the queue and update the projection atomically.
const consumeBatchSQL = `
WITH batch AS (
  SELECT id FROM order_events
  ORDER BY id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
), del AS (
  DELETE FROM order_events e USING batch b
  WHERE e.id = b.id
  RETURNING e.payload, e.created_at
), agg AS (
  SELECT (payload->>'customer_id')::bigint AS customer_id,
         count(*)                                AS n,
         sum((payload->>'amount_cents')::bigint) AS t
  FROM del GROUP BY 1
), up AS (
  INSERT INTO customer_stats (customer_id, order_count, total_cents, updated_at)
  SELECT customer_id, n, t, now() FROM agg ORDER BY customer_id
  ON CONFLICT (customer_id) DO UPDATE
  SET order_count = customer_stats.order_count + EXCLUDED.order_count,
      total_cents = customer_stats.total_cents + EXCLUDED.total_cents,
      updated_at  = now()
)
SELECT created_at FROM del`

func startModernWorker(ctx context.Context, pool *pgxpool.Pool, dsn string, n int, c *Counters, lat *LatencyRecorder) {
	// PG_NOTIFY=true enables the NOTIFY trigger and a LISTEN connection.
	// NOTE: every transaction that issues NOTIFY takes a database-wide lock at
	// commit, so all writers serialize. Polling with SKIP LOCKED does not.
	notify := envBool("PG_NOTIFY", false)
	action := "DISABLE"
	if notify {
		action = "ENABLE"
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE order_events "+action+" TRIGGER order_events_notify"); err != nil {
		log.Fatalf("toggle notify trigger: %v", err)
	}
	log.Printf("modern worker: concurrency=%d notify=%v", n, notify)
	wake := make(chan struct{}, n)
	if notify {
		go listenLoop(ctx, dsn, wake, n)
	}
	poll := time.Duration(envInt("POLL_INTERVAL_MS", 10)) * time.Millisecond
	batch := envInt("BATCH_SIZE", 500)
	for i := 0; i < n; i++ {
		go func() {
			for ctx.Err() == nil {
				got, err := consumeBatch(ctx, pool, batch, c, lat)
				if err != nil {
					if ctx.Err() == nil {
						c.Errors.Add(1)
						log.Printf("consume: %v", err)
						time.Sleep(50 * time.Millisecond)
					}
					continue
				}
				if got > 0 {
					continue // more work may be waiting: drain without sleeping
				}
				select {
				case <-wake:
				case <-time.After(poll):
				case <-ctx.Done():
				}
			}
		}()
	}
}

func consumeBatch(ctx context.Context, pool *pgxpool.Pool, batch int, c *Counters, lat *LatencyRecorder) (int, error) {
	var created []time.Time
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		var rows pgx.Rows
		rows, err = pool.Query(ctx, consumeBatchSQL, batch)
		if err != nil {
			return 0, err
		}
		created, err = pgx.CollectRows(rows, pgx.RowTo[time.Time])
		if err == nil || !isRetryable(err) {
			break
		}
	}
	if err != nil {
		return 0, err
	}
	if len(created) == 0 {
		return 0, nil
	}
	now := time.Now()
	for _, t := range created {
		lat.Add(now.Sub(t))
	}
	c.Processed.Add(int64(len(created)))
	c.Batches.Add(1)
	return len(created), nil
}

// listenLoop holds one dedicated connection with LISTEN and fans out wake-ups.
func listenLoop(ctx context.Context, dsn string, wake chan struct{}, n int) {
	for ctx.Err() == nil {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			log.Printf("listen connect: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if _, err = conn.Exec(ctx, "LISTEN order_events"); err == nil {
			for {
				if _, err = conn.WaitForNotification(ctx); err != nil {
					break
				}
				for i := 0; i < n; i++ {
					select {
					case wake <- struct{}{}:
					default:
					}
				}
			}
		}
		if ctx.Err() == nil {
			log.Printf("listen: %v", err)
		}
		_ = conn.Close(context.Background())
	}
}
