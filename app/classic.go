package main

// Classic event-driven stack: PostgreSQL (system of record) + Kafka (event bus)
// + Redis (read cache). The API performs a dual write: commit to PostgreSQL,
// then publish to Kafka. A crash between the two steps loses the event.

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	kafkaTopic = "order-events"
	kafkaGroup = "order-projector"
	cacheTTL   = time.Hour
)

type classicAPI struct {
	pool  *pgxpool.Pool
	kafka *kgo.Client
	rdb   *redis.Client
	c     *Counters
}

func ensureTopic(ctx context.Context, brokers []string) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatalf("kafka admin client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	partitions := int32(envInt("KAFKA_PARTITIONS", 12))
	for i := 0; i < 60; i++ {
		resp, err := adm.CreateTopics(ctx, partitions, 1, nil, kafkaTopic)
		if err == nil {
			if r, ok := resp[kafkaTopic]; ok && (r.Err == nil || errors.Is(r.Err, kerr.TopicAlreadyExists)) {
				return
			}
			err = resp[kafkaTopic].Err
		}
		log.Printf("waiting for kafka topic: %v", err)
		time.Sleep(time.Second)
	}
	log.Fatal("kafka topic not created")
}

func newClassicAPI(ctx context.Context, pool *pgxpool.Pool, brokers []string, redisAddr string, c *Counters) *classicAPI {
	ensureTopic(ctx, brokers)
	kc, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(kafkaTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // durable publish, idempotent producer
		kgo.ProducerLinger(0),
	)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, PoolSize: envInt("REDIS_POOL", 64)})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis: %v", err)
	}
	return &classicAPI{pool: pool, kafka: kc, rdb: rdb, c: c}
}

func (a *classicAPI) create(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	ctx := r.Context()
	o := Order{CustomerID: req.CustomerID, AmountCents: req.AmountCents, Status: "NEW", CreatedAt: time.Now().UTC()}

	// 1. System of record.
	err := a.pool.QueryRow(ctx,
		`INSERT INTO orders (customer_id, amount_cents, status, created_at)
		 VALUES ($1, $2, $3, $4) RETURNING id::text`,
		o.CustomerID, o.AmountCents, o.Status, o.CreatedAt).Scan(&o.ID)
	if err != nil {
		a.c.Errors.Add(1)
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}

	// 2. Publish the event (dual write, not atomic with step 1).
	ev, _ := json.Marshal(OrderEvent{Type: "OrderCreated", OrderID: o.ID, CustomerID: o.CustomerID, AmountCents: o.AmountCents, CreatedAt: o.CreatedAt})
	rec := &kgo.Record{Key: []byte(strconv.FormatInt(o.CustomerID, 10)), Value: ev}
	if err := a.kafka.ProduceSync(ctx, rec).FirstErr(); err != nil {
		a.c.PublishErrors.Add(1)
		writeJSON(w, 500, map[string]string{"error": "order saved but event not published: " + err.Error()})
		return
	}

	// 3. Write-through cache.
	body, _ := json.Marshal(o)
	if err := a.rdb.Set(ctx, "order:"+o.ID, body, cacheTTL).Err(); err != nil {
		log.Printf("redis set: %v", err)
	}
	a.c.Created.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(201)
	_, _ = w.Write(body)
}

func (a *classicAPI) get(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	key := "order:" + id
	if b, err := a.rdb.Get(ctx, key).Bytes(); err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		_, _ = w.Write(b)
		return
	} else if !errors.Is(err, redis.Nil) {
		log.Printf("redis get: %v", err)
	}
	o, err := loadOrder(ctx, a.pool, id)
	if err != nil {
		writeOrderErr(w, err)
		return
	}
	b, _ := json.Marshal(o)
	_ = a.rdb.Set(ctx, key, b, cacheTTL).Err()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	_, _ = w.Write(b)
}

func loadOrder(ctx context.Context, pool *pgxpool.Pool, id string) (Order, error) {
	var o Order
	err := pool.QueryRow(ctx,
		`SELECT id::text, customer_id, amount_cents, status, created_at FROM orders WHERE id = $1::uuid`, id).
		Scan(&o.ID, &o.CustomerID, &o.AmountCents, &o.Status, &o.CreatedAt)
	return o, err
}

func writeOrderErr(w http.ResponseWriter, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, 500, map[string]string{"error": err.Error()})
}

// ---------- worker ----------

func startClassicWorker(ctx context.Context, pool *pgxpool.Pool, brokers []string, n int, c *Counters, lat *LatencyRecorder) {
	ensureTopic(ctx, brokers)
	for i := 0; i < n; i++ {
		go runKafkaConsumer(ctx, pool, brokers, c, lat)
	}
}

func runKafkaConsumer(ctx context.Context, pool *pgxpool.Pool, brokers []string, c *Counters, lat *LatencyRecorder) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(kafkaGroup),
		kgo.ConsumeTopics(kafkaTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(time.Duration(envInt("KAFKA_FETCH_MAX_WAIT_MS", 10))*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer cl.Close()
	maxBatch := envInt("BATCH_SIZE", 500)

	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, maxBatch)
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("fetch error %s/%d: %v", t, p, err)
			}
		})
		records := fetches.Records()
		if len(records) == 0 {
			cl.AllowRebalance()
			continue
		}
		events := make([]OrderEvent, 0, len(records))
		for _, rec := range records {
			var ev OrderEvent
			if err := json.Unmarshal(rec.Value, &ev); err == nil {
				events = append(events, ev)
			}
		}
		ids, counts, totals := aggregate(events)
		// Retry the same batch until it succeeds. Skipping it would lose events:
		// the next CommitRecords call would move the offsets past them.
		for backoff := 10 * time.Millisecond; ; backoff = min(backoff*2, 2*time.Second) {
			if _, err = pool.Exec(ctx, upsertCustomerStatsSQL, ids, counts, totals); err == nil || ctx.Err() != nil {
				break
			}
			if !isRetryable(err) {
				c.Errors.Add(1)
				log.Printf("projection (retry in %s): %v", backoff, err)
			}
			time.Sleep(backoff)
		}
		if err != nil { // shutdown: do not commit, the batch is delivered again
			cl.AllowRebalance()
			return
		}
		// At-least-once: commit offsets only after the projection commit.
		if err := cl.CommitRecords(ctx, records...); err != nil {
			log.Printf("commit offsets: %v", err)
		}
		cl.AllowRebalance()
		now := time.Now()
		for _, ev := range events {
			lat.Add(now.Sub(ev.CreatedAt))
		}
		c.Processed.Add(int64(len(events)))
		c.Batches.Add(1)
	}
}
