// Command app is the order service used by both benchmark stacks.
//
//	app api     -> HTTP API (POST /orders, GET /orders/{id}, GET|POST /stats)
//	app worker  -> event consumer that maintains the customer_stats projection
//	app netproxy -> TCP proxy that adds network latency between services
//
// MODE=classic uses PostgreSQL + Redis + Kafka.
// MODE=modern  uses PostgreSQL only (outbox table + SKIP LOCKED + LISTEN/NOTIFY).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: app api|worker")
	}
	role := os.Args[1]
	mode := env("MODE", "modern")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if role == "netproxy" {
		runNetProxy(ctx)
		return
	}

	dsn := env("PG_DSN", "postgres://bench:bench@postgres:5432/bench")
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	counters := &Counters{}
	latency := &LatencyRecorder{}
	latency.Reset()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /stats/reset", func(w http.ResponseWriter, r *http.Request) {
		counters.Reset()
		latency.Reset()
		w.WriteHeader(http.StatusNoContent)
	})

	switch role {
	case "api":
		pool := mustPool(ctx, dsn, envInt("PG_POOL", 32))
		defer pool.Close()
		mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 200, map[string]any{
				"mode":           mode,
				"created":        counters.Created.Load(),
				"errors":         counters.Errors.Load(),
				"publish_errors": counters.PublishErrors.Load(),
			})
		})
		var api interface {
			create(http.ResponseWriter, *http.Request)
			get(http.ResponseWriter, *http.Request)
		}
		if mode == "classic" {
			api = newClassicAPI(ctx, pool, brokers, env("REDIS_ADDR", "redis:6379"), counters)
		} else {
			api = &modernAPI{pool: pool, c: counters}
		}
		mux.HandleFunc("POST /orders", api.create)
		mux.HandleFunc("GET /orders/{id}", api.get)
		serve(ctx, env("HTTP_ADDR", ":8080"), mux)

	case "worker":
		concurrency := envInt("WORKER_CONCURRENCY", 4)
		pool := mustPool(ctx, dsn, concurrency+2)
		defer pool.Close()
		mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 200, map[string]any{
				"mode":        mode,
				"processed":   counters.Processed.Load(),
				"batches":     counters.Batches.Load(),
				"errors":      counters.Errors.Load(),
				"e2e_latency": latency.Summary(),
			})
		})
		if mode == "classic" {
			startClassicWorker(ctx, pool, brokers, concurrency, counters, latency)
		} else {
			startModernWorker(ctx, pool, dsn, concurrency, counters, latency)
		}
		serve(ctx, env("HTTP_ADDR", ":8081"), mux)

	default:
		log.Fatalf("unknown role %q", role)
	}
}
