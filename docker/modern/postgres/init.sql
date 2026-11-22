-- Modern stack schema (PostgreSQL 18). PostgreSQL is the database, the event
-- bus and the cache at the same time.

CREATE TABLE orders (
    id           uuid        PRIMARY KEY DEFAULT uuidv7(), -- PG18: native, time-ordered UUIDv7
    customer_id  bigint      NOT NULL,
    amount_cents bigint      NOT NULL,
    status       text        NOT NULL,
    created_at   timestamptz NOT NULL
);

-- Transactional outbox / work queue. Rows are inserted in the same statement
-- as the order and deleted by the consumer in the same statement that updates
-- the projection.
CREATE TABLE order_events (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL,
    created_at  timestamptz NOT NULL
) WITH (
    -- High-churn queue table: vacuum often so dead tuples do not pile up.
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold    = 5000,
    autovacuum_vacuum_cost_delay   = 0
);

-- Read model maintained by the event consumer.
CREATE TABLE customer_stats (
    customer_id  bigint      PRIMARY KEY,
    order_count  bigint      NOT NULL,
    total_cents  bigint      NOT NULL,
    updated_at   timestamptz NOT NULL
);

-- Optional wake-up of consumers on commit. The worker enables this trigger only
-- when PG_NOTIFY=true, because NOTIFY takes a database-wide lock at commit time.
-- FOR EACH STATEMENT keeps one NOTIFY per insert statement.
CREATE FUNCTION notify_order_events() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('order_events', '');
    RETURN NULL;
END $$;

CREATE TRIGGER order_events_notify
AFTER INSERT ON order_events
FOR EACH STATEMENT EXECUTE FUNCTION notify_order_events();
