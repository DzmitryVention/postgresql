-- Classic stack schema (PostgreSQL 18, same version as the modern stack).
-- Events live in Kafka; the cache lives in Redis. PostgreSQL keeps only state.

CREATE TABLE orders (
    id           uuid        PRIMARY KEY DEFAULT uuidv7(), -- same key type as the modern stack
    customer_id  bigint      NOT NULL,
    amount_cents bigint      NOT NULL,
    status       text        NOT NULL,
    created_at   timestamptz NOT NULL
);

-- Read model maintained by the event consumer.
CREATE TABLE customer_stats (
    customer_id  bigint      PRIMARY KEY,
    order_count  bigint      NOT NULL,
    total_cents  bigint      NOT NULL,
    updated_at   timestamptz NOT NULL
);
