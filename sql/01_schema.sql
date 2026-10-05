-- =====================================================================
-- Real-Time Operations BI - schema
-- =====================================================================

-- Immutable event log (source of truth)
CREATE TABLE events (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT        NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL,
    order_id      BIGINT,
    customer_id   BIGINT,
    amount        NUMERIC(12,2),
    sku           TEXT,
    quantity      INT,
    stock_level   INT,
    reorder_point INT,
    city          TEXT,
    cancel_reason TEXT,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_events_time ON events (event_time);
CREATE INDEX idx_events_type_time ON events (event_type, event_time);

-- Staging table Spark writes into (rows are redirected by trigger, table stays empty)
CREATE TABLE events_ingest (
    event_id      TEXT,
    event_type    TEXT,
    event_time    TIMESTAMPTZ,
    order_id      BIGINT,
    customer_id   BIGINT,
    amount        NUMERIC(12,2),
    sku           TEXT,
    quantity      INT,
    stock_level   INT,
    reorder_point INT,
    city          TEXT,
    cancel_reason TEXT
);

-- Malformed / invalid messages
CREATE TABLE events_dead_letter (
    id          BIGSERIAL PRIMARY KEY,
    payload     TEXT,
    batch_id    BIGINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Current state of every order (one row per order, timestamps only)
CREATE TABLE orders (
    order_id      BIGINT PRIMARY KEY,
    customer_id   BIGINT,
    sku           TEXT,
    quantity      INT,
    amount        NUMERIC(12,2),
    city          TEXT,
    created_at    TIMESTAMPTZ,
    paid_at       TIMESTAMPTZ,
    shipped_at    TIMESTAMPTZ,
    delivered_at  TIMESTAMPTZ,
    cancelled_at  TIMESTAMPTZ,
    cancel_reason TEXT
);
CREATE INDEX idx_orders_created ON orders (created_at);

-- Current stock per SKU
CREATE TABLE inventory (
    sku           TEXT PRIMARY KEY,
    stock_level   INT         NOT NULL,
    reorder_point INT         NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

-- Business configuration
CREATE TABLE sla_config (
    sla_name TEXT PRIMARY KEY,
    minutes  NUMERIC NOT NULL
);
INSERT INTO sla_config VALUES
    ('processing', 8),   -- created -> shipped
    ('delivery',  40);   -- created -> delivered

-- ---------------------------------------------------------------------
-- Trigger 1: idempotent ingest (duplicates are silently dropped)
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_ingest_event() RETURNS trigger AS $$
BEGIN
    INSERT INTO events (event_id, event_type, event_time, order_id, customer_id,
                        amount, sku, quantity, stock_level, reorder_point,
                        city, cancel_reason)
    VALUES (NEW.event_id, NEW.event_type, NEW.event_time, NEW.order_id, NEW.customer_id,
            NEW.amount, NEW.sku, NEW.quantity, NEW.stock_level, NEW.reorder_point,
            NEW.city, NEW.cancel_reason)
    ON CONFLICT (event_id) DO NOTHING;
    RETURN NULL;  -- keep staging table empty
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ingest_event
BEFORE INSERT ON events_ingest
FOR EACH ROW EXECUTE FUNCTION fn_ingest_event();

-- ---------------------------------------------------------------------
-- Trigger 2: apply each NEW event to the operational tables.
-- Every branch is an upsert, so out-of-order events are safe.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_apply_event() RETURNS trigger AS $$
BEGIN
    CASE NEW.event_type
        WHEN 'ORDER_CREATED' THEN
            INSERT INTO orders (order_id, customer_id, sku, quantity, amount, city, created_at)
            VALUES (NEW.order_id, NEW.customer_id, NEW.sku, NEW.quantity, NEW.amount, NEW.city, NEW.event_time)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                sku         = EXCLUDED.sku,
                quantity    = EXCLUDED.quantity,
                amount      = EXCLUDED.amount,
                city        = EXCLUDED.city,
                created_at  = EXCLUDED.created_at;

        WHEN 'PAYMENT_COMPLETED' THEN
            INSERT INTO orders (order_id, paid_at) VALUES (NEW.order_id, NEW.event_time)
            ON CONFLICT (order_id) DO UPDATE SET paid_at = EXCLUDED.paid_at;

        WHEN 'ORDER_SHIPPED' THEN
            INSERT INTO orders (order_id, shipped_at) VALUES (NEW.order_id, NEW.event_time)
            ON CONFLICT (order_id) DO UPDATE SET shipped_at = EXCLUDED.shipped_at;

        WHEN 'ORDER_DELIVERED' THEN
            INSERT INTO orders (order_id, delivered_at) VALUES (NEW.order_id, NEW.event_time)
            ON CONFLICT (order_id) DO UPDATE SET delivered_at = EXCLUDED.delivered_at;

        WHEN 'ORDER_CANCELLED' THEN
            INSERT INTO orders (order_id, cancelled_at, cancel_reason)
            VALUES (NEW.order_id, NEW.event_time, NEW.cancel_reason)
            ON CONFLICT (order_id) DO UPDATE SET
                cancelled_at  = EXCLUDED.cancelled_at,
                cancel_reason = EXCLUDED.cancel_reason;

        WHEN 'INVENTORY_UPDATED' THEN
            INSERT INTO inventory (sku, stock_level, reorder_point, updated_at)
            VALUES (NEW.sku, NEW.stock_level, NEW.reorder_point, NEW.event_time)
            ON CONFLICT (sku) DO UPDATE SET
                stock_level   = EXCLUDED.stock_level,
                reorder_point = EXCLUDED.reorder_point,
                updated_at    = EXCLUDED.updated_at
            WHERE inventory.updated_at <= EXCLUDED.updated_at;

        ELSE
            NULL;  -- PAGE_VIEW etc. only live in the event log
    END CASE;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_apply_event
AFTER INSERT ON events
FOR EACH ROW EXECUTE FUNCTION fn_apply_event();