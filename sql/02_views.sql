-- =====================================================================
-- Reporting views (Power BI connects ONLY to these)
-- Business timezone: Europe/Istanbul
-- =====================================================================

CREATE OR REPLACE VIEW v_orders AS
SELECT
    o.*,
    CASE
        WHEN o.cancelled_at IS NOT NULL THEN 'CANCELLED'
        WHEN o.delivered_at IS NOT NULL THEN 'DELIVERED'
        WHEN o.shipped_at   IS NOT NULL THEN 'SHIPPED'
        WHEN o.paid_at      IS NOT NULL THEN 'PAID'
        ELSE 'CREATED'
    END AS status,
    EXTRACT(EPOCH FROM (o.shipped_at   - o.created_at)) / 60.0 AS processing_minutes,
    EXTRACT(EPOCH FROM (o.delivered_at - o.created_at)) / 60.0 AS delivery_minutes,
    (o.cancelled_at IS NULL
     AND COALESCE(o.shipped_at, now()) - o.created_at >
         (SELECT minutes FROM sla_config WHERE sla_name = 'processing') * interval '1 minute'
    ) AS processing_sla_breached,
    (o.cancelled_at IS NULL
     AND COALESCE(o.delivered_at, now()) - o.created_at >
         (SELECT minutes FROM sla_config WHERE sla_name = 'delivery') * interval '1 minute'
    ) AS delivery_sla_breached
FROM orders o
WHERE o.created_at IS NOT NULL;

-- ---- KPI cards (single row) -----------------------------------------
CREATE OR REPLACE VIEW v_kpi_today AS
WITH t AS (
    SELECT * FROM v_orders
    WHERE created_at >= (date_trunc('day', now() AT TIME ZONE 'Europe/Istanbul')
                         AT TIME ZONE 'Europe/Istanbul')
)
SELECT
    count(*)                                                                    AS orders_today,
    COALESCE(sum(amount) FILTER (WHERE paid_at IS NOT NULL
                                   AND cancelled_at IS NULL), 0)                AS revenue_today,
    count(*) FILTER (WHERE status IN ('CREATED', 'PAID'))                       AS pending_orders,
    ROUND(100.0 * count(*) FILTER (WHERE status = 'CANCELLED')
          / NULLIF(count(*), 0), 2)                                             AS cancellation_rate_pct,
    ROUND(avg(processing_minutes)::numeric, 2)                                  AS avg_processing_min,
    count(*) FILTER (WHERE processing_sla_breached
                        OR delivery_sla_breached)                               AS sla_breaches
FROM t;

-- ---- Orders per minute (last 60 min) --------------------------------
CREATE OR REPLACE VIEW v_orders_per_minute AS
SELECT
    date_trunc('minute', created_at AT TIME ZONE 'Europe/Istanbul') AS minute_local,
    count(*)                                                        AS orders,
    COALESCE(sum(amount), 0)                                        AS revenue
FROM orders
WHERE created_at >= now() - interval '60 minutes'
GROUP BY 1;

-- ---- Revenue per hour (today) ---------------------------------------
CREATE OR REPLACE VIEW v_revenue_hourly AS
SELECT
    date_trunc('hour', created_at AT TIME ZONE 'Europe/Istanbul') AS hour_local,
    count(*)                                                      AS orders,
    COALESCE(sum(amount) FILTER (WHERE paid_at IS NOT NULL
                                   AND cancelled_at IS NULL), 0)  AS revenue
FROM orders
WHERE created_at >= (date_trunc('day', now() AT TIME ZONE 'Europe/Istanbul')
                     AT TIME ZONE 'Europe/Istanbul')
GROUP BY 1;

-- ---- Cancellation rate per hour -------------------------------------
CREATE OR REPLACE VIEW v_cancellation_hourly AS
SELECT
    date_trunc('hour', created_at AT TIME ZONE 'Europe/Istanbul') AS hour_local,
    count(*)                                                      AS orders,
    count(*) FILTER (WHERE cancelled_at IS NOT NULL)              AS cancelled,
    ROUND(100.0 * count(*) FILTER (WHERE cancelled_at IS NOT NULL)
          / NULLIF(count(*), 0), 2)                               AS cancellation_rate_pct
FROM orders
WHERE created_at >= now() - interval '24 hours'
GROUP BY 1;

-- ---- Processing time per hour (avg + p95) ---------------------------
CREATE OR REPLACE VIEW v_processing_time_hourly AS
SELECT
    date_trunc('hour', created_at AT TIME ZONE 'Europe/Istanbul')        AS hour_local,
    ROUND(avg(processing_minutes)::numeric, 2)                           AS avg_processing_min,
    ROUND((percentile_cont(0.95) WITHIN GROUP (ORDER BY processing_minutes))::numeric, 2)
                                                                         AS p95_processing_min,
    count(*)                                                             AS shipped_orders
FROM v_orders
WHERE shipped_at IS NOT NULL
  AND created_at >= now() - interval '24 hours'
GROUP BY 1;

-- ---- Delivery SLA per hour ------------------------------------------
CREATE OR REPLACE VIEW v_delivery_sla_hourly AS
SELECT
    date_trunc('hour', created_at AT TIME ZONE 'Europe/Istanbul')        AS hour_local,
    count(*)                                                             AS delivered_orders,
    count(*) FILTER (WHERE NOT delivery_sla_breached)                    AS on_time,
    ROUND(100.0 * count(*) FILTER (WHERE NOT delivery_sla_breached)
          / NULLIF(count(*), 0), 2)                                      AS on_time_pct
FROM v_orders
WHERE delivered_at IS NOT NULL
  AND created_at >= now() - interval '24 hours'
GROUP BY 1;

-- ---- Inventory alerts -----------------------------------------------
CREATE OR REPLACE VIEW v_inventory_alerts AS
SELECT sku, stock_level, reorder_point, updated_at,
       (reorder_point - stock_level) AS units_below_reorder
FROM inventory
WHERE stock_level <= reorder_point;

-- ---- Website traffic per minute (last 60 min) -----------------------
CREATE OR REPLACE VIEW v_traffic_minute AS
SELECT
    date_trunc('minute', event_time AT TIME ZONE 'Europe/Istanbul') AS minute_local,
    count(*) AS page_views
FROM events
WHERE event_type = 'PAGE_VIEW'
  AND event_time >= now() - interval '60 minutes'
GROUP BY 1;

-- ---- Order status breakdown (donut / funnel) ------------------------
CREATE OR REPLACE VIEW v_order_status_today AS
SELECT status, count(*) AS orders
FROM v_orders
WHERE created_at >= (date_trunc('day', now() AT TIME ZONE 'Europe/Istanbul')
                     AT TIME ZONE 'Europe/Istanbul')
GROUP BY status;