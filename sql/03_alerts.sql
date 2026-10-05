-- =====================================================================
-- Threshold alerting
-- =====================================================================

CREATE TABLE alert_rules (
    rule_name TEXT PRIMARY KEY,
    threshold NUMERIC NOT NULL
);
INSERT INTO alert_rules VALUES
    ('cancellation_rate_pct',       5),   -- alert if rate > 5 %
    ('cancellation_window_minutes', 15),  -- rate is computed over the last N minutes
    ('min_orders_in_window',        20),  -- ignore tiny samples
    ('sla_breach_count',            20);  -- alert if breaches today > 20

CREATE TABLE alerts (
    alert_id     BIGSERIAL PRIMARY KEY,
    alert_key    TEXT        NOT NULL,
    alert_type   TEXT        NOT NULL,
    severity     TEXT        NOT NULL,
    message      TEXT        NOT NULL,
    metric_value NUMERIC,
    threshold    NUMERIC,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ
);
CREATE INDEX idx_alerts_open ON alerts (alert_key) WHERE resolved_at IS NULL;

-- Conditions that are violated RIGHT NOW
CREATE OR REPLACE VIEW v_alert_candidates AS
WITH cfg AS (
    SELECT
        (SELECT threshold FROM alert_rules WHERE rule_name = 'cancellation_rate_pct')       AS cancel_thr,
        (SELECT threshold FROM alert_rules WHERE rule_name = 'cancellation_window_minutes') AS win_min,
        (SELECT threshold FROM alert_rules WHERE rule_name = 'min_orders_in_window')        AS min_orders,
        (SELECT threshold FROM alert_rules WHERE rule_name = 'sla_breach_count')            AS sla_thr
),
win AS (
    SELECT
        count(*) AS created,
        count(*) FILTER (WHERE o.cancelled_at IS NOT NULL) AS cancelled
    FROM orders o, cfg
    WHERE o.created_at >= now() - cfg.win_min * interval '1 minute'
)
SELECT
    'CANCELLATION_RATE'::text AS alert_key,
    'CANCELLATION_RATE'::text AS alert_type,
    'CRITICAL'::text          AS severity,
    format('Cancellation rate %s%% over last %s min (threshold %s%%)',
           ROUND(100.0 * win.cancelled / win.created, 1), cfg.win_min, cfg.cancel_thr) AS message,
    ROUND(100.0 * win.cancelled / win.created, 2)::numeric AS metric_value,
    cfg.cancel_thr::numeric                                AS threshold
FROM win, cfg
WHERE win.created >= cfg.min_orders
  AND 100.0 * win.cancelled / win.created > cfg.cancel_thr

UNION ALL

SELECT
    'SLA_BREACH',
    'SLA_BREACH',
    'WARNING',
    format('%s SLA breaches today (threshold %s)', k.sla_breaches, cfg.sla_thr),
    k.sla_breaches::numeric,
    cfg.sla_thr::numeric
FROM v_kpi_today k, cfg
WHERE k.sla_breaches > cfg.sla_thr

UNION ALL

SELECT
    'INVENTORY:' || sku,
    'INVENTORY_LOW',
    'WARNING',
    format('SKU %s stock %s <= reorder point %s', sku, stock_level, reorder_point),
    stock_level::numeric,
    reorder_point::numeric
FROM v_inventory_alerts;