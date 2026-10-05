# Power BI Dashboard Guide

## Connect
1. Get Data -> PostgreSQL database
2. Server: `localhost:5432`, Database: `opsdb`
3. Select **DirectQuery**
4. Import only the views: `v_kpi_today`, `v_orders_per_minute`, `v_revenue_hourly`,
   `v_cancellation_hourly`, `v_processing_time_hourly`, `v_delivery_sla_hourly`,
   `v_inventory_alerts`, `v_order_status_today`, `v_traffic_minute`, `alerts`

## Page layout: "Real-Time Operations Center"
| Area          | Visual                         | Source                      |
|---------------|--------------------------------|-----------------------------|
| KPI cards     | 6 cards                        | `v_kpi_today`               |
| Left chart    | Line: orders / minute          | `v_orders_per_minute`       |
| Middle chart  | Column: revenue / hour         | `v_revenue_hourly`          |
| Right chart   | Line: cancellation rate + 5% reference line | `v_cancellation_hourly` |
| Bottom left   | Line: avg + p95 processing time| `v_processing_time_hourly`  |
| Bottom middle | Column: delivery on-time %     | `v_delivery_sla_hourly`     |
| Bottom right  | Table: inventory alerts        | `v_inventory_alerts`        |
| Side          | Table: open alerts (`resolved_at` is blank) | `alerts`       |

## Measures (DAX)

Orders Today = SUM(v_kpi_today[orders_today])
Revenue Today = SUM(v_kpi_today[revenue_today])
Pending Orders = SUM(v_kpi_today[pending_orders])
Cancel Rate % = AVERAGE(v_kpi_today[cancellation_rate_pct]) / 100
Avg Processing = AVERAGE(v_kpi_today[avg_processing_min])
SLA Breaches = SUM(v_kpi_today[sla_breaches])
Cancel Color = IF([Cancel Rate %] > 0.05, "
#D64550", "
#2E8B57")

