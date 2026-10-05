# Gerçek Zamanlı Operasyon BI (Real-Time Operations BI)

Bir e-ticaret şirketi için uçtan uca gerçek zamanlı operasyon izleme projesi:
**Kafka -> Spark Structured Streaming -> PostgreSQL -> Power BI** ve eşik tabanlı alert sistemi.

## Senaryo
Bir e-ticaret şirketi *şu anda* neler olduğunu görmek istiyor: siparişler, ödemeler,
teslimatlar, stok ve web sitesi trafiği. Bir şeyler ters gittiğinde de uyarılmak istiyor
(örneğin iptal oranı %5'in üzerine çıktığında veya stok yeniden sipariş noktasının altına düştüğünde).

## Mimari

```
 Uygulamalar (simüle edilmiş)
        |
        v
     Kafka  (topic: ecommerce.events, 3 partition, KRaft)
        |
        v
 Spark Structured Streaming
   - JSON parse ve doğrulama
   - geçersiz mesajlar -> events_dead_letter
        |
        v
 PostgreSQL
   events_ingest --(trigger, ON CONFLICT DO NOTHING)--> events (değiştirilemez log)
   events --(trigger, upsert)--> orders / inventory (güncel durum)
   SQL view'ları -> KPI, grafikler, SLA, alert
        |                         \
        v                          v
    Power BI (DirectQuery)     Alert checker (Python) -> konsol / Slack
```

## Event türleri
`ORDER_CREATED`, `PAYMENT_COMPLETED`, `ORDER_SHIPPED`, `ORDER_DELIVERED`,
`ORDER_CANCELLED`, `INVENTORY_UPDATED`, `PAGE_VIEW`

Örnek event:
```json
{
  "event_id": "b4c6...",
  "event_type": "ORDER_CREATED",
  "event_time": "2026-10-05T10:15:30+00:00",
  "order_id": 10294,
  "customer_id": 4821,
  "sku": "SKU-0012",
  "quantity": 1,
  "amount": 1499.0,
  "city": "Istanbul"
}
```

## Dashboard: Real-Time Operations Center
KPI'lar: Bugünkü Sipariş, Bugünkü Ciro, Bekleyen Sipariş, İptal Oranı, Ortalama İşlem Süresi, SLA İhlali.
Grafikler: dakikada sipariş, saatlik ciro, iptal oranı, işlem süresi (ortalama + p95),
teslimat SLA, stok uyarıları, sipariş durum dağılımı.
Detaylı kılavuz ve DAX ölçüleri için [`powerbi/README.md`](powerbi/README.md) dosyasına bak.

## Alert'ler
Kurallar `alert_rules` tablosunda tutulur (eşik değiştirmek için kod değişikliği gerekmez):

| Alert | Varsayılan kural |
|-------|------------------|
| İptal oranı | Son 15 dakikada > %5 (en az 20 sipariş) |
| SLA ihlali | Bugün > 20 |
| Stok | stok <= yeniden sipariş noktası (SKU bazında) |

Alert checker bir alert'i bir kez açar, koşul düzelince kapatır ve istenirse Slack'e
bildirim gönderir (`SLACK_WEBHOOK_URL`).

## Önemli mühendislik kararları
- **Idempotent veri alımı**: Tekrarlanan event'ler PostgreSQL'de `event_id` üzerinde
  `ON CONFLICT DO NOTHING` ile elenir. Böylece Spark'ın at-least-once teslimatı güvenli olur.
- **Sıra dışı event toleransı**: `orders` tablosu sadece zaman damgalarını tutar, durum (status) view içinde türetilir.
- **Dead letter queue**: Bozuk veya geçersiz event'ler sessizce silinmez, incelemek için saklanır.
- **BI izolasyonu**: Power BI sadece view'lardan okur. Şema değişse bile raporlar bozulmaz.
- **Gerçekçi simülasyon**: Yavaş siparişler (SLA ihlalleri) ve periyodik iptal artışları (alert tetiklemek için).

## Proje yapısı
```
src/producer/       event üretici (Kafka producer)
src/streaming/      Spark Structured Streaming job'ı
src/alerts/         eşik tabanlı alert kontrolcüsü
sql/                şema, view'lar, alert kuralları
powerbi/            dashboard kılavuzu
tests/              birim testler
```

## Hızlı başlangıç
Gereksinimler: Docker, Python 3.10+

```bash
cp .env.example .env
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

docker compose up -d                     # Kafka, PostgreSQL, Spark
python -m src.producer.event_generator   # 2. terminal
python -m src.alerts.alert_checker       # 3. terminal
```

Doğrulama:
```bash
docker compose logs -f spark
docker compose exec postgres psql -U ops -d opsdb -c "SELECT * FROM v_kpi_today;"
```

Sonra Power BI'ı bağla (`powerbi/README.md`).

## Teknolojiler
Apache Kafka (KRaft), Apache Spark 3.5 (Structured Streaming, PySpark), PostgreSQL 16,
Python, SQL, Docker Compose, Power BI (DirectQuery, DAX).
