# 🏠 Airbnb Lakehouse Pipeline

**Stack:** Kafka · MinIO · Apache Iceberg · PySpark · dbt · Trino · Airflow · Apache Superset  
**Data:** Airbnb Listings (~280K rows) + Reviews (~5.4M rows) — 10 thành phố toàn cầu

---

## 📁 Cấu trúc project

```
airbnb-lakehouse/
├── docker-compose.yml          ← Toàn bộ stack
├── data/                       ← Đặt Listings.csv + Reviews.csv vào đây
├── kafka/
│   ├── producer.py             ← Bước 1: CSV → Kafka
│   └── consumer.py             ← Bước 2: Kafka → MinIO Bronze
├── spark/
│   └── bronze_to_silver.py     ← Bước 3: PySpark clean data
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/            ← Views trên Silver tables
│       └── marts/              ← Gold tables (BI-ready)
├── airflow/
│   └── dags/
│       └── airbnb_pipeline.py  ← Orchestrate toàn bộ
├── trino/
│   ├── config.properties
│   └── catalog/
│       └── iceberg.properties
└── superset/
    └── superset_config.py
```

---

## 🚀 Khởi động (làm theo thứ tự)

### Bước 0 — Chuẩn bị data

```bash
# Giải nén data vào thư mục data/
unzip "Bài 16_ Travel & Hospitality.zip" -d ./data/
```

### Bước 1 — Khởi động toàn bộ stack

```bash
# Build & start tất cả services
docker-compose up -d

# Kiểm tra services đã chạy chưa
docker-compose ps
```

**Đợi khoảng 2-3 phút** để tất cả services healthy, sau đó mở:

| Service       | URL                        | Login         |
|---------------|---------------------------|---------------|
| Kafka UI      | http://localhost:8080      | (không cần)   |
| MinIO Console | http://localhost:9001      | minioadmin / minioadmin |
| Spark UI      | http://localhost:8081      | (không cần)   |
| Trino UI      | http://localhost:8083      | (không cần)   |
| Airflow UI    | http://localhost:8085      | admin / admin |
| Superset UI   | http://localhost:8088      | admin / admin |

### Bước 2 — Ingest data vào Kafka

```bash
# Cài dependencies
pip install kafka-python boto3

# Chạy producer (đẩy CSV vào Kafka)
python kafka/producer.py

# Trong terminal khác, chạy consumer (Kafka → MinIO Bronze)
python kafka/consumer.py
```

Kiểm tra Kafka UI tại http://localhost:8080 — bạn sẽ thấy 2 topics:
- `airbnb.listings.raw`
- `airbnb.reviews.raw`

Kiểm tra MinIO Console tại http://localhost:9001 — thấy files trong bucket `bronze/`.

### Bước 3 — PySpark: Bronze → Silver Iceberg

```bash
# Submit Spark job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages \
    org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
    org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/bronze_to_silver.py
```

Xem progress tại Spark UI: http://localhost:8081

### Bước 4 — dbt: Silver → Gold

```bash
# Vào container dbt
docker exec -it dbt bash

# Bên trong container:
cd /usr/app/dbt

# Test kết nối Trino
dbt debug

# Chạy staging models trước
dbt run --select staging.*

# Chạy Gold mart models
dbt run --select marts.*

# Chạy data quality tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve --port 8084   # xem lineage graph tại localhost:8084
```

### Bước 5 — Query với Trino

```bash
# Connect vào Trino CLI
docker exec -it trino trino

# Bên trong Trino CLI:
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.gold;

-- Xem pricing theo city
SELECT city, room_type, avg_price, median_price, total_listings
FROM iceberg.gold.mart_city_pricing
ORDER BY avg_price DESC;

-- Top 5 city đắt nhất (Entire place)
SELECT city, avg_price, median_price, superhost_pct
FROM iceberg.gold.mart_city_pricing
WHERE room_type = 'Entire place'
ORDER BY median_price DESC
LIMIT 5;
```

### Bước 6 — Superset Dashboard

1. Vào http://localhost:8088 → login admin/admin
2. **Settings → Database Connections → + Database**
3. Chọn **Trino**, điền:
   - Host: `trino`
   - Port: `8080`
   - Database: `iceberg`
4. **+ Dataset** → chọn schema `gold` → chọn `mart_city_pricing`
5. **+ Chart** → tạo các chart:
   - Bar chart: avg_price by city
   - Scatter plot: avg_rating vs avg_price
   - Line chart: review_count by month (từ mart_review_trends)
6. Gộp vào **Dashboard**

### Bước 7 — Airflow (tự động hoá)

1. Vào http://localhost:8085 → login admin/admin
2. Tìm DAG `airbnb_lakehouse_pipeline`
3. Bật DAG (toggle ON)
4. Trigger thủ công: **▶ Trigger DAG**
5. Xem Gantt chart các tasks chạy theo thứ tự

---

## 🧠 Kiến trúc dữ liệu (Medallion Architecture)

```
Bronze (raw)          Silver (cleaned)       Gold (aggregated)
─────────────         ────────────────       ─────────────────
JSON Lines            Iceberg tables         Iceberg tables
immutable             typed + validated      BI-ready marts
s3://bronze/          s3://iceberg/silver/   s3://iceberg/gold/
```

**Iceberg table format** cho phép:
- ✅ ACID transactions (không lo corrupt khi write)
- ✅ Time travel: `SELECT * FROM table FOR TIMESTAMP AS OF '2024-01-01'`
- ✅ Schema evolution: thêm cột mà không cần rewrite
- ✅ Partition pruning: query city='Paris' chỉ đọc partition Paris

---

## 📊 Các câu hỏi phân tích (analyst perspective)

```sql
-- 1. City nào có giá Airbnb cao nhất?
SELECT city, ROUND(AVG(avg_price), 0) as avg_price
FROM iceberg.gold.mart_city_pricing
GROUP BY city ORDER BY avg_price DESC;

-- 2. Superhost có giá cao hơn regular host không?
SELECT host_tier, ROUND(AVG(avg_listing_price), 0) as avg_price,
       COUNT(*) as host_count
FROM iceberg.gold.mart_host_performance
GROUP BY host_tier ORDER BY avg_price DESC;

-- 3. Review tăng trưởng theo năm như thế nào?
SELECT review_year, SUM(review_count) as total_reviews
FROM iceberg.gold.mart_review_trends
GROUP BY review_year ORDER BY review_year;

-- 4. Listing có nhiều amenities hơn → rating có cao hơn không?
SELECT
  CASE WHEN amenity_count < 5 THEN 'Few (<5)'
       WHEN amenity_count < 10 THEN 'Medium (5-10)'
       ELSE 'Many (10+)' END as amenity_bucket,
  ROUND(AVG(review_scores_rating), 2) as avg_rating,
  COUNT(*) as listing_count
FROM iceberg.silver.listings
GROUP BY 1 ORDER BY avg_rating DESC;
```

---

## 🛠️ Troubleshooting

**MinIO không tạo được bucket:**
```bash
docker restart minio-init
```

**Spark job bị OOM (Out of Memory):**
```bash
# Tăng memory trong docker-compose.yml
SPARK_WORKER_MEMORY: 6G
```

**Trino không kết nối được Iceberg:**
```bash
docker logs trino | grep ERROR
```

**Airflow DAG không thấy:**
```bash
docker exec airflow-scheduler airflow dags list
```
