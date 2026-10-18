# Kiến trúc hệ thống

## Sơ đồ tổng thể

```
Nguồn ngoài (API chứng khoán / DB / file)
        │
        ▼
   [ Apache NiFi ] ── batch ──▶ [ MinIO raw-zone ] ─┐
        │                                           │
        └─ streaming ─▶ [ Kafka ]                   │
                              │                     │
                              ▼                     ▼
                   [ Spark / Spark Streaming ]  (Spark-on-K8s)
                              │
                              ▼
                   [ Apache Iceberg ] ◀── metadata ── [ PostgreSQL ]
                              │                       (JDBC catalog)
                              ▼
                   [ dbt ] mô hình hóa dữ liệu
                              │
                              ▼
                   [ Trino ] truy vấn phân tán
                              │
                              ▼
                   [ OpenMetadata ] catalog & lineage
                              │
                   điều phối toàn bộ: [ Apache Airflow ]
```

## Luồng dữ liệu theo tầng Medallion

| Tầng | Bucket | Định dạng | Ai ghi — ai đọc |
|---|---|---|---|
| Raw (Bronze) | `raw-zone` | Parquet/Avro thô | NiFi ghi; Spark đọc |
| Cleansed (Silver) | `cleansed-zone` | Bảng Iceberg (ACID) | Spark ghi; Trino/Spark đọc |
| Curated (Gold) | `curated-zone` | Bảng Iceberg đã aggregate | dbt (qua Trino) ghi; BI đọc |

## Vai trò từng thành phần

| Thành phần | Nhiệm vụ |
|---|---|
| MinIO | Object storage S3-compatible, thay HDFS |
| PostgreSQL | JDBC Catalog cho Iceberg (nguồn sự thật) + metadata Airflow/OpenMetadata |
| Kafka | Đệm dữ liệu streaming, tách producer/consumer |
| Spark | Xử lý phân tán; executor sinh theo nhu cầu trên K8s |
| Iceberg | Table format: ACID, snapshot, time travel, schema evolution |
| dbt | Sinh SQL biến đổi silver → gold, chạy trên Trino |
| Trino | Query engine MPP, truy vấn liên nguồn |
| OpenMetadata | Catalog nghiệp vụ, lineage, tag PII |
| Airflow | Lập lịch và điều phối toàn pipeline |

## Quyết định thiết kế

- **Kubernetes (k3d 3 node)** thay Docker Compose — đáp ứng yêu cầu không dùng thuần Docker.
- **MinIO thay HDFS** — tách rời compute/storage, phù hợp môi trường RAM hạn chế.
- **Iceberg JDBC Catalog** (không dùng REST Catalog) — REST Catalog từng lỗi
  `UnknownHostException` khi commit metadata trong cụm on-prem.
- **Một PostgreSQL cho 3 database** để tiết kiệm RAM.
- **Spark-on-Kubernetes** thay cụm Master/Worker độc lập — executor sinh theo nhu cầu.
- **Cắt Superset, CKAN**; OpenSearch + OpenMetadata chạy theo đợt để vừa 16GB.
- Version ghim cố định để kết quả tái lập được.
