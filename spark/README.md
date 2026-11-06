# Spark và Iceberg của TLINH

Đầu mối: Linh (`tlinh`). Nhánh: `feat/gd2-3-spark-iceberg`.
Phạm vi: Spark 3.5.1, Iceberg 1.5.2, MinIO S3A, PostgreSQL JDBC catalog,
batch và streaming trên Kubernetes. Không cần cài Java/Spark trên host khi dùng image.

## 1. Các file chính

| File | Nội dung |
| --- | --- |
| `spark/Dockerfile` | Spark/Python/Java 17 và JAR đã tải khi build |
| `spark/Dockerfile.dockerignore` | Giới hạn context riêng cho image Spark, không ảnh hưởng build của thành viên khác |
| `spark/fetch_jars.py` | Tải dependency đúng version, kiểm tra checksum Maven |
| `k8s/40-spark-rbac.yaml` | ServiceAccount `spark`, Role/RoleBinding trong namespace |
| `k8s/41-spark-config.yaml` | Một executor 1 core/1 GB, Secret injection, driver networking |
| `k8s/42-spark-batch-job.yaml` | Job mẫu suspend, không chạy nhầm khi apply toàn bộ `k8s/` |
| `scripts/spark-k8s.sh` | Build/import/apply/submit từng job, không sửa Makefile chung |
| `spark-jobs/lakehouse.py` | Cấu hình chung, DDL và kiểm tra chất lượng |
| `spark-jobs/batch_ingest.py` | Raw Parquet sang daily table bằng MERGE |
| `spark-jobs/streaming_ingest.py` | Kafka sang raw envelope rồi bảng nến phút |
| `spark-jobs/verify_tables.py` | SELECT, count, snapshots, kiểm tra daily duplicate |
| `tests/spark/test_lakehouse.py` | Integration test dùng Spark và Iceberg thật |

## 2. Thiết kế bảng và giao tiếp với các thành viên

Catalog: `iceberg`, namespace: `stock`. PostgreSQL database: `metastore`.
Warehouse: `s3a://cleansed-zone/warehouse`. JAR JDBC PostgreSQL đã nằm trong image.

| Bảng | Khóa nghiệp vụ | Partition | Dữ liệu |
| --- | --- | --- | --- |
| `iceberg.stock.stock_prices_daily` | `symbol`, `trade_date` | `years(trade_date)` | OHLC, adj_close, volume, value, exchange |
| `iceberg.stock.quotes_raw` | Transport identity: topic/partition/offset | `days(ts)` | Kafka key/value nguyên bản, broker timestamp, transport coordinates |
| `iceberg.stock.stock_quotes_minute` | `symbol`, `ts`, `source` trong khoảng watermark | `days(ts)` | Nến phút có kiểu dữ liệu và các cột truy vết Kafka |

Giá theo đơn vị **nghìn đồng** từ nguồn; `value` theo nguồn. Timestamp không có offset
trong replay được hiểu theo `Asia/Ho_Chi_Minh`. `ts` ở `quotes_raw` là broker time,
còn `ts` ở bảng nến phút là event time trong JSON. Đây là hai mốc thời gian khác nhau.

- **TD:** giữ nguyên tên `stock_prices_daily` và các cột cần cho dbt. Trino dùng JDBC catalog
  tên `iceberg`, schema catalog V0 theo manifest hiện tại. Dimension/seed vẫn do TD quản lý.
- **NAP:** cung cấp `kafka-service:9092`, topic `stock-quotes`, key là symbol; value theo schema
  `etl/replay_to_kafka.py`: symbol, ts, open, high, low, close, volume, source.
- **PGB:** Airflow có thể submit job qua script và chờ Kubernetes Job hoàn thành.
- **Anh Đạt:** dùng bảng `.snapshots`, snapshot/time travel và query kiểm chứng trong phần dưới.

## 3. Chuẩn bị

Docker phải chạy; host có `docker`, `kubectl`, `k3d`, `python3`.
Cần cluster, namespace `lakehouse`, MinIO, PostgreSQL và Secret của nhóm.
Luôn kiểm tra đúng context trước khi triển khai:

```bash
kubectl config current-context
kubectl -n lakehouse get deploy,pvc,svc

bash scripts/spark-k8s.sh build
bash scripts/spark-k8s.sh import    # CLUSTER mặc định lakehouse
bash scripts/spark-k8s.sh apply
```

Cluster khác tên: `CLUSTER=tlinh-lakehouse bash scripts/spark-k8s.sh import`.
Dockerfile build theo kiến trúc host, hỗ trợ cả arm64 và amd64 từ base image.
Nếu chuyển sang cluster khác kiến trúc, cần build đúng platform và đổi image tương ứng.

Driver chạy `spark-submit --master k8s://... --deploy-mode client` bên trong Kubernetes Job.
Executor là pod riêng được driver tạo bằng ServiceAccount `spark`. `POD_IP` và
`POD_NAME` nối executor với driver và giúp dọn executor khi driver mất.
Đây là Spark-on-Kubernetes; không dùng Spark local bên trong pod cho job chính.

## 4. Chạy sample và batch

```bash
# Tạo Parquet của sample tại raw-zone/samples/, không ghi đè dataset đầy đủ.
bash scripts/spark-k8s.sh sample
# Đợi Job vừa tạo complete, sau đó:
bash scripts/spark-k8s.sh batch RAW_PATH=s3a://raw-zone/samples/stock_prices_daily
bash scripts/spark-k8s.sh verify
```

Mỗi lệnh trả tên Job mới, ví dụ `job.batch/spark-batch-xxxxx`.
Theo dõi và kiểm tra exit status của đúng Job đó:

```bash
kubectl -n lakehouse logs -f job/spark-batch-xxxxx
kubectl -n lakehouse wait --for=condition=complete job/spark-batch-xxxxx --timeout=300s
kubectl -n lakehouse get pods -o wide
```

Batch đầy đủ yêu cầu file `stock_prices_daily.parquet` đã nằm trong bucket `raw-zone`:

```bash
bash scripts/spark-k8s.sh batch
```

Batch chuẩn hóa mã/ngày/numeric, lọc volume bằng 0, null bắt buộc, NaN/Infinity,
giá không hợp lệ và OHLC mâu thuẫn. Bản ghi cùng khóa được chọn bằng hash ổn định
vì nguồn chưa có timestamp revision. MERGE cập nhật khóa đã có và thêm khóa mới,
không thay toàn bộ lịch sử. Input rỗng sau kiểm tra sẽ báo lỗi và giữ bảng hiện có.
Chạy lại vẫn có thể tạo snapshot mới, nhưng không nhân đôi business key.

Bronze vẫn giữ raw input để có thể thay đổi quy tắc lọc. Silver hiện chứa phiên có giao dịch.

## 5. Chạy streaming

Khi Kafka và replay producer đã sẵn sàng:

```bash
bash scripts/spark-k8s.sh stream
```

Mặc định trigger 60 giây, watermark 10 phút, tối đa 50.000 offsets mỗi trigger,
đọc từ earliest ở lần khởi tạo. Khi restart, checkpoint quyết định offsets tiếp tục.

Hai query độc lập:

1. Kafka sang `quotes_raw` bằng native Iceberg sink, giữ cả payload JSON lỗi.
2. Đọc append snapshots từ `quotes_raw`, parse/validate, watermark và
   `dropDuplicatesWithinWatermark`, ghi `stock_quotes_minute` bằng native Iceberg sink.

Checkpoint mặc định:

```text
s3a://cleansed-zone/checkpoints/stock_quotes_v1/raw
s3a://cleansed-zone/checkpoints/stock_quotes_v1/minute
```

Để chạy hết lượng dữ liệu đang có rồi thoát trong bài test:

```bash
bash scripts/spark-k8s.sh stream STREAM_AVAILABLE_NOW=1
bash scripts/spark-k8s.sh verify VERIFY_STREAM=1
```

Không chạy hai instance vào cùng checkpoint. Không xóa checkpoint để “sửa lỗi”:
checkpoint mới sẽ replay, có thể tạo bản ghi raw trùng. Watermark chỉ khử trùng trong
khoảng state còn giữ, không bảo đảm uniqueness toàn lịch sử. Replay cũ hơn watermark
sẽ chỉ còn trong raw; muốn xử lý lịch sử cần một backfill riêng.

Hai bảng được commit độc lập. Raw có thể đi trước Silver và Silver sẽ bắt kịp sau recovery.
Không overwrite/delete bảng raw khi query Silver đang đọc append snapshots.
Streaming tạo nhiều snapshot/file nhỏ, cần lên lịch compaction và snapshot retention
sau khi thống nhất cửa sổ recovery.

## 6. Các biến có thể truyền cho từng Job

| Biến | Mặc định hoặc cách dùng |
| --- | --- |
| `RAW_PATH` | `s3a://raw-zone/stock_prices_daily.parquet` |
| `MINIO_ENDPOINT` | `http://minio-service:9000` |
| `ICEBERG_CATALOG_URI` | `jdbc:postgresql://postgres-service:5432/metastore` |
| `ICEBERG_WAREHOUSE` | `s3a://cleansed-zone/warehouse` |
| `KAFKA_BOOTSTRAP` | `kafka-service:9092` |
| `KAFKA_TOPIC` | `stock-quotes` |
| `KAFKA_MAX_OFFSETS` | `50000` |
| `KAFKA_STARTING_OFFSETS` | `earliest`, chỉ ảnh hưởng checkpoint mới |
| `STREAM_TRIGGER` | `60 seconds` |
| `STREAM_WATERMARK` | `10 minutes` |
| `STREAM_AVAILABLE_NOW` | `1` để drain rồi dừng |
| `CHECKPOINT` | Root checkpoint; mỗi query dùng subdirectory riêng |
| `VERIFY_STREAM` | `1` để xác nhận cả hai bảng streaming có dữ liệu |

Credentials chỉ đọc từ Secret, đưa vào cả driver và executor, không truyền bằng
`spark-submit --conf` hoặc dòng lệnh. `spark.redaction.regex` che credential trong cấu hình Spark.

## 7. Integration test local bằng đúng image

```bash
docker run --rm --memory 3g stock-lakehouse-spark:3.5.1-iceberg1.5.2 \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  /opt/spark/work-dir/tests/test_lakehouse.py
```

Test sử dụng Spark và Iceberg thật, kiểm tra input lỗi, duplicate mâu thuẫn,
batch rerun/upsert, không mất dữ liệu khi input rỗng, streaming JSON lỗi,
restart checkpoint, watermark và múi giờ. Phần local không thay cho bài test
Kafka/JDBC/S3A trên Kubernetes.

## 8. Kiểm thử Kafka trong cluster riêng

Chỉ dùng fixture trong cluster riêng chưa có Kafka của NAP:

Fixture là broker tạm **không có PVC**. Nếu pod hoặc cluster restart, topic/log có
thể mất. Với lần smoke test mới, dùng catalog, warehouse và checkpoint test riêng;
không dùng checkpoint cũ để che lỗi mất Kafka log. Kafka của nhóm cần được NAP
triển khai với lưu trữ và cấu hình retention phù hợp.

```bash
kubectl apply -f tests/spark/kafka-fixture.yaml
kubectl -n lakehouse rollout status deployment/kafka-fixture
kubectl -n lakehouse exec deployment/kafka-fixture -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic stock-quotes --partitions 2 --replication-factor 1
python3 tests/spark/publish_fixture.py | \
  kubectl -n lakehouse exec -i deployment/kafka-fixture -- \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic stock-quotes --property parse.key=true --property 'key.separator=|'
bash scripts/spark-k8s.sh stream STREAM_AVAILABLE_NOW=1
bash scripts/spark-k8s.sh verify VERIFY_STREAM=1
```

## 9. Truy vấn bàn giao

Spark:

```sql
SELECT symbol, trade_date, close, volume
FROM iceberg.stock.stock_prices_daily ORDER BY trade_date DESC LIMIT 10;
SELECT count(*) FROM iceberg.stock.stock_quotes_minute;
SELECT snapshot_id, operation FROM iceberg.stock.stock_prices_daily.snapshots;
```

Trino đọc cùng catalog:

```sql
SELECT count(*) FROM iceberg.stock.stock_prices_daily;
SELECT symbol, ts, close, source FROM iceberg.stock.stock_quotes_minute LIMIT 10;
SELECT * FROM iceberg.stock."stock_prices_daily$snapshots";
```

## 10. Khi image MinIO cũ không tải được

Trong kiểm thử ngày 06/11/2026, quay.io trả 401 và binary archive trả 410.
Repo có Dockerfile **chỉ cho test** để build từ cùng tag mã nguồn chính thức:

```bash
docker build -f tests/spark/Dockerfile.minio -t tlinh-minio-test:2024-06-13 .
k3d image import -c tlinh-lakehouse tlinh-minio-test:2024-06-13
kubectl -n lakehouse set image deployment/minio minio=tlinh-minio-test:2024-06-13
bash scripts/spark-k8s.sh sample INIT_SAMPLE_BUCKETS=1
```

`INIT_SAMPLE_BUCKETS=1` tạo ba bucket bằng AWS SDK đã có trong image, để bỏ phụ thuộc
vào image `mc` bị lỗi. Chỉ dùng trong cluster test với MinIO đã sẵn sàng.
Không tự thay manifest MinIO của nhóm bằng fixture này.

## 11. Tài liệu kỹ thuật

- [Spark 3.5 Kubernetes](https://spark.apache.org/docs/3.5.7/running-on-kubernetes.html)
- [Iceberg 1.5.2 Structured Streaming](https://iceberg.apache.org/docs/1.5.2/spark-structured-streaming/)
- [Iceberg 1.5.2 JDBC catalog](https://iceberg.apache.org/docs/1.5.2/jdbc/)
