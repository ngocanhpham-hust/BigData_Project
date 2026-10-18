# GHI NHỚ — 2 tài liệu Kubernetes Lakehouse

> Nguồn: `~/Downloads/Lakehouse_Kubernetes_Report.pdf` (53 trang) và `~/Downloads/K8s.pdf` (37 trang).
> Toàn văn trích xuất: `report_text.txt` (báo cáo chi tiết) và `k8s_text.txt` (cheat sheet lệnh).

---

## A. QUAN HỆ GIỮA HAI TÀI LIỆU

| | `Lakehouse_Kubernetes_Report.pdf` | `K8s.pdf` |
|---|---|---|
| Vai trò | Báo cáo kỹ thuật đầy đủ, có lý thuyết + thực hành + troubleshooting | Cheat sheet vận hành: lệnh + YAML copy-paste |
| Số trang | 53 | 37 |
| Cấu trúc | 12 chương | 7 phần không đánh số |
| Đối tượng | Người đọc hiểu bản chất | Người thao tác lại từ đầu |
| Nội dung | Lý thuyết nền (K8s, Helm, PV/PVC, Iceberg snapshot, Trino MPP, DAMA-DMBOK), 2 case study, bảng lỗi | Cùng hệ thống nhưng rút gọn: lệnh cài, YAML, output, cổng truy cập |

**Kết luận:** `K8s.pdf` là bản rút gọn thực hành của `Lakehouse_Kubernetes_Report.pdf`. Nội dung **nhất quán**, không mâu thuẫn. Dùng báo cáo để hiểu/viết báo cáo, dùng K8s.pdf để gõ lệnh.

---

## B. HỆ THỐNG TRONG 2 TÀI LIỆU NÀY

**Đề tài:** Technical Deployment Report — Data Lakehouse Platform trên Kubernetes on-premises
**Cụm:** 3 node — `k8s-master` (10.1.117.91) + `k8s-worker1` (10.1.117.88) + `k8s-worker2` (10.1.117.235); SSH user `qtdl`
**Phiên bản lõi:** Kubernetes **v1.30.14**, containerd, Calico **v3.28**, Helm v3.21.3
**Mạng:** pod-network-cidr `192.168.0.0/16` (khác dải vật lý `10.1.117.0/24`)
**Namespace duy nhất:** `lakehouse`
**Storage:** Rancher `local-path-provisioner` v0.0.28, đặt làm StorageClass mặc định, `VOLUMEBINDINGMODE=WaitForFirstConsumer`, mỗi PVC 10Gi

**9 thành phần lõi (khác hẳn dự án Docker 9-container trước đó):**

| # | Thành phần | Image | NodePort | Ghi chú |
|---|---|---|---|---|
| 1 | MinIO | quay.io/minio/minio:latest | 30000 (API) / 30090 (console) | admin/password123; 3 bucket Medallion |
| 2 | PostgreSQL | postgres:16-alpine | 30432 | postgres/postgres123, DB `metastore` |
| 3 | Kafka (KRaft) | apache/kafka:3.7.0 | 30092 | `KAFKA_PROCESS_ROLES=broker,controller`, RF=1 |
| 4 | Spark | apache/spark:3.5.1 | 30080 (UI), 7077 | 1 Master + 2 Worker |
| 5 | Airflow | apache/airflow:2.9.1 | 30808 | admin/admin, LocalExecutor, DB = metastore |
| 6 | NiFi | apache/nifi:1.26.0 | 30909 (instance 1), 30910 (instance 2) | Flow-Based Programming |
| 7 | OpenMetadata | openmetadata/server:1.3.1 | 30858 | cần DB riêng + search engine |
| 8 | OpenSearch | opensearchproject/opensearch:2.12.0 | ClusterIP 9200 | single-node, DISABLE_SECURITY_PLUGIN=true, 512MB |
| 9 | Trino | trinodb/trino:latest (v483+) | 30800 | MPP, Iceberg connector |

**Thêm ngoài 9 lõi:** Iceberg REST Catalog (`tabulario/iceberg-rest:latest`, ClusterIP 8181) — **đã bị loại bỏ**, xem mục D.

**Mô hình Medallion — 3 bucket MinIO:**
- `raw-zone` — dữ liệu thô nguyên bản (Parquet/Avro), NiFi ghi, Spark đọc
- `cleansed-zone` — bảng Iceberg ACID, Spark ghi, Trino/Spark đọc
- `curated-zone` — bảng Iceberg đã join/aggregate theo nghiệp vụ, Spark/dbt ghi, BI đọc

---

## C. LUỒNG DỮ LIỆU CASE STUDY 1 (Oracle → Trino)

```
Oracle CMIS.BC_DVI_BCAO
  → NiFi (ExecuteSQL → ConvertAvroToParquet → UpdateAttribute → PutS3Object)
  → MinIO raw-zone/bc_dvi_bcao.parquet
  → Spark (JDBC Catalog) ghi bảng Iceberg demo.bc_dvi_bcao
  → PostgreSQL metastore (bảng iceberg_tables) + MinIO cleansed-zone/demo.db/bc_dvi_bcao/{data,metadata}
  → Trino: SELECT * FROM iceberg.demo.bc_dvi_bcao
```
Dữ liệu nghiệm thu thật: danh mục **Đội Quản lý điện** (PB0807 Chợ Gạo, PB0808 Châu Thành, PB0809 Cái Bè, PB0901 Bến Tre… thuộc PB07/PB10, tỉnh Đồng Tháp/Vĩnh Long) — cùng bối cảnh ngành điện EVN như báo cáo Docker trước.

## C2. CASE STUDY 2 (multi-source: File + API + DB)

```
File CSV sự cố  → NiFi Instance 1 (ListFile → FetchFile → ConvertRecord → PutS3Object) ─┐
API thời tiết   → NiFi Instance 2 (InvokeHTTP → EvaluateJsonPath → PublishKafka) → Kafka ─┤
Oracle CMIS     → NiFi Instance 2 ────────────────────────────────────────────────────────┤
                                                    ↓
                     Spark Structured Streaming (Kafka → raw-zone/weather, trigger 1 phút)
                                                    ↓
                     Spark Batch (join weather ⨝ outage_reports → demo.outage_weather_cleansed)
                                                    ↓
                     dbt (dbt-trino, qua Trino) → demo_curated.curated_outage_summary
                                                    ↓
                     Trino serving → OpenMetadata (kỹ thuật) + CKAN (người dùng cuối)
                     Airflow DAG @hourly điều phối toàn bộ
```
API test: `https://api.open-meteo.com/v1/forecast` (không cần key). Kafka topic `weather-raw`, 3 partitions, RF=1.
dbt **KHÔNG phải compute engine** — chỉ sinh SQL rồi gửi cho Trino thực thi.
CKAN: `ckan/ckan-base:2.10`, NodePort 30500 — chỉ để demo publish dataset.

---

## D. 10 BÀI HỌC / LỖI ĐÃ GẶP (phần giá trị nhất để nhớ)

1. **AWS SDK luôn cần 3 tham số** khi trỏ về S3 không phải AWS thật (MinIO): **Endpoint Override + Path-style Access + Region giả định (`us-east-1`)**. Lỗi điển hình: `AmazonS3Exception: The bucket is in this region: eu-west-3`, `SdkClientException: Region missing`.
2. **Iceberg REST Catalog thất bại** — `UnknownHostException` vì S3 SDK trong REST Catalog server không phân giải được service name nội bộ khi commit metadata → **chuyển sang JDBC Catalog (PostgreSQL)**. Đây là lý do deployment ban đầu (có `iceberg-rest`) bị bỏ.
3. **Trino v440+/v483+ đã bỏ tiền tố `hive.s3.*`** → phải dùng `s3.*`, bật `fs.native-s3.enabled=true`, và **bắt buộc** khai báo `iceberg.jdbc-catalog.catalog-name`.
4. **Phải mount nguyên thư mục `/etc/trino/catalog`**, không dùng `subPath` mount từng file — nếu không Trino báo `Catalog 'iceberg' not found`.
5. **Lệch metadata Spark ↔ Trino:** Spark tạo bảng để `table_namespace` rỗng trong `iceberg_tables` → Trino ẩn schema (`Schema 'demo' does not exist`). Fix: `UPDATE iceberg_tables SET table_namespace='demo' WHERE table_name='bc_dvi_bcao'`.
6. **OpenMetadata fail-fast:** kết nối PostgreSQL OK (hiện Running) nhưng sau 1–2 phút tự crash vì HealthCheck không tìm thấy OpenSearch → phải có search engine. Cũng cần **database riêng** `openmetadata_db`, không dùng chung `metastore`.
7. **Spark/Ivy:** `Permission Denied` khi ghi cache → thêm `--conf spark.jars.ivy=/tmp/.ivy2`; thiếu AWS SDK v2 → thêm `iceberg-aws-bundle:1.5.2`; luôn cần `client.region`/`s3.region=us-east-1`.
8. **Kafka KRaft** bỏ Zookeeper; `KAFKA_ADVERTISED_LISTENERS` dùng Service DNS (`kafka-service:9092`) để client ổn định khi Pod restart. RF=1 chỉ chấp nhận ở demo.
9. **NiFi Connection Queue + Backpressure:** ngưỡng mặc định 10.000 object hoặc 1GB; nghẽn thực tế gặp ở `UpdateAttribute → PutS3Object` do lệch thông lượng. **TUYỆT ĐỐI KHÔNG dùng "Empty Queue"** để thông tắc — xóa vĩnh viễn FlowFile, không khôi phục được. Chẩn đoán nghẽn luôn nhìn về **downstream**.
10. **Cấu hình thứ tự khi làm Controller Service NiFi:** bấm **APPLY trước** để lưu vào RAM, rồi mới bấm tia sét **ENABLE** — nếu Verify trước sẽ lỗi `Driver class not found`.

**Kỹ thuật hay dùng lại:** `kubectl get pod -l app=X -o jsonpath='{.items[0].metadata.name}'` để lấy tên Pod cho `kubectl cp`; `kubectl run mc-... --rm -i --tty --restart='Never'` chạy `mc` dạng Pod tạm; hậu tố `2>/dev/null || true` để script idempotent; `kubectl rollout restart` sau khi sửa ConfigMap (volume mount không tự reload).

---

## E. NGUYÊN TẮC VẬN HÀNH (chương 2.7 + 12.2)

- Cụm **dùng chung** với đồng nghiệp → luôn **audit read-only trước**: `kubectl get namespaces/nodes/storageclass`, `helm list --all-namespaces`, `kubectl get crd`, `kubectl auth can-i create deployments -n lakehouse`.
- Mọi thao tác phá hủy (Empty Queue, DROP TABLE, xóa PVC) là **không thể hoàn tác**.
- Bảng catalog trong RDBMS là **nguồn sự thật duy nhất** khi nhiều engine cùng đọc một Iceberg Catalog.
- Tách rời compute/storage để mở rộng độc lập từng lớp.
- Tài liệu tự cảnh báo: mật khẩu (`password123`, `postgres123`, `admin`) **chỉ dùng cho demo/POC**, phải chuyển sang Kubernetes Secret + mật khẩu mạnh trước khi dùng thật.

## F. ĐỊNH HƯỚNG MỞ RỘNG ĐÃ NÊU

Security (K8s Secret + RBAC + TLS) · HA/Scalability (Kafka ≥3 broker RF=3, Airflow **KubernetesExecutor** — tác giả bác bỏ CeleryExecutor) · Observability (Prometheus + Grafana, JMX exporter cho Kafka/Spark, node-exporter, kube-state-metrics) · Data Governance (ownership + tag PII trong OpenMetadata, Great Expectations/dbt tests trong Airflow, chuẩn hóa naming convention namespace Iceberg) · ML trên Data Lake (Spark MLlib).

---

## G. LIÊN HỆ VỚI DỰ ÁN ĐANG LÀM

| | Báo cáo Docker/EVN (`Báo cáo BigData.docx.pdf`) | Báo cáo K8s này |
|---|---|---|
| Hạ tầng | 9 container Docker Compose, Ubuntu 22.04 | 3 node K8s on-prem, v1.30.14 |
| Storage | HDFS (HA, Hadoop 3.4.2) + Kafka | **MinIO S3** (không có HDFS) |
| Table format | Iceberg + PostgreSQL JDBC catalog | Iceberg + PostgreSQL JDBC catalog (**giống**) |
| Query | Trino **475** | Trino **v483+** |
| Orchestration | Airflow + dbt | Airflow + dbt (**giống**) |
| Ingestion | NiFi (ExecuteSQL → Avro→JSON → Kafka) | NiFi (ExecuteSQL → Avro→Parquet → MinIO) |
| Thêm mới | — | OpenMetadata, OpenSearch, CKAN, Medallion 3 zone |

**Điểm chung cốt lõi:** cùng Lakehouse, cùng Iceberg + PostgreSQL JDBC Catalog + Trino + Airflow + dbt + NiFi. Khác biệt lớn nhất là **HDFS → MinIO** và **Docker Compose → Kubernetes**.
