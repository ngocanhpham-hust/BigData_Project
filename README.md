# Data LakeHouse — Phân tích dữ liệu chứng khoán

Hệ thống **Data LakeHouse** phục vụ phân tích dữ liệu chứng khoán, triển khai trên
**Kubernetes** (cụm k3d 3 node giả lập trên một máy 16GB RAM).

> Trạng thái: 🚧 đang dựng hạ tầng. Đã có namespace, MinIO, PostgreSQL trên K8s.

## 1. Kiến trúc tổng thể

```
Nguồn ngoài (API chứng khoán / DB / file)
        │
        ▼
   [ Apache NiFi ]  ── batch ──▶  [ MinIO raw-zone ]  ─┐
        │                                              │
        └── streaming ──▶ [ Kafka ]                    │
                              │                        │
                              ▼                        ▼
                     [ Spark / Spark Streaming ]  ── Spark-on-K8s
                              │
                              ▼
              [ Apache Iceberg ]  ◀── metadata ── [ PostgreSQL ]
                              │                        (JDBC catalog)
                              ▼
                     [ dbt ]  mô hình hóa dữ liệu
                              │
                              ▼
                     [ Trino ]  truy vấn phân tán
                              │
                              ▼
              [ OpenMetadata ]  catalog & lineage
                              │
                     điều phối: [ Apache Airflow ]
```

**Lưu trữ:** MinIO (S3-compatible) theo mô hình **Medallion 3 vùng**:
`raw-zone` (dữ liệu thô) → `cleansed-zone` (bảng Iceberg ACID) → `curated-zone` (bảng nghiệp vụ).

## 2. Thành phần & phiên bản mục tiêu

| Thành phần | Vai trò | Phiên bản |
|---|---|---|
| k3d / k3s | Cụm Kubernetes 3 node trên Docker | k3s v1.30.14 |
| MinIO | Object storage tương thích S3 | RELEASE.2024-06-13 |
| PostgreSQL | Iceberg JDBC catalog + metadata | 16-alpine |
| Apache Kafka | Message broker (KRaft, không Zookeeper) | 3.7.0 |
| Apache Spark | Xử lý batch + streaming, chạy on-Kubernetes | 3.5.1 |
| Apache Iceberg | Table format (ACID, time travel) | 1.5.2 |
| Apache NiFi | Thu thập & chuẩn hóa dữ liệu | 1.26.0 |
| Trino | SQL query engine phân tán | v483+ |
| Apache Airflow | Lập lịch & điều phối | 2.9.1 |
| OpenMetadata | Metadata catalog & lineage | 1.3.1 |
| OpenSearch | Search index cho OpenMetadata | 2.12.0 |
| dbt | Mô hình hóa dữ liệu (ELT) | 1.7+ |

**Đã cắt để vừa 16GB RAM:** Superset, CKAN. OpenSearch + OpenMetadata chạy theo đợt.

## 3. Topology cụm k3d

| Node | Vai trò | Ghi chú |
|---|---|---|
| `k3d-lakehouse-server-0` | control plane + chạy workload | k3s ≤ 0.5 GB |
| `k3d-lakehouse-agent-0` | worker | dữ liệu PVC bind-mount `~/lakehouse-data/k3d-agent0` |
| `k3d-lakehouse-agent-1` | worker | dữ liệu PVC bind-mount `~/lakehouse-data/k3d-agent1` |

Cổng map ra host: MinIO console `30090`, MinIO API `30000`, PostgreSQL `30432`, API server `6445`.

## 4. Cấu trúc thư mục

```
.
├── k8s/                 # manifest Kubernetes, apply theo thứ tự tên file
│   ├── 00-namespace.yaml
│   ├── 02-secrets.yaml
│   ├── 10-minio.yaml
│   ├── 11-minio-buckets-job.yaml
│   └── 20-postgres.yaml
├── scripts/             # create-cluster.sh, check_env.sh
├── spark-jobs/          # job Spark batch & streaming ghi Iceberg
├── dbt/                 # dbt project: staging → intermediate → marts
├── etl/                 # script lấy dữ liệu API chứng khoán
├── data/                # dữ liệu local — không commit
├── docs/                # planning/, reference/, architecture.md
├── report/              # báo cáo
└── Makefile             # lệnh tiện dụng
```

## 5. Yêu cầu máy

- RAM: **16 GB** (hệ điều hành ~5 GB, còn lại ~11 GB cho Docker)
- Docker Desktop: **Memory 11 GB**, **Disk 100 GB** (Settings → Resources)
- CPU: ≥ 4 vCPU
- Công cụ: `docker`, `kubectl`, `k3d`

```bash
brew install k3d kubectl
```

## 6. Bắt đầu

```bash
make cluster-up        # dựng cụm k3d 3 node + MinIO + PostgreSQL
make k8s-status        # xem pod, PVC, service
```

Kiểm tra nhanh:

```bash
kubectl get nodes -o wide
kubectl -n lakehouse get pods,pvc,svc
kubectl -n lakehouse logs job/minio-init-buckets
```

Truy cập MinIO console: <http://localhost:30090> — `admin` / `password123`.

Chi tiết: [`k8s/README.md`](k8s/README.md)

## 7. Ngân sách RAM & chiến lược chạy theo đợt

Với 16 GB, không chạy tất cả thành phần đồng thời. Chia 2 đợt:

```bash
# Đợt 1 — Ingestion & Processing (NiFi → MinIO → Spark → Iceberg)
kubectl -n lakehouse scale deployment trino openmetadata opensearch --replicas=0

# Đợt 2 — Serving & Catalog (Trino query, OpenMetadata lineage)
kubectl -n lakehouse scale deployment nifi kafka --replicas=0
kubectl -n lakehouse scale deployment trino openmetadata opensearch --replicas=1
```

⚠️ Ở đợt 2, bật **OpenSearch trước**, đợi `Running` rồi mới bật OpenMetadata —
làm ngược lại OpenMetadata sẽ crash vì thiếu search engine.

## 8. Lộ trình thực hiện

- [x] **GĐ 0** — Chốt dữ liệu, kiến trúc, cấu trúc dự án
- [x] **GĐ 1** — Cụm k3d 3 node + namespace + MinIO + PostgreSQL
- [ ] **GĐ 2** — Kafka + Spark-on-Kubernetes + NiFi
- [ ] **GĐ 3** — Iceberg JDBC catalog + pipeline raw → cleansed
- [ ] **GĐ 4** — Trino + dbt
- [ ] **GĐ 5** — Airflow + OpenMetadata + OpenSearch
- [ ] **GĐ 6** — Đo hiệu năng + tự động hóa qua API
- [ ] **GĐ 7** — Viết báo cáo + đóng gói

## 9. Ghi chú

- Mật khẩu trong `k8s/02-secrets.yaml` là **demo**, khớp báo cáo gốc. Không dùng cho môi trường thật.
- Không commit mật khẩu thật, dữ liệu thô, key. Xem `.gitignore`.
- Version ghim cố định để kết quả tái lập được.
- Dữ liệu PVC nằm ở `~/lakehouse-data`, không mất khi xoá cụm.
