# Triển khai trên Kubernetes (k3d)

Cụm **k3d 3 node** (1 server + 2 agent) chạy trên một máy, phù hợp giới hạn 16GB RAM.
Đây là Kubernetes thật — có PVC, ConfigMap, Secret, Deployment/ReplicaSet, scheduler,
self-healing — không phải Docker Compose.

## Điều kiện

- Docker Desktop: **Memory ≥ 11GB**, **Disk ≥ 100GB** (Settings → Resources)
- `brew install k3d kubectl`
- Đã cài `docker` và đang chạy

## Dựng cụm

```bash
make cluster-up
```

Script sẽ tạo cụm, chờ node Ready, apply namespace + MinIO + PostgreSQL và tạo 3 bucket.

Kiểm tra:

```bash
kubectl get nodes -o wide
kubectl -n lakehouse get pods,pvc,svc -o wide
kubectl -n lakehouse logs job/minio-init-buckets
```

## StorageClass

k3s **đã tích hợp sẵn** `local-path` và đặt làm StorageClass mặc định, nên **không cần**
cài Rancher local-path-provisioner như bản dùng `kubeadm` trong báo cáo gốc.

```bash
kubectl get storageclass
# local-path (default)   rancher.io/local-path
```

## Cổng truy cập

| Dịch vụ | URL / Địa chỉ | Tài khoản |
|---|---|---|
| MinIO Console | http://localhost:30090 | `admin` / `password123` |
| MinIO API (S3) | http://localhost:30000 | `admin` / `password123` |
| PostgreSQL | localhost:30432 | `postgres` / `postgres123` |

Từ **bên trong cụm**, dùng Service DNS: `minio-service:9000`, `postgres-service:5432`.

## Cấu trúc manifest

| File | Nội dung |
|---|---|
| `00-namespace.yaml` | Namespace `lakehouse` |
| `02-secrets.yaml` | Secret MinIO + PostgreSQL (mật khẩu demo) |
| `10-minio.yaml` | PVC + Deployment + Service (NodePort 30000/30090) |
| `11-minio-buckets-job.yaml` | Job tạo 3 bucket Medallion |
| `20-postgres.yaml` | ConfigMap init + PVC + Deployment + Service (NodePort 30432) |

## Về dữ liệu và PVC

`local-path` tạo PV **trên node cục bộ**. PV có node-affinity nên Pod sẽ luôn được
scheduler đặt đúng node đang giữ dữ liệu — không cần ghim `nodeSelector`.

Script dựng cụm đã bind-mount `~/lakehouse-data/k3d-agent{0,1}` vào
`/var/lib/rancher/k3s/storage` của từng agent, nên **xoá cụm không mất dữ liệu**:

```bash
make cluster-down            # xoá cụm
ls ~/lakehouse-data          # dữ liệu vẫn còn
make cluster-up              # dựng lại, PVC bind vào dữ liệu cũ
```

## Ghi chú thiết kế

- **Một PostgreSQL cho 3 database** (`metastore`, `airflow_meta`, `openmetadata_db`)
  để tiết kiệm RAM — khác báo cáo gốc dùng chung một DB cho mọi thứ.
- `strategy: Recreate` cho MinIO và PostgreSQL vì PVC là `ReadWriteOnce`,
  không thể rolling update với 2 Pod cùng mount.
- Mọi container đều có `requests` + `limits` theo ngân sách RAM 16GB.
- Mật khẩu nằm trong Secret, không hardcode trong Deployment.
