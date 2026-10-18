#!/usr/bin/env bash
# Dựng cụm k3d 3 node (1 server + 2 agent) trên một máy.
# Dữ liệu PVC được bind-mount ra ~/lakehouse-data để KHÔNG mất khi xoá cụm.
set -euo pipefail

CLUSTER="${CLUSTER:-lakehouse}"
K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.30.14-k3s1}"
DATA_DIR="${DATA_DIR:-$HOME/lakehouse-data}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Thiếu '$1'. Cài trước: $2"; exit 1; }
}
need docker "https://www.docker.com/products/docker-desktop/"
need k3d    "brew install k3d"
need kubectl "brew install kubectl"

if k3d cluster list | grep -q "^${CLUSTER}"; then
  echo "Cụm '${CLUSTER}' đã tồn tại. Xoá bằng: k3d cluster delete ${CLUSTER}"
  exit 0
fi

mkdir -p "${DATA_DIR}/k3d-agent0" "${DATA_DIR}/k3d-agent1"

echo "==> Tạo cụm ${CLUSTER} (k3s ${K3S_IMAGE})"
k3d cluster create "${CLUSTER}" \
  --image "${K3S_IMAGE}" \
  --servers 1 \
  --agents 2 \
  --api-port 6445 \
  -p "30000:30000@loadbalancer" \
  -p "30090:30090@loadbalancer" \
  -p "30432:30432@loadbalancer" \
  --k3s-arg "--disable=traefik@server:0" \
  --k3s-arg "--disable=servicelb@server:0" \
  -v "${DATA_DIR}/k3d-agent0:/var/lib/rancher/k3s/storage@agent:0" \
  -v "${DATA_DIR}/k3d-agent1:/var/lib/rancher/k3s/storage@agent:1"

echo
echo "==> Chờ các node Ready"
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl get nodes -o wide

echo
echo "==> Kiểm tra StorageClass mặc định (k3s tích hợp sẵn local-path)"
kubectl get storageclass

echo
echo "==> Dựng namespace + MinIO + PostgreSQL"
kubectl apply -f "$(dirname "$0")/../k8s/"
kubectl -n lakehouse wait --for=condition=Ready pod -l app=minio --timeout=180s || true
kubectl -n lakehouse wait --for=condition=Ready pod -l app=postgres --timeout=180s || true

echo
echo "==> Trạng thái"
kubectl -n lakehouse get pods,pvc,svc -o wide

cat <<MSG

────────────────────────────────────────────────────────────
Xong. Truy cập (qua cổng đã map ra host):
  MinIO console : http://localhost:30090
  MinIO API     : http://localhost:30000
  PostgreSQL    : localhost:30432

Kiểm tra bucket đã tạo:
  kubectl -n lakehouse logs job/minio-init-buckets

Dữ liệu PVC nằm tại: ${DATA_DIR}
Xoá cụm (dữ liệu vẫn còn trong ${DATA_DIR}):
  k3d cluster delete ${CLUSTER}
────────────────────────────────────────────────────────────
MSG
