# Lệnh tiện dụng cho dự án Lakehouse trên Kubernetes (k3d).
# Dùng: make <target>
.DEFAULT_GOAL := help
SHELL := /bin/bash

CLUSTER ?= lakehouse
NS      ?= lakehouse
SVC     ?= minio

help: ## Hiển thị danh sách lệnh
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ─── Cụm Kubernetes ───
cluster-up: ## Dựng cụm k3d 3 node + MinIO + PostgreSQL
	CLUSTER=$(CLUSTER) ./scripts/create-cluster.sh

cluster-down: ## Xoá cụm (dữ liệu còn trong ~/lakehouse-data)
	k3d cluster delete $(CLUSTER)

cluster-status: ## Xem node và toàn bộ pod
	kubectl get nodes -o wide
	@echo
	kubectl get pods -A -o wide

# ─── Manifest ───
k8s-apply: ## Apply toàn bộ manifest trong k8s/
	kubectl apply -f k8s/

k8s-delete: ## Xoá toàn bộ manifest (giữ namespace)
	kubectl delete -f k8s/ --ignore-not-found

k8s-status: ## Xem tài nguyên trong namespace lakehouse
	kubectl -n $(NS) get all,pvc -o wide

k8s-logs: ## Xem log: make k8s-logs SVC=minio
	kubectl -n $(NS) logs -f deploy/$(SVC)

k8s-shell: ## Vào shell: make k8s-shell SVC=postgres
	kubectl -n $(NS) exec -it deploy/$(SVC) -- bash

buckets: ## Tạo lại 3 bucket Medallion
	kubectl -n $(NS) delete job minio-init-buckets --ignore-not-found
	kubectl apply -f k8s/11-minio-buckets-job.yaml

# ─── Tiện ích ───
env: ## Tạo file .env từ mẫu
	@test -f .env || cp .env.example .env
	@echo ".env đã sẵn sàng"

.PHONY: help cluster-up cluster-down cluster-status k8s-apply k8s-delete \
        k8s-status k8s-logs k8s-shell buckets env
