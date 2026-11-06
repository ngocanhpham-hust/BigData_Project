#!/usr/bin/env bash
# Independent entry point: do not change the team's shared Makefile.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS=lakehouse
IMAGE=stock-lakehouse-spark:3.5.1-iceberg1.5.2
case "${1:-}" in
  build)
    docker build -f "${ROOT}/spark/Dockerfile" -t "$IMAGE" "$ROOT"
    ;;
  import)
    k3d image import -c "${CLUSTER:-lakehouse}" "$IMAGE"
    ;;
  apply)
    kubectl apply -f "${ROOT}/k8s/40-spark-rbac.yaml" -f "${ROOT}/k8s/41-spark-config.yaml"
    ;;
  sample|batch|stream|verify)
    # Use kubectl's local object transformation, leaving the shared template unchanged.
    # Arguments after MODE are explicit environment overrides, e.g. RAW_PATH=...
    mode="$1"; shift
    kubectl set env --local -f "${ROOT}/k8s/42-spark-batch-job.yaml" \
      "SPARK_JOB_MODE=$mode" "$@" -o json | \
      python3 -c '
import json, sys
job = json.load(sys.stdin)
container = job["spec"]["template"]["spec"]["containers"][0]
mode = next(e["value"] for e in container["env"] if e["name"] == "SPARK_JOB_MODE")
job["metadata"].pop("name", None)
job["metadata"]["generateName"] = "spark-" + mode + "-"
job["spec"]["suspend"] = False
container["command"][-1] = mode
if mode == "stream":
    job["spec"].pop("ttlSecondsAfterFinished", None)
json.dump(job, sys.stdout)
' | kubectl create -f -
    ;;
  *)
    echo 'Usage: bash scripts/spark-k8s.sh build|import|apply|sample|batch|stream|verify [KEY=VALUE ...]' >&2
    exit 2
    ;;
esac
