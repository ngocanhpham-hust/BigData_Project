#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  sample) job=prepare_sample.py ;;
  batch) job=batch_ingest.py ;;
  stream) job=streaming_ingest.py ;;
  verify) job=verify_tables.py ;;
  *) echo 'Usage: submit.sh sample|batch|stream|verify' >&2; exit 2 ;;
esac
exec /opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode client \
  --properties-file /opt/spark/conf/lakehouse/spark-defaults.conf \
  --conf "spark.driver.host=${POD_IP:?missing POD_IP}" \
  --conf "spark.kubernetes.driver.pod.name=${POD_NAME:?missing POD_NAME}" \
  "/opt/spark/work-dir/jobs/${job}"
