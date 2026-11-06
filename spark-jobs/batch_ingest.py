"""Idempotent daily prices: raw-zone -> Iceberg stock_prices_daily."""
import json
import os

from lakehouse import DAILY_TABLE, build_spark, clean_daily, ensure_tables, upsert_daily


def ingest(spark, raw_path):
    ensure_tables(spark)
    raw = spark.read.parquet(raw_path)
    clean = clean_daily(raw).persist()
    try:
        raw_count, accepted = raw.count(), clean.count()
        if accepted == 0:
            raise ValueError("No valid trading rows; existing Iceberg data was not modified.")
        upsert_daily(spark, clean)
        result = {"raw": raw_count, "accepted": accepted,
                  "filtered_or_duplicate": raw_count - accepted,
                  "table_rows": spark.table(DAILY_TABLE).count(), "table": DAILY_TABLE}
        print("[batch] " + json.dumps(result), flush=True)
        return result
    finally:
        clean.unpersist()


def main():
    spark = build_spark("lakehouse-batch-ingest")
    try:
        ingest(spark, os.getenv("RAW_PATH", "s3a://raw-zone/stock_prices_daily.parquet"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
