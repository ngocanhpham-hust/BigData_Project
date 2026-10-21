"""Job Spark Structured Streaming: Kafka -> Iceberg (bang nen 1 phut). Chua trien khai Kafka trong cum.

Se dung khi them Kafka (GD 2); checkpoint luu tren MinIO, khong dung HDFS.
"""
import os

from pyspark.sql import SparkSession

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
CHECKPOINT = os.getenv("CHECKPOINT", "s3a://cleansed-zone/checkpoints/stock_quotes")


def main() -> None:
    spark = SparkSession.builder.appName("lakehouse-streaming-ingest").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.stock")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS iceberg.stock.quotes_raw "
        "(key STRING, value STRING, ts TIMESTAMP) USING iceberg"
    )
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .load()
        .selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value", "timestamp AS ts")
    )
    (
        stream.writeStream.format("iceberg")
        .option("checkpointLocation", CHECKPOINT)
        .toTable("iceberg.stock.quotes_raw")
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
