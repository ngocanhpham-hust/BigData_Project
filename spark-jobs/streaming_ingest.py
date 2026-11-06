"""Kafka -> immutable envelope table -> validated, deduplicated minute bars.

Each native Iceberg sink has a separate durable checkpoint. Invalid JSON stays
in quotes_raw for audit. Duplicate suppression is bounded by the watermark;
starting with a new checkpoint is a replay, not a recovery.
"""
import os

from lakehouse import MINUTE_TABLE, RAW_QUOTES_TABLE, build_spark, ensure_tables, parse_quotes


def start_raw(spark, transport, checkpoint, available_now=False):
    writer = (transport.writeStream.format("iceberg").outputMode("append")
              .option("fanout-enabled", "true").option("checkpointLocation", checkpoint)
              .queryName("stock-quotes-raw"))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(
        processingTime=os.getenv("STREAM_TRIGGER", "60 seconds"))
    return writer.toTable(RAW_QUOTES_TABLE)


def start_clean(spark, checkpoint, available_now=False):
    source = spark.readStream.format("iceberg").load(RAW_QUOTES_TABLE)
    clean = (parse_quotes(source)
             .withWatermark("ts", os.getenv("STREAM_WATERMARK", "10 minutes"))
             .dropDuplicatesWithinWatermark(["symbol", "ts", "source"]))
    writer = (clean.writeStream.format("iceberg").outputMode("append")
              .option("fanout-enabled", "true").option("checkpointLocation", checkpoint)
              .queryName("stock-quotes-minute"))
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(
        processingTime=os.getenv("STREAM_TRIGGER", "60 seconds"))
    return writer.toTable(MINUTE_TABLE)


def main():
    spark = build_spark("lakehouse-streaming-ingest")
    queries = []
    try:
        ensure_tables(spark)
        root = os.getenv("CHECKPOINT", "s3a://cleansed-zone/checkpoints/stock_quotes_v1").rstrip("/")
        available = os.getenv("STREAM_AVAILABLE_NOW") == "1"
        kafka = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092"))
                 .option("subscribe", os.getenv("KAFKA_TOPIC", "stock-quotes"))
                 .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
                 .option("maxOffsetsPerTrigger", os.getenv("KAFKA_MAX_OFFSETS", "50000"))
                 .option("failOnDataLoss", "true").load()
                 .selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value",
                             "timestamp AS ts", "topic", "partition", "offset"))
        queries.append(start_raw(spark, kafka, root + "/raw", available))
        if available:
            queries[0].awaitTermination()
        queries.append(start_clean(spark, root + "/minute", available))
        if available:
            queries[-1].awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
