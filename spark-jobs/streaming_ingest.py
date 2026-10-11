"""Job Spark Structured Streaming: Kafka -> Iceberg. TODO Giai doan 5."""
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("lakehouse-streaming-ingest").getOrCreate()
    print("[streaming] checkpoint: hdfs://hadoop-namenode/checkpoints")
    spark.stop()


if __name__ == "__main__":
    main()
