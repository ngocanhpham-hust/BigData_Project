"""Job Spark batch: raw-zone (Parquet gia ngay) -> lam sach -> bang Iceberg cleansed-zone.

Chay tren K8s (mac dinh) hoac local de thu:
    LOCAL=1 RAW_PATH=data/raw/stock_prices_daily.parquet spark-submit spark-jobs/batch_ingest.py
"""
import os

from pyspark.sql import SparkSession, functions as F

# Dich vu trong cum K8s (namespace lakehouse). Mac dinh la mat khau demo trong k8s/02-secrets.yaml.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-service:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")
PG_URI = os.getenv("ICEBERG_CATALOG_URI", "jdbc:postgresql://postgres-service:5432/metastore")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://cleansed-zone/warehouse")
RAW_PATH = os.getenv("RAW_PATH", "s3a://raw-zone/stock_prices_daily.parquet")
LOCAL = os.getenv("LOCAL") == "1"
PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3"
)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("lakehouse-batch-ingest")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
    )
    if LOCAL:  # thu nhanh khong can K8s: catalog Hadoop tren dia
        builder = builder.config("spark.sql.catalog.iceberg.type", "hadoop")
    else:
        builder = (
            builder.config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
            .config("spark.sql.catalog.iceberg.uri", PG_URI)
            .config("spark.sql.catalog.iceberg.jdbc.user", PG_USER)
            .config("spark.sql.catalog.iceberg.jdbc.password", PG_PASSWORD)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
    return builder.getOrCreate()


def main() -> None:
    spark = build_spark()
    raw = spark.read.parquet(RAW_PATH)

    clean = (
        raw.filter(F.col("volume") > 0)  # volume = 0: ma khong giao dich trong ngay
        .withColumn("trade_date", F.to_date("trade_date"))
        .dropna(subset=["symbol", "trade_date", "adj_close"])
        .dropDuplicates(["symbol", "trade_date"])
        .select("symbol", "trade_date", "exchange", "open", "high", "low", "close",
                "adj_close", "volume", "value")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.stock")
    (
        clean.writeTo("iceberg.stock.stock_prices_daily")
        .using("iceberg")
        .partitionedBy(F.years("trade_date"))
        .createOrReplace()
    )
    print(f"[batch] raw={raw.count()} -> cleansed={spark.table('iceberg.stock.stock_prices_daily').count()}")
    spark.stop()


if __name__ == "__main__":
    main()
