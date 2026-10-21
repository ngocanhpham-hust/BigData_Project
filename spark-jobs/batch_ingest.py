"""Job Spark batch: doc du lieu tho -> ghi bang Iceberg. TODO Giai doan 5."""
import os

from pyspark.sql import SparkSession

# Dich vu trong cum K8s (namespace lakehouse). Mac dinh la mat khau demo trong k8s/02-secrets.yaml.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-service:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")
PG_URI = os.getenv("ICEBERG_CATALOG_URI", "jdbc:postgresql://postgres-service:5432/metastore")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://cleansed-zone/warehouse")


def main() -> None:
    spark = (
        SparkSession.builder.appName("lakehouse-batch-ingest")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.iceberg.uri", PG_URI)
        .config("spark.sql.catalog.iceberg.jdbc.user", PG_USER)
        .config("spark.sql.catalog.iceberg.jdbc.password", PG_PASSWORD)
        .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.test.analytics (
            name         STRING,
            user_id      STRING,
            total_visits BIGINT
        ) USING iceberg
        """
    )
    print("[batch] da tao bang iceberg.test.analytics (neu chua co)")
    spark.stop()


if __name__ == "__main__":
    main()
