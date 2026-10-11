"""Job Spark batch: doc du lieu tho -> ghi bang Iceberg. TODO Giai doan 5."""
from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("lakehouse-batch-ingest")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.iceberg.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://hadoop-namenode/warehouse")
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
