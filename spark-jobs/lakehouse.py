"""Shared Spark configuration, table contracts and deterministic cleaning."""
import os

from pyspark.sql import SparkSession, Window, functions as F, types as T

DAILY_TABLE = "iceberg.stock.stock_prices_daily"
RAW_QUOTES_TABLE = "iceberg.stock.quotes_raw"
MINUTE_TABLE = "iceberg.stock.stock_quotes_minute"
DAILY_COLUMNS = ["symbol", "trade_date", "exchange", "open", "high", "low", "close",
                 "adj_close", "volume", "value"]
PRICE_COLUMNS = ["open", "high", "low", "close"]
MINUTE_SCHEMA = T.StructType([
    T.StructField("symbol", T.StringType()), T.StructField("ts", T.StringType()),
    *[T.StructField(c, T.DoubleType()) for c in PRICE_COLUMNS],
    T.StructField("volume", T.DoubleType()), T.StructField("source", T.StringType()),
])
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4", "org.postgresql:postgresql:42.7.3",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
])


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing {name}; inject it from the Kubernetes Secret.")
    return value


def build_spark(app_name="lakehouse"):
    builder = (SparkSession.builder.appName(app_name)
               .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
               .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
               .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
               .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
               .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
               .config("spark.redaction.regex", "(?i)secret|password|token|access[.]?key"))
    if os.getenv("SPARK_JARS_BAKED") != "1":
        builder = builder.config("spark.jars.packages", PACKAGES)
    local = os.getenv("LOCAL") == "1"
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "data/processed/warehouse" if local
                          else "s3a://cleansed-zone/warehouse")
    builder = builder.config("spark.sql.catalog.iceberg.warehouse", warehouse)
    if local:
        builder = builder.config("spark.sql.catalog.iceberg.type", "hadoop")
    else:
        builder = (builder
                   .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
                   .config("spark.sql.catalog.iceberg.uri", os.getenv("ICEBERG_CATALOG_URI",
                           "jdbc:postgresql://postgres-service:5432/metastore"))
                   .config("spark.sql.catalog.iceberg.jdbc.user", required_env("POSTGRES_USER"))
                   .config("spark.sql.catalog.iceberg.jdbc.password", required_env("POSTGRES_PASSWORD"))
                   .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio-service:9000"))
                   .config("spark.hadoop.fs.s3a.access.key", required_env("MINIO_ROOT_USER"))
                   .config("spark.hadoop.fs.s3a.secret.key", required_env("MINIO_ROOT_PASSWORD"))
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"))
    return builder.getOrCreate()


def ensure_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.stock")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
        symbol STRING, trade_date DATE, exchange STRING,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adj_close DOUBLE,
        volume BIGINT, value DOUBLE
    ) USING iceberg PARTITIONED BY (years(trade_date))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {RAW_QUOTES_TABLE} (
        key STRING, value STRING, ts TIMESTAMP, topic STRING, partition INT, offset BIGINT
    ) USING iceberg PARTITIONED BY (days(ts))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {MINUTE_TABLE} (
        symbol STRING, ts TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
        volume BIGINT, source STRING, topic STRING, partition INT, offset BIGINT
    ) USING iceberg PARTITIONED BY (days(ts))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')""")


def valid_prices():
    valid = F.lit(True)
    for col in PRICE_COLUMNS:
        valid = valid & F.col(col).isNotNull() & ~F.isnan(col) & (F.col(col) > 0) & (F.col(col) < 1e100)
    return (valid & (F.col("high") >= F.greatest(*[F.col(c) for c in PRICE_COLUMNS]))
            & (F.col("low") <= F.least(*[F.col(c) for c in PRICE_COLUMNS])))


def clean_daily(raw):
    missing = set(DAILY_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"Daily source missing columns: {sorted(missing)}")
    rows = raw.select(
        F.upper(F.trim("symbol")).alias("symbol"),
        F.expr("try_cast(trade_date AS DATE)").alias("trade_date"),
        F.upper(F.trim("exchange")).alias("exchange"),
        *[F.expr(f"try_cast({c} AS DOUBLE)").alias(c) for c in PRICE_COLUMNS + ["adj_close"]],
        F.expr("try_cast(volume AS DOUBLE)").alias("_volume"),
        F.expr("try_cast(try_cast(volume AS DOUBLE) AS BIGINT)").alias("volume"),
        F.expr("try_cast(value AS DOUBLE)").alias("value"),
    ).filter(
        (F.length("symbol") > 0) & F.col("trade_date").isNotNull()
        & (F.col("volume") > 0) & (F.col("volume") == F.col("_volume"))
        & F.col("adj_close").isNotNull() & ~F.isnan("adj_close")
        & (F.col("adj_close") > 0) & (F.col("adj_close") < 1e100) & valid_prices()
    ).select(*DAILY_COLUMNS)
    # No source revision timestamp: choose a stable payload, not an alleged latest revision.
    order = Window.partitionBy("symbol", "trade_date").orderBy(
        F.sha2(F.to_json(F.struct(*DAILY_COLUMNS)), 256))
    return rows.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").drop("_rank")


def upsert_daily(spark, clean):
    clean.createOrReplaceTempView("incoming_daily")
    spark.sql(f"""MERGE INTO {DAILY_TABLE} t USING incoming_daily s
        ON t.symbol = s.symbol AND t.trade_date = s.trade_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


def parse_quotes(raw):
    parsed = raw.withColumn("bar", F.from_json("value", MINUTE_SCHEMA)).select(
        F.upper(F.trim("bar.symbol")).alias("symbol"),
        F.expr("try_cast(bar.ts AS TIMESTAMP)").alias("ts"),
        *[F.col(f"bar.{c}").alias(c) for c in PRICE_COLUMNS],
        F.col("bar.volume").alias("_volume"),
        F.col("bar.volume").cast("long").alias("volume"),
        F.coalesce(F.col("bar.source"), F.lit("unknown")).alias("source"),
        "topic", "partition", "offset",
    )
    return parsed.filter(
        (F.length("symbol") > 0) & F.col("ts").isNotNull() & valid_prices()
        & (F.col("volume") >= 0) & (F.col("volume") == F.col("_volume"))
    ).drop("_volume")
