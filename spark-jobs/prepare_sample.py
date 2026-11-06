"""Land the committed CSV fixture as Parquet without touching full raw data."""
import os

from lakehouse import build_spark, required_env


def main():
    spark = build_spark("lakehouse-prepare-sample")
    try:
        if os.getenv("INIT_SAMPLE_BUCKETS") == "1":
            # Explicit opt-in fallback for an isolated test cluster without mc.
            jvm = spark._jvm
            auth = jvm.com.amazonaws.auth
            endpoint = jvm.com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(
                os.getenv("MINIO_ENDPOINT", "http://minio-service:9000"), "us-east-1")
            credentials = auth.AWSStaticCredentialsProvider(auth.BasicAWSCredentials(
                required_env("MINIO_ROOT_USER"), required_env("MINIO_ROOT_PASSWORD")))
            client = (jvm.com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
                      .withEndpointConfiguration(endpoint).withCredentials(credentials)
                      .withPathStyleAccessEnabled(True).build())
            try:
                for bucket in ("raw-zone", "cleansed-zone", "curated-zone"):
                    if not client.doesBucketExistV2(bucket):
                        client.createBucket(bucket)
            finally:
                client.shutdown()
        source = os.getenv("SAMPLE_CSV", "/opt/spark/work-dir/data/sample/stock_prices_daily_sample.csv")
        target = os.getenv("SAMPLE_RAW_PATH", "s3a://raw-zone/samples/stock_prices_daily")
        rows = spark.read.option("header", True).csv(source)
        rows.write.mode("overwrite").parquet(target)
        print(f"[sample] rows={rows.count()} path={target}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
