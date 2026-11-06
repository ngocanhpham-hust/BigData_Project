"""Read the shared JDBC catalog and fail on missing data or duplicate keys."""
import os

from lakehouse import DAILY_TABLE, MINUTE_TABLE, RAW_QUOTES_TABLE, build_spark


def main():
    spark = build_spark("lakehouse-verify")
    try:
        tables = [DAILY_TABLE]
        if os.getenv("VERIFY_STREAM") == "1":
            tables += [RAW_QUOTES_TABLE, MINUTE_TABLE]
        for table in tables:
            count = spark.table(table).count()
            print(f"[verify] {table} rows={count}", flush=True)
            if count == 0:
                raise ValueError(f"Expected non-empty table {table}")
            spark.sql(f"SELECT * FROM {table} LIMIT 5").show(truncate=False)
            spark.sql(f"SELECT snapshot_id, operation FROM {table}.snapshots").show()
        duplicates = spark.sql(f"""SELECT symbol, trade_date FROM {DAILY_TABLE}
            GROUP BY symbol, trade_date HAVING count(*) > 1""").count()
        if duplicates:
            raise ValueError(f"Daily table has {duplicates} duplicate business keys")
        print("[verify] PASS", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
