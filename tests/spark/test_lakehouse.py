"""Real Spark/Iceberg integration tests. Run inside the pinned Spark image."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, "/opt/spark/work-dir/jobs")
from batch_ingest import ingest
from lakehouse import (DAILY_TABLE, RAW_QUOTES_TABLE, MINUTE_TABLE, build_spark,
                       clean_daily, ensure_tables, upsert_daily)
from streaming_ingest import start_clean, start_raw


class LakehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        os.environ["LOCAL"] = "1"
        os.environ["SPARK_JARS_BAKED"] = "1"
        os.environ["SPARK_SHUFFLE_PARTITIONS"] = "2"
        os.environ["ICEBERG_WAREHOUSE"] = cls.tmp.name + "/warehouse"
        cls.spark = build_spark("lakehouse-integration-tests")
        cls.spark.sparkContext.setLogLevel("WARN")
        ensure_tables(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.tmp.cleanup()

    def daily(self, **overrides):
        row = dict(symbol=" fpt ", trade_date="2026-10-01", exchange="hose",
                   open="100", high="102", low="98", close="101", adj_close="101",
                   volume="1000.0", value="101000000")
        row.update(overrides)
        return row

    def test_01_clean_validation_and_deterministic_duplicates(self):
        rows = [self.daily(), self.daily(), self.daily(close="100"),
                self.daily(symbol=""), self.daily(volume="0"),
                self.daily(volume="2.5"), self.daily(trade_date="bad-date"),
                self.daily(adj_close="NaN"), self.daily(high="90"),
                self.daily(open="Infinity")]
        raw = self.spark.createDataFrame(rows)
        first = clean_daily(raw).collect()
        second = clean_daily(raw.orderBy("close", ascending=False)).collect()
        self.assertEqual(first, second)
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0].symbol, "FPT")
        self.assertEqual(first[0].volume, 1000)

    def test_02_batch_rerun_and_incremental_update(self):
        path = self.tmp.name + "/input"
        raw = self.spark.createDataFrame([self.daily(), self.daily(), self.daily(volume="0")])
        raw.write.parquet(path)
        first = ingest(self.spark, path)
        second = ingest(self.spark, path)
        self.assertEqual((first["raw"], first["accepted"], second["table_rows"]), (3, 1, 1))
        update = clean_daily(self.spark.createDataFrame([
            self.daily(close="100"), self.daily(symbol="VNM", trade_date="2026-10-02")]))
        upsert_daily(self.spark, update)
        self.assertEqual(self.spark.table(DAILY_TABLE).count(), 2)
        self.assertEqual(self.spark.sql(f"SELECT close FROM {DAILY_TABLE} WHERE symbol='FPT'").first()[0], 100)
        bad_path = self.tmp.name + "/empty"
        self.spark.createDataFrame([self.daily(volume="0")]).write.parquet(bad_path)
        with self.assertRaises(ValueError):
            ingest(self.spark, bad_path)
        self.assertEqual(self.spark.table(DAILY_TABLE).count(), 2)

    def test_03_stream_checkpoints_deduplication_and_late_data(self):
        source = Path(self.tmp.name) / "stream-input"
        source.mkdir()
        root = self.tmp.name + "/checkpoints"
        schema = "key string, value string, ts timestamp, topic string, partition int, offset long"

        def bar(ts):
            return dict(symbol="FPT", ts=ts, open=100., high=102., low=98., close=101.,
                        volume=1000, source="synthetic")

        def publish(filename, payloads, offset):
            records = [dict(key="FPT", value=json.dumps(payload) if isinstance(payload, dict) else payload,
                            ts="2026-10-01T09:10:00+07:00", topic="stock-quotes",
                            partition=0, offset=offset + i) for i, payload in enumerate(payloads)]
            (source / filename).write_text("\n".join(json.dumps(r) for r in records))

        def run():
            transport = self.spark.readStream.schema(schema).json(str(source))
            start_raw(self.spark, transport, root + "/raw", True).awaitTermination(90)
            start_clean(self.spark, root + "/minute", True).awaitTermination(90)

        publish("first.json", [bar("2026-10-01 09:00:00"), bar("2026-10-01 09:00:00"),
                               bar("2026-10-01 09:05:00"), "{bad-json"], 0)
        run()
        self.assertEqual(self.spark.table(RAW_QUOTES_TABLE).count(), 4)
        self.assertEqual(self.spark.table(MINUTE_TABLE).count(), 2)
        run()  # Same source and checkpoints must not replay committed rows.
        self.assertEqual(self.spark.table(RAW_QUOTES_TABLE).count(), 4)
        self.assertEqual(self.spark.table(MINUTE_TABLE).count(), 2)
        publish("second.json", [bar("2026-10-01 08:00:00"), bar("2026-10-01 09:06:00"),
                                bar("2026-10-01 09:00:00")], 4)
        run()
        self.assertEqual(self.spark.table(RAW_QUOTES_TABLE).count(), 7)
        self.assertEqual(self.spark.table(MINUTE_TABLE).count(), 3)
        hour = self.spark.sql(f"SELECT hour(min(ts)) FROM {MINUTE_TABLE}").first()[0]
        self.assertEqual(hour, 9, "Vietnam local event time must not shift by seven hours")


if __name__ == "__main__":
    unittest.main(verbosity=2)
