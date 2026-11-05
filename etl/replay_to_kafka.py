"""Replay nen 1 phut vao Kafka topic `stock-quotes` de gia lap du lieu realtime.

Moi dong nen -> 1 message: key = symbol, value = JSON {symbol, ts, open, high, low, close, volume, source}.
Doc nguon theo thu tu thoi gian; --speed cho biet tang toc bao nhieu lan so voi thoi gian that
(--speed 0 = gui het tuc thi, dung de do tai).

Vi du:
    # xem thu 5 message, khong can Kafka
    python etl/replay_to_kafka.py --source data/sample/minute_bars_sample.csv --dry-run --limit 5
    # gui that (can: pip install kafka-python)
    KAFKA_BOOTSTRAP=localhost:9092 python etl/replay_to_kafka.py \
        --source data/raw/synthetic/minute_bars_2026.parquet --symbols FPT,VNM --speed 600
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import pandas as pd

DEFAULT_TOPIC = "stock-quotes"
COLUMNS = ["symbol", "ts", "open", "high", "low", "close", "volume", "source"]


def load(source: str, symbols: list[str] | None, date: str | None) -> pd.DataFrame:
    df = pd.read_csv(source) if source.endswith(".csv") else pd.read_parquet(source)
    df["ts"] = pd.to_datetime(df["ts"])
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    if date:
        df = df[df["ts"].dt.strftime("%Y-%m-%d") == date]
    if "source" not in df.columns:
        df["source"] = "synthetic"
    return df[COLUMNS].sort_values(["ts", "symbol"]).reset_index(drop=True)


def to_message(row: pd.Series) -> tuple[bytes, bytes]:
    payload = {
        "symbol": row["symbol"],
        "ts": row["ts"].strftime("%Y-%m-%d %H:%M:%S"),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": int(row["volume"]),
        "source": row["source"],
    }
    return row["symbol"].encode(), json.dumps(payload, ensure_ascii=False).encode()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default="data/raw/synthetic/minute_bars_2026.parquet")
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC))
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--symbols", help="loc theo ma, cach nhau dau phay (vd FPT,VNM)")
    ap.add_argument("--date", help="chi gui 1 ngay, dang YYYY-MM-DD")
    ap.add_argument("--speed", type=float, default=60.0,
                    help="he so tang toc so voi thoi gian that (mac dinh 60: 1 phut nen = 1 giay). 0 = khong cho")
    ap.add_argument("--limit", type=int, help="chi gui N message dau")
    ap.add_argument("--dry-run", action="store_true", help="in message ra man hinh, khong ket noi Kafka")
    args = ap.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None
    df = load(args.source, symbols, args.date)
    if args.limit:
        df = df.head(args.limit)
    if df.empty:
        print("Khong co dong nao sau khi loc.", file=sys.stderr)
        return 1

    producer = None
    if not args.dry_run:
        try:
            from kafka import KafkaProducer
        except ImportError:
            print("Thieu thu vien: pip install kafka-python (hoac dung --dry-run)", file=sys.stderr)
            return 1
        producer = KafkaProducer(bootstrap_servers=args.bootstrap, linger_ms=20, acks="all")

    print(f"[replay] {len(df):,} message -> {args.topic} @ {'dry-run' if args.dry_run else args.bootstrap}, speed={args.speed}")
    prev_ts = df["ts"].iloc[0]
    started = time.time()
    for i, row in enumerate(df.itertuples(index=False), 1):
        row = pd.Series(row._asdict())
        if args.speed > 0 and row["ts"] > prev_ts:
            time.sleep((row["ts"] - prev_ts).total_seconds() / args.speed)
        prev_ts = row["ts"]
        key, value = to_message(row)
        if producer is None:
            print(key.decode(), value.decode())
        else:
            producer.send(args.topic, key=key, value=value)
        if i % 10000 == 0:
            print(f"[replay] {i:,}/{len(df):,}")
    if producer is not None:
        producer.flush()
    print(f"[replay] xong {len(df):,} message trong {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
