"""Print keyed Kafka console-producer messages from the team's minute sample."""
import csv
import json
from pathlib import Path

source = Path(__file__).resolve().parents[2] / "data/sample/minute_bars_sample.csv"
with source.open() as handle:
    for row in csv.DictReader(handle):
        for key in ("open", "high", "low", "close"):
            row[key] = float(row[key])
        row["volume"] = int(row["volume"])
        print(row["symbol"] + "|" + json.dumps(row))
