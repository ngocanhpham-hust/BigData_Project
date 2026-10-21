"""Tao file mau nho (commit duoc) de TV1/TV2 test luong khi chua can du lieu day du.

Dau ra: data/sample/stock_prices_daily_sample.csv, dim_symbol_sample.csv, minute_bars_sample.csv
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "sample"
SYMBOLS = ["VNM", "FPT", "HPG", "VCB", "SSI"]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    daily = pd.read_parquet(RAW / "stock_prices_daily.parquet")
    daily = daily[daily["symbol"].isin(SYMBOLS)].sort_values(["symbol", "trade_date"])
    daily.groupby("symbol").tail(20).to_csv(OUT / "stock_prices_daily_sample.csv", index=False)

    dim = pd.read_csv(RAW / "dim_symbol.csv")
    dim[dim["symbol"].isin(SYMBOLS)].to_csv(OUT / "dim_symbol_sample.csv", index=False)

    files = sorted((RAW / "synthetic").glob("minute_bars_*.parquet"))
    if files:
        bars = pd.read_parquet(files[-1])
        bars = bars[bars["symbol"].isin(SYMBOLS)].sort_values(["symbol", "ts"])
        bars.groupby("symbol").head(20).to_csv(OUT / "minute_bars_sample.csv", index=False)
    print(f"Da ghi file mau vao {OUT}")


if __name__ == "__main__":
    main()
