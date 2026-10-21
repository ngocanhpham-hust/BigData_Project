"""Sinh du lieu MO PHONG: nhan gia ngay thanh thanh nen 1 phut de co hang chuc trieu dong.

Moi dong gia ngay (O,H,L,C,V) duoc bung thanh --bars-per-day thanh nen 1 phut:
duong gia di tu Open den Close (cau Brown + nhieu), bi kep trong [Low, High] va
cham dung High/Low; khoi luong chia theo duong cong chu U (dau/cuoi phien cao).
DAY LA DU LIEU MO PHONG, chi dung de do hieu nang - khong phai gia khop lenh that.

Dau vao : data/raw/stock_prices_daily.parquet (chay fetch_stock_data.py truoc)
Dau ra  : data/raw/synthetic/minute_bars_<nam>.parquet

Chay:
  python etl/generate_synthetic.py                       # ~30 trieu dong
  python etl/generate_synthetic.py --target-rows 5000000
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
OUT_DIR = RAW_DIR / "synthetic"

# Phien HOSE: 09:00-11:30 va 13:00-14:45 -> 150 + 105 = 255 phut; dung 240 phut dau cho gon
SESSION_MINUTES = np.concatenate([np.arange(9 * 60, 11 * 60 + 30), np.arange(13 * 60, 14 * 60 + 45)])


def expand_chunk(day: pd.DataFrame, bars: int, rng: np.random.Generator) -> pd.DataFrame:
    """Bung moi dong ngay thanh `bars` nen 1 phut (vectorized)."""
    n = len(day)
    o = day["open"].to_numpy()[:, None]
    h = day["high"].to_numpy()[:, None]
    lo = day["low"].to_numpy()[:, None]
    c = day["close"].to_numpy()[:, None]
    vol = day["volume"].to_numpy()[:, None]

    t = np.linspace(0, 1, bars + 1)[None, :]
    steps = rng.standard_normal((n, bars + 1))
    walk = np.cumsum(steps, axis=1)
    bridge = walk - t * walk[:, -1:]  # cau Brown: ve 0 tai t=0 va t=1
    spread = np.maximum(h - lo, 1e-6)
    path = o + (c - o) * t + bridge / np.sqrt(bars) * spread * 0.5
    path = np.clip(path, lo, h)
    path[:, 0], path[:, -1] = o[:, 0], c[:, 0]

    # Dam bao co 1 nen cham High va 1 nen cham Low
    rows = np.arange(n)
    path[rows, rng.integers(1, bars, n)] = h[:, 0]
    path[rows, rng.integers(1, bars, n)] = lo[:, 0]

    op, cl = path[:, :-1], path[:, 1:]
    wick = np.abs(rng.standard_normal((n, bars))) * spread * 0.02
    hi = np.minimum(np.maximum(op, cl) + wick, h)
    low = np.maximum(np.minimum(op, cl) - wick, lo)

    x = np.linspace(-1, 1, bars)
    weights = 1.0 + 1.5 * x**2 + rng.random((n, bars)) * 0.5  # chu U
    weights /= weights.sum(axis=1, keepdims=True)
    volume = np.rint(weights * vol).astype(np.int64)

    minutes = SESSION_MINUTES[np.linspace(0, len(SESSION_MINUTES) - 1, bars).astype(int)]
    dates = pd.to_datetime(day["trade_date"]).to_numpy("datetime64[D]")[:, None]
    ts = dates.astype("datetime64[m]") + minutes[None, :].astype("timedelta64[m]")

    op, cl, hi, low = op.round(2), cl.round(2), hi.round(2), low.round(2)
    hi, low = np.maximum(hi, np.maximum(op, cl)), np.minimum(low, np.minimum(op, cl))  # lam tron khong pha rang buoc

    return pd.DataFrame(
        {
            "symbol": np.repeat(day["symbol"].to_numpy(), bars),
            "ts": ts.ravel(),
            "open": op.ravel(),
            "high": hi.ravel(),
            "low": low.ravel(),
            "close": cl.ravel(),
            "volume": volume.ravel(),
            "source": "synthetic",
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target-rows", type=int, default=30_000_000)
    parser.add_argument("--bars-per-day", type=int, default=240)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--chunk-days", type=int, default=5000, help="so dong ngay xu ly moi lan (gioi han RAM)")
    args = parser.parse_args()

    daily = pd.read_parquet(RAW_DIR / "stock_prices_daily.parquet")
    daily = daily[(daily["volume"] > 0) & (daily["high"] >= daily["low"]) & (daily["low"] > 0)]
    daily = daily.sort_values(["trade_date", "symbol"], ascending=[False, True])

    needed = -(-args.target_rows // args.bars_per_day)
    daily = daily.head(needed)  # lay cac phien gan day nhat cho den khi du so dong
    print(f"Bung {len(daily):,} dong ngay x {args.bars_per_day} nen = {len(daily) * args.bars_per_day:,} dong "
          f"({daily['trade_date'].min()} -> {daily['trade_date'].max()})")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for old in OUT_DIR.glob("minute_bars_*.parquet"):
        old.unlink()
    rng = np.random.default_rng(args.seed)
    daily["year"] = pd.to_datetime(daily["trade_date"]).dt.year

    total = 0
    for year, part in daily.groupby("year"):
        pieces = [
            expand_chunk(part.iloc[i : i + args.chunk_days], args.bars_per_day, rng)
            for i in range(0, len(part), args.chunk_days)
        ]
        out = pd.concat(pieces, ignore_index=True)
        out.to_parquet(OUT_DIR / f"minute_bars_{year}.parquet", index=False)
        total += len(out)
        print(f"  {year}: {len(out):,} dong")
    print(f"Tong: {total:,} dong -> {OUT_DIR}")


if __name__ == "__main__":
    main()
