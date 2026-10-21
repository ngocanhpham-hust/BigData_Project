"""Cao du lieu chung khoan VN tu API cong khai cua VNDirect (finfo).

Tao ra (trong data/raw/):
  dim_symbol.csv              danh sach ma: symbol, company_name, exchange, listed_date, icb_code, sector
  stock_prices_daily.parquet  gia ngay OHLCV tat ca ma, tu --start den hom nay
  _cache/<MA>.parquet         cache tung ma (chay lai se bo qua ma da cao)

Chay:
  python etl/fetch_stock_data.py                    # tat ca ma, tu 2013
  python etl/fetch_stock_data.py --symbols VNM,FPT  # chi vai ma de thu
Gia o don vi nghin dong (so goc cua API).
"""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

from icb_sectors import ICB_SECTORS

BASE = "https://api-finfo.vndirect.com.vn/v4"
VIETCAP_SYMBOLS = "https://trading.vietcap.com.vn/api/price/symbols/getAll"  # co icbCode2 (nganh ICB)
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
CACHE_DIR = RAW_DIR / "_cache"

PRICE_COLUMNS = {
    "code": "symbol",
    "date": "trade_date",
    "floor": "exchange",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "adOpen": "adj_open",
    "adHigh": "adj_high",
    "adLow": "adj_low",
    "adClose": "adj_close",
    "nmVolume": "volume",
    "nmValue": "value",
    "ptVolume": "pt_volume",
    "ptValue": "pt_value",
    "pctChange": "pct_change",
}


def get_json(url: str, params: dict, retries: int = 5) -> dict:
    """GET co retry + backoff khi bi gioi han toc do hoac loi mang."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=120)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            print(f"  loi mang ({exc}), thu lai...", file=sys.stderr)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"That bai sau {retries} lan: {url} {params}")


def fetch_icb_codes() -> dict:
    """Ma nganh ICB cua tung ma tu Vietcap (header Origin/Referer bat buoc, tra ve gzip)."""
    headers = {**HEADERS, "Origin": "https://trading.vietcap.com.vn", "Referer": "https://trading.vietcap.com.vn/"}
    resp = requests.get(VIETCAP_SYMBOLS, headers=headers, timeout=60)
    resp.raise_for_status()
    return {r["symbol"]: r.get("icbCode2") for r in resp.json() if r.get("type") == "STOCK"}


def fetch_symbols() -> pd.DataFrame:
    data = get_json(f"{BASE}/stocks", {"q": "type:STOCK~status:listed", "size": 3000})["data"]
    df = pd.DataFrame(data).rename(
        columns={"code": "symbol", "companyName": "company_name", "floor": "exchange", "listedDate": "listed_date"}
    )
    df["company_name"] = df["company_name"].str.strip()
    icb = fetch_icb_codes()
    df["icb_code"] = df["symbol"].map(icb)
    df["sector"] = df["icb_code"].map(ICB_SECTORS).fillna("Unknown")
    cols = ["symbol", "company_name", "exchange", "listed_date", "icb_code", "sector"]
    return df[cols].sort_values("symbol").reset_index(drop=True)


def fetch_prices(symbol: str, start: str) -> pd.DataFrame:
    rows, page, total = [], 1, None
    while True:
        payload = get_json(
            f"{BASE}/stock_prices",
            {"q": f"code:{symbol}~date:gte:{start}", "sort": "date:asc", "size": 1000, "page": page},
        )
        total = payload.get("totalElements", total)
        rows += payload["data"]
        if not payload["data"] or total is None or len(rows) >= total:
            break
        page += 1
    if total is not None and len(rows) != total:  # API tra thieu trang khi qua tai -> bao loi de chay lai
        raise RuntimeError(f"{symbol}: nhan {len(rows)}/{total} dong")
    if not rows:
        return pd.DataFrame(columns=list(PRICE_COLUMNS.values()))
    return pd.DataFrame(rows)[list(PRICE_COLUMNS)].rename(columns=PRICE_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default="2013-01-01", help="ngay bat dau (YYYY-MM-DD)")
    parser.add_argument("--symbols", help="danh sach ma cach nhau dau phay; bo trong = tat ca")
    parser.add_argument("--delay", type=float, default=0.2, help="giay nghi sau moi ma (moi luong)")
    parser.add_argument("--workers", type=int, default=6, help="so luong goi song song (API tra loi cham)")
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    dim = fetch_symbols()
    dim.to_csv(RAW_DIR / "dim_symbol.csv", index=False, encoding="utf-8")
    print(f"dim_symbol: {len(dim)} ma ({dim['exchange'].value_counts().to_dict()})")

    symbols = args.symbols.split(",") if args.symbols else dim["symbol"].tolist()
    failed = []
    todo = [s for s in symbols if not (CACHE_DIR / f"{s}.parquet").exists()]

    def crawl(sym: str) -> str:
        fetch_prices(sym, args.start).to_parquet(CACHE_DIR / f"{sym}.parquet", index=False)
        time.sleep(args.delay)
        return sym

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(crawl, sym): sym for sym in todo}
        for done, fut in enumerate(as_completed(futures), 1):
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001 - bo qua ma loi, bao cuoi cung
                failed.append(futures[fut])
                print(f"  {futures[fut]}: LOI {exc}", file=sys.stderr)
            if done % 50 == 0:
                print(f"  da xong {done}/{len(todo)} ma", flush=True)

    frames = [pd.read_parquet(p) for p in sorted(CACHE_DIR.glob("*.parquet"))]
    frames = [f for f in frames if len(f)]
    prices = pd.concat(frames, ignore_index=True)
    prices["trade_date"] = pd.to_datetime(prices["trade_date"]).dt.date
    prices = prices[prices["symbol"].isin(dim["symbol"])]  # bo ma khong con trong danh sach niem yet
    # bo phien hom nay vi chua dong cua (du lieu chua day du)
    prices = prices[prices["trade_date"] < pd.Timestamp.today().date()]
    prices = prices.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    prices.to_parquet(RAW_DIR / "stock_prices_daily.parquet", index=False)
    print(f"stock_prices_daily: {len(prices):,} dong, {prices['symbol'].nunique()} ma, "
          f"{prices['trade_date'].min()} -> {prices['trade_date'].max()}")
    if failed:
        print(f"Ma loi (chay lai de thu tiep): {','.join(failed)}", file=sys.stderr)


if __name__ == "__main__":
    main()
