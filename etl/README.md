# etl/ — Thu thập và sinh dữ liệu chứng khoán

## Dữ liệu hiện có (trong `data/raw/`, KHÔNG commit)

| File | Nội dung | Quy mô |
|---|---|---|
| `dim_symbol.csv` | Danh sách mã: `symbol, company_name, exchange, listed_date, icb_code, sector` | 1.519 mã (HOSE 406, HNX 299, UPCoM 814) |
| `stock_prices_daily.parquet` | Giá ngày thật, 2013-01-02 → 2026-10-20 | ~4,05 triệu dòng |
| `synthetic/minute_bars_2026.parquet` | **Dữ liệu mô phỏng**: nến 1 phút bung từ giá ngày | 30 triệu dòng |

Dữ liệu mẫu nhỏ (có commit) nằm ở `data/sample/` — dùng để test luồng NiFi/Kafka/Spark.

## Tải dữ liệu đầy đủ (không cần cào lại)

Dữ liệu thật đã được đính kèm ở GitHub Release **`data-v1`**:
<https://github.com/ngocanhpham-hust/BigData_Project/releases/tag/data-v1>

```bash
mkdir -p data/raw
gh release download data-v1 --repo ngocanhpham-hust/BigData_Project --dir data/raw
# hoặc tải tay 2 file trên trang release rồi bỏ vào data/raw/

# dữ liệu 1 phút mô phỏng (30 triệu dòng) sinh lại từ bảng giá ngày, ~1 phút:
python etl/generate_synthetic.py
```

## Schema

**`stock_prices_daily`** (giá tính bằng **nghìn đồng**, đúng như nguồn)

| Cột | Ý nghĩa |
|---|---|
| `symbol`, `trade_date`, `exchange` | mã, ngày giao dịch, sàn |
| `open, high, low, close` | giá gốc (chưa điều chỉnh) |
| `adj_open, adj_high, adj_low, adj_close` | giá đã điều chỉnh cổ tức/chia tách |
| `volume`, `value` | khối lượng, giá trị khớp lệnh |
| `pt_volume`, `pt_value` | khối lượng, giá trị thỏa thuận |
| `pct_change` | % thay đổi trong ngày |

**`minute_bars`** (mô phỏng): `symbol, ts, open, high, low, close, volume, source='synthetic'`

**`dim_symbol.sector`**: ngành ICB cấp supersector (19 nhóm, tiếng Anh) cho **cả 1.519 mã**, lấy từ trường `icbCode2` của Vietcap (`etl/icb_sectors.py` đổi mã → tên). Đã đối chiếu với mã lớn: VNM = Food & Beverage, VCB = Banks, FPT = Technology, HPG = Basic Resources, PLX = Oil & Gas.

## Cách chạy lại

```bash
python3 -m venv .venv && .venv/bin/pip install pandas pyarrow requests numpy

.venv/bin/python etl/fetch_stock_data.py                # cào giá ngày (~15–30 phút, có cache, chạy lại tiếp tục được)
.venv/bin/python etl/fetch_stock_data.py --symbols VNM,FPT   # thử vài mã
.venv/bin/python etl/generate_synthetic.py              # sinh 30 triệu dòng (~1 phút)
.venv/bin/python etl/generate_synthetic.py --target-rows 5000000
.venv/bin/python etl/make_sample.py                     # tạo lại file mẫu data/sample/
```

## Nguồn và lưu ý

- Nguồn: API công khai `api-finfo.vndirect.com.vn` (VNDirect). Không cần key nhưng **chậm và dễ quá tải**; script đã có retry, nghỉ giữa các lần gọi, cache từng mã và kiểm tra số dòng nhận về khớp `totalElements`.
- Ngành: `trading.vietcap.com.vn/api/price/symbols/getAll` (cần header `Origin`/`Referer`, trả về gzip). VNDirect không có trường ngành.
- TCBS, Yahoo, Stooq, SSI fiin không dùng được khi thử (chặn Cloudflare, giới hạn tốc độ, hoặc trả rỗng); thư viện `vnstock` không cài được trên Python 3.14.
- **Dòng giá ngày có `volume = 0`** là ngày mã không giao dịch (nhiều ở UPCoM) — giữ nguyên ở bảng ngày, bỏ khỏi dữ liệu mô phỏng.
- **Dữ liệu 1 phút là MÔ PHỎNG**, không phải khớp lệnh thật: mỗi ngày bung từ (O,H,L,C,V) thật, Open/High/Low/Close/Volume tổng của ngày khớp đúng với dòng ngày. Báo cáo phải ghi rõ điều này. Chỉ dùng để đo hiệu năng.
- Mô phỏng lấy các phiên gần nhất cho đến khi đủ số dòng (hiện ~6 tháng gần nhất, ~1.450 mã).
- Replay vào Kafka (topic `stock-quotes`) chưa làm — cần Kafka của TV1.
