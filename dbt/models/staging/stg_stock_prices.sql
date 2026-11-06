-- Tang staging: chuan hoa cot + loi suat ngay tu adj_close (da dieu chinh co tuc/chia tach).
select
    symbol,
    trade_date,
    exchange,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    value,
    adj_close / lag(adj_close) over (partition by symbol order by trade_date) - 1 as daily_return
from {{ source('lakehouse', 'stock_prices_daily') }}
