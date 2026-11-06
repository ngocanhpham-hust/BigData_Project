-- Bang fact: gia ngay theo ma (theo yeu cau README: symbol, date, open, high, low, close, volume).
select
    symbol,
    trade_date as date,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    value
from {{ ref('stg_stock_prices') }}
