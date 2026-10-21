-- Tang staging: them loi suat ngay tu adj_close.
select
    symbol,
    trade_date,
    exchange,
    close,
    adj_close,
    volume,
    value,
    adj_close / lag(adj_close) over (partition by symbol order by trade_date) - 1 as daily_return
from {{ source('lakehouse', 'stock_prices_daily') }}
