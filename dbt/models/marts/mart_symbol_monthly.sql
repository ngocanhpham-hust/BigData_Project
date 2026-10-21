-- Tang marts: thong ke theo ma va thang, phuc vu BI.
select
    symbol,
    date_trunc('month', trade_date) as month,
    avg(adj_close)                  as avg_adj_close,
    sum(volume)                     as total_volume,
    sum(value)                      as total_value,
    avg(daily_return)               as avg_daily_return
from {{ ref('stg_stock_prices') }}
group by symbol, date_trunc('month', trade_date)
