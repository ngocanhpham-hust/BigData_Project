-- Thanh khoan theo nganh va thang (tong gia tri khop, so ma giao dich).
select
    d.sector,
    date_trunc('month', p.trade_date) as month,
    sum(p.value)                      as total_value,
    sum(p.volume)                     as total_volume,
    count(distinct p.symbol)          as active_symbols
from {{ ref('stg_stock_prices') }} p
join {{ ref('dim_symbol') }} d on d.symbol = p.symbol
group by d.sector, date_trunc('month', p.trade_date)
