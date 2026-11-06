-- Top ma bien dong manh: do lech chuan loi suat ngay trong 252 phien gan nhat cua moi ma.
-- Chi xet ma co du 200 phien va thanh khoan binh quan > 1 ty dong/phien (don vi value: dong).
with ranked as (
    select
        symbol,
        daily_return,
        value,
        row_number() over (partition by symbol order by trade_date desc) as rn
    from {{ ref('int_price_indicators') }}
    where daily_return is not null
),

recent as (
    select
        symbol,
        count(*)                   as sessions,
        stddev_samp(daily_return)  as volatility_252d,
        avg(value)                 as avg_value
    from ranked
    where rn <= 252
    group by symbol
)

select
    r.symbol,
    d.exchange,
    d.sector,
    r.sessions,
    r.volatility_252d,
    r.avg_value
from recent r
left join {{ ref('dim_symbol') }} d on d.symbol = r.symbol
where r.sessions >= 200
  and r.avg_value > 1000000000
