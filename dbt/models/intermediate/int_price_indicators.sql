-- Tang intermediate: duong trung binh (MA) va do bien dong cua tung ma.
-- MA/volatility chi co gia tri khi du so phien trong cua so (tranh MA sai o dau chuoi).
with base as (
    select * from {{ ref('stg_stock_prices') }}
),

windowed as (
    select
        *,
        avg(adj_close) over w20  as ma20_raw,
        avg(adj_close) over w50  as ma50_raw,
        avg(adj_close) over w200 as ma200_raw,
        count(*)       over w20  as n20,
        count(*)       over w50  as n50,
        count(*)       over w200 as n200,
        stddev_samp(daily_return) over w20 as volatility20_raw,
        avg(value)     over w20  as avg_value20
    from base
    window
        w20  as (partition by symbol order by trade_date rows between 19  preceding and current row),
        w50  as (partition by symbol order by trade_date rows between 49  preceding and current row),
        w200 as (partition by symbol order by trade_date rows between 199 preceding and current row)
)

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
    daily_return,
    case when n20  = 20  then ma20_raw  end as ma20,
    case when n50  = 50  then ma50_raw  end as ma50,
    case when n200 = 200 then ma200_raw end as ma200,
    case when n20  = 20  then volatility20_raw end as volatility20,
    avg_value20
from windowed
