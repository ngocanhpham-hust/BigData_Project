-- Tin hieu MA moi nhat moi ma: gia so voi MA20/MA50/MA200.
with latest as (
    select *, row_number() over (partition by symbol order by trade_date desc) as rn
    from {{ ref('int_price_indicators') }}
)

select
    symbol,
    trade_date,
    adj_close,
    ma20,
    ma50,
    ma200,
    case when ma20 > ma50 then 'MA20>MA50' else 'MA20<=MA50' end as short_vs_mid,
    case when adj_close > ma200 then 'above_MA200' else 'below_MA200' end as vs_long
from latest
where rn = 1
