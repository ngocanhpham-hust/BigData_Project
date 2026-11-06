-- Tang staging: danh sach ma + san + nganh ICB (seed tu data/raw/dim_symbol.csv).
select
    symbol,
    company_name,
    exchange,
    cast(listed_date as date) as listed_date,
    icb_code,
    sector
from {{ ref('seed_dim_symbol') }}
