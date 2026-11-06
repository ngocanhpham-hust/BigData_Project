-- Bang dim: thong tin ma (san, nganh ICB).
select symbol, company_name, exchange, sector, icb_code, listed_date
from {{ ref('stg_dim_symbol') }}
