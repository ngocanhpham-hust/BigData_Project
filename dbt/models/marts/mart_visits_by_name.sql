-- Tang marts: bang phan tich phuc vu BI. TODO Giai doan 7.
select
    name,
    sum(total_visits) as total
from {{ ref('stg_analytics') }}
group by name
order by total desc
