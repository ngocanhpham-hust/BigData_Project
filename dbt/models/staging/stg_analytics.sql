-- Tang staging: chuan hoa du lieu tho. TODO Giai doan 7.
select
    name,
    user_id,
    total_visits
from {{ source('lakehouse', 'analytics') }}
