# 20260310_001 - PoChun Hsu - [Create]  Table
# 20260320_001 - PoChun Hsu - [Alter]   Move rename to sstaging. Move filter to intermediate.

{{ config(materialized='table') }}

select
    created_date,
    product_type,
    count(*) as transaction_count,
    round(avg(price), 0) as average_price
from {{ ref('int_product_clean') }}
group by
    created_date,
    product_type
order by
    created_date
