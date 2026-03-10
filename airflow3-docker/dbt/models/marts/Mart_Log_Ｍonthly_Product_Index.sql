# 20260310_001 - PoChun Hsu - [Create]  Table

{{ config(materialized='table') }}

select
    date_trunc('month', created_date)::date as created_month,
    product_type,
    sum(transaction_count) as transaction_count,
    round(avg(average_price),0) as average_price
from {{ ref('Mart_Log_Daily_Product_Index') }}
group by
    date_trunc('month', created_date),
    product_type
order by
    created_month
