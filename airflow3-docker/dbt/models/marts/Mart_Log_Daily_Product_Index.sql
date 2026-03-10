{{ config(materialized='table') }}

select
    date(created_date) as created_date,
    product_type,
    count(*) as transaction_count,
    round(avg(price), 0) as average_price
from {{ source('raw', 'ptt_macshop_articles_product_detail') }}
where created_date is not null
  and product_type is not null
  and price <= 100000
group by
    date(created_date),
    product_type
order by
    created_date
