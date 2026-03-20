{{ config(materialized='view') }}

select
    created_date::timestamp as created_timestamp,
    date(created_date) as created_date,
    lower(product_type) as product_type,
    price::numeric as price
from {{ source('raw', 'ptt_macshop_articles_product_detail') }}
