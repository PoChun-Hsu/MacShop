-- #20260310_001 - PoChun Hsu - [Create]  Table
-- #20260320_001 - PoChun Hsu - [Alter]   Move rename to sstaging. Move filter to intermediate.
-- #20260320_002 - PoChun Hsu - [Alter]   incremetal sync table.
-- #20260320_003 - PoChun Hsu - [Add]     clustered index.

{{ config(
    materialized='incremental',
    unique_key=['created_date','product_type'], 
    post_hook=[
        "create index if not exists idx_mart_created_product on {{ this }} (created_date, product_type)",
        "cluster {{ this }} using idx_mart_created_product"
    ] 
) }} -- #20260320_002 -- #20260320_003

with source_data as (

    select
        created_date,
        product_type,
        price
    from {{ ref('int_product_clean') }}

    {% if is_incremental() %}
        -- 只重算最近 90 天
        where created_date >= current_date - interval '90 day'
    {% endif %}

)

select
    created_date,
    product_type,
    count(*) as transaction_count,
    round(avg(price), 0) as average_price
from source_data
group by
    created_date,
    product_type
