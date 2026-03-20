# 20260326_001 - PoChun Hsu - [Create]  view for filter.
  
{{ config(materialized='view') }}

select *
from {{ ref('stg_ptt_product') }}
where created_date is not null
  and product_type is not null
  and price is not null
  and price <= 100000
