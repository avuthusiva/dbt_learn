{{ config(materialized='incremental',unique_key='id') }}
select * from {{ source("bronze", "orders_inc") }}

{% if is_incremental() %}
where created_at > (select coalesce(max(created_at),'1991-01-01') from {{ this }})
{% endif %}