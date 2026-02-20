{{ config(severity='warn') }}
select * from {{ source('bronze','orders') }}
where quantity < 0 or unit_price < 0