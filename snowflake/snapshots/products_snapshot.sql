{% snapshot products_snapshot %}
{{
    config(
        target_schema = 'snapshot',
        unique_key= 'id',
        updated_at= 'created_at',
        strategy = 'timestamp'
    )
}}
select * from {{ source("bronze", "products") }}
{% endsnapshot %}