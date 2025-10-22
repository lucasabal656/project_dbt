{% snapshot snapshot_customers %}
    {{
        config(
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='check',
            check_cols=['customer_name', 'phone', 'nation_id'],
            invalidate_hard_deletes=True
        )
    }}

    select
        customer_id,
        customer_name,
        phone,
        nation_id
    from {{ ref('stg_customer') }}

{% endsnapshot %}
