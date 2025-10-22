{% snapshot snapshot_part %}
    {{
        config(
            target_schema='snapshots',
            unique_key='part_id',
            strategy='check',
            check_cols=['name', 'brand', 'retail_price'],
            invalidate_hard_deletes=True
        )
    }}

    select
        part_id,
        name,
        brand,
        size,
        container,
        retail_price
    from {{ ref('stg_part') }}

{% endsnapshot %}
