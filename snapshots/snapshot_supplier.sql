{% snapshot snapshot_suppliers %}
    {{
        config(
            target_schema='snapshots',
            unique_key='supplier_id',
            strategy='check',
            check_cols=['name', 'phone', 'nation_id'],
            invalidate_hard_deletes=True
        )
    }}

    select
        supplier_id,
        name,
        phone,
        nation_id
    from {{ ref('stg_supplier') }}

{% endsnapshot %}
