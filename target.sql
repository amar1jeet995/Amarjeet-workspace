{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

SELECT
    id,
    order_date,
    status,
    amount,
    updated_at
FROM {{ source('raw_data', 'orders') }}

-- The incremental logic
{% if is_incremental() %}

  -- This filter only runs on subsequent updates
  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})

{% endif %}
