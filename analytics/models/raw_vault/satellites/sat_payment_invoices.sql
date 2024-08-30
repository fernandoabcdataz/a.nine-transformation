{{ config(
    materialized='incremental',
    unique_key='payment_hk',
    tags=['raw_vault', 'satellite']
) }}

SELECT
    {{ dbt_utils.surrogate_key(['payment_id']) }} as payment_hkey,
    list_type,
    list_item,
    _loaded_at as valid_from,
    '9999-12-31'::timestamp as valid_to
FROM {{ ref('stg_xero__payment_lists') }}
{% if is_incremental() %}
WHERE _loaded_at > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}