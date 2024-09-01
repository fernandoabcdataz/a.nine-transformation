{{ config(
    materialized='incremental',
    unique_key='payment_hkey',
    tags=['raw_vault', 'hub']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_id']) }} as payment_hkey,
    payment_id as payment_id,
    _loaded_at as loaded_at,
    'xero' as record_source
FROM 
    {{ ref('stg_xero__payments') }}
{% if is_incremental() %}
WHERE 
    _loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}