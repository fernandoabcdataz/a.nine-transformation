{{ config(
    materialized='incremental',
    unique_key='payment_hkey',
    tags=['raw_vault', 'satellite']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_id']) }} as payment_hkey,
    {{ dbt_utils.generate_surrogate_key([
        'list_type',
        'list_item'
    ]) }} as hash_diff,
    list_type,
    list_item,
    _loaded_at as loaded_at,
    'xero' as record_source
FROM 
    {{ ref('stg_xero__payment__list_invoices') }}
{% if is_incremental() %}
WHERE 
    _loaded_at > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}